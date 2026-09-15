package timingwheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// timingWheel 分层时间轮（简单版，仅供学习对照，不对外导出）。
//
// 每一层由 wheelSize 个槽位组成，每个槽位代表 tick 时长，一圈覆盖 interval = tick * wheelSize。
// 当任务的过期时间超过当前层一圈的覆盖范围时，会被投递到上一层（上层 tick 等于当前层的 interval），
// 从而以指数级扩大可表示的时间跨度，同时每一格仍然只处理 O(1) 个槽位。
//
// 驱动方式：每层一个固定 time.Ticker，每 tick 前进一格（advance 逐格补齐遗漏）。
// 降层策略与生产版 DelayTimingWheel 一致：根层向上对齐、上层向下对齐，
// 到期前从根层重新投递自动路由降层，使跨层任务最终在根层以 tick 精度到期。
type timingWheel struct {
	tick      time.Duration // 每格时长
	wheelSize int64         // 槽位数量
	interval  time.Duration // 一圈时长

	currentTick atomic.Int64 // 当前推进到的时间点（纳秒，原子访问）

	slots []*list.List // 槽位，每个槽位是一个定时任务链表
	mu    sync.Mutex   // 保护 slots

	overflow     *timingWheel // 上一层时间轮
	root         *timingWheel // 根层引用，用于到期前降层重投递（根层为 nil）
	overflowOnce sync.Once    // 惰性创建上层

	// onExpired 是整槽任务到期时的批量处理回调，由使用方注入。
	onExpired func([]Task)

	ticker   *time.Ticker
	stopCh   chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// newTimingWheel 创建一个时间轮，但不会启动，需要调用 start。
func newTimingWheel(tick time.Duration, wheelSize int64, onExpired func([]Task)) *timingWheel {
	slots := make([]*list.List, wheelSize)
	for i := range slots {
		slots[i] = list.New()
	}
	return &timingWheel{
		tick:      tick,
		wheelSize: wheelSize,
		interval:  tick * time.Duration(wheelSize),
		slots:     slots,
		onExpired: onExpired,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// start 启动时间轮的后台驱动协程。
// currentTick 会向下对齐到 tick 边界，保证后续槽位索引计算与任务投递保持一致。
func (tw *timingWheel) start() {
	tw.ticker = time.NewTicker(tw.tick)
	now := time.Now().UnixNano()
	now = now / int64(tw.tick) * int64(tw.tick)
	tw.currentTick.Store(now)
	go tw.run()
}

// stop 停止时间轮，并级联停止上层时间轮。
func (tw *timingWheel) stop() {
	tw.stopOnce.Do(func() {
		close(tw.stopCh)
		if tw.ticker != nil {
			tw.ticker.Stop()
		}
		<-tw.done
		if tw.overflow != nil {
			tw.overflow.stop()
		}
	})
}

// run 是时间轮的驱动循环，每 tick 前进一次。
func (tw *timingWheel) run() {
	defer close(tw.done)
	for {
		select {
		case <-tw.ticker.C:
			tw.advance(time.Now().UnixNano())
		case <-tw.stopCh:
			return
		}
	}
}

// advance 将时间轮推进到 now。若中间有遗漏（例如协程调度卡顿导致多个 tick 没处理），
// 会逐格补齐，确保不会漏掉任何过期的槽位。
//
// 这里用 CAS 推进 currentTick：根层由自身 run 协程驱动，而跨层降级时上层协程也会
// 调用根层的 advance（见 reinsert），两者并发。若用 Load+Store 会产生竞态并可能使
// 时钟倒退，导致槽位被重复处理或任务被投递到"过去"的槽位。
func (tw *timingWheel) advance(now int64) {
	tick := int64(tw.tick)
	for {
		current := tw.currentTick.Load()
		if current+tick > now {
			return
		}
		if tw.currentTick.CompareAndSwap(current, current+tick) {
			tw.advanceOne(current + tick)
		}
	}
}

// advanceOne 处理时间点 t 对应的槽位：
//   - 已取消的任务直接丢弃；
//   - 已到期的任务取出并交给 onExpired 批量处理；
//   - 尚未到期的任务（向下对齐提前到达、或被续期）重新投递：降层到下一层恢复精度。
func (tw *timingWheel) advanceOne(t int64) {
	idx := (t / int64(tw.tick)) % tw.wheelSize
	bucket := tw.slots[idx]

	tw.mu.Lock()
	var expired, reAdd []Task
	for e := bucket.Front(); e != nil; {
		tm := e.Value.(Task)
		next := e.Next()
		bucket.Remove(e)
		if tm.IsCancelled() {
			// 已取消，丢弃即可
		} else if tm.GetExpiration() <= t {
			expired = append(expired, tm)
		} else {
			reAdd = append(reAdd, tm)
		}
		e = next
	}
	tw.mu.Unlock()

	// 整槽任务交给一个协程批量处理，一次加锁完成校验与删除，降低锁竞争。
	if len(expired) > 0 {
		go tw.onExpired(expired)
	}
	for _, tm := range reAdd {
		tw.reinsert(tm, t)
	}
}

// reinsert 将"尚未到期"的任务重新投递：若非根层，说明任务因向下对齐而提前
// 到达粗粒度槽位，从根层重新投递、根据根层时钟自动路由降层；若已是根层，
// 则说明任务被续期，重新投递到根层的未来槽位。
// 若目标层时钟已越过任务到期时间（add 返回 false），说明任务其实已过期，
// 直接交给 onExpired 处理，避免静默丢失。
func (tw *timingWheel) reinsert(tm Task, t int64) {
	if tw.root != nil {
		tw.root.advance(t)
		tw.root.addOrExpire(tm)
	} else {
		tw.addOrExpire(tm)
	}
}

// addOrExpire 重新投递任务；若根层时钟已越过任务到期时间（add 返回 false），
// 说明任务其实已过期，直接交给 onExpired 处理，避免静默丢失。
func (tw *timingWheel) addOrExpire(tm Task) {
	if !tw.add(tm) {
		go tw.onExpired([]Task{tm})
	}
}

// add 将任务投递到合适的层与槽位。
func (tw *timingWheel) add(tm Task) bool {
	now := tw.currentTick.Load()
	tick := int64(tw.tick)
	exp := tm.GetExpiration()
	if exp <= now {
		return false // 已过期
	}
	var aligned int64
	if tw.root == nil {
		// 根层：向上对齐，flush 时直接到期，避免非跨层任务反复重投递。
		aligned = (exp + tick - 1) / tick * tick
	} else {
		// 上层：向下对齐，保证槽位时间不晚于真实到期时间，从而能降层恢复精度。
		aligned = exp / tick * tick
		if aligned <= now {
			aligned = now + tick // 至少落在下一格，避免放入已过期的槽位
		}
	}
	if aligned < now+int64(tw.interval) {
		// 落在当前层一圈范围内
		idx := (aligned / tick) % tw.wheelSize
		tw.mu.Lock()
		tw.slots[idx].PushBack(tm)
		tw.mu.Unlock()
		return true
	}
	// 超出当前层覆盖范围，交给上一层
	tw.overflowOnce.Do(func() {
		tw.overflow = newTimingWheel(tw.interval, tw.wheelSize, tw.onExpired)
		if tw.root == nil {
			tw.overflow.root = tw
		} else {
			tw.overflow.root = tw.root
		}
		tw.overflow.start()
	})
	return tw.overflow.add(tm)
}
