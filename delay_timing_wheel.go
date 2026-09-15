// Package timingwheel 实现分层时间轮，用于对大量带过期时间的任务做高效调度，
// 避免全表扫描。核心类型 Task 由使用方实现，时间轮只关心到期时间与取消状态。
//
// 对外提供基于延迟队列（最小堆）的 DelayTimingWheel（用 NewDelayTimingWheel 构造）；
// 同包的简单版实现（timing_wheel.go）仅供学习对照，不对外导出。
package timingwheel

import (
	"container/heap"
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// Task 是时间轮调度的最小单元。时间轮只关心任务的到期时间（纳秒时间戳）与
// 取消状态，不关心任务承载的具体数据类型，从而与上层（缓存等）解耦、保持可复用。
// 由使用方实现；时间轮仅在任务到期或取消时读取其状态。
type Task interface {
	IsCancelled() bool
	GetExpiration() int64
}

// 本文件实现了基于延迟队列（最小堆）的分层时间轮 DelayTimingWheel。
//
// 与简单版 TimingWheel（见 timing_wheel.go）的核心区别仅在于驱动方式：
//   - 简单版：每层一个固定 time.Ticker，每 tick 前进一格，即使槽位为空也会醒来（空转）；
//   - 本版本：用一个 DelayQueue（最小堆）记录"有任务的槽位"的到期时间，
//     所有层共享同一个队列、只在 root 上跑一个 run 协程，无任务时完全休眠。
//
// 两者的降层策略一致：都是"根层向上对齐、上层向下对齐，到期前从根层重新投递
// 自动路由降层"。只是简单版各层时钟由 ticker 逐格推进，本版本由 advanceClock
// 递归推进各层时钟，借鉴 Kafka（RussellLuo/timingwheel）的经典分层时间轮实现。
//
// 两者的分层、投递、取消语义一致，因此方便对照学习。

// slot 是时间轮中的一个槽位，对应一个固定的时间点（expiration）。
type slot struct {
	expiration int64 // 槽位对应的时间点（纳秒，原子访问）
	index      int   // 在延迟队列堆中的位置，-1 表示不在堆中

	mu     sync.Mutex
	timers *list.List // 任务链表
}

func newSlot() *slot {
	return &slot{index: -1, timers: list.New()}
}

// setExpiration 设置槽位的到期时间，返回是否发生变化。
// 只有变化时才需要将槽位（重新）入队，从而保证同一圈内不会重复入队。
func (s *slot) setExpiration(exp int64) bool {
	return atomic.SwapInt64(&s.expiration, exp) != exp
}

func (s *slot) getExpiration() int64 {
	return atomic.LoadInt64(&s.expiration)
}

// slotHeap 实现 container/heap.Interface，按槽位到期时间维护一个最小堆。
type slotHeap []*slot

func (h slotHeap) Len() int { return len(h) }
func (h slotHeap) Less(i, j int) bool {
	return h[i].getExpiration() < h[j].getExpiration()
}
func (h slotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *slotHeap) Push(x any) {
	s := x.(*slot)
	s.index = len(*h)
	*h = append(*h, s)
}
func (h *slotHeap) Pop() any {
	old := *h
	n := len(old)
	s := old[n-1]
	old[n-1] = nil // 避免堆持有引用
	s.index = -1
	*h = old[:n-1]
	return s
}

// delayQueue 是一个按到期时间排序的延迟队列，底层是最小堆。
// 它保证只有"最早到期的槽位"到期时才被取出，从而避免固定 ticker 的空转。
type delayQueue struct {
	C chan *slot // 已到期的槽位会从这里送出

	mu       sync.Mutex
	pq       slotHeap
	sleeping int32         // 原子标记：poll 是否正处于休眠等待
	wakeupC  chan struct{} // 用于唤醒休眠中的 poll
}

func newDelayQueue() *delayQueue {
	return &delayQueue{
		C:       make(chan *slot),
		wakeupC: make(chan struct{}),
	}
}

// offer 将槽位入队（或更新其在堆中的位置）。若它成为新的最早到期槽位，
// 则唤醒休眠中的 poll 协程。
func (dq *delayQueue) offer(s *slot) {
	dq.mu.Lock()
	if s.index >= 0 {
		heap.Fix(&dq.pq, s.index) // 已在堆中，更新位置
	} else {
		heap.Push(&dq.pq, s) // 新入队
	}
	top := dq.pq[0]
	dq.mu.Unlock()

	if top == s {
		// 成为堆顶，唤醒 poll（仅当其正处于休眠状态，避免无谓唤醒）。
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			dq.wakeupC <- struct{}{}
		}
	}
}

// poll 循环从堆中取出最早到期的槽位，发送到 C。无到期项时休眠，
// 直到被 offer 唤醒、或等待到最近一个槽位的到期时间、或收到退出信号。
func (dq *delayQueue) poll(exitC chan struct{}) {
	for {
		now := time.Now().UnixNano()

		dq.mu.Lock()
		var s *slot
		var delta int64
		if dq.pq.Len() == 0 {
			delta = 0 // 堆空
		} else {
			top := dq.pq[0]
			if top.getExpiration() <= now {
				s = heap.Pop(&dq.pq).(*slot) // 已到期，取出
			} else {
				delta = top.getExpiration() - now // 还需等待
			}
		}
		if s == nil {
			// 与上面的堆检查保持原子性，避免与 offer 产生竞态。
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if s != nil {
			select {
			case dq.C <- s:
			case <-exitC:
				return
			}
			continue
		}

		if delta == 0 {
			// 堆空，等待被唤醒。
			select {
			case <-dq.wakeupC:
			case <-exitC:
				return
			}
		} else {
			// 堆顶尚未到期，睡到到期时间；期间可被唤醒。
			t := time.NewTimer(time.Duration(delta))
			select {
			case <-dq.wakeupC:
				t.Stop()
			case <-t.C:
				// 自然到期醒来。若 sleeping 已被 offer 置 0，
				// 说明有 offer 正在阻塞发送 wakeupC，需 drain 以解除其阻塞。
				if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
					<-dq.wakeupC
				}
			case <-exitC:
				t.Stop()
				return
			}
		}
	}
}

// DelayTimingWheel 是基于延迟队列的分层时间轮。
type DelayTimingWheel struct {
	tick      time.Duration // 每格时长
	wheelSize int64         // 槽位数量
	interval  time.Duration // 一圈时长

	currentTick atomic.Int64 // 当前推进到的时间点（纳秒）

	slots []*slot
	queue *delayQueue // 所有层共享同一个延迟队列

	overflow     *DelayTimingWheel // 上一层时间轮
	overflowOnce sync.Once         // 惰性创建上层

	onExpired func([]Task) // 整槽到期时的批量处理回调

	isRoot bool // 是否根层：根层向上对齐，上层向下对齐

	exitC    chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewDelayTimingWheel 创建一个基于延迟队列的分层时间轮。
// tick 为根层每格时长，wheelSize 为每层槽位数量，onExpired 为任务到期时的批量回调。
// 返回的时间轮需调用 Start 启动，用完后调用 Stop 停止。
func NewDelayTimingWheel(tick time.Duration, wheelSize int64, onExpired func([]Task)) *DelayTimingWheel {
	return newDelayTimingWheelWithQueue(tick, wheelSize, onExpired, newDelayQueue(), true)
}

// newDelayTimingWheelWithQueue 内部构造：上层复用根层的延迟队列，
// 因此整个分层时间轮只需根层一个 run 协程驱动。
func newDelayTimingWheelWithQueue(tick time.Duration, wheelSize int64, onExpired func([]Task), queue *delayQueue, isRoot bool) *DelayTimingWheel {
	slots := make([]*slot, wheelSize)
	for i := range slots {
		slots[i] = newSlot()
	}
	return &DelayTimingWheel{
		tick:      tick,
		wheelSize: wheelSize,
		interval:  tick * time.Duration(wheelSize),
		slots:     slots,
		queue:     queue,
		onExpired: onExpired,
		isRoot:    isRoot,
		exitC:     make(chan struct{}),
	}
}

// Start 启动时间轮的后台协程。仅根层需要调用；上层共享根层的队列与协程。
func (tw *DelayTimingWheel) Start() {
	now := time.Now().UnixNano()
	tw.currentTick.Store(truncate(now, int64(tw.tick)))

	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		tw.queue.poll(tw.exitC)
	}()

	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		tw.run()
	}()
}

// run 处理延迟队列送出的到期槽位。所有层的槽位都从这里被取出并处理。
func (tw *DelayTimingWheel) run() {
	for {
		select {
		case s := <-tw.queue.C:
			// 用真实时间推进时钟，而不是槽位的到期时间。
			// 上层槽位向下对齐后可能落在"过去"（例如根层 tick 与上层 tick 不对齐时），
			// 若用槽位到期时间推进会导致时钟落后，进而使降层重投递在边界处反复溢出、任务丢失。
			tw.advanceClock(time.Now().UnixNano())
			tw.flush(s)
		case <-tw.exitC:
			return
		}
	}
}

// Stop 停止时间轮。上层共享同一队列，无需级联停止。
func (tw *DelayTimingWheel) Stop() {
	tw.stopOnce.Do(func() {
		close(tw.exitC)
		tw.wg.Wait()
	})
}

// advanceClock 把时钟推进到 expiration，并级联推进各上层时钟，保证所有层
// 的 currentTick 一致，使后续从根层重新投递（降层）能正确路由。
func (tw *DelayTimingWheel) advanceClock(expiration int64) {
	current := tw.currentTick.Load()
	if expiration >= current+int64(tw.tick) {
		current = truncate(expiration, int64(tw.tick))
		tw.currentTick.Store(current)
		if tw.overflow != nil {
			tw.overflow.advanceClock(current)
		}
	}
}

// Add 将任务投递到合适的层与槽位，返回是否成功接收。
// 若任务已过期（当前时钟已越过其到期时间），返回 false 且不会触发回调。
func (tw *DelayTimingWheel) Add(tm Task) bool {
	tick := int64(tw.tick)
	now := tw.currentTick.Load()
	exp := tm.GetExpiration()
	if exp <= now {
		return false // 已过期
	}
	var aligned int64
	if tw.isRoot {
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
		s := tw.slots[idx]
		s.mu.Lock()
		s.timers.PushBack(tm)
		s.mu.Unlock()
		if s.setExpiration(aligned) {
			tw.queue.offer(s)
		}
		return true
	}
	// 超出当前层覆盖范围，交给上一层
	tw.overflowOnce.Do(func() {
		tw.overflow = newDelayTimingWheelWithQueue(
			tw.interval, tw.wheelSize, tw.onExpired, tw.queue, false)
		// 上层时钟必须对齐到上层 tick（interval），否则上层向下对齐的槽位时间
		// 会与根层时钟错位，产生"过去"的槽位，导致任务在降层时反复溢出。
		tw.overflow.currentTick.Store(truncate(tw.currentTick.Load(), int64(tw.interval)))
	})
	return tw.overflow.Add(tm)
}

// flush 处理一个到期槽位：
//   - 已取消的任务直接丢弃；
//   - 已到期的任务交给 onExpired 批量处理；
//   - 尚未到期的任务（向下对齐提前到达、或被续期）从根层重新投递，自动路由降层。
func (tw *DelayTimingWheel) flush(s *slot) {
	exp := s.getExpiration()
	s.setExpiration(-1) // 重置，允许下一圈重新入队

	s.mu.Lock()
	var expired, reAdd []Task
	for e := s.timers.Front(); e != nil; {
		tm := e.Value.(Task)
		next := e.Next()
		s.timers.Remove(e)
		if tm.IsCancelled() {
			// 已取消，丢弃
		} else if tm.GetExpiration() <= exp {
			expired = append(expired, tm)
		} else {
			reAdd = append(reAdd, tm)
		}
		e = next
	}
	s.mu.Unlock()

	if len(expired) > 0 {
		go tw.onExpired(expired)
	}
	for _, tm := range reAdd {
		tw.addOrExpire(tm)
	}
}

// addOrExpire 重新投递任务；若目标层时钟已越过任务到期时间（Add 返回 false），
// 说明任务其实已过期，直接交给 onExpired 处理，避免静默丢失。
func (tw *DelayTimingWheel) addOrExpire(tm Task) {
	if !tw.Add(tm) {
		go tw.onExpired([]Task{tm})
	}
}

// truncate 将 x 向下对齐到 unit 的整数倍。
func truncate(x, unit int64) int64 {
	return x / unit * unit
}
