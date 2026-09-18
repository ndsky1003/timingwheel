package timingwheel

import (
	"sync"
	"sync/atomic"
	"time"
)

// Task 是时间轮中一个已排期的定时任务。调用 Cancel 可取消它。
//
// Cancel 是惰性的：任务到期后若已开始执行、或已执行完毕，再调用
// Cancel 不会撤销已经/正在执行的回调。
type Task struct {
	fn        func()
	interval  time.Duration // 周期任务的执行间隔；0 表示一次性任务
	cancelled atomic.Bool
}

// Cancel 取消该定时任务，幂等。任务尚未到期时，到期后不会执行 fn。
func (t *Task) Cancel() {
	t.cancelled.Store(true)
}

// TimingWheel 是一个单层分桶时间轮：把任务按「对齐到 Tick 边界的到期时刻」
// 分桶，由单个对齐 ticker 每 Tick 批量执行到期桶中的任务。
//
// 与最小堆定时器相比，TimingWheel 的排期是 O(1)（直接落桶），代价是到期
// 时刻被量化到 Tick 边界，误差最大一个 Tick。适合秒级/毫秒级、海量
// 任务的场景；需要纳秒精度或延迟跨度极大的场景应改用分层时间轮或最小堆。
//
// fn 在到期处理循环的锁外执行，因此可以在 fn 中调用 AddAt、AddAfter、
// AddInterval 或 Task.Cancel；但不得调用 Stop（否则会等待自身而永久阻塞）。
type TimingWheel struct {
	tick    time.Duration
	startNs int64 // run 启动时刻，用于首次 flush 覆盖启动至今的边界

	mu      sync.Mutex
	lastDue int64             // 已 flush 到的最晚边界，用于 Add 时检测已过期
	buckets map[int64][]*Task // 到期时刻（对齐到 Tick）-> 任务列表

	stopC    chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewTimingWheel 创建一个时间轮并启动到期处理循环。tick 是到期检查的粒度，
// 也即到期时刻的量化误差上限；tick <= 0 时退化为 1 秒。
func NewTimingWheel(tick time.Duration) *TimingWheel {
	if tick <= 0 {
		tick = time.Second
	}
	w := &TimingWheel{
		tick:    tick,
		startNs: time.Now().UnixNano(),
		buckets: make(map[int64][]*Task),
		stopC:   make(chan struct{}),
	}
	w.wg.Add(1)
	go w.run()
	return w
}

// AddAfter 在 delay 之后执行 fn，返回可用于取消的 Task。
// delay <= 0 时，任务会落在最近的下一个 Tick 边界触发。
func (w *TimingWheel) AddAfter(delay time.Duration, fn func()) *Task {
	if delay < 0 {
		delay = 0
	}
	due := w.align(time.Now().UnixNano() + int64(delay))
	t := &Task{fn: fn}
	w.addTask(due, t)
	return t
}

// AddAt 在 deadline 时刻（量化到 Tick 边界）之后执行 fn，返回可用于
// 取消的 Task。deadline 已过期时等价于 AddAfter(0, fn)。
func (w *TimingWheel) AddAt(deadline time.Time, fn func()) *Task {
	return w.AddAfter(time.Until(deadline), fn)
}

// AddInterval 每隔 interval 执行一次 fn，直到 Cancel 或 Stop。
// 每次执行完后按理论到期边界重排，周期固定为 interval（受 tick 量化，
// 误差不累积），且不会重叠执行。interval <= 0 时 panic。
func (w *TimingWheel) AddInterval(interval time.Duration, fn func()) *Task {
	if interval <= 0 {
		panic("timingwheel: non-positive interval for AddInterval")
	}
	due := w.align(time.Now().UnixNano() + int64(interval))
	t := &Task{fn: fn, interval: interval}
	w.addTask(due, t)
	return t
}

// Stop 停止到期处理循环并丢弃所有未执行的任务。幂等，调用后该时间轮
// 不应再被使用。
func (w *TimingWheel) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopC)
		w.wg.Wait()

		w.mu.Lock()
		w.buckets = make(map[int64][]*Task)
		w.mu.Unlock()
	})
}

// run 是对齐到 Tick 边界的到期处理循环：逐格推进，flush 到期桶。
func (w *TimingWheel) run() {
	defer w.wg.Done()
	tickNs := int64(w.tick)
	var lastDue int64
	for {
		now := time.Now()
		next := now.Truncate(w.tick).Add(w.tick)
		t := time.NewTimer(time.Until(next))
		select {
		case <-t.C:
			n := time.Now().UnixNano()
			cur := n / tickNs * tickNs
			if lastDue == 0 {
				// 首次 flush 从 run 启动时刻对齐到 Tick 边界开始，
				// 覆盖启动至今可能被调度延迟跳过的所有边界，避免丢任务。
				lastDue = w.startNs/tickNs*tickNs - tickNs
			}
			// 逐个 flush 从上次到当前之间的每个边界，避免调度延迟漏掉到期桶。
			for d := lastDue + tickNs; d <= cur; d += tickNs {
				w.flush(d)
			}
			lastDue = cur
		case <-w.stopC:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			return
		}
	}
}

// flush 取出 due 时刻的任务桶，在锁外逐个执行回调。
func (w *TimingWheel) flush(due int64) {
	w.mu.Lock()
	w.lastDue = due
	ts := w.buckets[due]
	delete(w.buckets, due)
	w.mu.Unlock()

	for _, t := range ts {
		if t.cancelled.Load() {
			continue
		}
		t.fn()
		if t.interval > 0 && !t.cancelled.Load() {
			w.rebucket(t, due)
		}
	}
}

// addTask 落桶。若 due 已过期（不晚于已 flush 的最晚边界），则顺延到下一个
// 边界，避免投递到已被 flush 删除的桶而永久丢失。
func (w *TimingWheel) addTask(due int64, t *Task) {
	w.mu.Lock()
	if due <= w.lastDue {
		due = w.lastDue + int64(w.tick)
	}
	w.buckets[due] = append(w.buckets[due], t)
	w.mu.Unlock()
}

// rebucket 把周期任务重新排到下一个执行时刻对应的桶。
// due 是本次到期的理论边界，下一个执行时刻 = align(due + interval)，不能用
// time.Now() 重算（其越过边界的微小延迟会被 ceil 多出一个 tick，导致间隔膨胀）。
func (w *TimingWheel) rebucket(t *Task, due int64) {
	next := w.align(due + int64(t.interval))
	w.addTask(next, t)
}

// align 把时刻向上对齐到 Tick 边界，保证落到未来的桶。
func (w *TimingWheel) align(x int64) int64 {
	tickNs := int64(w.tick)
	return (x + tickNs - 1) / tickNs * tickNs
}
