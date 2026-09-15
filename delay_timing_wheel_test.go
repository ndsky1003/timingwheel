package timingwheel

import (
	"sync/atomic"
	"testing"
	"time"
)

// testTask 是 Task 接口的一个测试实现，用于验证 DelayTimingWheel。
type testTask struct {
	expiration int64
	cancelled  atomic.Int32
	fired      atomic.Int32
}

func (t *testTask) IsCancelled() bool    { return t.cancelled.Load() == 1 }
func (t *testTask) GetExpiration() int64 { return t.expiration }
func (t *testTask) cancel()              { t.cancelled.Store(1) }

func TestDelayWheelExpire(t *testing.T) {
	var fired atomic.Int32
	tw := NewDelayTimingWheel(5*time.Millisecond, 4, func(tasks []Task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	n := 100
	for i := 0; i < n; i++ {
		tw.Add(&testTask{expiration: time.Now().Add(20 * time.Millisecond).UnixNano()})
	}

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == int32(n) })
}

func TestDelayWheelCrossLayers(t *testing.T) {
	var fired atomic.Int32
	tw := NewDelayTimingWheel(time.Millisecond, 4, func(tasks []Task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	// 远大于当前层一圈（4ms），会跨越多层时间轮
	tw.Add(&testTask{expiration: time.Now().Add(time.Second).UnixNano()})

	waitFor(t, 3*time.Second, func() bool { return fired.Load() == 1 })
}

func TestDelayWheelCancel(t *testing.T) {
	var fired atomic.Int32
	tw := NewDelayTimingWheel(5*time.Millisecond, 4, func(tasks []Task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	tm := &testTask{expiration: time.Now().Add(50 * time.Millisecond).UnixNano()}
	tw.Add(tm)
	tm.cancel()

	time.Sleep(100 * time.Millisecond)
	if fired.Load() != 0 {
		t.Fatalf("取消的任务不应触发，实际触发 %d 次", fired.Load())
	}
}

func TestDelayWheelBatch(t *testing.T) {
	var fired atomic.Int32
	tw := NewDelayTimingWheel(5*time.Millisecond, 4, func(tasks []Task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	// 同一槽位放多个任务，验证批量回调
	n := 10
	for i := 0; i < n; i++ {
		tw.Add(&testTask{expiration: time.Now().Add(20 * time.Millisecond).UnixNano()})
	}

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == int32(n) })
}

func TestDelayWheelPrecision(t *testing.T) {
	var firedAt atomic.Int64
	tw := NewDelayTimingWheel(50*time.Millisecond, 4, func(tasks []Task) {
		firedAt.Store(time.Now().UnixNano())
	})
	tw.Start()
	defer tw.Stop()

	start := time.Now()
	// 300ms 跨层：底层一圈 200ms，第二层 tick=200ms。
	// 旧的向上对齐（ceil）会推迟到 400ms 才触发；向下对齐 + 降层后应在 ~300ms。
	tw.Add(&testTask{expiration: start.Add(300 * time.Millisecond).UnixNano()})

	waitFor(t, 3*time.Second, func() bool { return firedAt.Load() != 0 })
	elapsed := time.Since(start)
	if elapsed >= 360*time.Millisecond {
		t.Fatalf("跨层任务精度退化：实际 %v，期望约 300ms（旧 ceil 约 400ms）", elapsed)
	}
}
