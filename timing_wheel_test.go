package timingwheel

import (
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 轮询等待条件满足，超时则失败。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("等待条件超时（%v）", timeout)
	}
}

func TestWheelExpire(t *testing.T) {
	var fired atomic.Int32
	tw := newTimingWheel(5*time.Millisecond, 4, func(tasks []Task) {
		fired.Add(int32(len(tasks)))
	})
	tw.start()
	defer tw.stop()

	n := 100
	for i := 0; i < n; i++ {
		tw.add(&testTask{expiration: time.Now().Add(20 * time.Millisecond).UnixNano()})
	}

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == int32(n) })
}

func TestWheelCrossLayers(t *testing.T) {
	var fired atomic.Int32
	tw := newTimingWheel(time.Millisecond, 4, func(tasks []Task) {
		fired.Add(int32(len(tasks)))
	})
	tw.start()
	defer tw.stop()

	// 远大于当前层一圈（4ms），会跨越多层时间轮
	tw.add(&testTask{expiration: time.Now().Add(time.Second).UnixNano()})

	waitFor(t, 3*time.Second, func() bool { return fired.Load() == 1 })
}

func TestWheelCancel(t *testing.T) {
	var fired atomic.Int32
	tw := newTimingWheel(5*time.Millisecond, 4, func(tasks []Task) {
		fired.Add(int32(len(tasks)))
	})
	tw.start()
	defer tw.stop()

	tm := &testTask{expiration: time.Now().Add(50 * time.Millisecond).UnixNano()}
	tw.add(tm)
	tm.cancel()

	time.Sleep(100 * time.Millisecond)
	if fired.Load() != 0 {
		t.Fatalf("取消的任务不应触发，实际触发 %d 次", fired.Load())
	}
}

func TestWheelPrecision(t *testing.T) {
	var firedAt atomic.Int64
	tw := newTimingWheel(100*time.Millisecond, 4, func(tasks []Task) {
		firedAt.Store(time.Now().UnixNano())
	})
	tw.start()
	defer tw.stop()

	start := time.Now()
	// 900ms 跨层：底层一圈 400ms，第二层 tick=400ms。
	// 旧的向上对齐（ceil）会推迟到 1200~1600ms；向下对齐 + 降层后约 900~1200ms。
	// 简单版由固定 ticker 逐格推进，存在最多一个上层 tick 的相位误差，
	// 因此这里只断言"不丢失且不显著晚于 ceil 的最坏情况"。
	tw.add(&testTask{expiration: start.Add(900 * time.Millisecond).UnixNano()})

	waitFor(t, 3*time.Second, func() bool { return firedAt.Load() != 0 })
	elapsed := time.Since(start)
	if elapsed >= 1500*time.Millisecond {
		t.Fatalf("跨层任务过期过晚：实际 %v，期望约 900~1200ms（旧 ceil 可达 1600ms）", elapsed)
	}
}
