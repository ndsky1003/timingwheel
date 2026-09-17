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
	tw := NewWheel(5 * time.Millisecond)
	defer tw.Stop()

	n := 100
	for i := 0; i < n; i++ {
		tw.AddAfter(20*time.Millisecond, func() { fired.Add(1) })
	}

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == int32(n) })
}

func TestWheelBatch(t *testing.T) {
	var fired atomic.Int32
	tw := NewWheel(5 * time.Millisecond)
	defer tw.Stop()

	// 同一到期边界放置多个任务，验证批量 flush。
	n := 10
	for i := 0; i < n; i++ {
		tw.AddAfter(20*time.Millisecond, func() { fired.Add(1) })
	}

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == int32(n) })
}

func TestWheelAddAt(t *testing.T) {
	var fired atomic.Int32
	tw := NewWheel(5 * time.Millisecond)
	defer tw.Stop()

	tw.AddAt(time.Now().Add(30*time.Millisecond), func() { fired.Add(1) })

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == 1 })
}

func TestWheelAddAtExpired(t *testing.T) {
	var fired atomic.Int32
	tw := NewWheel(5 * time.Millisecond)
	defer tw.Stop()

	// 已过期的 deadline 等价于 AddAfter(0)，落在下一个 tick 边界触发。
	tw.AddAt(time.Now().Add(-time.Second), func() { fired.Add(1) })

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == 1 })
}

func TestWheelCancel(t *testing.T) {
	var fired atomic.Int32
	tw := NewWheel(5 * time.Millisecond)
	defer tw.Stop()

	task := tw.AddAfter(50*time.Millisecond, func() { fired.Add(1) })
	task.Cancel()
	task.Cancel() // 幂等

	time.Sleep(100 * time.Millisecond)
	if fired.Load() != 0 {
		t.Fatalf("取消的任务不应触发，实际触发 %d 次", fired.Load())
	}
}

func TestWheelAddInterval(t *testing.T) {
	var fired atomic.Int32
	tw := NewWheel(5 * time.Millisecond)
	defer tw.Stop()

	task := tw.AddInterval(20*time.Millisecond, func() { fired.Add(1) })

	waitFor(t, 2*time.Second, func() bool { return fired.Load() >= 3 })

	task.Cancel()
	got := fired.Load()
	time.Sleep(80 * time.Millisecond)
	if fired.Load() != got {
		t.Fatalf("取消后周期任务仍在执行：取消前 %d，之后 %d", got, fired.Load())
	}
}

func TestWheelAddIntervalPanic(t *testing.T) {
	tw := NewWheel(time.Millisecond)
	defer tw.Stop()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("AddInterval(0) 应 panic")
		}
	}()
	tw.AddInterval(0, func() {})
}

func TestWheelPrecision(t *testing.T) {
	var firedAt atomic.Int64
	tw := NewWheel(50 * time.Millisecond)
	defer tw.Stop()

	start := time.Now()
	tw.AddAfter(300*time.Millisecond, func() {
		firedAt.Store(time.Now().UnixNano())
	})

	waitFor(t, 3*time.Second, func() bool { return firedAt.Load() != 0 })
	elapsed := time.Since(start)
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("任务到期过晚：实际 %v，期望约 300~400ms", elapsed)
	}
}

func TestWheelZeroTick(t *testing.T) {
	var fired atomic.Int32
	tw := NewWheel(0) // tick <= 0 退化为 1 秒
	defer tw.Stop()

	tw.AddAfter(time.Millisecond, func() { fired.Add(1) })

	waitFor(t, 3*time.Second, func() bool { return fired.Load() == 1 })
}

func TestWheelStopIdempotent(t *testing.T) {
	tw := NewWheel(time.Millisecond)
	tw.Stop()
	tw.Stop() // 重复调用不应 panic
}
