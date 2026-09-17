package timingwheel

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestYearLongTask 验证 1 年后的任务能成功投递：到期时刻用 int64 纳秒时间戳
// 表示，1 年（约 3.15e16ns）远小于 int64 上限，不应溢出，对齐后也不应 panic。
func TestYearLongTask(t *testing.T) {
	tw := NewWheel(time.Second)
	defer tw.Stop()

	task := tw.AddAfter(365*24*time.Hour, func() {})
	if task == nil {
		t.Fatal("1 年后的任务投递失败")
	}
	task.Cancel()
}

// TestLongDelayPrecision 验证长时间跨度任务到期精度不因跨度增大而退化：
// 每次用 time.Now() 重算而非逐 tick 累积，因此 5 秒任务偏差仍在一个 tick 量级。
func TestLongDelayPrecision(t *testing.T) {
	tick := 10 * time.Millisecond
	var firedAt atomic.Int64
	tw := NewWheel(tick)
	defer tw.Stop()

	start := time.Now()
	d := 5 * time.Second
	tw.AddAfter(d, func() {
		firedAt.Store(time.Now().UnixNano())
	})

	waitFor(t, 10*time.Second, func() bool { return firedAt.Load() != 0 })

	elapsed := firedAt.Load() - start.UnixNano()
	deviation := time.Duration(elapsed) - d
	if deviation < 0 {
		deviation = -deviation
	}
	t.Logf("到期偏差=%v（任务设定 %v 后到期）", deviation, d)

	// 对齐误差上限一个 tick，再留调度余量；远小于秒级退化即可说明无累积漂移。
	if deviation > 5*tick {
		t.Fatalf("长跨度任务精度退化：偏差 %v，超过 %v", deviation, 5*tick)
	}
}
