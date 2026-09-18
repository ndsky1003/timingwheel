package timingwheel

import (
	"sync/atomic"
	"testing"
	"time"
)

func benchNoop() {}

// BenchmarkTimingWheelAddAfter 单 goroutine 投递吞吐（tick 设大，避免 flush 干扰）。
func BenchmarkTimingWheelAddAfter(b *testing.B) {
	tw := NewTimingWheel(time.Hour)
	defer tw.Stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.AddAfter(time.Minute, benchNoop)
	}
}

// BenchmarkTimingWheelAddAfterParallel 多 goroutine 并发投递吞吐（测锁竞争）。
func BenchmarkTimingWheelAddAfterParallel(b *testing.B) {
	tw := NewTimingWheel(time.Hour)
	defer tw.Stop()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tw.AddAfter(time.Minute, benchNoop)
		}
	})
}

// BenchmarkTimingWheelAddInterval 周期任务投递吞吐。
func BenchmarkTimingWheelAddInterval(b *testing.B) {
	tw := NewTimingWheel(time.Hour)
	defer tw.Stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.AddInterval(time.Minute, benchNoop)
	}
}

// BenchmarkStdAfterFunc 标准库 time.AfterFunc 投递吞吐，作为基线对比。
// 注：AfterFunc 会累积 runtime timer，随数量增长 addtimer 退化，这正好体现
// 时间轮 O(1) 落桶相对堆 O(log n) 的海量任务优势。
func BenchmarkStdAfterFunc(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		time.AfterFunc(time.Minute, benchNoop)
	}
}

// BenchmarkStdNewTimer 标准库 time.NewTimer 投递吞吐（不含到期执行）。
func BenchmarkStdNewTimer(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := time.NewTimer(time.Minute)
		t.Stop()
	}
}

// TestFlushThroughput 海量任务同一时刻到期的批量触发吞吐。
func TestFlushThroughput(t *testing.T) {
	const n = 1_000_000
	var fired atomic.Int64
	tw := NewTimingWheel(time.Millisecond)
	defer tw.Stop()

	start := time.Now()
	for i := 0; i < n; i++ {
		tw.AddAfter(20*time.Millisecond, func() { fired.Add(1) })
	}
	insertDur := time.Since(start)

	waitFor(t, 10*time.Second, func() bool { return fired.Load() == n })
	flushDur := time.Since(start) - insertDur

	t.Logf("投递 %d 任务：%v（%.0f ops/s）", n, insertDur, float64(n)/insertDur.Seconds())
	t.Logf("到期批量触发：%v（%.0f tasks/s）", flushDur, float64(n)/flushDur.Seconds())
}
