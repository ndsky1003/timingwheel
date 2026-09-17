package timingwheel

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWheelConcurrentStress 并发投递海量任务，混合不同到期时间，验证不丢失。
func TestWheelConcurrentStress(t *testing.T) {
	var fired atomic.Int64
	tw := NewWheel(time.Millisecond)
	defer tw.Stop()

	var wg sync.WaitGroup
	n := 2000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := time.Duration(i%50) * time.Millisecond
			tw.AddAfter(d, func() { fired.Add(1) })
		}(i)
	}
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool { return fired.Load() == int64(n) })
}

// TestWheelConcurrentCancel 并发投递中穿插取消，验证 -race 下无竞态且取消生效。
func TestWheelConcurrentCancel(t *testing.T) {
	var fired atomic.Int64
	tw := NewWheel(time.Millisecond)
	defer tw.Stop()

	var wg sync.WaitGroup
	n := 1000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := time.Duration(i%20) * time.Millisecond
			task := tw.AddAfter(d, func() { fired.Add(1) })
			if i%2 == 0 {
				task.Cancel()
			}
		}(i)
	}
	wg.Wait()

	// 偶数索引任务被取消，触发数应为 n/2。
	waitFor(t, 5*time.Second, func() bool { return fired.Load() == int64(n/2) })
}
