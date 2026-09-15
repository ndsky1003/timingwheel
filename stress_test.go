package timingwheel

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWheelConcurrentStress(t *testing.T) {
	var fired atomic.Int64
	tw := newTimingWheel(time.Millisecond, 8, func(tasks []Task) {
		fired.Add(int64(len(tasks)))
	})
	tw.start()
	defer tw.stop()

	var wg sync.WaitGroup
	n := 2000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 混合不同到期时间，部分跨层
			d := time.Duration(i%50) * time.Millisecond
			tw.add(&testTask{expiration: time.Now().Add(d).UnixNano()})
		}(i)
	}
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool { return fired.Load() == int64(n) })
}

func TestDelayWheelConcurrentStress(t *testing.T) {
	var fired atomic.Int64
	tw := NewDelayTimingWheel(time.Millisecond, 8, func(tasks []Task) {
		fired.Add(int64(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	var wg sync.WaitGroup
	n := 2000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := time.Duration(i%50) * time.Millisecond
			tw.Add(&testTask{expiration: time.Now().Add(d).UnixNano()})
		}(i)
	}
	wg.Wait()

	waitFor(t, 5*time.Second, func() bool { return fired.Load() == int64(n) })
}
