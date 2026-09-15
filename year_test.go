package timingwheel

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestYearLongTaskLayers 验证 1 年后的任务能否成功投递，以及需要多少层、各层 tick 颗粒度。
func TestYearLongTaskLayers(t *testing.T) {
	tw := NewDelayTimingWheel(time.Second, 64, func(tasks []Task) {})
	tw.Start()
	defer tw.Stop()

	exp := time.Now().Add(365 * 24 * time.Hour).UnixNano()
	if !tw.Add(&testTask{expiration: exp}) {
		t.Fatal("1 年后的任务投递失败")
	}

	var layers []time.Duration
	for w := tw; w != nil; w = w.overflow {
		layers = append(layers, w.tick)
	}
	t.Logf("tick=1s wheelSize=64，1 年任务跨越层数=%d", len(layers))
	for i, tk := range layers {
		t.Logf("  第 %d 层 tick=%v（一圈=%v）", i, tk, tk*time.Duration(tw.wheelSize))
	}
	if len(layers) < 4 {
		t.Fatalf("1 年任务应跨越至少 4 层，实际 %d 层", len(layers))
	}
}

// TestDowngradePrecision 用缩放的 tick 模拟"远大于一圈"的任务，
// 验证跨多层降级后最终到期精度仍是根层 tick，而非上层粗颗粒度。
func TestDowngradePrecision(t *testing.T) {
	tick := 10 * time.Millisecond
	var firedAt atomic.Int64
	tw := NewDelayTimingWheel(tick, 4, func(tasks []Task) {
		firedAt.Store(time.Now().UnixNano())
	})
	tw.Start()
	defer tw.Stop()

	start := time.Now()
	d := 5 * time.Second // 根层一圈 40ms，5s/40ms = 125 倍，需跨越约 4~5 层
	tw.Add(&testTask{expiration: start.Add(d).UnixNano()})

	waitFor(t, 10*time.Second, func() bool { return firedAt.Load() != 0 })

	elapsed := firedAt.Load() - start.UnixNano()
	deviation := time.Duration(elapsed) - d
	if deviation < 0 {
		deviation = -deviation
	}
	t.Logf("到期偏差=%v（任务设定 %v 后到期）", deviation, d)

	// 最终精度应接近根层 tick（10ms），远小于上层最粗 tick（2.56s）。
	if deviation > tick {
		t.Fatalf("降级后精度退化：偏差 %v，超过根层 tick %v", deviation, tick)
	}
}

// TestYearLongTaskOverflowDepth 参数化验证层数符合 log_{wheelSize}(1 年/tick)。
func TestYearLongTaskOverflowDepth(t *testing.T) {
	cases := []struct {
		wheelSize int64
		want      int // 约 log_{wheelSize}(3.15e7 秒)
	}{
		{64, 5},
		{16, 7},
		{4, 13},
	}
	for _, c := range cases {
		t.Run(strconv.FormatInt(c.wheelSize, 10), func(t *testing.T) {
			tw := NewDelayTimingWheel(time.Second, c.wheelSize, func(tasks []Task) {})
			tw.Start()
			defer tw.Stop()

			exp := time.Now().Add(365 * 24 * time.Hour).UnixNano()
			if !tw.Add(&testTask{expiration: exp}) {
				t.Fatal("投递失败")
			}
			depth := 0
			for w := tw; w != nil; w = w.overflow {
				depth++
			}
			t.Logf("wheelSize=%d 实际层数=%d 期望约=%d", c.wheelSize, depth, c.want)
			if depth != c.want {
				t.Fatalf("层数不符：实际 %d，期望约 %d", depth, c.want)
			}
		})
	}
}
