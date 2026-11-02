// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/stats"
)

type matrixFloatWindowTestSample struct {
	timestamp      int64
	startTimestamp int64
	value          float64
	stale          bool
}

func (s matrixFloatWindowTestSample) T() int64  { return s.timestamp }
func (s matrixFloatWindowTestSample) ST() int64 { return s.startTimestamp }
func (s matrixFloatWindowTestSample) F() float64 {
	if s.stale {
		return math.Float64frombits(value.StaleNaN)
	}
	return s.value
}
func (matrixFloatWindowTestSample) H() *histogram.Histogram       { return nil }
func (matrixFloatWindowTestSample) FH() *histogram.FloatHistogram { return nil }
func (matrixFloatWindowTestSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s matrixFloatWindowTestSample) Copy() chunks.Sample         { return s }

func matrixFloatWindowTestSamples(end int64, gap bool) []chunks.Sample {
	var samples []chunks.Sample
	for ts := int64(0); ts <= end; ts++ {
		if gap && ts >= 9 && ts < 14 {
			continue
		}
		samples = append(samples, matrixFloatWindowTestSample{
			timestamp:      ts,
			startTimestamp: 1000 + 10*ts,
			value:          float64(ts),
			stale:          ts == 5,
		})
	}
	return samples
}

func TestMatrixIterSliceFloatWindow(t *testing.T) {
	for _, tc := range []struct {
		name string
		gap  bool
		end  int64
	}{
		{name: "overlap compaction", end: 24},
		{name: "gap resets the window", gap: true, end: 24},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			budget := &queryBudget{maxSamplesRead: 23}
			ev := &evaluator{maxSamples: 4, budget: budget, samplesStats: stats.NewQuerySamples(true)}
			ev.samplesStats.InitStepTracking(0, 20, 1)
			it := ev.newMatrixIterator(ctx, 4)
			samples := matrixFloatWindowTestSamples(tc.end, tc.gap)
			series := storage.NewListSeries(labels.EmptyLabels(), samples)
			it.Reset(series.Iterator(nil))

			window := matrixFloatWindow{
				floats:          make([]FPoint, 0, 8),
				startTimestamps: make([]int64, 0, 8),
			}
			startTimestamps := &StartTimestamps{Floats: window.startTimestamps}
			var floats []FPoint
			compactions := 0
			for step := int64(0); step <= 20; step++ {
				beforeHead, beforeLen := window.head, len(window.floats)
				floats, _, startTimestamps = ev.matrixIterSlice(ctx, it, step, step+4, floats, nil, startTimestamps, int(step), &window)
				points := window.points()
				wantPoints := make([]FPoint, 0, 4)
				wantStartTimestamps := make([]int64, 0, 4)
				for _, sample := range samples {
					if sample.T() > step && sample.T() <= step+4 && !value.IsStaleNaN(sample.F()) {
						wantPoints = append(wantPoints, FPoint{T: sample.T(), F: sample.F()})
						wantStartTimestamps = append(wantStartTimestamps, sample.ST())
					}
				}
				require.Equal(t, wantPoints, points)
				require.Equal(t, wantPoints, floats)
				require.Equal(t, wantStartTimestamps, startTimestamps.Floats)
				require.Equal(t, len(points), ev.currentSamples)
				if tc.gap && len(points) == 0 {
					require.Zero(t, window.head)
					require.Empty(t, window.floats)
					require.Empty(t, startTimestamps.Floats)
				}
				if !tc.gap && beforeHead > 0 && len(window.floats) < beforeLen {
					compactions++
				}
				if len(points) > 0 {
					it.ReduceDelta(1)
				}
			}
			if !tc.gap {
				require.Positive(t, compactions)
				require.Equal(t, 8, cap(window.floats))
				require.Equal(t, 8, cap(window.startTimestamps))
				require.Equal(t, int64(23), budget.samplesRead)
				require.Equal(t, int64(23), ev.samplesStats.SamplesRead)
				wantReadsPerStep := make([]int64, 21)
				wantReadsPerStep[0] = 4
				for step := 2; step < len(wantReadsPerStep); step++ {
					wantReadsPerStep[step] = 1
				}
				require.Equal(t, wantReadsPerStep, ev.samplesStats.SamplesReadPerStep)
			}
			require.Equal(t, 4, ev.samplesStats.PeakSamples)
		})
	}
}

func TestMatrixIterSliceFloatWindowBufferGrowth(t *testing.T) {
	for _, tc := range []struct {
		name     string
		floatCap int
		startCap int
	}{
		{name: "without start timestamps", floatCap: 8},
		{name: "float backing grows first", floatCap: 8, startCap: 16},
		{name: "start timestamp backing grows first", floatCap: 16, startCap: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			ev := &evaluator{maxSamples: 100, samplesStats: stats.NewQuerySamples(true)}
			ev.samplesStats.InitStepTracking(0, 0, 1)
			it := ev.newMatrixIterator(ctx, 10)
			samples := matrixFloatWindowTestSamples(10, false)
			series := storage.NewListSeries(labels.EmptyLabels(), samples)
			it.Reset(series.Iterator(nil))

			window := matrixFloatWindow{floats: make([]FPoint, 0, tc.floatCap)}
			var startTimestamps *StartTimestamps
			if tc.startCap > 0 {
				window.startTimestamps = make([]int64, 0, tc.startCap)
				startTimestamps = &StartTimestamps{Floats: window.startTimestamps}
			}
			var floats []FPoint
			floats, _, startTimestamps = ev.matrixIterSlice(ctx, it, 0, 6, floats, nil, startTimestamps, 0, &window)
			it.ReduceDelta(4)
			floats, _, startTimestamps = ev.matrixIterSlice(ctx, it, 1, 10, floats, nil, startTimestamps, 0, &window)

			wantFloats := []FPoint{
				{T: 2, F: 2},
				{T: 3, F: 3},
				{T: 4, F: 4},
				{T: 6, F: 6},
				{T: 7, F: 7},
				{T: 8, F: 8},
				{T: 9, F: 9},
				{T: 10, F: 10},
			}
			wantStarts := []int64{1020, 1030, 1040, 1060, 1070, 1080, 1090, 1100}
			require.Equal(t, wantFloats, floats)
			require.Equal(t, wantFloats, window.points())
			if startTimestamps != nil {
				require.Equal(t, wantStarts, startTimestamps.Floats)
				require.Equal(t, wantStarts, window.startTimestamps)
			}
			require.Zero(t, window.head)
		})
	}
}

func TestMatrixIterator_checkContext_CancellationAccounting(t *testing.T) {
	for _, quota := range []int64{0, 30} {
		for _, billed := range []int{0, 7, 20} {
			for _, step := range []int{0, -1} {
				t.Run(fmt.Sprintf("quota=%d/billed=%d/step=%d", quota, billed, step), func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					ev := &evaluator{budget: &queryBudget{maxSamplesRead: quota}, samplesStats: stats.NewQuerySamples(true)}
					ev.samplesStats.InitStepTracking(0, 0, 1)
					ev.consumeMatrixSamples(10, 0) // A previous selector's cost.
					it := ev.newMatrixIterator(ctx, 30)
					series := storage.NewListSeries(labels.EmptyLabels(), chunks.GenerateSamples(0, 21))
					it.Reset(series.Iterator(nil))
					it.beginFrame(ctx, step)
					require.Equal(t, chunkenc.ValFloat, it.seek(-1, -1, 20))
					// Charge actual buffered consumption, then interrupt the drain.
					buf := it.Buffer()
					for range billed {
						require.Equal(t, chunkenc.ValFloat, buf.Next())
						_, _ = buf.At()
						ev.consumeMatrixSamples(1, step)
					}
					cancel()
					require.PanicsWithValue(t, ErrQueryCanceled(env), it.checkContext)
					want := int64(30)
					if step < 0 {
						want = 10
					}
					require.Equal(t, want, ev.budget.samplesRead, "commit only unbilled storage input")
					require.Equal(t, want, ev.samplesStats.SamplesRead)
					require.Equal(t, []int64{want}, ev.samplesStats.SamplesReadPerStep)
				})
			}
		}
	}
}
