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

package tsdb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// BenchmarkLabelValues_SlowPath benchmarks the performance of LabelValues when the matcher
// is far ahead of the candidate posting list. This reproduces the performance regression
// described in #14551 where dense candidates caused O(N) iteration instead of O(log N) seeking.
func BenchmarkLabelValues_SlowPath(b *testing.B) {
	// Create a head with some data.
	opts := DefaultHeadOptions()
	opts.ChunkDirRoot = b.TempDir()
	h, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(b, err)
	defer h.Close()

	app := h.AppenderV2(context.Background())
	// 1. Create a large number of series for a "candidate" label (e.g. "job").
	// We want these to NOT match the target matcher, but be candidates for a different label.
	// We use "job=api" and "instance=..."
	// We want the interaction to be:
	// LabelValues("instance", "job"="api")
	// "job"="api" will have 1 series at the END.
	// "instance" will have 100k series.

	// Actually, let's stick to the reproduction case:
	// distinct values for "val1".
	// "b"="1" matcher.

	// Create 100k series with the same label value ("common") but without the matcher label.
	// This results in a single large posting list for that value, simulating a dense candidate.
	for i := range 100000 {
		_, err := app.Append(0, labels.FromStrings("val1", "common", "extra", strconv.Itoa(i)), 0, time.Now().UnixMilli(), 1, nil, nil, storage.AOptions{})
		require.NoError(b, err)
	}

	// Create 1 series that matches the label "b=1", with a series ID greater than all previous ones.
	// This forces the intersection to skip over all 100k previous candidates.
	_, err = app.Append(0, labels.FromStrings("val1", "common", "b", "1"), 0, time.Now().UnixMilli(), 1, nil, nil, storage.AOptions{})
	require.NoError(b, err)

	require.NoError(b, app.Commit())

	ctx := context.Background()
	matcher := labels.MustNewMatcher(labels.MatchEqual, "b", "1")

	// Use the correct method to access label values.
	idx, err := h.Index()
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// "val1"="common" has 100k+1 postings.
		// "b=1" has 1 posting (the last one).
		vals, err := idx.LabelValues(ctx, "val1", nil, matcher)
		require.NoError(b, err)
		require.Equal(b, []string{"common"}, vals)
	}
}

// BenchmarkLabelValues_LimitedMatcher measures a complete Head-backed label-values read with the API hint for limit 10.
func BenchmarkLabelValues_LimitedMatcher(b *testing.B) {
	const cardinality = 20000
	ctx := context.Background()
	db, err := Open(b.TempDir(), nil, nil, DefaultOptions(), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})

	app := db.AppenderV2(ctx)
	order := rand.New(rand.NewPCG(1, 2)).Perm(cardinality)
	want := make([]string, 0, 11)
	for j, i := range order {
		pod := fmt.Sprintf("pod_%06d", i)
		if j < 11 {
			want = append(want, pod)
		}
		if _, err := app.Append(0, labels.FromStrings("__name__", "up", "job", "all", "pod", pod), 0, 1000, 1, nil, nil, storage.AOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	slices.Sort(want)
	if err := app.Commit(); err != nil {
		b.Fatal(err)
	}

	matcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")
	hints := &storage.LabelHints{Limit: 11}

	// Validate outside the benchmark loop while the querier still owns the returned strings.
	q, err := db.Querier(0, 2000)
	if err != nil {
		b.Fatal(err)
	}
	got, _, readErr := q.LabelValues(ctx, "pod", hints, matcher)
	if readErr != nil {
		if closeErr := q.Close(); closeErr != nil {
			b.Error(closeErr)
		}
		b.Fatal(readErr)
	}
	valid := slices.Equal(want, got)
	var mismatch string
	if !valid {
		mismatch = fmt.Sprintf("incorrect values: got %v, want %v", got, want)
	}
	closeErr := q.Close()
	if closeErr != nil {
		b.Fatal(closeErr)
	}
	if !valid {
		b.Fatal(mismatch)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		q, err := db.Querier(0, 2000)
		if err != nil {
			b.Fatal(err)
		}
		_, _, readErr := q.LabelValues(ctx, "pod", hints, matcher)
		closeErr := q.Close()
		if readErr != nil {
			b.Fatal(readErr)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
}

// Ensure wlog/wal needed for NewHead.
var _ = wlog.WL{}
