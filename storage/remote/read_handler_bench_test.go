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

package remote

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/util/teststorage"
)

// BenchmarkSampledReadEndpointWithNativeHistograms measures a complete sampled
// remote read with many native histogram samples per returned series.
func BenchmarkSampledReadEndpointWithNativeHistograms(b *testing.B) {
	const (
		seriesCount      = 100
		samplesPerSeries = 240
		stepMs           = int64(15000)
	)

	store := teststorage.New(b)
	app := store.AppenderV2(b.Context())
	for i := range seriesCount {
		seriesLabels := labels.FromStrings(
			"__name__", "sampled_read_histogram",
			"instance", strconv.Itoa(i),
			"job", "benchmark",
		)
		var ref storage.SeriesRef
		for j := range samplesPerSeries {
			h := tsdbutil.GenerateTestHistogram(int64(j))
			if j > 0 {
				h.CounterResetHint = histogram.NotCounterReset
			}
			var err error
			ref, err = app.Append(ref, seriesLabels, 0, int64(j)*stepMs, 0, h, nil, storage.AOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := app.Commit(); err != nil {
		b.Fatal(err)
	}

	handler := NewReadHandler(nil, nil, store, func() config.Config { return config.Config{} }, 1_000_000, 1, 0)
	matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "sampled_read_histogram")
	if err != nil {
		b.Fatal(err)
	}
	query, err := ToQuery(0, int64(samplesPerSeries-1)*stepMs, []*labels.Matcher{matcher}, nil)
	if err != nil {
		b.Fatal(err)
	}
	requestBytes, err := proto.Marshal(&prompb.ReadRequest{
		Queries:               []*prompb.Query{query},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
	})
	if err != nil {
		b.Fatal(err)
	}
	compressed := snappy.Encode(nil, requestBytes)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/read", http.NoBody)
	serve := func() *httptest.ResponseRecorder {
		request.Body = io.NopCloser(bytes.NewReader(compressed))
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder
	}

	recorder := serve()
	if recorder.Code != http.StatusOK {
		b.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	decoded, err := snappy.Decode(nil, recorder.Body.Bytes())
	if err != nil {
		b.Fatal(err)
	}
	var response prompb.ReadResponse
	if err := proto.Unmarshal(decoded, &response); err != nil {
		b.Fatal(err)
	}
	if len(response.Results) != 1 || len(response.Results[0].Timeseries) != seriesCount {
		b.Fatal("unexpected response series count")
	}
	for _, series := range response.Results[0].Timeseries {
		if len(series.Histograms) != samplesPerSeries || len(series.Samples) != 0 {
			b.Fatal("unexpected response sample count")
		}
		if series.Histograms[0].IsFloatHistogram() {
			b.Fatal("expected integer histograms")
		}
		if got, want := series.Histograms[samplesPerSeries-1].Timestamp, int64(samplesPerSeries-1)*stepMs; got != want {
			b.Fatalf("last timestamp %d, want %d", got, want)
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		recorder := serve()
		if recorder.Code != http.StatusOK {
			b.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
		}
	}
	b.ReportMetric(seriesCount*samplesPerSeries, "samples/response")
}
