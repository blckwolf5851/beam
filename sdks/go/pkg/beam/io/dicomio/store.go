// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dicomio provides an API for reading and writing resources to Google
// Cloud Healthcare Dicom stores.
// Experimental.
package dicomio

import (
	"context"
	"net/http"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/gcs"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn3x0[context.Context, StoreQuery, func(StoreQuery)]((*storeFn)(nil))
	register.Emitter1[StoreQuery]()
}

type StoreQuery struct {
	Parent       string
	DicomWebPath string
	DicomData    []byte
}

type storeFn struct {
	fnCommonVariables
	buffer     []StoreQuery
	bufferSize int
	maxWorker  int
}

func (fn *storeFn) StartBundle(ctx context.Context, _ func(StoreQuery)) error {
	fn.buffer = make([]StoreQuery, fn.bufferSize)
	return nil
}

func (fn *storeFn) FinishBundle(ctx context.Context, emitFailedResource func(StoreQuery)) {
	fn.flush(ctx, emitFailedResource)
}

func (fn *storeFn) ProcessElement(ctx context.Context, storeQuery StoreQuery, emitFailedResource func(StoreQuery)) {
	fn.buffer = append(fn.buffer, storeQuery)
	if len(fn.buffer) >= fn.bufferSize {
		fn.flush(ctx, emitFailedResource)
	}
}

func (fn *storeFn) flush(ctx context.Context, emitFailedResource func(StoreQuery)) {
	jobs := make(chan StoreQuery)
	results := make(chan int64) // Record whether success or not
	for w := 1; w <= fn.maxWorker; w++ {
		go fn.makeRequests(ctx, jobs, results, emitFailedResource)
	}
	numJobs := len(fn.buffer)
	for _, q := range fn.buffer {
		jobs <- q
	}
	close(jobs)
	// Clear buffer
	fn.buffer = make([]StoreQuery, fn.bufferSize)
	// Aggregate results
	var successCount int64 = 0
	var failureCount int64 = 0
	for a := 1; a <= numJobs; a++ {
		success := <-results
		successCount += success
		failureCount += 1 - success
	}

	fn.resourcesErrorCount.Inc(ctx, failureCount)
	fn.resourcesSuccessCount.Inc(ctx, successCount)
}

func (fn *storeFn) makeRequests(ctx context.Context, jobs <-chan StoreQuery, results chan<- int64, emitFailedResource func(StoreQuery)) {
	for q := range jobs {
		resp, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (*http.Response, error) {
			return fn.client.storeInstance(q.Parent, q.DicomWebPath, q.DicomData)
		})
		if err != nil {
			results <- 1
			emitFailedResource(q)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode > 299 {
			results <- 0
			emitFailedResource(q)
			continue
		}
		results <- 1
	}
}

// Store consumes DICOM resources as input PCollection<StoreQuery> and imports them
// into a given Google Cloud Healthcare DICOM store. It does so by creating batch
// files in the provided Google Cloud Storage `tempDir` and importing those files
// to the store through DICOM import API method: https://cloud.google.com/healthcare-api/docs/concepts/dicom-import.
// If `tempDir` is not provided, it falls back to the dataflow temp_location flag.
// Resources that fail to be included in the batch files are included as the
// first output PCollection. In case a batch file fails to be imported, it will
// be moved to the `deadLetterDir` and an error message will be provided in the
// second output PCollection. If `deadLetterDir` is not provided, the failed
// import files will be deleted and be irretrievable, but the error message will
// still be provided.
func Store(s beam.Scope, storeQueries beam.PCollection) beam.PCollection {
	s = s.Scope("dicomio.Store")

	return storeInBatches(s, storeQueries, nil)
}

// This is useful as an entry point for testing because we can provide a fake DICOM store client.
func storeInBatches(s beam.Scope, storeQueries beam.PCollection, client dicomStoreClient) beam.PCollection {
	bufferSize := 8
	maxWorker := 5
	failedStoreDeadLetter := beam.ParDo(
		s,
		&storeFn{
			fnCommonVariables: fnCommonVariables{client: client},
			buffer:            make([]StoreQuery, bufferSize),
			bufferSize:        bufferSize,
			maxWorker:         maxWorker,
		}, storeQueries)
	return failedStoreDeadLetter
}
