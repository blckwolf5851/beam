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

package dicomio

import (
	"net/http"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestStore(t *testing.T) {
	testCases := []struct {
		name                 string
		client               dicomStoreClient
		expectedErrorCount   int
		expectedSuccessCount int
		expectedHttpResponse *http.Response
	}{
		{
			name:                 "Store returns error",
			client:               requestReturnErrorFakeClient,
			expectedErrorCount:   1,
			expectedSuccessCount: 0,
			expectedHttpResponse: emptyBodyReaderFakeResponse,
		},
		{
			name: "Store returns successfully",
			client: &fakeDicomStoreClient{fakeStoreInstance: func(string, string, []byte) (*http.Response, error) {
				return emptyBodyReaderFakeResponse, nil
			}},
			expectedErrorCount:   0,
			expectedSuccessCount: 1,
			expectedHttpResponse: emptyBodyReaderFakeResponse,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p, s, testStoreQueryPCollection := ptest.CreateList([]StoreQuery{{}})
			deadLetter := storeInBatches(
				s,
				testStoreQueryPCollection,
				testCase.client,
			)

			passert.Empty(s, deadLetter)
			passert.Count(s, deadLetter, "", testCase.expectedErrorCount)

			pipelineResult := ptest.RunAndValidate(t, p)
			validateCounter(t, pipelineResult, operationErrorCounterName, testCase.expectedErrorCount)
			validateCounter(t, pipelineResult, successCounterName, testCase.expectedSuccessCount)
			validateCounter(t, pipelineResult, errorCounterName, testCase.expectedErrorCount)
		})
	}
}
