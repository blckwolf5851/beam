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
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestSearch_Errors(t *testing.T) {
	testCases := []struct {
		name           string
		client         dicomStoreClient
		containedError string
	}{
		{
			name:           "Search request returns error",
			client:         requestReturnErrorFakeClient,
			containedError: fakeRequestReturnErrorMessage,
		},
		{
			name:           "Search request returns bad status",
			client:         badStatusFakeClient,
			containedError: strconv.Itoa(http.StatusForbidden),
		},
		{
			name:           "Search request response body fails to be read",
			client:         bodyReaderErrorFakeClient,
			containedError: fakeBodyReaderErrorMessage,
		},
		{
			name:           "Search request response body failed to be decoded",
			client:         emptyResponseBodyFakeClient,
			containedError: io.EOF.Error(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			p, s, testSearchQueryPCollection := ptest.CreateList([]SearchDicomQuery{{}})
			resources, failedSearches := searchStudies(s, "parent", "dicomWebPath", testSearchQueryPCollection, testCase.client)
			passert.Empty(s, resources)
			passert.Count(s, failedSearches, "", 1)
			passert.True(s, failedSearches, func(errorMsg string) bool {
				return strings.Contains(errorMsg, testCase.containedError)
			})
			pipelineResult := ptest.RunAndValidate(t, p)
			validateCounter(t, pipelineResult, errorCounterName, 1)
			validateCounter(t, pipelineResult, successCounterName, 0)
		})
	}
}
