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
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"google.golang.org/api/healthcare/v1"
)

var (
	testOperationResult = operationResults{Successes: 5, Failures: 2}

	readstudiesConfig           = readConfig{resourceScope: resourceScopeStudies, metadataOnly: false}
	readseriesConfig            = readConfig{resourceScope: resourceScopeSeries, metadataOnly: false}
	readinstancesConfig         = readConfig{resourceScope: resourceScopeInstances, metadataOnly: false}
	readstudiesMetadataConfig   = readConfig{resourceScope: resourceScopeStudies, metadataOnly: true}
	readseriesMetadataConfig    = readConfig{resourceScope: resourceScopeSeries, metadataOnly: true}
	readinstancesMetadataConfig = readConfig{resourceScope: resourceScopeInstances, metadataOnly: true}

	fakeRequestReturnErrorMessage = "internal error"
	requestReturnErrorFakeClient  = &fakeDicomStoreClient{
		fakeReadStudiesMetadata: func([]byte, []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeReadSeriesMetadata: func([]byte, []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeReadInstancesMetadata: func([]byte, []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeReadStudy: func([]byte, []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeReadSeries: func([]byte, []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeReadInstance: func([]byte, []byte) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeSearchStudies: func(string, string, map[string]string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeSearchSeries: func(string, string, map[string]string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeSearchInstances: func(string, string, map[string]string) (*http.Response, error) {
			return nil, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeDeidentify: func(string, string, *healthcare.DeidentifyConfig) (operationResults, error) {
			return operationResults{}, errors.New(fakeRequestReturnErrorMessage)
		},
		fakeImportResources: func(string, string) (operationResults, error) {
			return operationResults{}, errors.New(fakeRequestReturnErrorMessage)
		},
	}

	badStatusFakeResponse = &http.Response{
		Body:       io.NopCloser(bytes.NewBufferString("response")),
		StatusCode: http.StatusForbidden,
	}
	badStatusReadFunc = func([]byte, []byte) (*http.Response, error) {
		return badStatusFakeResponse, nil
	}
	badStatusSearchFunc = func(string, string, map[string]string) (*http.Response, error) {
		return badStatusFakeResponse, nil
	}
	badStatusFakeClient = &fakeDicomStoreClient{
		fakeReadStudiesMetadata:   badStatusReadFunc,
		fakeReadSeriesMetadata:    badStatusReadFunc,
		fakeReadInstancesMetadata: badStatusReadFunc,
		fakeReadStudy:             badStatusReadFunc,
		fakeReadSeries:            badStatusReadFunc,
		fakeReadInstance:          badStatusReadFunc,
		fakeSearchStudies:         badStatusSearchFunc,
		fakeSearchSeries:          badStatusSearchFunc,
		fakeSearchInstances:       badStatusSearchFunc,
	}

	fakeBodyReaderErrorMessage  = "ReadAll fail"
	bodyReaderErrorFakeResponse = &http.Response{
		Body: &fakeReaderCloser{
			fakeRead: func([]byte) (int, error) {
				return 0, errors.New(fakeBodyReaderErrorMessage)
			},
		},
		StatusCode: http.StatusOK,
	}
	badResponseReadFunc = func([]byte, []byte) (*http.Response, error) {
		return bodyReaderErrorFakeResponse, nil
	}
	badResponseSearchFunc = func(string, string, map[string]string) (*http.Response, error) {
		return bodyReaderErrorFakeResponse, nil
	}
	bodyReaderErrorFakeClient = &fakeDicomStoreClient{
		fakeReadStudiesMetadata:   badResponseReadFunc,
		fakeReadSeriesMetadata:    badResponseReadFunc,
		fakeReadInstancesMetadata: badResponseReadFunc,
		fakeReadStudy:             badResponseReadFunc,
		fakeReadSeries:            badResponseReadFunc,
		fakeReadInstance:          badResponseReadFunc,
		fakeSearchStudies:         badResponseSearchFunc,
		fakeSearchSeries:          badResponseSearchFunc,
		fakeSearchInstances:       badResponseSearchFunc,
	}

	emptyBodyReaderFakeResponse = &http.Response{
		Body:       io.NopCloser(bytes.NewBuffer(nil)),
		StatusCode: http.StatusOK,
	}
	emptyResponseReadFunc = func([]byte, []byte) (*http.Response, error) {
		return emptyBodyReaderFakeResponse, nil
	}
	emptyResponseSearchFunc = func(string, string, map[string]string) (*http.Response, error) {
		return emptyBodyReaderFakeResponse, nil
	}
	emptyResponseBodyFakeClient = &fakeDicomStoreClient{
		fakeReadStudiesMetadata:   emptyResponseReadFunc,
		fakeReadSeriesMetadata:    emptyResponseReadFunc,
		fakeReadInstancesMetadata: emptyResponseReadFunc,
		fakeReadStudy:             emptyResponseReadFunc,
		fakeReadSeries:            emptyResponseReadFunc,
		fakeReadInstance:          emptyResponseReadFunc,
		fakeSearchStudies:         emptyResponseSearchFunc,
		fakeSearchSeries:          emptyResponseSearchFunc,
		fakeSearchInstances:       emptyResponseSearchFunc,
	}
)

type fakeDicomStoreClient struct {
	fakeReadStudiesMetadata   func([]byte, []byte) (*http.Response, error)
	fakeReadSeriesMetadata    func([]byte, []byte) (*http.Response, error)
	fakeReadInstancesMetadata func([]byte, []byte) (*http.Response, error)
	fakeReadStudy             func([]byte, []byte) (*http.Response, error)
	fakeReadSeries            func([]byte, []byte) (*http.Response, error)
	fakeReadInstance          func([]byte, []byte) (*http.Response, error)
	fakeSearchStudies         func(string, string, map[string]string) (*http.Response, error)
	fakeSearchSeries          func(string, string, map[string]string) (*http.Response, error)
	fakeSearchInstances       func(string, string, map[string]string) (*http.Response, error)
	fakeDeidentify            func(string, string, *healthcare.DeidentifyConfig) (operationResults, error)
	fakeImportResources       func(string, string) (operationResults, error)
}

func (c *fakeDicomStoreClient) readStudiesMetadata(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.fakeReadStudiesMetadata(parent, dicomWebPath)
}
func (c *fakeDicomStoreClient) readSeriesMetadata(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.fakeReadSeriesMetadata(parent, dicomWebPath)
}
func (c *fakeDicomStoreClient) readInstancesMetadata(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.fakeReadInstancesMetadata(parent, dicomWebPath)
}
func (c *fakeDicomStoreClient) readStudy(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.fakeReadStudy(parent, dicomWebPath)
}
func (c *fakeDicomStoreClient) readSeries(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.fakeReadSeries(parent, dicomWebPath)
}
func (c *fakeDicomStoreClient) readInstance(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.fakeReadInstance(parent, dicomWebPath)
}

func (c *fakeDicomStoreClient) searchStudies(storePath, resourceType string, queries map[string]string) (*http.Response, error) {
	return c.fakeSearchStudies(storePath, resourceType, queries)
}
func (c *fakeDicomStoreClient) searchSeries(storePath, resourceType string, queries map[string]string) (*http.Response, error) {
	return c.fakeSearchSeries(storePath, resourceType, queries)
}
func (c *fakeDicomStoreClient) searchInstances(storePath, resourceType string, queries map[string]string) (*http.Response, error) {
	return c.fakeSearchInstances(storePath, resourceType, queries)
}

func (c *fakeDicomStoreClient) deidentify(srcStorePath, dstStorePath string, deidConfig *healthcare.DeidentifyConfig) (operationResults, error) {
	return c.fakeDeidentify(srcStorePath, dstStorePath, deidConfig)
}

func (c *fakeDicomStoreClient) importResources(storePath, gcsURI string) (operationResults, error) {
	return c.fakeImportResources(storePath, gcsURI)
}

// Useful to fake the Body of a http.Response.
type fakeReaderCloser struct {
	io.Closer
	fakeRead func([]byte) (int, error)
}

func (m *fakeReaderCloser) Read(b []byte) (int, error) {
	return m.fakeRead(b)
}

func validateCounter(t *testing.T, pipelineResult beam.PipelineResult, expectedCounterName string, expectedCount int) {
	t.Helper()

	counterResults := pipelineResult.Metrics().Query(func(mr beam.MetricResult) bool {
		return mr.Name() == expectedCounterName
	}).Counters()

	if expectedCount == 0 && len(counterResults) == 0 {
		return
	}

	if len(counterResults) != 1 {
		t.Fatalf("got %v counters with name %v, expected 1", len(counterResults), expectedCounterName)
	}
	counterResult := counterResults[0]

	if counterResult.Result() != int64(expectedCount) {
		t.Fatalf("counter %v result is %v, expected %v", expectedCounterName, counterResult.Result(), expectedCount)
	}
}
