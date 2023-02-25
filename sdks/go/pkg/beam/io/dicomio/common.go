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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const (
	UserAgent                   = "apache-beam-io-google-cloud-platform-healthcare/" + core.SdkVersion
	baseMetricPrefix            = "dicomio/"
	errorCounterName            = baseMetricPrefix + "resource_error_count"
	operationErrorCounterName   = baseMetricPrefix + "operation_error_count"
	operationSuccessCounterName = baseMetricPrefix + "operation_success_count"
	successCounterName          = baseMetricPrefix + "resource_success_count"
	pageTokenParameterKey       = "_page_token"
)

var backoffDuration = [...]time.Duration{time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}

func executeAndRecordLatency[T any](ctx context.Context, latencyMs *beam.Distribution, executionSupplier func() (T, error)) (T, error) {
	timeBeforeReadRequest := time.Now()
	result, err := executionSupplier()
	latencyMs.Update(ctx, time.Since(timeBeforeReadRequest).Milliseconds())
	return result, err
}

func extractBodyFrom(response *http.Response) (string, error) {
	err := googleapi.CheckResponse(response)
	if err != nil {
		return "", errors.Wrapf(err, "response contains bad status: [%v]", response.Status)
	}

	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(bodyBytes), nil
}

type operationCounters struct {
	successCount, errorCount beam.Counter
}

func (c *operationCounters) setup(namespace string) {
	c.successCount = beam.NewCounter(namespace, operationSuccessCounterName)
	c.errorCount = beam.NewCounter(namespace, operationErrorCounterName)
}

type operationResults struct {
	Successes int64 `json:"success,string"`
	Failures  int64 `json:"failure,string"`
}

type dicomStoreClient interface {
	readStudiesMetadata(parent, dicomWebPath []byte) (*http.Response, error)
	readSeriesMetadata(parent, dicomWebPath []byte) (*http.Response, error)
	readInstancesMetadata(parent, dicomWebPath []byte) (*http.Response, error)
	readStudy(parent, dicomWebPath []byte) (*http.Response, error)
	readSeries(parent, dicomWebPath []byte) (*http.Response, error)
	readInstance(parent, dicomWebPath []byte) (*http.Response, error)
	searchStudies(parent, dicomWebPath string, queries map[string]string) (*http.Response, error)
	searchSeries(parent, dicomWebPath string, queries map[string]string) (*http.Response, error)
	searchInstances(parent, dicomWebPath string, queries map[string]string) (*http.Response, error)
	deidentify(srcStorePath, dstStorePath string, deidConfig *healthcare.DeidentifyConfig) (operationResults, error)
	importResources(storePath, gcsURI string) (operationResults, error)
}

type dicomStoreClientImpl struct {
	healthcareService *healthcare.Service
}

// Create new healthcare service
func newDicomStoreClient() *dicomStoreClientImpl {
	healthcareService, err := healthcare.NewService(context.Background(), option.WithUserAgent(UserAgent))
	if err != nil {
		panic("Failed to initialize Google Cloud Healthcare Service. Reason: " + err.Error())
	}
	return &dicomStoreClientImpl{healthcareService}
}

// Get the Dicom store service from healthcare service
func (c *dicomStoreClientImpl) dicomStoreService() *healthcare.ProjectsLocationsDatasetsDicomStoresService {
	return c.healthcareService.Projects.Locations.Datasets.DicomStores
}

// Returns instance associated with the given study presented as metadata with the bulk data removed.
//   - parent: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
//   - dicomWebPath: /studies/{study_uid}/metadata
func (c *dicomStoreClientImpl) readStudiesMetadata(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.dicomStoreService().Studies.RetrieveMetadata(string(parent), string(dicomWebPath)).Do()
}

// Returns instance associated with the given study&series presented as metadata with the bulk data removed.
//   - parent: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
//   - dicomWebPath: /studies/{study_uid}/series/{series_uid}/metadata
func (c *dicomStoreClientImpl) readSeriesMetadata(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.dicomStoreService().Studies.Series.RetrieveMetadata(string(parent), string(dicomWebPath)).Do()
}

// Returns instance associated with the given study&serie&instances presented as metadata with the bulk data removed.
//   - parent: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
//   - dicomWebPath: studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/metadata
func (c *dicomStoreClientImpl) readInstancesMetadata(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.dicomStoreService().Studies.Series.Instances.RetrieveMetadata(string(parent), string(dicomWebPath)).Do()
}

// Returns all instances within the given study.
//   - parent: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
//   - dicomWebPath: /studies/{study_uid}
func (c *dicomStoreClientImpl) readStudy(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.dicomStoreService().Studies.RetrieveStudy(string(parent), string(dicomWebPath)).Do()
}

// Returns all instances within the given study and series.
//   - parent: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
//   - dicomWebPath: /studies/{study_uid}/series/{series_uid}
func (c *dicomStoreClientImpl) readSeries(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.dicomStoreService().Studies.Series.RetrieveSeries(string(parent), string(dicomWebPath)).Do()
}

// Returns instance associated with the given study, series, and SOP Instance UID
//   - parent: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
//   - dicomWebPath: studies/{study_uid}/series/{series_uid}/instances/{instance_uid}
func (c *dicomStoreClientImpl) readInstance(parent, dicomWebPath []byte) (*http.Response, error) {
	return c.dicomStoreService().Studies.Series.Instances.RetrieveInstance(string(parent), string(dicomWebPath)).Do()
}

// Construct list of query parameters from string-string map
func constructQueryParameters(queries map[string]string) []googleapi.CallOption {
	queryParams := make([]googleapi.CallOption, 0)
	for key, value := range queries {
		queryParams = append(queryParams, googleapi.QueryParameter(key, value))
	}
	return queryParams
}

// Return list of matching Dicom studies
//   - parent: projects/%s/locations/%s/datasets/%s/dicomStores/%s
//   - dicomWebPath: studies
//   - queries: {PatientName: Sally} (for searchable keys, see: https://cloud.google.com/healthcare-api/docs/dicom#search_parameters)
func (c *dicomStoreClientImpl) searchStudies(parent, dicomWebPath string, queries map[string]string) (*http.Response, error) {
	queryParams := constructQueryParameters(queries)
	return c.dicomStoreService().SearchForStudies(parent, dicomWebPath).Do(queryParams...)
}

// Return list of matching Dicom series
//   - parent: projects/%s/locations/%s/datasets/%s/dicomStores/%s
//   - dicomWebPath: studies / studies/xxxx / series
//   - queries: {Modality: CT} (for searchable keys, see: https://cloud.google.com/healthcare-api/docs/dicom#search_parameters)
func (c *dicomStoreClientImpl) searchSeries(parent, dicomWebPath string, queries map[string]string) (*http.Response, error) {
	queryParams := constructQueryParameters(queries)
	return c.dicomStoreService().SearchForSeries(parent, dicomWebPath).Do(queryParams...)
}

// Return list of matching Dicom instances
//   - parent: projects/%s/locations/%s/datasets/%s/dicomStores/%s
//   - dicomWebPath: studies / studies/xxxx / series
//   - queries: {SOPInstanceUID: xxx} (for searchable keys, see: https://cloud.google.com/healthcare-api/docs/dicom#search_parameters)
func (c *dicomStoreClientImpl) searchInstances(parent, dicomWebPath string, queries map[string]string) (*http.Response, error) {
	queryParams := constructQueryParameters(queries)
	return c.dicomStoreService().SearchForInstances(parent, dicomWebPath).Do(queryParams...)
}

// De-identifies data from the source store and writes it to the destination store.
//   - srcStorePath: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
//   - dstStorePath: projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}
func (c *dicomStoreClientImpl) deidentify(srcStorePath, dstStorePath string, deidConfig *healthcare.DeidentifyConfig) (operationResults, error) {
	deidRequest := &healthcare.DeidentifyDicomStoreRequest{
		Config:           deidConfig,
		DestinationStore: dstStorePath,
	}
	operation, err := c.dicomStoreService().Deidentify(srcStorePath, deidRequest).Do()
	if err != nil {
		return operationResults{}, err
	}
	return c.pollTilCompleteAndCollectResults(operation)
}

// Imports data into the DICOM store by copying it from the specified source.
//   - storePath: `projects/{project_id}/locations/{location_id}/datasets/{dataset_id}/dicomStores/{dicom_store_id}`
func (c *dicomStoreClientImpl) importResources(storePath, gcsURI string) (operationResults, error) {
	importRequest := &healthcare.ImportDicomDataRequest{
		GcsSource: &healthcare.GoogleCloudHealthcareV1DicomGcsSource{Uri: gcsURI},
	}
	operation, err := c.dicomStoreService().Import(storePath, importRequest).Do()
	if err != nil {
		return operationResults{}, err
	}
	return c.pollTilCompleteAndCollectResults(operation)
}

func (c *dicomStoreClientImpl) pollTilCompleteAndCollectResults(operation *healthcare.Operation) (operationResults, error) {
	operation, err := c.healthcareService.Projects.Locations.Datasets.Operations.Get(operation.Name).Do()
	for i := 0; err == nil && !operation.Done; {
		time.Sleep(backoffDuration[i])
		if i < len(backoffDuration)-1 {
			i += 1
		}

		operation, err = c.healthcareService.Projects.Locations.Datasets.Operations.Get(operation.Name).Do()
	}

	if err != nil {
		return operationResults{}, err
	}

	if operation.Error != nil {
		return operationResults{}, errors.New(operation.Error.Message)
	}

	return parseOperationCounterResultsFrom(operation.Metadata)
}

func parseOperationCounterResultsFrom(operationMetadata []byte) (operationResults, error) {
	var operationCounterField struct {
		Counter struct {
			operationResults
		} `json:"counter"`
	}
	err := json.NewDecoder(bytes.NewReader(operationMetadata)).Decode(&operationCounterField)
	if err != nil {
		return operationResults{}, err
	}
	return operationCounterField.Counter.operationResults, nil
}

type fnCommonVariables struct {
	client                dicomStoreClient
	resourcesErrorCount   beam.Counter
	resourcesSuccessCount beam.Counter
	latencyMs             beam.Distribution
}

func (fnc *fnCommonVariables) setup(namespace string) {
	if fnc.client == nil {
		fnc.client = newDicomStoreClient()
	}
	fnc.resourcesErrorCount = beam.NewCounter(namespace, errorCounterName)
	fnc.resourcesSuccessCount = beam.NewCounter(namespace, successCounterName)
	fnc.latencyMs = beam.NewDistribution(namespace, baseMetricPrefix+"latency_ms")
}
