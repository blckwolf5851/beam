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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"golang.org/x/exp/slices"
)

type ResourceScope string

const (
	ResourceScopeNone      ResourceScope = "none" // Will infer scope if not set
	ResourceScopeStudies                 = "studies"
	ResourceScopeSeries                  = "series"
	ResourceScopeInstances               = "instances"
)

func init() {
	// register.DoFn4x0[context.Context, ReadDicomQuery, func(string), func(string)]((*readDicomFn)(nil))
	register.DoFn4x0[context.Context, ReadDicomMetadataQuery, func(string), func(string)]((*readDicomMetadataFn)(nil))
	register.Emitter1[string]()
}

/* Read Dicom files Fn*/
type ReadDicomQuery struct {
	parent        []byte
	dicomWebPath  []byte
	resourceScope ResourceScope `default:"none"`
}

type readDicomFn struct {
	fnCommonVariables
}

func (fn readDicomFn) String() string {
	return "readDicomFn"
}

func (fn *readDicomFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
}

func (fn *readDicomFn) ProcessElement(ctx context.Context, query ReadDicomQuery, emitResource, emitDeadLetter func(string)) {
	response, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (*http.Response, error) {
		expectedResourceScopes := []ResourceScope{ResourceScopeStudies, ResourceScopeSeries, ResourceScopeInstances}
		if !slices.Contains(expectedResourceScopes, query.resourceScope) {
			query.resourceScope = inferResourceScope(string(query.dicomWebPath))
		}
		switch query.resourceScope {
		case ResourceScopeStudies:
			return fn.client.readStudy(query.parent, query.dicomWebPath)
		case ResourceScopeSeries:
			return fn.client.readSeries(query.parent, query.dicomWebPath)
		case ResourceScopeInstances:
			return fn.client.readInstance(query.parent, query.dicomWebPath)
		}
		return nil, errors.Errorf("Invalid resource scope [%v], expected one of [resourceScopeStudies, resourceScopeSeries, resourceScopeInstances]", query.resourceScope)
	})
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "read resource request returned error on input: [%v, %v]", query.parent, query.dicomWebPath).Error())
		return
	}

	body, err := extractBodyFrom(response)
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "could not extract body from read resource [%v, %v] response", query.parent, query.dicomWebPath).Error())
		return
	}

	fn.resourcesSuccessCount.Inc(ctx, 1)
	emitResource(body)
}

// Read fetches resources from Google Cloud Healthcare DICOM stores based on the
// resource path. It consumes a KV<[]byte, []byte> of notifications from the
// DICOM store of resource paths, and fetches the actual resource object on the
// path in the notification. It outputs two PCollection<string>. The first
// contains the fetched object as a JSON-encoded string, and the second is a
// dead-letter with an error message, in case the object failed to be fetched.
// See: https://cloud.google.com/healthcare-api/docs/how-tos/dicom-resources#getting_a_dicom_resource.
func ReadDicom(s beam.Scope, readDicomQueries beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("dicomio.Read")
	return readDicom(s, readDicomQueries, nil)
}

// This is useful as an entry point for testing because we can provide a fake DICOM store client.
func readDicom(s beam.Scope, readDicomQueries beam.PCollection, client dicomStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &readDicomFn{fnCommonVariables: fnCommonVariables{client: client}}, readDicomQueries)
}

/* Read Dicom metadata Fn*/
type ReadDicomMetadataQuery struct {
	parent        []byte
	dicomWebPath  []byte
	resourceScope ResourceScope `default:"none"`
}
type readDicomMetadataFn struct {
	fnCommonVariables
}

func (fn readDicomMetadataFn) String() string {
	return "readDicomMetadataFn"
}

func (fn *readDicomMetadataFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
}

func (fn *readDicomMetadataFn) ProcessElement(ctx context.Context, query ReadDicomMetadataQuery, emitResource, emitDeadLetter func(string)) {
	response, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (*http.Response, error) {
		expectedResourceScopes := []ResourceScope{ResourceScopeStudies, ResourceScopeSeries, ResourceScopeInstances}
		if !slices.Contains(expectedResourceScopes, query.resourceScope) {
			query.resourceScope = inferResourceScope(string(query.dicomWebPath))
		}
		switch query.resourceScope {
		case ResourceScopeStudies:
			return fn.client.readStudyMetadata(query.parent, query.dicomWebPath)
		case ResourceScopeSeries:
			return fn.client.readSeriesMetadata(query.parent, query.dicomWebPath)
		case ResourceScopeInstances:
			return fn.client.readInstanceMetadata(query.parent, query.dicomWebPath)
		}
		return nil, errors.Errorf("Invalid resource scope [%v], expected one of [resourceScopeStudies, resourceScopeSeries, resourceScopeInstances]", query.resourceScope)
	})
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "read resource request returned error on input: [%v, %v]", query.parent, query.dicomWebPath).Error())
		return
	}

	body, err := extractBodyFrom(response)
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "could not extract body from read resource [%v, %v] response", query.parent, query.dicomWebPath).Error())
		return
	}

	fn.resourcesSuccessCount.Inc(ctx, 1)
	emitResource(body)
}

// Read fetches resources from Google Cloud Healthcare DICOM stores based on the
// resource path. It consumes a KV<[]byte, []byte> of notifications from the
// DICOM store of resource paths, and fetches the actual resource object on the
// path in the notification. It outputs two PCollection<string>. The first
// contains the fetched object as a JSON-encoded string, and the second is a
// dead-letter with an error message, in case the object failed to be fetched.
// See: https://cloud.google.com/healthcare-api/docs/how-tos/dicom-resources#getting_a_dicom_resource.
func ReadDicomMetadata(s beam.Scope, readDicomMetadataQueries beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("dicomio.Read")
	return readDicomMetadata(s, readDicomMetadataQueries, nil)
}

// This is useful as an entry point for testing because we can provide a fake DICOM store client.
func readDicomMetadata(s beam.Scope, readDicomMetadataQueries beam.PCollection, client dicomStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &readDicomMetadataFn{fnCommonVariables: fnCommonVariables{client: client}}, readDicomMetadataQueries)
}
