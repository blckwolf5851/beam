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
	"log"
	"net/http"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"golang.org/x/exp/slices"
)

type resourceScope string

const (
	resourceScopeNone      resourceScope = "none"
	resourceScopeStudies                 = "studies"
	resourceScopeSeries                  = "series"
	resourceScopeInstances               = "instances"
)

type readConfig struct {
	resourceScope
	metadataOnly bool
}

func init() {
	register.DoFn5x0[context.Context, []byte, []byte, func(string), func(string)]((*readDicomFn)(nil))
	register.Emitter1[string]()
}

type readDicomFn struct {
	readConfig
	fnCommonVariables
}

func (fn readDicomFn) String() string {
	return "readStudiesMetadataFn"
}

func (fn *readDicomFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
}

func (fn *readDicomFn) ProcessElement(ctx context.Context, parent, dicomWebPath []byte, emitResource, emitDeadLetter func(string)) {
	response, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (*http.Response, error) {
		expectedResourceScopes := []resourceScope{resourceScopeStudies, resourceScopeSeries, resourceScopeInstances}
		if !slices.Contains(expectedResourceScopes, fn.readConfig.resourceScope) {
			fn.readConfig.resourceScope = inferResourceScope(string(dicomWebPath))
			fn.readConfig.metadataOnly = inferMetadataOnly(string(dicomWebPath))
		}
		switch fn.readConfig.resourceScope {
		case resourceScopeStudies:
			if fn.readConfig.metadataOnly {
				return fn.client.readStudiesMetadata(parent, dicomWebPath)
			} else {
				return fn.client.readStudy(parent, dicomWebPath)
			}
		case resourceScopeSeries:
			if fn.readConfig.metadataOnly {
				return fn.client.readSeriesMetadata(parent, dicomWebPath)
			} else {
				return fn.client.readSeries(parent, dicomWebPath)
			}
		case resourceScopeInstances:
			if fn.readConfig.metadataOnly {
				return fn.client.readInstancesMetadata(parent, dicomWebPath)
			} else {
				return fn.client.readInstance(parent, dicomWebPath)
			}
		}

		if fn.readConfig.metadataOnly {
			return fn.client.readStudiesMetadata(parent, dicomWebPath)
		} else {
			return fn.client.readStudy(parent, dicomWebPath)
		}
	})
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "read resource request returned error on input: [%v, %v]", parent, dicomWebPath).Error())
		return
	}

	body, err := extractBodyFrom(response)
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "could not extract body from read resource [%v, %v] response", parent, dicomWebPath).Error())
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
func Read(s beam.Scope, cfg readConfig, resourcePaths beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("dicomio.Read")
	expectedResourceScopes := []resourceScope{resourceScopeStudies, resourceScopeSeries, resourceScopeInstances}
	if !slices.Contains(expectedResourceScopes, cfg.resourceScope) {
		log.Println("Read config is not provided, [resourceScope, metadataOnly] will be inferred.")
	}

	return read(s, cfg, resourcePaths, nil)
}

// This is useful as an entry point for testing because we can provide a fake DICOM store client.
func read(s beam.Scope, cfg readConfig, resourcePaths beam.PCollection, client dicomStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &readDicomFn{readConfig: cfg, fnCommonVariables: fnCommonVariables{client: client}}, resourcePaths)
}
