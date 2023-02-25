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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"google.golang.org/api/healthcare/v1"
	"google.golang.org/api/option"
)

const (
	baseMetricPrefix = "dicomio/"
	userAgent        = "apache-beam-io-google-cloud-platform-healthcare/" + core.SdkVersion
)

type dicomStoreClient interface {
	readResource(resourcePath string) (*http.Response, error)
}

type dicomStoreClientImpl struct {
	dicomService *healthcare.ProjectsLocationsDatasetsDicomStoresService
}

func newDicomStoreClient() *dicomStoreClientImpl {
	healthcareService, err := healthcare.NewService(context.Background(), option.WithUserAgent(userAgent))
	if err != nil {
		panic("Failed to initialize Google Cloud Healthcare Service. Reason: " + err.Error())
	}
	return &dicomStoreClientImpl{dicomService: healthcare.NewProjectsLocationsDatasetsDicomStoresService(healthcareService)}
}

func (c *dicomStoreClientImpl) readResource(resourcePath string) (*http.Response, error) {
	return c.dicomService.Read(resourcePath).Do()
}
