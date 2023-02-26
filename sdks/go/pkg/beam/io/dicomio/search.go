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
)

func init() {
	register.DoFn4x0[context.Context, SearchDicomQuery, func(string, string), func(string)]((*searchStudiesFn)(nil))
	register.Emitter1[string]()
	register.Emitter2[string, string]()
}

// SearchDicomQuery concisely represents a DICOM search query, and should be used as
// input type for the Search transform.
type SearchDicomQuery struct {
	// An identifier for the query, if there is source information to propagate
	// through the pipeline.
	Identifier string
	// Query parameters for a DICOM search request as per https://www.hl7.org/dicom/search.html.
	Parameters map[string]string
}

type responseLinkFields struct {
	Relation string `json:"relation"`
	Url      string `json:"url"`
}

type searchStudiesFn struct {
	fnCommonVariables
	// Path to DICOM store where search will be performed.
	Parent       string
	DicomWebPath string
}

func (fn searchStudiesFn) String() string {
	return "searchStudiesFn"
}

func (fn *searchStudiesFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
}

func (fn *searchStudiesFn) ProcessElement(ctx context.Context, query SearchDicomQuery, emitFoundResources func(string, string), emitDeadLetter func(string)) {
	response, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() (*http.Response, error) {
		return fn.client.searchStudies(fn.Parent, fn.DicomWebPath, query.Parameters)
	})
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "error occurred while performing search for query: [%v]", query).Error())
		return
	}

	body, err := extractBodyFrom(response)
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "could not extract body from search study resource [%v, %v] response", fn.Parent, fn.DicomWebPath).Error())
		return
	}

	fn.resourcesSuccessCount.Inc(ctx, 1)
	emitFoundResources(query.Identifier, body)
}

// Search transform searches for resources in a Google Cloud Healthcare DICOM
// store based on input queries. It consumes a PCollection<dicomio.SearchQuery>
// and outputs two PCollections, the first a tuple (identifier, searchResults)
// where `identifier` is the SearchQuery identifier field and `searchResults` is
// a slice of all found resources as a JSON-encoded string. The second
// PCollection is a dead-letter for the input queries that caused errors when
// performing the search.
// See: https://cloud.google.com/healthcare-api/docs/how-tos/dicom-search
func SearchStudies(s beam.Scope, parent, dicomWebPath string, searchQueries beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("dicomio.Search")
	return searchStudies(s, parent, dicomWebPath, searchQueries, nil)
}

// This is useful as an entry point for testing because we can provide a fake DICOM store client.
func searchStudies(s beam.Scope, parent, dicomWebPath string, searchQueries beam.PCollection, client dicomStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &searchStudiesFn{fnCommonVariables: fnCommonVariables{client: client}, Parent: parent, DicomWebPath: dicomWebPath}, searchQueries)
}
