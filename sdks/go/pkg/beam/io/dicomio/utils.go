package dicomio

import (
	"strings"
)

func inferResourceScope(dicomWebPath string) ResourceScope {
	scope := ResourceScopeNone
	if strings.Contains(dicomWebPath, "/studies/") {
		scope = ResourceScopeStudies
	}
	if strings.Contains(dicomWebPath, "/series/") {
		scope = ResourceScopeSeries
	}
	if strings.Contains(dicomWebPath, "/instances/") {
		scope = ResourceScopeInstances
	}
	return scope
}

func inferMetadataOnly(dicomWebPath string) bool {
	return strings.HasSuffix(dicomWebPath, "/metadata")
}
