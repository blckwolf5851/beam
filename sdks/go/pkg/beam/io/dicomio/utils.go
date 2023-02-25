package dicomio

import (
	"strings"
)

func inferResourceScope(dicomWebPath string) resourceScope {
	scope := resourceScopeNone
	if strings.Contains(dicomWebPath, "/studies/") {
		scope = resourceScopeStudies
	}
	if strings.Contains(dicomWebPath, "/series/") {
		scope = resourceScopeSeries
	}
	if strings.Contains(dicomWebPath, "/instances/") {
		scope = resourceScopeInstances
	}
	return scope
}

func inferMetadataOnly(dicomWebPath string) bool {
	return strings.HasSuffix(dicomWebPath, "/metadata")
}
