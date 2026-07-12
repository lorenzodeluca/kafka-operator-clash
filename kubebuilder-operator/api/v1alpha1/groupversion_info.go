/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha1

// Package v1alpha1 contains API Schema definitions for the messaging v1alpha1 API group.
// +kubebuilder:object:generate=true
// +groupName=messaging.kb.lorenzodeluca.it

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// SchemeGroupVersion is group version used to register these objects.
	SchemeGroupVersion = schema.GroupVersion{Group: "messaging.kb.lorenzodeluca.it", Version: "v1alpha1"}

	// GroupVersion is an alias for SchemeGroupVersion, for backward compatibility.
	GroupVersion = SchemeGroupVersion

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(func(scheme *runtime.Scheme) error {
		metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
		return nil
	})

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
