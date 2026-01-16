// everest-operator
// Copyright (C) 2022 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterGroupSpec defines the desired state of ClusterGroup
type ClusterGroupSpec struct {
	// clusters is a list of database clusters that belong to this group.
	// When any cluster in the group reconciles, all other clusters will be triggered to reconcile as well.
	// +optional
	// +listType=map
	// +listMapKey=name
	Clusters []ClusterReference `json:"clusters"`

	// paused indicates whether the cluster group reconciliation is paused.
	// When true, the controller will not reconcile this ClusterGroup or its member clusters.
	// +optional
	// +kubebuilder:default=false
	Paused bool `json:"paused,omitempty"`
}

// ClusterReference defines a reference to a database cluster
type ClusterReference struct {
	// name is the name of the database cluster
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// type is the type of database cluster (e.g., "pxc", "psmdb", "postgresql", "starrocks")
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=pxc;psmdb;postgresql;clickhouse;starrocks;cassandra
	Type string `json:"type"`
}

// EndpointStatus represents an endpoint exposed by the ClusterGroup
type EndpointStatus struct {
	// name of the endpoint (e.g. primary, readonly, proxy, metrics)
	Name string `json:"name"`

	// endpoint address (e.g. host:port or URL)
	Endpoint string `json:"endpoint"`
}

// ClusterGroupStatus defines the observed state of ClusterGroup
type ClusterGroupStatus struct {
	// observedGeneration is the generation observed by the controller.
	// It is used to determine if the spec has changed and reconciliation is needed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase represents the current phase of the ClusterGroup.
	// Possible values: "Pending", "Reconciling", "Ready", "PartiallyReady", "Failed"
	// +optional
	// +kubebuilder:validation:Enum=Pending;Reconciling;Ready;PartiallyReady;Failed
	Phase string `json:"phase,omitempty"`

	// totalClusters is the total number of clusters in the group
	// +optional
	TotalClusters int `json:"totalClusters,omitempty"`

	// readyClusters is the number of clusters that are ready
	// +optional
	ReadyClusters int `json:"readyClusters,omitempty"`

	// clusters contains the status of each cluster in the group
	// +listType=map
	// +listMapKey=name
	// +optional
	Clusters []ClusterStatus `json:"clusters,omitempty"`

	// clusterGenerations tracks the generation of each cluster
	// Key is cluster name, value is the generation
	// +optional
	ClusterGenerations map[string]int64 `json:"clusterGenerations,omitempty"`

	// lastReconcileTime is the last time the group was reconciled
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// endpoints exposed by the cluster group
	// +listType=map
	// +listMapKey=name
	// +optional
	Endpoints []EndpointStatus `json:"endpoints,omitempty"`

	// conditions represent the current state of the ClusterGroup resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ClusterStatus represents the status of a single cluster in the group
type ClusterStatus struct {
	// name is the name of the cluster
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// type is the type of the cluster
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// Database status
	// +optional
	DatabaseStatus DatabaseClusterStatus `json:"databaseStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cg
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Clusters",type=integer,JSONPath=`.status.totalClusters`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyClusters`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterGroup is the Schema for the clustergroups API.
// It represents a group of database clusters that should be reconciled together.
type ClusterGroup struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of ClusterGroup
	// +kubebuilder:validation:Required
	Spec ClusterGroupSpec `json:"spec"`

	// status defines the observed state of ClusterGroup
	// +optional
	Status ClusterGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterGroupList contains a list of ClusterGroup
type ClusterGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterGroup{}, &ClusterGroupList{})
}

// Helper methods

// IsReady returns true if all clusters in the group are ready
func (cg *ClusterGroup) IsReady() bool {
	return cg.Status.Phase == "Ready" &&
		cg.Status.TotalClusters > 0 &&
		cg.Status.ReadyClusters == cg.Status.TotalClusters
}

// IsReconciling returns true if the group is currently reconciling
func (cg *ClusterGroup) IsReconciling() bool {
	return cg.Status.Phase == "Reconciling"
}

// NeedsReconcile returns true if the spec has changed and reconciliation is needed
func (cg *ClusterGroup) NeedsReconcile() bool {
	return cg.Status.ObservedGeneration != cg.Generation
}

// GetClusterByName returns the cluster reference by name
func (cg *ClusterGroup) GetClusterByName(name string) *ClusterReference {
	for i := range cg.Spec.Clusters {
		if cg.Spec.Clusters[i].Name == name {
			return &cg.Spec.Clusters[i]
		}
	}
	return nil
}

// GetClusterStatus returns the status of a cluster by name
func (cg *ClusterGroup) GetClusterStatus(name string) *ClusterStatus {
	for i := range cg.Status.Clusters {
		if cg.Status.Clusters[i].Name == name {
			return &cg.Status.Clusters[i]
		}
	}
	return nil
}
