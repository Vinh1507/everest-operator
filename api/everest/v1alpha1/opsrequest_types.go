package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OpsRequestPhase represents the phase of the OpsRequest
type OpsRequestPhase string

const (
	OpsRequestPhasePending    OpsRequestPhase = "Pending"
	OpsRequestPhaseProcessing OpsRequestPhase = "Processing"
	OpsRequestPhaseSucceeded  OpsRequestPhase = "Succeeded"
	OpsRequestPhaseFailed     OpsRequestPhase = "Failed"
	OpsRequestPhaseCanceled   OpsRequestPhase = "Canceled"
)

// ActionType represents the type of operation to perform
type ActionType string

const (
	ActionStart             ActionType = "Start"
	ActionStop              ActionType = "Stop"
	ActionRestart           ActionType = "Restart"
	ActionSwitchover        ActionType = "Switchover"
	ActionVerticalScaling   ActionType = "VerticalScaling"
	ActionHorizontalScaling ActionType = "HorizontalScaling"
	ActionVolumeExpansion   ActionType = "VolumeExpansion"
	ActionReconfiguring     ActionType = "Reconfiguring"
	ActionUpgrade           ActionType = "Upgrade"
	ActionBackup            ActionType = "Backup"
	ActionRestore           ActionType = "Restore"
	ActionExpose            ActionType = "Expose"
	ActionCustom            ActionType = "Custom"
)

// OpsRequestSpec defines the desired operation
type OpsRequestSpec struct {
	// GroupRef references the target group
	// +kubebuilder:validation:Required
	GroupRef GroupReference `json:"groupRef"`

	// clusters is a list of database clusters that belong to this group.
	// When any cluster in the group reconciles, all other clusters will be triggered to reconcile as well.
	// +optional
	// +listType=map
	// +listMapKey=name
	Clusters []ClusterReference `json:"clusters"`

	// Type specifies the type of operation
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=Start;Stop;Restart;Switchover;VerticalScaling;HorizontalScaling;VolumeExpansion;Reconfiguring;Upgrade;Backup;Restore;Expose;Custom
	Type ActionType `json:"type"`

	// Params contains operation-specific parameters
	// +optional
	Params map[string]string `json:"params,omitempty"`

	// Timeout specifies the maximum time to wait for the operation to complete
	// Format: 1h, 30m, 10s
	// +optional
	// +kubebuilder:default="30m"
	Timeout string `json:"timeout,omitempty"`

	// Force indicates whether to force the operation even if it might cause downtime
	// +optional
	Force bool `json:"force,omitempty"`
}

// GroupReference contains information to identify a group
type GroupReference struct {
	// Name of the group
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace of the group (defaults to OpsRequest namespace)
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// OpsRequestClusterStatus defines the status of an operation on a specific cluster
type OpsRequestClusterStatus struct {
	// ClusterName is the name of the cluster
	ClusterName string `json:"clusterName"`

	// ClusterNamespace is the namespace of the cluster
	ClusterNamespace string `json:"clusterNamespace"`

	// Phase represents the current phase of the operation on this cluster
	Phase OpsRequestPhase `json:"phase,omitempty"`

	// Message provides human-readable information
	Message string `json:"message,omitempty"`

	// Reason provides a machine-readable explanation
	Reason string `json:"reason,omitempty"`

	// Progress indicates the operation progress (0-100)
	Progress int32 `json:"progress,omitempty"`

	// Output contains operation-specific output data
	Output map[string]string `json:"output,omitempty"`
}

// OpsRequestStatus defines the observed state
type OpsRequestStatus struct {
	// Phase represents the current phase of the operation
	// +kubebuilder:validation:Enum=Pending;Processing;Succeeded;Failed;Canceled
	Phase OpsRequestPhase `json:"phase,omitempty"`

	// ClusterStatus contains status for each cluster
	// +optional
	ClusterStatus []OpsRequestClusterStatus `json:"clusterStatus,omitempty"`

	// Conditions represent the latest available observations
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// Message provides human-readable information
	Message string `json:"message,omitempty"`

	// Reason provides a machine-readable explanation
	Reason string `json:"reason,omitempty"`

	// ObservedGeneration reflects the generation observed by the controller
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// StartTime is when the operation started
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the operation completed
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Progress indicates the operation progress (0-100)
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Progress int32 `json:"progress,omitempty"`

	// Output contains operation-specific output data
	// +optional
	Output map[string]string `json:"output,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ops
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Engine",type=string,JSONPath=`.spec.engineRef.name`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Progress",type=integer,JSONPath=`.status.progress`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// OpsRequest is the Schema for the opsrequests API
type OpsRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OpsRequestSpec   `json:"spec,omitempty"`
	Status OpsRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// OpsRequestList contains a list of OpsRequest
type OpsRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OpsRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OpsRequest{}, &OpsRequestList{})
}

// IsComplete returns true if the operation has completed (succeeded, failed, or canceled)
func (r *OpsRequest) IsComplete() bool {
	return r.Status.Phase == OpsRequestPhaseSucceeded ||
		r.Status.Phase == OpsRequestPhaseFailed ||
		r.Status.Phase == OpsRequestPhaseCanceled
}

// IsProcessing returns true if the operation is currently being processed
func (r *OpsRequest) IsProcessing() bool {
	return r.Status.Phase == OpsRequestPhaseProcessing
}

// GetTargetNamespace returns the namespace of the target cluster
func (r *OpsRequest) GetTargetNamespace() string {
	if r.Spec.GroupRef.Namespace != "" {
		return r.Spec.GroupRef.Namespace
	}
	return r.Namespace
}
