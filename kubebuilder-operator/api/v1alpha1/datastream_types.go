package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// DataStreamSpec defines the desired state of DataStream
type DataStreamSpec struct {
	// TopicName is the name of the Kafka topic to manage.
	// +kubebuilder:validation:MinLength=1
	TopicName string `json:"topicName"`

	// Partitions defines the total partition count for the topic.
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum=1
	Partitions int32 `json:"partitions,omitempty"`

	// ReplicationFactor defines the replication factor.
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum=1
	ReplicationFactor int16 `json:"replicationFactor,omitempty"`
}

// DataStreamStatus defines the observed state of DataStream
type DataStreamStatus struct {
	// Conditions track state transitions and lifecycle status.
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// TopicCreated confirms whether the Kafka topic exists on the broker.
	TopicCreated bool `json:"topicCreated,omitempty"`

	// ConfigMapRef holds the name of the generated connection details ConfigMap.
	ConfigMapRef string `json:"configMapRef,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DataStream is the Schema for the datastreams API
type DataStream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DataStreamSpec   `json:"spec"`
	Status DataStreamStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DataStreamList contains a list of DataStream
type DataStreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DataStream `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &DataStream{}, &DataStreamList{})
		metav1.AddToGroupVersion(s, GroupVersion)
		return nil
	})
}
