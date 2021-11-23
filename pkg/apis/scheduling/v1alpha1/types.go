/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ElasticQuota sets elastic quota restrictions per namespace
type ElasticQuota struct {
	metav1.TypeMeta `json:",inline"`

	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ElasticQuotaSpec defines the Min and Max for Quota.
	// +optional
	Spec ElasticQuotaSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// ElasticQuotaStatus defines the observed use.
	// +optional
	Status ElasticQuotaStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ElasticQuotaSpec defines the Min and Max for Quota.
type ElasticQuotaSpec struct {
	// Min is the set of desired guaranteed limits for each named resource.
	// +optional
	Min v1.ResourceList `json:"min,omitempty" protobuf:"bytes,1,rep,name=min, casttype=ResourceList,castkey=ResourceName"`

	// Max is the set of desired max limits for each named resource. The usage of max is based on the resource configurations of
	// successfully scheduled pods.
	// +optional
	Max v1.ResourceList `json:"max,omitempty" protobuf:"bytes,2,rep,name=max, casttype=ResourceList,castkey=ResourceName"`
}

// ElasticQuotaStatus defines the observed use.
type ElasticQuotaStatus struct {
	// Used is the current observed total usage of the resource in the namespace.
	// +optional
	Used v1.ResourceList `json:"used,omitempty" protobuf:"bytes,1,rep,name=used,casttype=ResourceList,castkey=ResourceName"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ElasticQuotaList is a list of ElasticQuota items.
type ElasticQuotaList struct {
	metav1.TypeMeta `json:",inline"`

	// Standard list metadata.
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items is a list of ElasticQuota objects.
	Items []ElasticQuota `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PodGroupPhase is the phase of a pod group at the current time.
type PodGroupPhase string

// These are the valid phase of podGroups.
const (
	// PodGroupPending means the pod group has been accepted by the system, but scheduler can not allocate
	// enough resources to it.
	PodGroupPending PodGroupPhase = "Pending"

	// PodGroupRunning means `spec.minMember` pods of PodGroups has been in running phase.
	PodGroupRunning PodGroupPhase = "Running"

	// PodGroupPreScheduling means all of pods has been are waiting to be scheduled, enqueue waitingPod
	PodGroupPreScheduling PodGroupPhase = "PreScheduling"

	// PodGroupScheduling means some of pods has been scheduling in running phase but have not reach the `spec.
	// minMember` pods of PodGroups.
	PodGroupScheduling PodGroupPhase = "Scheduling"

	// PodGroupScheduled means `spec.minMember` pods of PodGroups have been scheduled finished and pods have been in running
	// phase.
	PodGroupScheduled PodGroupPhase = "Scheduled"

	// PodGroupUnknown means part of `spec.minMember` pods are running but the other part can not
	// be scheduled, e.g. not enough resource; scheduler will wait for related controller to recover it.
	PodGroupUnknown PodGroupPhase = "Unknown"

	// PodGroupFinished means all of `spec.minMember` pods are successfully.
	PodGroupFinished PodGroupPhase = "Finished"

	// PodGroupFailed means at least one of `spec.minMember` pods is failed.
	PodGroupFailed PodGroupPhase = "Failed"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroup is a collection of Pod; used for batch workload.
type PodGroup struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of the pod group.
	// +optional
	Spec PodGroupSpec `json:"spec,omitempty"`

	// Status represents the current information about a pod group.
	// This data may not be up to date.
	// +optional
	Status PodGroupStatus `json:"status,omitempty"`
}

// PodGroupSpec represents the template of a pod group.
type PodGroupSpec struct {
	// MinMember defines the minimal number of members/tasks to run the pod group;
	// if there's not enough resources to start all tasks, the scheduler
	// will not start anyone.
	MinMember int32 `json:"minMember,omitempty"`

	// MinResources defines the minimal resource of members/tasks to run the pod group;
	// if there's not enough resources to start all tasks, the scheduler
	// will not start anyone.
	MinResources *v1.ResourceList `json:"minResources,omitempty"`

	// ScheduleTimeoutSeconds defines the maximal time of members/tasks to wait before run the pod group;
	ScheduleTimeoutSeconds *int32 `json:"scheduleTimeoutSeconds,omitempty"`
}

// PodGroupStatus represents the current state of a pod group.
type PodGroupStatus struct {
	// Current phase of PodGroup.
	Phase PodGroupPhase `json:"phase,omitempty"`

	// OccupiedBy marks the workload (e.g., deployment, statefulset) UID that occupy the podgroup.
	// It is empty if not initialized.
	OccupiedBy string `json:"occupiedBy,omitempty"`

	// The number of actively running pods.
	// +optional
	Scheduled int32 `json:"scheduled,omitempty"`

	// The number of actively running pods.
	// +optional
	Running int32 `json:"running,omitempty"`

	// The number of pods which reached phase Succeeded.
	// +optional
	Succeeded int32 `json:"succeeded,omitempty"`

	// The number of pods which reached phase Failed.
	// +optional
	Failed int32 `json:"failed,omitempty"`

	// ScheduleStartTime of the group
	ScheduleStartTime metav1.Time `json:"scheduleStartTime,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroupList is a collection of pod groups.
type PodGroupList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the list of PodGroup
	Items []PodGroup `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AppGroup is a collection of Pods belonging to the same application
type AppGroup struct {
	metav1.TypeMeta `json:",inline"`

	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// AppGroupSpec defines the Min and Max for Quota.
	// +optional
	Spec AppGroupSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// AppGroupStatus defines the observed use.
	// +optional
	Status AppGroupStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// AppGroupSpec represents the template of a app group.
type AppGroupSpec struct {
	// NumMembers defines the number of Pods belonging to the App Group
	NumMembers int32 `json:"numMembers,omitempty" protobuf:"bytes,1,opt,name=numMembers"`

	// The preferred Topology Sorting Algorithm
	TopologySortingAlgorithm string `json:"topologySortingAlgorithm,omitempty" protobuf:"bytes,2,opt,name=topologySortingAlgorithm"`

	// Pods defines the pods belonging to the group
	Pods AppGroupPodList `json:"pods,omitempty" protobuf:"bytes,3,rep,name=pods, casttype=AppGroupPodList"`
}

// AppGroupPod represents the number of Pods belonging to the App Group.
// +protobuf=true
type AppGroupPod struct {
	// Name of the Pod.
	PodName string `json:"podName,omitempty" protobuf:"bytes,1,opt,name=podName"`

	// Dependencies of the Pod.
	Dependencies DependenciesList `json:"dependencies,omitempty" protobuf:"bytes,2,rep,name=dependencies, casttype=DependenciesList"`
}

// AppGroupPodList contains an array of Pod objects.
// +protobuf=true
type AppGroupPodList []AppGroupPod

// DependenciesInfo contains information about one dependency.
// +protobuf=true
type DependenciesInfo struct {
	// Name of the Pod.
	PodName string `json:"podName,omitempty" protobuf:"bytes,1,opt,name=podName"`

	// MinBandwidth between pods
	// +optional
	MinBandwidth resource.Quantity `json:"minBandwidth,omitempty" protobuf:"bytes,2,opt,name=minBandwidth"`

	// Max Network Cost between pods
	// +optional
	MaxNetworkCost int64 `json:"maxNetworkCost,omitempty" protobuf:"bytes,3,opt,name=maxNetworkCost"`
}

// DependenciesList contains an array of ResourceInfo objects.
// +protobuf=true
type DependenciesList []DependenciesInfo

// AppGroupStatus represents the current state of a app group.
type AppGroupStatus struct {
	// The number of actively running pods.
	// +optional
	RunningPods int32 `json:"runningPods,omitempty" protobuf:"bytes,1,opt,name=runningPods"`

	// PodsScheduled defines pod allocations (Pod name, Pod id, hostname).
	// +optional
	PodsScheduled ScheduledList `json:"podsScheduled,omitempty" protobuf:"bytes,2,rep,name=podsScheduled,casttype=ScheduledList"`

	// ScheduleStartTime of the group
	ScheduleStartTime metav1.Time `json:"scheduleStartTime,omitempty" protobuf:"bytes,3,opt,name=scheduleStartTime"`

	// TopologyCalculationTime of the group
	TopologyCalculationTime metav1.Time `json:"topologyCalculationTime,omitempty" protobuf:"bytes,4,opt,name=topologyCalculationTime"`

	// Topology order for TopSort plugin (QueueSort)
	TopologyOrder TopologyList `json:"topologyOrder,omitempty" protobuf:"bytes,5,rep,name=topologyOrder,casttype=TopologyList"`
}

// AppGroupScheduled represents the Pod Affinities of a given Pod
// +protobuf=true
type ScheduledInfo struct {
	// Pod Name
	PodName string `json:"podName,omitempty" protobuf:"bytes,1,opt,name=podName"`

	// Replica ID
	ReplicaID string `json:"replicaID,omitempty" protobuf:"bytes,2,opt,name=replicaID"`

	// Pod Hostname
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,3,opt,name=hostname"`
}

// ScheduledList contains an array of Pod Affinities.
// +protobuf=true
type ScheduledList []ScheduledInfo

// AppGroupTopology represents the calculated order for the given AppGroup
// +protobuf=true
type TopologyInfo struct {
	PodName string `json:"podName,omitempty" protobuf:"bytes,1,opt,name=podName"`
	Index   int32  `json:"index,omitempty" protobuf:"bytes,2,opt,name=index"`
}

// TopologyList contains an array of Pod orders for TopologySorting algorithm.
// +protobuf=true
type TopologyList []TopologyInfo

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AppGroupList is a collection of app groups.
type AppGroupList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the list of AppGroup
	Items []AppGroup `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetworkTopology defines network costs in the cluster between regions and zones
type NetworkTopology struct {
	metav1.TypeMeta `json:",inline"`

	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// NetworkTopologySpec defines the Min and Max for Quota.
	// +optional
	Spec NetworkTopologySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// NetworkTopologyStatus defines the observed use.
	// +optional
	Status NetworkTopologyStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// NetworkTopologySpec represents the template of a NetworkTopology.
type NetworkTopologySpec struct {
	// The manual defined weights of the cluster
	Weights WeightList `json:"weights,omitempty" protobuf:"bytes,1,rep,name=weights,casttype=WeightList"`

	// ConfigmapName to be used for cost calculation
	ConfigmapName string `json:"configmapName,omitempty" protobuf:"bytes,2,opt,name=configmapName"`
}

// NetworkTopologyStatus represents the current state of a Network Topology.
type NetworkTopologyStatus struct {
	// The total number of nodes in the cluster
	NodeCount int64 `json:"nodeCount,omitempty" protobuf:"bytes,1,opt,name=nodeCount"`

	// The calculation time for the weights in the network topology CRD
	WeightCalculationTime metav1.Time `json:"weightCalculationTime,omitempty" protobuf:"bytes,2,opt,name=weightCalculationTime"`

	// The calculated weights in the topology.
	// +optional
	// Weights WeightList `json:"weightList,omitempty"`
}

// WeightList contains an array of WeightInfo objects.
// +protobuf=true
type WeightList []WeightInfo

// CostList contains an array of OriginInfo objects.
// +protobuf=true
type CostList []OriginInfo

// WeightInfo contains information about all network costs for a given algorithm.
// +protobuf=true
type WeightInfo struct {
	// Algorithm Name for network cost calculation (e.g., userDefined)
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	// Costs between regions
	RegionCostList CostList `json:"regionCostList,omitempty" protobuf:"bytes,2,rep,name=regionCostList,casttype=CostList"`

	// Costs between zones
	ZoneCostList CostList `json:"zoneCostList,omitempty" protobuf:"bytes,3,rep,name=zoneCostList,casttype=CostList"`
}

// OriginInfo contains information about network costs for a particular Origin.
// +protobuf=true
type OriginInfo struct {
	// Name of the origin (e.g., Region Name, Zone Name).
	Origin string `json:"origin,omitempty" protobuf:"bytes,1,opt,name=origin"`

	// Costs for the particular origin.
	Costs []CostInfo `json:"costs,omitempty" protobuf:"bytes,2,rep,name=costs,casttype=CostInfo"`
}

// CostInfo contains information about networkCosts.
// +protobuf=true
type CostInfo struct {
	// Name of the destination (e.g., Region Name, Zone Name).
	Destination string `json:"destination,omitempty" protobuf:"bytes,1,opt,name=destination"`

	// Bandwidth capacity between origin and destination.
	// +optional
	BandwidthCapacity resource.Quantity `json:"bandwidthCapacity,omitempty" protobuf:"bytes,2,opt,name=bandwidthCapacity"`

	// Bandwidth allocated between origin and destination.
	// +optional
	BandwidthAllocated resource.Quantity `json:"bandwidthAllocated,omitempty" protobuf:"bytes,3,opt,name=bandwidthAllocated"`

	// Network Cost between origin and destination (e.g., Dijkstra shortest path, etc)
	NetworkCost int64 `json:"networkCost,omitempty" protobuf:"bytes,4,opt,name=networkCost"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetworkTopologyList is a collection of netTopologies.
type NetworkTopologyList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the list of AppGroup
	Items []NetworkTopology `json:"items"`
}
