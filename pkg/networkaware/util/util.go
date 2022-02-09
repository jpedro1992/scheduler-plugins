/*
Copyright 2021 The Kubernetes Authors.

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

package util

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	clientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	informers "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	schedlister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
	"sort"
)

// CostKey : represents a key for a map concerning network costs (origin / destination)
type CostKey struct {
	Origin      string
	Destination string
}

// Scheduled List : contains several ScheduledInfo
type ScheduledList []ScheduledInfo

// Scheduled Info : contains information regarding a workload deployment
type ScheduledInfo struct {
	// Workload Name
	Name string

	// Workload Selector
	Selector string

	// Workload Replica ID
	ReplicaID string

	// Workload's Hostname
	Hostname string
}

// GetNodeRegion : retrieve the node's topology region
func GetNodeRegion(node *v1.Node) string {
	labels := node.Labels
	if labels == nil {
		return ""
	}

	region, _ := labels[v1.LabelTopologyRegion]
	if region == "" {
		return ""
	}

	return region
}

// GetNodeZone : retrieve the node's topology zone
func GetNodeZone(node *v1.Node) string {
	labels := node.Labels
	if labels == nil {
		return ""
	}

	zone, _ := labels[v1.LabelTopologyZone]
	if zone == "" {
		return ""
	}

	return zone
}

// Sort TopologyList by TopologyKey
type ByTopologyKey v1alpha1.TopologyList

func (s ByTopologyKey) Len() int {
	return len(s)
}

func (s ByTopologyKey) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByTopologyKey) Less(i, j int) bool {
	return s[i].TopologyKey < s[j].TopologyKey
}

// Sort OriginList by Origin
type ByOrigin v1alpha1.OriginList

func (s ByOrigin) Len() int {
	return len(s)
}

func (s ByOrigin) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByOrigin) Less(i, j int) bool {
	return s[i].Origin < s[j].Origin
}

// Sort CostList by Destination
type ByDestination v1alpha1.CostList

func (s ByDestination) Len() int {
	return len(s)
}

func (s ByDestination) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByDestination) Less(i, j int) bool {
	return s[i].Destination < s[j].Destination
}

// FindPodOrder : returns the pod's order index based on its AppGroup
func FindPodOrder(t v1alpha1.AppGroupTopologyList, selector string) int32 {
	low := 0
	high := len(t) - 1

	for low <= high {
		mid := (low + high) / 2
		if t[mid].Workload.Selector == selector {
			return t[mid].Index // Return the index
		} else if t[mid].Workload.Selector < selector {
			low = mid + 1
		} else if t[mid].Workload.Selector > selector {
			high = mid - 1
		}
	}
	return -1
}

// FindOriginCosts : returns the CostList related to a certain Origin
func FindOriginCosts(originList v1alpha1.OriginList, origin string) v1alpha1.CostList {
	low := 0
	high := len(originList) - 1

	for low <= high {
		mid := (low + high) / 2
		if originList[mid].Origin == origin {
			return originList[mid].CostList // Return the CostList
		} else if originList[mid].Origin < origin {
			low = mid + 1
		} else if originList[mid].Origin > origin {
			high = mid - 1
		}
	}
	// Costs were not found
	return v1alpha1.CostList{}
}

// FindTopologyKey : returns the OriginList related to a certain TopologyList
func FindTopologyKey(topologyList v1alpha1.TopologyList, key v1alpha1.TopologyKey) v1alpha1.OriginList {
	low := 0
	high := len(topologyList) - 1

	for low <= high {
		mid := (low + high) / 2
		if topologyList[mid].TopologyKey == key {
			return topologyList[mid].OriginList // Return the OriginList
		} else if topologyList[mid].TopologyKey < key {
			low = mid + 1
		} else if topologyList[mid].TopologyKey > key {
			high = mid - 1
		}
	}
	// Topology Key was not found
	return v1alpha1.OriginList{}
}

// assignedPod : selects pods that are assigned (scheduled and running).
func AssignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// InitAppGroupInformer : initializes a AppGroup Informer
func InitAppGroupInformer(masterOverride, kubeConfigPath *string) (*schedlister.AppGroupLister, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(*masterOverride, *kubeConfigPath)
	if err != nil {
		klog.Errorf("Cannot create kubeconfig based on: %s, %s, %v", *masterOverride, *kubeConfigPath, err)
		return nil, err
	}

	agClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		klog.Errorf("Cannot create clientset for AppGroup Informer: %s, %s", kubeConfig, err)
		return nil, err
	}

	agInformerFactory := informers.NewSharedInformerFactory(agClient, 0)
	agInformer := agInformerFactory.Scheduling().V1alpha1().AppGroups()
	appGroupLister := agInformer.Lister()

	klog.V(5).Infof("start appGroupInformer")
	ctx := context.Background()
	agInformerFactory.Start(ctx.Done())
	agInformerFactory.WaitForCacheSync(ctx.Done())

	return &appGroupLister, nil
}

// InitNetworkTopologyInformer : initializes a NetworkTopology Informer
func InitNetworkTopologyInformer(masterOverride, kubeConfigPath *string) (*schedlister.NetworkTopologyLister, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(*masterOverride, *kubeConfigPath)
	if err != nil {
		klog.Errorf("Cannot create kubeconfig based on: %s, %s, %v", *masterOverride, *kubeConfigPath, err)
		return nil, err
	}

	ntClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		klog.Errorf("Cannot create clientset for NetworkTopology Informer: %s, %s", kubeConfig, err)
		return nil, err
	}

	ntInformerFactory := informers.NewSharedInformerFactory(ntClient, 0)
	ntInformer := ntInformerFactory.Scheduling().V1alpha1().NetworkTopologies()
	appGroupLister := ntInformer.Lister()

	klog.V(5).Infof("start networkTopology Informer")
	ctx := context.Background()
	ntInformerFactory.Start(ctx.Done())
	ntInformerFactory.WaitForCacheSync(ctx.Done())

	return &appGroupLister, nil
}

// getScheduledList : get Pods already scheduled in the cluster for that specific AppGroup
func GetScheduledList(pods []*v1.Pod) ScheduledList {
	// scheduledList: Deployment name, replicaID, hostname
	scheduledList := ScheduledList{}

	for _, p := range pods {
		if AssignedPod(p) {
			scheduledInfo := ScheduledInfo{
				Name:      p.Name,
				Selector:  util.GetPodAppGroupSelector(p),
				ReplicaID: string(p.GetUID()),
				Hostname:  p.Spec.NodeName,
			}
			scheduledList = append(scheduledList, scheduledInfo)
		}
	}
	return scheduledList
}

// GetDependencyList gets workload dependencies established in the AppGroup CR
func GetDependencyList(pod *v1.Pod, ag *v1alpha1.AppGroup) []v1alpha1.DependenciesInfo {

	// Check Dependencies of the given pod
	var dependencyList []v1alpha1.DependenciesInfo

	// Get Labels of the given pod
	podLabels := pod.GetLabels()

	for _, w := range ag.Spec.Workloads {
		if w.Workload.Selector == podLabels[v1alpha1.AppGroupSelectorLabel] {
			for _, dependency := range w.Dependencies {
				dependencyList = append(dependencyList, dependency)
			}
		}
	}
	klog.V(6).Info("dependencyList: ", dependencyList)

	// Return the dependencyList
	return dependencyList
}

// SortNetworkTopologyCosts : sort costs if manual weights were selected
func SortNetworkTopologyCosts(weightsName string, networkTopology *v1alpha1.NetworkTopology) {
	if weightsName != v1alpha1.NetworkTopologyNetperfCosts { // Manual weights were selected
		for _, w := range networkTopology.Spec.Weights {
			// Sort Costs by TopologyKey, might not be sorted since were manually defined
			sort.Sort(ByTopologyKey(w.TopologyList))
		}
	}
	return
}
