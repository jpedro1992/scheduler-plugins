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

package networkoverhead

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"math"
	pluginconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	schedlister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"
	networkawareutil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
	"sort"
)

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name         = "NetworkOverhead"

	// MaxCost : MaxCost used in the NetworkTopology for costs between origins and destinations
	MaxCost      = 100

	// SameHostname : If pods belong to the same host, then consider cost as 0
	SameHostname = 0

	// SameZone : If pods belong to hosts in the same zone, then consider cost as 1
	SameZone     = 1
)

// NetworkOverhead : Filter and Score nodes based on Pod's AppGroup requirements: MaxNetworkCosts requirements among Pods with dependencies
type NetworkOverhead struct {
	handle      framework.Handle
	podLister   corelisters.PodLister
	agLister    *schedlister.AppGroupLister
	ntLister    *schedlister.NetworkTopologyLister
	namespaces  []string
	weightsName string
	ntName      string
}

// Name : returns name of the plugin.
func (no *NetworkOverhead) Name() string {
	return Name
}

func getArgs(obj runtime.Object) (*pluginconfig.NetworkOverheadArgs, error) {
	NetworkOverheadArgs, ok := obj.(*pluginconfig.NetworkOverheadArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NetworkOverhead, got %T", obj)
	}

	return NetworkOverheadArgs, nil
}

// ScoreExtensions : an interface for Score extended functionality
func (no *NetworkOverhead) ScoreExtensions() framework.ScoreExtensions {
	return no
}

// New : create an instance of a NetworkMinCost plugin
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	klog.V(4).Infof("Creating new instance of the NetworkOverhead plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	agLister, err := networkawareutil.InitAppGroupInformer(&args.MasterOverride, &args.KubeConfigPath)
	if err != nil {
		return nil, err
	}

	ntLister, err := networkawareutil.InitNetworkTopologyInformer(&args.MasterOverride, &args.KubeConfigPath)
	if err != nil {
		return nil, err
	}

	no := &NetworkOverhead{
		handle:      handle,
		podLister:   handle.SharedInformerFactory().Core().V1().Pods().Lister(),
		agLister:    agLister,
		ntLister:    ntLister,
		namespaces:  args.Namespaces,
		weightsName: args.WeightsName,
		ntName:      args.NetworkTopologyName,
	}
	return no, nil
}

// Filter : evaluate if node can respect maxNetworkCost requirements
func (no *NetworkOverhead) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {

	klog.V(4).Infof("Filter: pod %q on node %q", pod.GetName(), nodeInfo.Node().Name)
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	// Check if Pod belongs to an App Group
	agName := util.GetPodAppGroupLabel(pod)
	if len(agName) == 0 { // Return
		return nil
	}

	appGroup, err := findAppGroupNetworkOverhead(agName, no)
	if err != nil {
		klog.ErrorS(err, "Error while returning AppGroup")
		return nil
	}

	networkTopology, err := findNetworkTopologyNetworkOverhead(no)
	if err != nil {
		klog.ErrorS(err, "Error while returning NetworkTopology")
		return nil
	}

	klog.V(6).Info("AppGroup CR: ", appGroup.Name)
	klog.V(6).Info("Network Topology CR: ", networkTopology.Name)

	// Get Dependencies of the given pod
	dependencyList := getDependencyList(pod, appGroup)

	// If the pod has no dependencies, return
	if dependencyList == nil {
		return nil
	}

	// Get pods from lister
	selector := labels.Set(map[string]string{v1alpha1.AppGroupLabel: agName}).AsSelector()
	pods, err := no.podLister.List(selector)
	if err != nil {
		klog.ErrorS(err, "Error while returning pods from appGroup, return")
		return nil
	}

	if pods == nil{
		klog.ErrorS(err, "No pods yet allocated, return")
		return nil
	}

	// Pods already scheduled: Deployment name, replicaID, hostname
	scheduledList := getScheduledList(pods)

	klog.V(6).Info("scheduledList: ", scheduledList)

	// Check if pods already available
	if scheduledList == nil{
		klog.ErrorS(err, "Scheduled list is empty, return")
		return nil
	}

	// Check if bandwidth and network requirements can be met
	region := networkawareutil.GetNodeRegion(nodeInfo.Node())
	zone := networkawareutil.GetNodeZone(nodeInfo.Node())

	klog.V(6).Info("Node Region: ", region)
	klog.V(6).Info("Node Zone: ", zone)

	// Sort Costs if manual weights were selected
	sortNetworkTopologyCosts(no, networkTopology)

	// Create map for cost / destinations. Search for requirements faster...
	var costMap = make(map[networkawareutil.CostKey]int64)

	// Populate Costmap
	populateCostMap(no, costMap, networkTopology, region, zone)

	klog.V(6).Info("costMap: ", costMap)

	var numOK int64
	var numNotOK int64
	var ok error

	numOK, numNotOK, ok = checkMaxNetworkCostRequirements(scheduledList, dependencyList, nodeInfo, region, zone, costMap, no)
	if ok != nil {
		return framework.NewStatus(framework.Error, "pod hostname not found")
	}

	klog.V(4).Infof("NumNotOk: %v / numOK: %v ", numNotOK, numOK)

	if numNotOK > numOK {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("Node %v does not meet several network requirements from Workload dependencies: OK: %v NotOK: %v", nodeInfo.Node().Name, numOK, numNotOK))
	}
	return nil
}

// Score : evaluate score for a node
func (no *NetworkOverhead) Score(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {

	klog.V(4).Infof("Calculating score for pod %q on node %q", pod.GetName(), nodeName)
	score := framework.MinNodeScore

	// Check if Pod belongs to an App Group
	agName := util.GetPodAppGroupLabel(pod)
	if len(agName) == 0 { // Score all nodes equally
		return score, framework.NewStatus(framework.Success, "Pod does not belong to an AppGroup: min score")
	}

	appGroup, err := findAppGroupNetworkOverhead(agName, no)
	if err != nil {
		klog.ErrorS(err, "Error while returning AppGroup")
		return score, framework.NewStatus(framework.Error, "Error while returning AppGroup: min score")
	}

	networkTopology, err := findNetworkTopologyNetworkOverhead(no)
	if err != nil {
		klog.ErrorS(err, "Error while returning NetworkTopology")
		return score, framework.NewStatus(framework.Error, "Error while returning NetworkTopology: min score")
	}

	klog.V(6).Info("AppGroup CR: ", appGroup.Name)
	klog.V(6).Info("Network Topology CR: ", networkTopology.Name)

	/// Get Dependencies of the given pod
	dependencyList := getDependencyList(pod, appGroup)

	// If the pod has no dependencies, return min score
	if dependencyList == nil {
		return score, framework.NewStatus(framework.Success, "The pod does not have dependencies: minimum score")
	}

	// Get pods from lister
	selector := labels.Set(map[string]string{v1alpha1.AppGroupLabel: agName}).AsSelector()
	pods, err := no.podLister.List(selector)
	if err != nil {
		return score, framework.NewStatus(framework.Error, fmt.Sprintf("getting pods from lister: %v", err))
	}

	if pods == nil{
		return score, framework.NewStatus(framework.Success, "No pods yet allocated: minimum score")
	}

	// Pods already scheduled: Deployment name, replicaID, hostname
	scheduledList := getScheduledList(pods)

	klog.V(6).Info("scheduledList: ", scheduledList)

	// Check if pods already available
	if scheduledList == nil{ //appGroup.Status.PodsScheduled == nil {
		return score, framework.NewStatus(framework.Success, "No Pods yet allocated for the AppGroup: min score")
	}

	// Get NodeInfo from nodeName
	nodeInfo, err := no.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return score, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	// Retrieve Region and Zone from node
	region := networkawareutil.GetNodeRegion(nodeInfo.Node())
	zone := networkawareutil.GetNodeZone(nodeInfo.Node())

	klog.V(6).Info("Node Region: ", region)
	klog.V(6).Info("Node Zone: ", zone)

	// Sort Costs if manual weights were selected
	sortNetworkTopologyCosts(no, networkTopology)

	// Create map for cost / destinations. Search for requirements faster...
	var costMap = make(map[networkawareutil.CostKey]int64)

	// Populate Costmap
	populateCostMap(no, costMap, networkTopology, region, zone)

	klog.V(6).Info("costMap: ", costMap)

	var cost int64 = 0
	var ok error

	cost, ok = getAccumulatedCost(scheduledList, dependencyList, nodeName, region, zone, costMap, no)
	if ok != nil {
		return score, framework.NewStatus(framework.Error, fmt.Sprintf("getting pod hostname from Snapshot: %v", ok))
	}

	// Return Accumulated Cost as score
	score = cost

	klog.V(4).Infof("pod:%s; node:%s; finalScore=%d", pod.GetName(), nodeName, score)
	return score, framework.NewStatus(framework.Success, "Accumulated cost added as score, normalization ensures lower costs are favored")
}

// NormalizeScore : normalize scores
func (no *NetworkOverhead) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// Lower scores correspond to lower latency
	klog.V(4).Infof("Scores before normalization: %s", scores)

	// Get Min and Max Scores to normalize between framework.MaxNodeScore and framework.MinNodeScore
	minCost, maxCost := getMinMaxScores(scores)

	// If all nodes were given the minimum score, return
	if minCost == 0 && maxCost == 0 {
		return nil
	}

	var normCost float64
	for i := range scores {
		if maxCost != minCost { // If max != min
			// node_normalized_cost = MAX_SCORE * ( ( nodeScore - minCost) / (maxCost - minCost)
			// nodeScore = MAX_SCORE - node_normalized_cost
			normCost = float64(framework.MaxNodeScore) * float64(scores[i].Score-minCost) / float64(maxCost-minCost)
			scores[i].Score = framework.MaxNodeScore - int64(normCost)
		} else { // If maxCost = minCost, avoid division by 0
			normCost = float64(scores[i].Score - minCost)
			scores[i].Score = framework.MaxNodeScore - int64(normCost)
		}
	}
	klog.V(4).Infof("Scores after normalization: %s", scores)
	return nil
}

// MinMax : get min and max scores from NodeScoreList
func getMinMaxScores(scores framework.NodeScoreList) (int64, int64) {
	var max int64 = math.MinInt64 // Set to min value
	var min int64 = math.MaxInt64 // Set to max value

	for _, nodeScore := range scores {
		if nodeScore.Score > max {
			max = nodeScore.Score
		}
		if nodeScore.Score < min {
			min = nodeScore.Score
		}
	}

	return min, max
}

// getScheduledList : get Pods already scheduled in the cluster for that specific AppGroup
func getScheduledList(pods []*v1.Pod) networkawareutil.ScheduledList {
	// scheduledList: Deployment name, replicaID, hostname
	scheduledList := networkawareutil.ScheduledList{}

	for _, p := range pods {
		if networkawareutil.AssignedPod(p) {
			scheduledInfo := networkawareutil.ScheduledInfo{
				Name:   	p.Name,
				Selector: 	util.GetPodAppGroupSelector(p),
				ReplicaID: 	string(p.GetUID()),
				Hostname:  	p.Spec.NodeName,
			}
			scheduledList = append(scheduledList, scheduledInfo)
		}
	}
	return scheduledList
}

// getDependencyList : get workload dependencies established in the AppGroup CR
func getDependencyList(pod *v1.Pod, ag *v1alpha1.AppGroup) []v1alpha1.DependenciesInfo {

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

// sortNetworkTopologyCosts : sort costs if manual weights were selected
func sortNetworkTopologyCosts(no *NetworkOverhead, networkTopology *v1alpha1.NetworkTopology) {
	if no.weightsName != v1alpha1.NetworkTopologyNetperfCosts { // Manual weights were selected
		for _, w := range networkTopology.Spec.Weights {
			// Sort Costs by TopologyKey, might not be sorted since were manually defined
			sort.Sort(networkawareutil.ByTopologyKey(w.TopologyList))
		}
	}
	return
}

// populateCostMap : Populates costMap based on the node being filtered/scored
func populateCostMap(no *NetworkOverhead, costMap map[networkawareutil.CostKey]int64, networkTopology *v1alpha1.NetworkTopology, region string, zone string) {
	seen := false
	for _, w := range networkTopology.Spec.Weights { // Check the weights List
		if w.Name == no.weightsName { // If its the Preferred algorithm
			if region != "" { // Add Region Costs
				// Binary search through CostList: find the Topology Key for region
				topologyList := networkawareutil.FindTopologyKey(w.TopologyList, v1alpha1.NetworkTopologyRegion)

				if no.weightsName != v1alpha1.NetworkTopologyNetperfCosts {
					// Sort Costs by origin, might not be sorted since were manually defined
					sort.Sort(networkawareutil.ByOrigin(topologyList))
				}

				// Binary search through TopologyList: find the costs for the given Region
				costs := networkawareutil.FindOriginCosts(topologyList, region)

				// Add Region Costs
				for _, c := range costs {
					costMap[networkawareutil.CostKey{ // Add the cost to the map
						Origin:      region,
						Destination: c.Destination}] = c.NetworkCost
				}
			}
			if zone != "" { // Add Zone Costs
				// Binary search through CostList: find the Topology Key for zone
				topologyList := networkawareutil.FindTopologyKey(w.TopologyList, v1alpha1.NetworkTopologyZone)

				if no.weightsName != v1alpha1.NetworkTopologyNetperfCosts {
					// Sort Costs by origin, might not be sorted since were manually defined
					sort.Sort(networkawareutil.ByOrigin(topologyList))
				}

				// Binary search through TopologyList: find the costs for the given Region
				costs := networkawareutil.FindOriginCosts(topologyList, zone)

				// Add Zone Costs
				for _, c := range costs {
					costMap[networkawareutil.CostKey{ // Add the cost to the map
						Origin:      zone,
						Destination: c.Destination}] = c.NetworkCost
				}
			}
			seen = true
		} else if seen == true { // Costs are sorted by origin, thus stop here
			break
		}
	}
}

// checkMaxNetworkCostRequirements : verifies the number of met and unmet dependencies based on the pod being filtered
func checkMaxNetworkCostRequirements(scheduledList networkawareutil.ScheduledList, dependencyList []v1alpha1.DependenciesInfo, nodeInfo *framework.NodeInfo, region string,
	zone string, costMap map[networkawareutil.CostKey]int64, no *NetworkOverhead) (int64, int64, error) {
	var numOK int64 = 0
	var numNotOK int64 = 0

	// check if maxNetworkCost fits
	for _, podAllocated := range scheduledList { // For each pod already allocated
		if podAllocated.Hostname != "" { // if already updated by the controller
			for _, d := range dependencyList { // For each pod dependency
				if podAllocated.Selector == d.Workload.Selector { // If the pod allocated is an established dependency
					if podAllocated.Hostname == nodeInfo.Node().Name { // If the Pod hostname is the node being filtered, requirements are checked via extended resources
						numOK += 1
					} else { // If Nodes are not the same
						// Get NodeInfo from pod Hostname
						podHostname, err := no.handle.SnapshotSharedLister().NodeInfos().Get(podAllocated.Hostname)
						if err != nil {
							klog.ErrorS(nil, "getting pod hostname %q from Snapshot: %v", podHostname, err)
							return numOK, numNotOK, err
						}

						// Get zone and region from Pod Hostname
						regionPodHostname := networkawareutil.GetNodeRegion(podHostname.Node())
						zonePodHostname := networkawareutil.GetNodeZone(podHostname.Node())

						if regionPodHostname == "" && zonePodHostname == "" { // Node has no zone and region defined
							numNotOK += 1
						} else if region == regionPodHostname { // If Nodes belong to the same region
							if zone == zonePodHostname { // If Nodes belong to the same zone
								numOK += 1
							} else { // belong to a different zone, check maxNetworkCost
								cost, costOK := costMap[networkawareutil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
									Origin:      zone, // Time Complexity: O(1)
									Destination: zonePodHostname,
								}]
								if costOK {
									if cost <= d.MaxNetworkCost {
										numOK += 1
									} else {
										numNotOK += 1
									}
								}
							}
						} else { // belong to a different region
							cost, costOK := costMap[networkawareutil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
								Origin:      region, // Time Complexity: O(1)
								Destination: regionPodHostname,
							}]
							if costOK {
								if cost <= d.MaxNetworkCost {
									numOK += 1
								} else {
									numNotOK += 1
								}
							}
						}
					}
				}
			}
		}
	}
	return numOK, numNotOK, nil
}

// getAccumulatedCost : calculate the accumulated cost based on the Pod's dependencies
func getAccumulatedCost(scheduledList networkawareutil.ScheduledList, dependencyList []v1alpha1.DependenciesInfo, nodeName string, region string,
	zone string, costMap map[networkawareutil.CostKey]int64, no *NetworkOverhead) (int64, error) {
	var cost int64 = 0

	// calculate accumulated shortest path
	for _, podAllocated := range scheduledList { // For each pod already allocated
		if podAllocated.Hostname != "" { // if already updated by the controller
			for _, d := range dependencyList { // For each pod dependency
				if podAllocated.Selector == d.Workload.Selector { // If the pod allocated is an established dependency
					if podAllocated.Hostname == nodeName { // If the Pod hostname is the node being scored
						cost += SameHostname
					} else { // If Nodes are not the same
						// Get NodeInfo from pod Hostname
						podHostname, err := no.handle.SnapshotSharedLister().NodeInfos().Get(podAllocated.Hostname)
						if err != nil {
							klog.ErrorS(nil, "getting pod hostname %q from Snapshot: %v", podHostname, err)
							return cost, err
						}
						// Get zone and region from Pod Hostname
						regionPodHostname := networkawareutil.GetNodeRegion(podHostname.Node())
						zonePodHostname := networkawareutil.GetNodeZone(podHostname.Node())

						if regionPodHostname == "" && zonePodHostname == "" { // Node has no zone and region defined
							cost += MaxCost
						} else if region == regionPodHostname { // If Nodes belong to the same region
							if zone == zonePodHostname { // If Nodes belong to the same zone
								cost += SameZone
							} else { // belong to a different zone
								value, ok := costMap[networkawareutil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
									Origin:      zone, // Time Complexity: O(1)
									Destination: zonePodHostname,
								}]
								if ok {
									cost += value // Add the cost to the sum
								} else {
									cost += MaxCost
								}
							}
						} else { // belong to a different region
							value, ok := costMap[networkawareutil.CostKey{ // Retrieve the cost from the map (origin: region, destination: pod regionHostname)
								Origin:      region, // Time Complexity: O(1)
								Destination: regionPodHostname,
							}]
							if ok {
								cost += value // Add the cost to the sum
							} else {
								cost += MaxCost
							}
						}
					}
				}
			}
		}
	}
	return cost, nil
}

func findAppGroupNetworkOverhead(agName string, no *NetworkOverhead) (*v1alpha1.AppGroup, error) {
	klog.V(6).Infof("namespaces: %s", no.namespaces)
	var err error
	for _, namespace := range no.namespaces {
		klog.V(6).Infof("ag.lister: %v", no.agLister)

		// AppGroup could not be placed in several namespaces simultaneously
		lister := no.agLister
		appGroup, err := (*lister).AppGroups(namespace).Get(agName)
		if err != nil {
			klog.V(4).Infof("Cannot get AppGroup from AppGroupNamespaceLister: %v", err)
			continue
		}
		if appGroup != nil {
			return appGroup, nil
		}
	}
	return nil, err
}

func findNetworkTopologyNetworkOverhead(no *NetworkOverhead) (*v1alpha1.NetworkTopology, error) {
	klog.V(6).Infof("namespaces: %s", no.namespaces)
	var err error
	for _, namespace := range no.namespaces {
		klog.V(6).Infof("nt.lister: %v", no.ntLister)
		// NetworkTopology could not be placed in several namespaces simultaneously
		lister := no.ntLister
		networkTopology, err := (*lister).NetworkTopologies(namespace).Get(no.ntName)
		if err != nil {
			klog.V(4).Infof("Cannot get networkTopology from networkTopologyNamespaceLister: %v", err)
			continue
		}
		if networkTopology != nil {
			return networkTopology, nil
		}
	}
	return nil, err
}
