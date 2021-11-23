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

package networkmincost

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"math"
	pluginConfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	schedLister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"
	networkAwareUtil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
	"sort"
)

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name         = "NetworkMinCost"
	MaxCost      = 100
	SameHostname = 0
	SameZone     = 1
)

// NetworkMinCost : scheduler plugin
type NetworkMinCost struct {
	handle      framework.Handle
	agLister    *schedLister.AppGroupLister
	ntLister    *schedLister.NetworkTopologyLister
	namespaces  []string
	weightsName string
	ntName      string
}

// Name : returns name of the plugin.
func (pl *NetworkMinCost) Name() string {
	return Name
}

func getArgs(obj runtime.Object) (*pluginConfig.NetworkMinCostArgs, error) {
	NetworkMinCostArgs, ok := obj.(*pluginConfig.NetworkMinCostArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NetworkMinCost, got %T", obj)
	}

	return NetworkMinCostArgs, nil
}

// ScoreExtensions : an interface for Score extended functionality
func (pl *NetworkMinCost) ScoreExtensions() framework.ScoreExtensions {
	return pl
}

// New : create an instance of a NetworkMinCost plugin
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	klog.V(4).Infof("Creating new instance of the NetworkMinCost plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	agLister, err := networkAwareUtil.InitAppGroupInformer(&args.MasterOverride, &args.KubeConfigPath)
	if err != nil {
		return nil, err
	}

	ntLister, err := networkAwareUtil.InitNetworkTopologyInformer(&args.MasterOverride, &args.KubeConfigPath)
	if err != nil {
		return nil, err
	}

	pl := &NetworkMinCost{
		handle:      handle,
		agLister:    agLister,
		ntLister:    ntLister,
		namespaces:  args.Namespaces,
		weightsName: args.WeightsName,
		ntName:      args.NetworkTopologyName,
	}
	return pl, nil
}

// Score : evaluate score for a node
func (pl *NetworkMinCost) Score(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {

	klog.V(6).Infof("Calculating score for pod %q on node %q", pod.GetName(), nodeName)
	score := framework.MinNodeScore

	// Check if Pod belongs to an App Group
	agName := util.GetAppGroupLabel(pod)
	if len(agName) == 0 { // Score all nodes equally
		return score, framework.NewStatus(framework.Success, "Pod does not belong to an AppGroup: min score")
	}

	appGroup, err := findAppGroupNetworkMinCost(agName, pl)
	if err != nil {
		klog.ErrorS(err, "Error while returning AppGroup")
		return score, framework.NewStatus(framework.Error, "Error while returning AppGroup: min score")
	}

	networkTopology, err := findNetworkTopologyNetworkMinCost(pl.ntName, pl)
	if err != nil {
		klog.ErrorS(err, "Error while returning NetworkTopology")
		return score, framework.NewStatus(framework.Error, "Error while returning NetworkTopology: min score")
	}

	klog.V(6).Info("AppGroup CRD: ", appGroup.Name)
	klog.V(6).Info("Network Topology CRD: ", networkTopology.Name)

	// Check if pods already available
	if appGroup.Status.PodsScheduled == nil {
		return score, framework.NewStatus(framework.Success, "No Pods yet allocated for the AppGroup: min score")
	}

	// Check Dependencies of the given pod
	var dependencyList []v1alpha1.DependenciesInfo
	for _, p := range appGroup.Spec.Pods {
		if p.PodName == pod.Name {
			for _, dependency := range p.Dependencies {
				dependencyList = append(dependencyList, dependency)
			}
		}
	}

	// If the pod has no dependencies, return min score
	if dependencyList == nil {
		return score, framework.NewStatus(framework.Success, "The pod does not have dependencies: minimum score")
	}

	klog.V(6).Info("dependencyList: ", dependencyList)

	// Create map for cost / destinations. Search for costs faster...
	var costMap = make(map[networkAwareUtil.CostKey]int64)

	// Get NodeInfo from nodeName
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return score, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	// Retrieve Region and Zone from node
	region := networkAwareUtil.GetNodeRegion(nodeInfo.Node())
	zone := networkAwareUtil.GetNodeZone(nodeInfo.Node())

	klog.V(6).Info("Node Region: ", region)
	klog.V(6).Info("Node Zone: ", zone)

	if pl.weightsName == "UserDefined" { // Manual weights were selected
		for _, w := range networkTopology.Spec.Weights {
			// Sort Costs by origin, might not be sorted since were manually defined
			sort.Sort(networkAwareUtil.ByOrigin(w.RegionCostList))
			sort.Sort(networkAwareUtil.ByOrigin(w.ZoneCostList))
		}
	}

	seen := false
	for _, w := range networkTopology.Spec.Weights { // Check the weights List
		if w.Name == pl.weightsName { // If its the Preferred algorithm
			if region != "" { // Add Region Costs
				// Binary search through CostList: find the costs for the given Region
				costs := networkAwareUtil.FindOriginCosts(w.RegionCostList, region)

				// Add Region Costs
				for _, c := range costs {
					costMap[networkAwareUtil.CostKey{ // Add the cost to the map
						Origin:      region,
						Destination: c.Destination}] = c.NetworkCost
				}
			}
			if zone != "" { // Add Zone Costs
				// Binary search through CostList: find the costs for the given Region
				costs := networkAwareUtil.FindOriginCosts(w.ZoneCostList, zone)

				// Add Zone Costs
				for _, c := range costs {
					costMap[networkAwareUtil.CostKey{ // Add the cost to the map
						Origin:      zone,
						Destination: c.Destination}] = c.NetworkCost
				}
			}
			seen = true
		} else if seen == true { // Costs are sorted by origin, thus stop here
			break
		}
	}

	klog.V(6).Info("costMap: ", costMap)

	var cost int64 = 0
	// calculate accumulated shortest path
	for _, podAllocated := range appGroup.Status.PodsScheduled { // For each pod already allocated
		for _, d := range dependencyList { // For each pod dependency
			if podAllocated.PodName == d.PodName { // If the pod allocated is an established dependency
				if podAllocated.Hostname == nodeName { // If the Pod hostname is the node being scored
					cost += SameHostname
				} else { // If Nodes are not the same
					// Get NodeInfo from pod Hostname
					podHostname, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(podAllocated.Hostname)
					if err != nil {
						return score, framework.NewStatus(framework.Error, fmt.Sprintf("getting pod hostname %q from Snapshot: %v", podHostname, err))
					}
					// Get zone and region from Pod Hostname
					regionPodHostname := networkAwareUtil.GetNodeRegion(podHostname.Node())
					zonePodHostname := networkAwareUtil.GetNodeZone(podHostname.Node())

					if regionPodHostname == "" && zonePodHostname == "" { // Node has no zone and region defined
						cost += MaxCost
					} else if region == regionPodHostname { // If Nodes belong to the same region
						if zone == zonePodHostname { // If Nodes belong to the same zone
							cost += SameZone
						} else { // belong to a different zone
							value, ok := costMap[networkAwareUtil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
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
						value, ok := costMap[networkAwareUtil.CostKey{ // Retrieve the cost from the map (origin: region, destination: pod regionHostname)
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

	// Return Accumulated Cost as score
	score = cost

	klog.V(6).Infof("pod:%s; node:%s; finalScore=%d", pod.GetName(), nodeName, score)
	return score, framework.NewStatus(framework.Success, "Accumulated cost added as score, normalization ensures lower costs are favored")
}

// NormalizeScore : normalize scores
func (pl *NetworkMinCost) NormalizeScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// Lower scores correspond to lower latency
	// Get Min and Max Scores to normalize between framework.MaxNodeScore and framework.MinNodeScore
	minCost, maxCost := GetMinMaxScores(scores)

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
	return nil
}

// MinMax : get min and max scores from NodeScoreList
func GetMinMaxScores(scores framework.NodeScoreList) (int64, int64) {
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

func findAppGroupNetworkMinCost(agName string, n *NetworkMinCost) (*v1alpha1.AppGroup, error) {
	klog.V(5).Infof("namespaces: %s", n.namespaces)
	var err error
	for _, namespace := range n.namespaces {
		klog.V(5).Infof("ag.lister: %v", n.agLister)

		// AppGroup could not be placed in several namespaces simultaneously
		lister := n.agLister
		appGroup, err := (*lister).AppGroups(namespace).Get(agName)
		if err != nil {
			klog.V(5).Infof("Cannot get AppGroup from AppGroupNamespaceLister: %v", err)
			continue
		}
		if appGroup != nil {
			return appGroup, nil
		}
	}
	return nil, err
}

func findNetworkTopologyNetworkMinCost(ntName string, n *NetworkMinCost) (*v1alpha1.NetworkTopology, error) {
	klog.V(5).Infof("namespaces: %s", n.namespaces)
	var err error
	for _, namespace := range n.namespaces {
		klog.V(5).Infof("nt.lister: %v", n.ntLister)
		// NetworkTopology could not be placed in several namespaces simultaneously
		lister := n.ntLister
		networkTopology, err := (*lister).NetworkTopologies(namespace).Get(ntName)
		if err != nil {
			klog.V(5).Infof("Cannot get networkTopology from networkTopologyNamespaceLister: %v", err)
			continue
		}
		if networkTopology != nil {
			return networkTopology, nil
		}
	}
	return nil, err
}
