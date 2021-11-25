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

package nodenetworkcostfit

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	pluginConfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	schedLister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"
	networkAwareUtil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
	"sort"
)

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name = "NodeNetworkCostFit"
)

// NodeMaxNetworkCostFit : scheduler plugin
type NodeNetworkCostFit struct {
	handle      framework.Handle
	agLister    *schedLister.AppGroupLister
	ntLister    *schedLister.NetworkTopologyLister
	namespaces  []string
	weightsName string
	ntName      string
}

// Name : returns name of the plugin.
func (pl *NodeNetworkCostFit) Name() string {
	return Name
}

func getArgs(obj runtime.Object) (*pluginConfig.NodeNetworkCostFitArgs, error) {
	NodeNetworkCostFitArgs, ok := obj.(*pluginConfig.NodeNetworkCostFitArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NodeNetworkCostFitArgs, got %T", obj)
	}

	return NodeNetworkCostFitArgs, nil
}

// New : create an instance of a NetworkMinCost plugin
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	klog.V(4).Infof("Creating new instance of the NodeNetworkCostFit plugin")

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

	pl := &NodeNetworkCostFit{
		handle:      handle,
		agLister:    agLister,
		ntLister:    ntLister,
		namespaces:  args.Namespaces,
		weightsName: args.WeightsName,
		ntName:      args.NetworkTopologyName,
	}
	return pl, nil
}

// Filter : evaluate if node can respect maxNetworkCost requirements
func (pl *NodeNetworkCostFit) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {

	klog.V(6).Infof("Filter: pod %q on node %q", pod.GetName(), nodeInfo.Node().Name)
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	// Check if Pod belongs to an App Group
	agName := util.GetAppGroupLabel(pod)
	if len(agName) == 0 { // Return
		return nil
	}

	appGroup, err := findAppGroupCheckAppGroupRequirements(agName, pl)
	if err != nil {
		klog.ErrorS(err, "Error while returning AppGroup")
		return nil
	}

	networkTopology, err := findNetworkTopologyCheckAppGroupRequirements(pl.ntName, pl)
	if err != nil {
		klog.ErrorS(err, "Error while returning NetworkTopology")
		return nil
	}

	klog.Info("AppGroup CRD: ", appGroup.Name)
	klog.Info("Network Topology CRD: ", networkTopology.Name)

	// Check if pods already available, otherwise return
	if appGroup.Status.PodsScheduled == nil {
		return nil
	}

	// Check Dependencies of the given pod
	var dependencyList []v1alpha1.DependenciesInfo
	for _, p := range appGroup.Spec.Pods {
		if p.PodName == pod.GetName() {
			for _, dependency := range p.Dependencies {
				dependencyList = append(dependencyList, dependency)
			}
		}
	}

	// If the pod has no dependencies, return
	if dependencyList == nil {
		return nil
	}

	klog.Info("dependencyList: ", dependencyList)

	// Check if bandwidth and network requirements can be met
	region := networkAwareUtil.GetNodeRegion(nodeInfo.Node())
	zone := networkAwareUtil.GetNodeZone(nodeInfo.Node())

	if pl.weightsName == "UserDefined" { // Manual weights were selected
		for _, w := range networkTopology.Spec.Weights {
			// Sort Costs by origin, might not be sorted since were manually defined
			sort.Sort(networkAwareUtil.ByOrigin(w.RegionCostList))
			sort.Sort(networkAwareUtil.ByOrigin(w.ZoneCostList))
		}
	}

	// Create map for cost / destinations. Search for requirements faster...
	var costMap = make(map[networkAwareUtil.CostKey]int64)

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

	var numOK int64 = 0
	var numNotOK int64 = 0
	// check if maxNetworkCost fits
	for _, podAllocated := range appGroup.Status.PodsScheduled { // For each pod already allocated
		for _, d := range dependencyList { // For each pod dependency
			if podAllocated.PodName == d.PodName { // If the pod allocated is an established dependency
				if podAllocated.Hostname == nodeInfo.Node().Name { // If the Pod hostname is the node being filtered, requirements are checked via extended resources
					numOK += 1
				} else { // If Nodes are not the same
					// Get NodeInfo from pod Hostname
					podHostname, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(podAllocated.Hostname)
					if err != nil {
						return framework.NewStatus(framework.Error, "pod hostname not found")
					}

					// Get zone and region from Pod Hostname
					regionPodHostname := networkAwareUtil.GetNodeRegion(podHostname.Node())
					zonePodHostname := networkAwareUtil.GetNodeZone(podHostname.Node())

					if regionPodHostname == "" && zonePodHostname == "" { // Node has no zone and region defined
						numNotOK += 1
					} else if region == regionPodHostname { // If Nodes belong to the same region
						if zone == zonePodHostname { // If Nodes belong to the same zone
							numOK += 1
						} else { // belong to a different zone, check maxNetworkCost
							cost, costOK := costMap[networkAwareUtil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
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
						cost, costOK := costMap[networkAwareUtil.CostKey{ // Retrieve the cost from the map (origin: zone, destination: pod zoneHostname)
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
	klog.Infof("NumNotOk: %v / numOK: %v ", numNotOK, numOK)

	if numNotOK > numOK {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("Node %v does not meet several network requirements from Pod dependencies: OK: %v NotOK: %v", nodeInfo.Node().Name, numOK, numNotOK))
	}
	return nil
}

func findAppGroupCheckAppGroupRequirements(agName string, n *NodeNetworkCostFit) (*v1alpha1.AppGroup, error) {
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

func findNetworkTopologyCheckAppGroupRequirements(ntName string, n *NodeNetworkCostFit) (*v1alpha1.NetworkTopology, error) {
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
