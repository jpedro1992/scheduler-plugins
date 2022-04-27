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

package controller

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corelister "k8s.io/client-go/listers/core/v1"
	"reflect"
	networkAwareUtil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	v1alpha1 "sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	schedclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions/scheduling/v1alpha1"
	schedlister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
	"sort"
)

// NetworkTopologyController is a controller that process Network Topology using provided Handler interface
type NetworkTopologyController struct {
	eventRecorder         record.EventRecorder
	ntQueue               workqueue.RateLimitingInterface
	ntLister              schedlister.NetworkTopologyLister
	agLister        	  schedlister.AppGroupLister
	nodeLister            corelister.NodeLister
	podLister       	  corelister.PodLister
	configmapLister       corelister.ConfigMapLister
	ntListerSynced        cache.InformerSynced
	nodeListerSynced      cache.InformerSynced
	podListerSynced		  cache.InformerSynced
	configmapListerSynced cache.InformerSynced
	ntClient              schedclientset.Interface
	lock                  sync.RWMutex // lock for network graph and cost calculation.
	nodeCount             int64        // Number of nodes in the cluster.
	regionGraph           *util.Graph  // Network Graph for region cost calculation.
	zoneGraph             *util.Graph  // Network Graph for zone cost calculation.
	nodeGraph             *util.Graph  // Network Graph for node cost calculation.
	topologyMap           map[util.TopologyKey]bool
	ZoneMap               map[util.ZoneKey]bool
	BandwidthAllocatable  map[networkAwareUtil.CostKey]resource.Quantity
}

// NewNetworkTopologyController returns a new *NewNetworkTopologyController
func NewNetworkTopologyController(client kubernetes.Interface,
	ntInformer schedinformer.NetworkTopologyInformer,
	agInformer schedinformer.AppGroupInformer,
	nodeInformer coreinformer.NodeInformer,
	podInformer coreinformer.PodInformer,
	comfigmapInformer coreinformer.ConfigMapInformer,
	ntClient schedclientset.Interface) *NetworkTopologyController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})

	ctrl := &NetworkTopologyController{
		eventRecorder: broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "NetworkTopologyController"}),
		ntQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "NetworkTopology"),
	}

	// NetworkTopology Informer
	klog.V(5).InfoS("Setting up NetworkTopology event handlers")
	ntInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.ntAdded,
		UpdateFunc: ctrl.ntUpdated,
		DeleteFunc: ctrl.ntDeleted,
	})

	// Node Informer
	klog.V(5).InfoS("Setting up Node event handlers")
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.nodeAdded,
		UpdateFunc: ctrl.nodeUpdated,
		DeleteFunc: ctrl.nodeDeleted,
	})

	// Pod Informer
	klog.V(5).InfoS("Setting up Pod event handlers")
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.podAdded,
		UpdateFunc: ctrl.podUpdated,
		DeleteFunc: ctrl.podDeleted,
	})

	ctrl.ntLister = ntInformer.Lister()
	ctrl.agLister = agInformer.Lister()
	ctrl.nodeLister = nodeInformer.Lister()
	ctrl.podLister = podInformer.Lister()
	ctrl.configmapLister = comfigmapInformer.Lister()
	ctrl.ntListerSynced = ntInformer.Informer().HasSynced
	ctrl.nodeListerSynced = nodeInformer.Informer().HasSynced
	ctrl.podListerSynced = podInformer.Informer().HasSynced
	ctrl.configmapListerSynced = comfigmapInformer.Informer().HasSynced
	ctrl.ntClient = ntClient

	ctrl.regionGraph = util.NewGraph()
	ctrl.zoneGraph = util.NewGraph()
	ctrl.nodeGraph = util.NewGraph()
	ctrl.topologyMap = make(map[util.TopologyKey]bool)
	ctrl.ZoneMap = make(map[util.ZoneKey]bool)
	//ctrl.BandwidthCapacity = make(map[networkAwareUtil.CostKey]resource.Quantity)
	ctrl.BandwidthAllocatable = make(map[networkAwareUtil.CostKey]resource.Quantity)

	return ctrl
}

// Run starts listening on channel events
func (ctrl *NetworkTopologyController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.ntQueue.ShutDown()

	klog.InfoS("Starting Network Topology controller")
	defer klog.InfoS("Shutting Network Topology controller")

	if !cache.WaitForCacheSync(stopCh, ctrl.ntListerSynced, ctrl.nodeListerSynced) {
		klog.Error("Cannot sync caches")
		return
	}

	klog.InfoS("Network Topology sync finished")

	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.worker, time.Second, stopCh)
	}
	<-stopCh
}

// ntAdded reacts to a NT creation
func (ctrl *NetworkTopologyController) ntAdded(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	klog.V(5).InfoS("Enqueue Network Topology ", "network Topology", key)
	ctrl.ntQueue.Add(key)
}

// ntUpdated reacts to a NT update
func (ctrl *NetworkTopologyController) ntUpdated(old, new interface{}) {
	ctrl.ntAdded(new)
}

// ntDeleted reacts to a NetworkTopology deletion
func (ctrl *NetworkTopologyController) ntDeleted(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.V(5).InfoS("Enqueue deleted network topology key", "networkTopology", key)
	ctrl.ntQueue.AddRateLimited(key)
}

// nodeAdded reacts to a node addition
func (ctrl *NetworkTopologyController) nodeAdded(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.Error("unexpected object type in node added")
		return
	}

	region := networkAwareUtil.GetNodeRegion(node)
	zone := networkAwareUtil.GetNodeZone(node)

	func() {
		ctrl.lock.Lock()
		defer ctrl.lock.Unlock()
		// Add node to total
		ctrl.nodeCount++

		if region != "" && zone != "" {
			// Add region to graph
			// ctrl.regionGraph.AddEdge(region, region, 0)

			// Add zone to graph
			// ctrl.zoneGraph.AddEdge(zone, zone, 0)

			// Add the region / zone to the map
			ctrl.topologyMap[util.TopologyKey{
				Region: region,
				Zone:   zone}] = true
		}

		// Add node to graph
		// ctrl.nodeGraph.AddEdge(node.Name, node.Name, 0)

	}()
	klog.V(5).Infof("Added node %v - Total node count: %v", node.Name, ctrl.nodeCount)
	return
}

// nodeUpdated reacts to a node update
func (ctrl *NetworkTopologyController) nodeUpdated(old, new interface{}) {
	// Check if zone label has been modified ...
	newNode, ok := new.(*v1.Node)
	if !ok {
		klog.Error("unexpected object type in node added")
		return
	}

	oldNode, err := old.(*v1.Node)
	if !err {
		klog.Error("unexpected object type in node added")
		return
	}

	var oldRegion string
	var oldZone string
	if old != nil {
		oldRegion = networkAwareUtil.GetNodeRegion(oldNode)
		oldZone = networkAwareUtil.GetNodeZone(oldNode)
	}

	newRegion := networkAwareUtil.GetNodeRegion(newNode)
	newZone := networkAwareUtil.GetNodeZone(newNode)

	// If the zone of the node did not changed, we don't need to do anything.
	if oldZone == newZone && oldRegion == newRegion {
		return
	}
	// Otherwise update zone of the given Node
	ctrl.nodeDeleted(old)
	ctrl.nodeAdded(new)
}

// nodeDeleted reacts to a node removal
func (ctrl *NetworkTopologyController) nodeDeleted(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.Error("unexpected object type in node deleted")
		return
	}

	_, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	func() {
		ctrl.lock.Lock()
		defer ctrl.lock.Unlock()
		// Remove node from total
		ctrl.nodeCount--

		// Remove all Edges from graph
		// ctrl.nodeGraph.RemoveEdge(node.Name)
	}()

	klog.V(5).Infof("Removed node %v - Total node count: %v", node.Name, ctrl.nodeCount)
}

// podAdded reacts to a Pod creation
func (ctrl *NetworkTopologyController) podAdded(obj interface{}) {
	pod := obj.(*v1.Pod)
	agName := util.GetPodAppGroupLabel(pod)
	if len(agName) == 0 {
		return
	}

	ag, err := ctrl.agLister.AppGroups(pod.Namespace).Get(agName)
	if err != nil {
		klog.ErrorS(err, "Error retrieving AppGroup...")
		return
	}

	klog.V(5).InfoS("Pod's App group: ", "AppGroup", klog.KObj(ag), "pod", klog.KObj(pod))

	// Get Dependencies of the given pod
	dependencyList := networkAwareUtil.GetDependencyList(pod, ag)
	klog.V(5).Info("dependencyList: ", dependencyList)

	// If the pod has no dependencies, return
	if dependencyList == nil {
		return
	}

	// Get pods from lister
	selector := labels.Set(map[string]string{v1alpha1.AppGroupLabel: agName}).AsSelector()
	pods, err := ctrl.podLister.List(selector)
	if err != nil {
		klog.ErrorS(err, "Getting deployed pods from lister...")
		return
	}

	// No pods yet allocated...
	if pods == nil{
		return
	}

	// Get Pods already scheduled: Deployment name, replicaID, hostname
	scheduledList := networkAwareUtil.GetScheduledList(pods)
	klog.V(6).Info("scheduledList: ", scheduledList)

	// Check if pods already available
	if scheduledList == nil{
		return
	}

	// Get Node from pod.Spec.Nodename
	hostname, err := ctrl.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		klog.ErrorS(err, "Getting pod hostname from nodeLister...")
		return
	}

	// Retrieve Region and Zone from node
	region := networkAwareUtil.GetNodeRegion(hostname)
	zone := networkAwareUtil.GetNodeZone(hostname)

	// reserve bandwidth
	for _, podAllocated := range scheduledList { // For each pod already allocated
		if podAllocated.Hostname != "" { // if already updated by the controller
			for _, d := range dependencyList { // For each pod dependency
				if podAllocated.Selector == d.Workload.Selector { // If the pod allocated is an established dependency
					if podAllocated.Hostname == pod.Spec.NodeName { // If the pod's hostname is the same
						klog.Info("[Pod added] Same Hostname do nothing for this dependency... ")
					} else { // If Nodes are not the same
						// Get NodeInfo from pod Hostname
						podHostname, err := ctrl.nodeLister.Get(podAllocated.Hostname)
						if err != nil {
							klog.ErrorS(err, "Getting pod hostname from nodeLister...")
							return
						}
						// Get zone and region from Pod Hostname
						regionPodHostname := networkAwareUtil.GetNodeRegion(podHostname)
						zonePodHostname := networkAwareUtil.GetNodeZone(podHostname)

						if regionPodHostname == "" && zonePodHostname == "" { // Node has no zone and region defined
							klog.Info("[Pod added] Null region/zone do nothing for this dependency... ")
						} else if region == regionPodHostname { // If Nodes belong to the same region
							if zone == zonePodHostname { // If Nodes belong to the same zone
								klog.Info("[Pod added] Same Zone do nothing for this dependency... ")
							} else { // belong to a different zone
								value, ok := ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Retrieve the current allocatable bandwidth from the map (origin: zone, destination: pod zoneHostname)
									Origin:      zone,
									Destination: zonePodHostname,
								}]
								if ok {
									value.Add(d.MinBandwidth)
									ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
										Origin:      zone,
										Destination:  zonePodHostname}] = value
								} else {
									klog.Infof("[zones] Bandwidth allocatable not found in map... add minBandwidth")
									capacity := *resource.NewQuantity(0, resource.DecimalSI)
									capacity.Add(d.MinBandwidth)

									ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
										Origin:      zone,
										Destination:  zonePodHostname}] = capacity
								}
							}
						} else { // belong to a different region
							value, ok := ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Retrieve the current allocable bandwidth from the map (origin: region, destination: pod regionHostname)
								Origin:      region,
								Destination: regionPodHostname,
							}]
							if ok {
								value.Add(d.MinBandwidth)
								ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
									Origin:      region,
									Destination:  regionPodHostname}] = value
							} else {
								klog.Infof("[regions] Bandwidth allocatable not found in map... add minBandwidth")
								capacity := *resource.NewQuantity(0, resource.DecimalSI)
								capacity.Add(d.MinBandwidth)

								ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
									Origin:      region,
									Destination:  regionPodHostname}] = capacity
							}
						}
					}
				}
			}
		}
	}
}

// podDeleted reacts to a pod delete
func (ctrl *NetworkTopologyController) podDeleted(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.Error("unexpected object type in pod deleted")
		return
	}

	_, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	agName := util.GetPodAppGroupLabel(pod)
	if len(agName) == 0 {
		return
	}

	func() {
		ctrl.lock.Lock()
		defer ctrl.lock.Unlock()

		ag, err := ctrl.agLister.AppGroups(pod.Namespace).Get(agName)
		if err != nil {
			klog.ErrorS(err, "Error retrieving AppGroup...")
			return
		}

		klog.V(5).InfoS("[Pod deleted] Pod's App group: ", "AppGroup", klog.KObj(ag), "pod", klog.KObj(pod))

		// Get Dependencies of the given pod
		dependencyList := networkAwareUtil.GetDependencyList(pod, ag)
		klog.V(5).Info("[Pod deleted] dependencyList: ", dependencyList)

		// If the pod has no dependencies, return
		if dependencyList == nil {
			return
		}

		// Get pods from lister
		selector := labels.Set(map[string]string{v1alpha1.AppGroupLabel: agName}).AsSelector()
		pods, err := ctrl.podLister.List(selector)
		if err != nil {
			klog.ErrorS(err, "[Pod deleted] Getting deployed pods from lister...")
			return
		}

		// No pods yet allocated...
		if pods == nil{
			return
		}

		// Get Pods already scheduled: Deployment name, replicaID, hostname
		scheduledList := networkAwareUtil.GetScheduledList(pods)
		klog.V(6).Info("scheduledList: ", scheduledList)

		// Check if pods already available
		if scheduledList == nil{
			return
		}

		// Get Node from pod.Spec.Nodename
		hostname, err := ctrl.nodeLister.Get(pod.Spec.NodeName)
		if err != nil {
			klog.ErrorS(err, "[Pod deleted] Getting pod hostname from nodeLister...")
			return
		}

		// Retrieve Region and Zone from node
		region := networkAwareUtil.GetNodeRegion(hostname)
		zone := networkAwareUtil.GetNodeZone(hostname)

		// delete reserved bandwidth
		for _, podAllocated := range scheduledList { // For each pod already allocated
			if podAllocated.Hostname != "" { // if already updated by the controller
				for _, d := range dependencyList { // For each pod dependency
					if podAllocated.Selector == d.Workload.Selector { // If the pod allocated is an established dependency
						if podAllocated.Hostname == pod.Spec.NodeName { // If the pod's hostname is the same
							klog.Info("[Pod deleted] Same hostname do nothing for this dependency... ")
						} else { // If Nodes are not the same
							// Get NodeInfo from pod Hostname
							podHostname, err := ctrl.nodeLister.Get(podAllocated.Hostname)
							if err != nil {
								klog.ErrorS(err, "Getting pod hostname from nodeLister...")
								return
							}
							// Get zone and region from Pod Hostname
							regionPodHostname := networkAwareUtil.GetNodeRegion(podHostname)
							zonePodHostname := networkAwareUtil.GetNodeZone(podHostname)

							if regionPodHostname == "" && zonePodHostname == "" { // Node has no zone and region defined
								klog.Info("[Pod deleted] Null region/zone do nothing for this dependency... ")
							} else if region == regionPodHostname { // If Nodes belong to the same region
								if zone == zonePodHostname { // If Nodes belong to the same zone
									klog.Info("[Pod deleted] Same Zone do nothing for this dependency... ")
								} else { // belong to a different zone
									value, ok := ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Retrieve the current allocatable bandwidth from the map (origin: zone, destination: pod zoneHostname)
										Origin:      zone,
										Destination: zonePodHostname,
									}]
									if ok {
										value.Sub(d.MinBandwidth)
										ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
											Origin:      zone,
											Destination: zonePodHostname}] = value
									} else {
										klog.Infof("[zones] Bandwidth allocatable not found in map... add 0")
										capacity := *resource.NewQuantity(0, resource.DecimalSI)

										ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
											Origin:      zone,
											Destination: zonePodHostname}] = capacity
									}
								}
							} else { // belong to a different region
								value, ok := ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Retrieve the current allocable bandwidth from the map (origin: region, destination: pod regionHostname)
									Origin:      region,
									Destination: regionPodHostname,
								}]
								if ok {
									value.Sub(d.MinBandwidth)
									ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
										Origin:      region,
										Destination: regionPodHostname}] = value
								} else {
									klog.Infof("[regions] Bandwidth allocatable not found in map... add 0")
									capacity := *resource.NewQuantity(0, resource.DecimalSI)

									ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Add the updated bandwidth to the map
										Origin:      region,
										Destination: regionPodHostname}] = capacity
								}
							}
						}
					}
				}
			}
		}
	}()
	ctrl.podAdded(obj)
}

// pgUpdated reacts to a PG update
func (ctrl *NetworkTopologyController) podUpdated(old, new interface{}) {
	ctrl.podAdded(new)
}

func (ctrl *NetworkTopologyController) worker() {
	for ctrl.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *NetworkTopologyController) processNextWorkItem() bool {
	keyObj, quit := ctrl.ntQueue.Get()
	if quit {
		return false
	}
	defer ctrl.ntQueue.Done(keyObj)

	key, ok := keyObj.(string)
	if !ok {
		ctrl.ntQueue.Forget(keyObj)
		runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", keyObj))
		return true
	}
	if err := ctrl.syncHandler(key); err != nil {
		runtime.HandleError(err)
		klog.ErrorS(err, "Error syncing network topology", "networkTopology", key)
		return true
	}
	return true
}

// syncHandle syncs network topology and convert status
func (ctrl *NetworkTopologyController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	defer func() {
		if err != nil {
			ctrl.ntQueue.AddRateLimited(key)
			return
		}
	}()
	nt, err := ctrl.ntLister.NetworkTopologies(namespace).Get(name)
	if apierrs.IsNotFound(err) {
		klog.V(5).InfoS("Network Topology has been deleted", "networkTopology", key)
		return nil
	}
	if err != nil {
		klog.V(3).ErrorS(err, "Unable to retrieve Network Topology from store", "networkTopology", key)
		return err
	}

	ntCopy := nt.DeepCopy()

	nodes, err := ctrl.nodeLister.List(labels.Everything())
	if err != nil {
		klog.ErrorS(err, "List nodes failed during syncHandler", "networkTopology", klog.KObj(ntCopy))
		return err
	}

	configmap, err := ctrl.configmapLister.ConfigMaps(namespace).Get(ntCopy.Spec.ConfigmapName)
	if apierrs.IsNotFound(err) {
		klog.V(5).InfoS("ConfigMap has been deleted", "networkTopology", key)
		return nil
	}
	if err != nil {
		klog.V(3).ErrorS(err, "Unable to retrieve ConfigMap from store", "networkTopology", key)
		return err
	}

	klog.V(5).Infof("ConfigMap %v retrieved...", configmap.Name)

	// Update Status of Network Topology CRD

	// NodeCount
	ctrl.lock.Lock()
	ntCopy.Status.NodeCount = ctrl.nodeCount
	ctrl.lock.Unlock()

	// Weights
	ctrl.lock.Lock()

	if ntCopy.Status.WeightCalculationTime.IsZero() {
		klog.InfoS("Initial Calculation of Weight List...")

		var manualCosts v1alpha1.TopologyList
		var manualRegionCosts v1alpha1.OriginList
		var manualZoneCosts v1alpha1.OriginList

		for _, w := range ntCopy.Spec.Weights {
			if w.Name == "UserDefined" {
				manualCosts = w.TopologyList
				for _, c := range w.TopologyList {
					if c.TopologyKey == v1alpha1.NetworkTopologyRegion {
						manualRegionCosts = c.OriginList
					}
					if c.TopologyKey == v1alpha1.NetworkTopologyZone {
						manualZoneCosts = c.OriginList
					}
				}
			}
		}

		err := updateGraph(ctrl, nodes, configmap)
		if err != nil {
			runtime.HandleError(err)
			klog.ErrorS(err, "Error updating Weight List", "networkTopology", key)
			return err
		}

		klog.V(5).Infof("Graph: %v", ctrl.nodeGraph)

		weights := v1alpha1.WeightList{}

		weights = append(weights, v1alpha1.WeightInfo{
									Name:           "UserDefined",
									TopologyList: 	manualCosts,
		})

		weights = append(weights, v1alpha1.WeightInfo{
			Name:           v1alpha1.NetworkTopologyNetperfCosts,
			TopologyList:  getTopologyList(ctrl, nodes , manualRegionCosts, manualZoneCosts),
		})

		ntCopy.Spec.Weights = weights

		ntCopy.Status.WeightCalculationTime = metav1.Time{Time: time.Now()}

	} else if time.Now().Sub(ntCopy.Status.WeightCalculationTime.Time) > 5*time.Minute {
		klog.InfoS("Recalculation of Weight List... Every 5 min...")

		var manualCosts v1alpha1.TopologyList
		var manualRegionCosts v1alpha1.OriginList
		var manualZoneCosts v1alpha1.OriginList

		for _, w := range ntCopy.Spec.Weights {
			if w.Name == "UserDefined" {
				manualCosts = w.TopologyList
				for _, c := range w.TopologyList {
					if c.TopologyKey == v1alpha1.NetworkTopologyRegion {
						manualRegionCosts = c.OriginList
					}
					if c.TopologyKey == v1alpha1.NetworkTopologyZone {
						manualZoneCosts = c.OriginList
					}
				}
			}
		}

		err := updateGraph(ctrl, nodes, configmap)
		if err != nil {
			runtime.HandleError(err)
			klog.ErrorS(err, "Error updating Weight List", "networkTopology", key)
			return err
		}

		weights := v1alpha1.WeightList{}

		weights = append(weights, v1alpha1.WeightInfo{
			Name:           "UserDefined",
			TopologyList: 	manualCosts,
		}, )

		weights = append(weights, v1alpha1.WeightInfo{
			Name:           v1alpha1.NetworkTopologyNetperfCosts,
			TopologyList:  getTopologyList(ctrl, nodes , manualRegionCosts, manualZoneCosts),
		}, )

		ntCopy.Spec.Weights = weights
		ntCopy.Status.WeightCalculationTime = metav1.Time{Time: time.Now()}
	}

	ctrl.lock.Unlock()

	// Patch ntCopy
	err = ctrl.patchNetworkTopology(nt, ntCopy)
	if err == nil {
		ctrl.ntQueue.Forget(nt)
	}
	return err
}

func (ctrl *NetworkTopologyController) patchNetworkTopology(old, new *v1alpha1.NetworkTopology) error {
	if !reflect.DeepEqual(old, new) {
		patch, err := util.CreateMergePatch(old, new)
		if err != nil {
			return err
		}

		_, err = ctrl.ntClient.SchedulingV1alpha1().NetworkTopologies(old.Namespace).Patch(context.TODO(), old.Name, types.MergePatchType,
			patch, metav1.PatchOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

// Update the weights based on latency measurements saved in the configmap
func updateGraph(ctrl *NetworkTopologyController, nodes []*v1.Node, configmap *v1.ConfigMap) error {
	klog.InfoS("NetworkTopology SyncHandler: Update costs in the network graph... ")

	// Rebuild the graph
	ctrl.regionGraph = util.NewGraph()
	ctrl.zoneGraph = util.NewGraph()
	ctrl.nodeGraph = util.NewGraph()

	for _, n1 := range nodes {
		r1 := networkAwareUtil.GetNodeRegion(n1)
		z1 := networkAwareUtil.GetNodeZone(n1)

		for _, n2 := range nodes {
			// Avoid adding costs for origin = destination
			if n1.Name != n2.Name {
				r2 := networkAwareUtil.GetNodeRegion(n2)
				z2 := networkAwareUtil.GetNodeZone(n2)

				klog.V(5).Infof("N1: %v - N2: %v - Region1: %v - Region2: %v - Zone1: %v - Zone2: %v", n1.Name, n2.Name, r1, r2, z1, z2)

				// get cost from configmap
				key := util.GetConfigmapCostQuery(n1.Name, n2.Name)

				klog.V(5).Infof("Key: %v", key)
				klog.V(5).Infof("configmap.Data: %v", configmap.Data)

				cost, err := strconv.Atoi(configmap.Data[key])
				if err != nil {
					klog.ErrorS(err, "Error converting cost...")
				}

				klog.V(5).Infof("Cost: %v", cost)

				// Update Cost in the graph
				ctrl.nodeGraph.AddEdge(n1.Name, n2.Name, cost)

				if r1 != r2 { // Different region
					current, err := ctrl.regionGraph.GetPath(r1, r2)
					if err != nil { // add average cost!
							cost = (cost + current) / 2
							ctrl.regionGraph.AddEdge(r1, r2, cost)
					}
					ctrl.regionGraph.AddEdge(r1, r2, cost)
				} else if z1 != z2 { // Same region Different zone
					// Add zone key to map
					ctrl.ZoneMap[util.ZoneKey{
						Z1: z1,
						Z2: z2,
					}] = true

					current, err := ctrl.zoneGraph.GetPath(z1, z2)
					if err != nil { // Add average cost
						cost = (cost + current) / 2
						ctrl.zoneGraph.AddEdge(z1, z2, cost)
					}
					ctrl.zoneGraph.AddEdge(z1, z2, cost)
				}
			}
		}
	}
	return nil
}

func getTopologyList(ctrl *NetworkTopologyController, nodes []*v1.Node, manualRegionCosts v1alpha1.OriginList, manualZoneCosts v1alpha1.OriginList) v1alpha1.TopologyList {
	var topologyList v1alpha1.TopologyList

	topologyList = append(topologyList, v1alpha1.TopologyInfo{
		TopologyKey: v1alpha1.NetworkTopologyRegion,
		OriginList:  getRegionList(ctrl, nodes, manualRegionCosts),
	})

	topologyList = append(topologyList, v1alpha1.TopologyInfo{
		TopologyKey: v1alpha1.NetworkTopologyZone,
		OriginList:  getZoneList(ctrl, nodes, manualZoneCosts),
	})
	return topologyList
}

func getRegionList(ctrl *NetworkTopologyController, nodes []*v1.Node, manualRegionCosts v1alpha1.OriginList) v1alpha1.OriginList {
	var regionList v1alpha1.OriginList
	var regions []string

	// Sort Costs by origin, might not be sorted since were manually defined
	sort.Sort(networkAwareUtil.ByOrigin(manualRegionCosts))

	for _, n := range nodes {
		r := networkAwareUtil.GetNodeRegion(n)
		if !contains(regions, r) {
			regions = append(regions, r)
		}
	}

	klog.V(2).Infof("[getRegionList] Regions %v ", regions)

	for _, r1 := range regions {
		// init vars
		var costInfo []v1alpha1.CostInfo

		for _, r2 := range regions {
			if r1 != r2 {
				cost, _ := ctrl.regionGraph.GetPath(r1, r2)

				allocatable, ok := ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Retrieve the current allocatable bandwidth from the map (origin: zone, destination: pod zoneHostname)
					Origin:      r1,
					Destination: r2,
				}]

				originCosts := networkAwareUtil.FindOriginCosts(manualRegionCosts, r1)

				klog.V(5).Infof("[getRegionList] originCosts: %v", originCosts)

				bandwidthCapacity := *resource.NewScaledQuantity(1, resource.Giga)

				if originCosts !=nil {
					// Sort Costs by destination, might not be sorted since were manually defined
					sort.Sort(networkAwareUtil.ByDestination(originCosts))

					bandwidthCapacity = util.FindOriginBandwidthCapacity(originCosts, r2)

					if bandwidthCapacity == resource.MustParse("0"){
						bandwidthCapacity = *resource.NewScaledQuantity(1, resource.Giga)
					}
				}

				klog.V(5).Infof("[getRegionList] Bandwidth Capacity between %v and %v: %v", r1, r2, bandwidthCapacity)

				if ok {
					info := v1alpha1.CostInfo{
						Destination:        r2,
						BandwidthCapacity:  bandwidthCapacity,
						BandwidthAllocated: allocatable,
						NetworkCost:        int64(cost),
					}
					klog.V(5).Infof("[getRegionList] OK: Origin %v - Destination %v - Cost: %v - Allocatable: %v", r1, r2, info.NetworkCost, info.BandwidthAllocated)
					costInfo = append(costInfo, info)
				}else{
					info := v1alpha1.CostInfo{
						Destination:        r2,
						BandwidthCapacity:  bandwidthCapacity,
						BandwidthAllocated: *resource.NewQuantity(0, resource.DecimalSI), // consider as zero
						NetworkCost:        int64(cost),
					}
					klog.V(5).Infof("[getRegionList] Bandwidth allocatable Not found: Origin %v - Destination %v - Cost: %v - Allocatable: %v", r1, r2, info.NetworkCost, info.BandwidthAllocated)
					costInfo = append(costInfo, info)
				}
			}
		}

		// Sort Costs by Destination
		sort.Sort(networkAwareUtil.ByDestination(costInfo))

		originInfo := v1alpha1.OriginInfo{
			Origin: r1,
			CostList:  costInfo,
		}
		regionList = append(regionList, originInfo)
	}

	// Sort regionList by origin
	sort.Sort(networkAwareUtil.ByOrigin(regionList))
	return regionList
}

func getZoneList(ctrl *NetworkTopologyController, nodes []*v1.Node, manualZoneCosts v1alpha1.OriginList) v1alpha1.OriginList {
	var zoneList v1alpha1.OriginList
	var zones []string

	for _, n := range nodes {
		z := networkAwareUtil.GetNodeZone(n)
		if !contains(zones, z) {
			zones = append(zones, z)
		}
	}

	klog.V(2).Infof("[getZoneList] Zones %v ", zones)

	for _, z1 := range zones {
		// init vars
		var costInfo []v1alpha1.CostInfo

		for _, z2 := range zones {
			if z1 != z2 {
				value, ok := ctrl.ZoneMap[util.ZoneKey{ // Check if zones belong to the same region
					Z1: z1,
					Z2: z2,
				}]

				if ok && value {
					cost, _ := ctrl.zoneGraph.GetPath(z1, z2)

					allocatable, ok := ctrl.BandwidthAllocatable[networkAwareUtil.CostKey{ // Retrieve the current allocatable bandwidth from the map (origin: zone, destination: pod zoneHostname)
						Origin:      z1,
						Destination: z2,
					}]

					originCosts := networkAwareUtil.FindOriginCosts(manualZoneCosts, z1)

					klog.V(5).Infof("[getZoneList] originCosts: %v", originCosts)

					bandwidthCapacity := *resource.NewScaledQuantity(1, resource.Giga)

					if originCosts !=nil {
						// Sort Costs by destination, might not be sorted since were manually defined
						sort.Sort(networkAwareUtil.ByDestination(originCosts))

						bandwidthCapacity= util.FindOriginBandwidthCapacity(originCosts, z2)

						if bandwidthCapacity == resource.MustParse("0"){
							bandwidthCapacity = *resource.NewScaledQuantity(1, resource.Giga)
						}
					}

					klog.Infof("[getZoneList] Bandwidth Capacity between %v and %v: %v", z1, z2, bandwidthCapacity)

					if ok {
						info := v1alpha1.CostInfo{
							Destination:        z2,
							BandwidthCapacity:  bandwidthCapacity,
							BandwidthAllocated: allocatable,
							NetworkCost:        int64(cost),
						}

						klog.V(5).Infof("[getZoneList] Origin %v - Destination %v - Cost: %v - Allocatable: %v", z1, z2, info.NetworkCost, info.BandwidthAllocated)
						costInfo = append(costInfo, info)
					} else{
						info := v1alpha1.CostInfo{
							Destination:        z2,
							BandwidthCapacity:  bandwidthCapacity,
							BandwidthAllocated: *resource.NewQuantity(0, resource.DecimalSI), // Consider as zero
							NetworkCost:        int64(cost),
						}

						klog.V(5).Infof("[getZoneList] Bandwidth Allocatable not found: Origin %v - Destination %v - Cost: %v - Allocatable: %v", z1, z2, info.NetworkCost, info.BandwidthAllocated)
						costInfo = append(costInfo, info)
					}
				}
			}
		}

		// Sort Costs by Destination -> new
		sort.Sort(networkAwareUtil.ByDestination(costInfo))

		originInfo := v1alpha1.OriginInfo{
			Origin: z1,
			CostList:  costInfo,
		}
		zoneList = append(zoneList, originInfo)
	}

	// Sort Costs by origin
	sort.Sort(networkAwareUtil.ByOrigin(zoneList))
	return zoneList
}

// contains checks if a string is present in a slice
func contains(s []string, str string) bool {
	for _, value := range s {
		if value == str {
			return true
		}
	}
	return false
}
