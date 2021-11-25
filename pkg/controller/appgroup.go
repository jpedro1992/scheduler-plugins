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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"reflect"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	schedclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions/scheduling/v1alpha1"
	schedlister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

// AppGroupController is a controller that process App groups using provided Handler interface
type AppGroupController struct {
	eventRecorder   record.EventRecorder
	agQueue         workqueue.RateLimitingInterface
	agLister        schedlister.AppGroupLister
	podLister       corelister.PodLister
	agListerSynced  cache.InformerSynced
	podListerSynced cache.InformerSynced
	agClient        schedclientset.Interface
}

// NewAppGroupController returns a new *AppGroupController
func NewAppGroupController(client kubernetes.Interface,
	agInformer schedinformer.AppGroupInformer,
	podInformer coreinformer.PodInformer,
	agClient schedclientset.Interface) *AppGroupController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events(v1.NamespaceAll)})

	ctrl := &AppGroupController{
		eventRecorder: broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "AppGroupController"}),
		agQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "AppGroup"),
	}

	klog.V(5).InfoS("Setting up AppGroup event handlers")
	agInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.agAdded,
		UpdateFunc: ctrl.agUpdated,
		DeleteFunc: ctrl.agDeleted,
	})

	klog.V(5).InfoS("Setting up Pod event handlers")
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.podAdded,
		UpdateFunc: ctrl.podUpdated,
		DeleteFunc: ctrl.podDeleted,
	})

	ctrl.agLister = agInformer.Lister()
	ctrl.podLister = podInformer.Lister()
	ctrl.agListerSynced = agInformer.Informer().HasSynced
	ctrl.podListerSynced = podInformer.Informer().HasSynced
	ctrl.agClient = agClient
	return ctrl
}

// Run starts listening on channel events
func (ctrl *AppGroupController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.agQueue.ShutDown()

	klog.InfoS("Starting App Group controller")
	defer klog.InfoS("Shutting App Group controller")

	if !cache.WaitForCacheSync(stopCh, ctrl.agListerSynced, ctrl.podListerSynced) {
		klog.Error("Cannot sync caches")
		return
	}
	klog.InfoS("App Group sync finished")
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.worker, time.Second, stopCh)
	}

	<-stopCh
}

// agAdded reacts to a AG creation
func (ctrl *AppGroupController) agAdded(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	ag := obj.(*schedv1alpha1.AppGroup)

	// From PodGroup: If startScheduleTime - createTime > 2days, do not enqueue again because pod may have been GCed
	if ag.Status.RunningPods == 0 &&
		ag.Status.ScheduleStartTime.Sub(ag.CreationTimestamp.Time) > 48*time.Hour {
		return
	}

	klog.InfoS("Enqueue AppGroup ", "app group", key)
	ctrl.agQueue.Add(key)
}

// agUpdated reacts to a AP update
func (ctrl *AppGroupController) agUpdated(old, new interface{}) {
	ctrl.agAdded(new)
}

// agDeleted reacts to a AppGroup deletion
func (ctrl *AppGroupController) agDeleted(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	klog.V(5).InfoS("Enqueue deleted app group key", "appGroup", key)
	ctrl.agQueue.AddRateLimited(key)
}

// podAdded reacts to a Pod creation
func (ctrl *AppGroupController) podAdded(obj interface{}) {
	pod := obj.(*v1.Pod)
	agName := util.GetAppGroupLabel(pod)
	if len(agName) == 0 {
		return
	}
	ag, err := ctrl.agLister.AppGroups(pod.Namespace).Get(agName)
	if err != nil {
		klog.ErrorS(err, "Error while adding pod")
		return
	}
	klog.V(5).InfoS("Add App group when pod gets added", "AppGroup", klog.KObj(ag), "pod", klog.KObj(pod))
	ctrl.agAdded(ag)
}

// podDeleted reacts to a pod delete
func (ctrl *AppGroupController) podDeleted(obj interface{}) {
	_, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	ctrl.podAdded(obj)
}

// pgUpdated reacts to a PG update
func (ctrl *AppGroupController) podUpdated(old, new interface{}) {
	ctrl.podAdded(new)
}

func (ctrl *AppGroupController) worker() {
	for ctrl.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *AppGroupController) processNextWorkItem() bool {
	keyObj, quit := ctrl.agQueue.Get()
	if quit {
		return false
	}
	defer ctrl.agQueue.Done(keyObj)

	key, ok := keyObj.(string)
	if !ok {
		ctrl.agQueue.Forget(keyObj)
		runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", keyObj))
		return true
	}
	if err := ctrl.syncHandler(key); err != nil {
		runtime.HandleError(err)
		klog.ErrorS(err, "Error syncing app group", "appGroup", key)
		return true
	}

	return true
}

// syncHandle syncs app group and convert status
func (ctrl *AppGroupController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	defer func() {
		if err != nil {
			ctrl.agQueue.AddRateLimited(key)
			return
		}
	}()
	ag, err := ctrl.agLister.AppGroups(namespace).Get(name)
	if apierrs.IsNotFound(err) {
		klog.V(5).InfoS("App group has been deleted", "appGroup", key)
		return nil
	}
	if err != nil {
		klog.V(3).ErrorS(err, "Unable to retrieve app group from store", "appGroup", key)
		return err
	}

	agCopy := ag.DeepCopy()
	selector := labels.Set(map[string]string{util.AppGroupLabel: agCopy.Name}).AsSelector()

	pods, err := ctrl.podLister.List(selector)

	if err != nil {
		klog.ErrorS(err, "List pods for App group failed", "AppGroup", klog.KObj(agCopy))
		return err
	}
	// Update Status of AppGroup CRD

	// Running
	var numPodsRunning int32 = 0
	if len(pods) != 0 {
		for _, pod := range pods {
			switch pod.Status.Phase {
			case v1.PodRunning:
				numPodsRunning++
			}
		}
	}

	agCopy.Status.RunningPods = numPodsRunning
	klog.V(5).Info("RunningPods: ", numPodsRunning)

	// scheduled: ScheduledAppGroup Struct name, replicaID, hostname
	scheduledList := schedv1alpha1.ScheduledList{}

	for _, p := range pods {
		ls := p.GetLabels()
		scheduledInfo := schedv1alpha1.ScheduledInfo{
			PodName: ls[util.DeploymentLabel],
			ReplicaID: string(p.GetUID()),
			Hostname:  p.Spec.Hostname,
		}
		scheduledList = append(scheduledList, scheduledInfo)
	}

	klog.Info("scheduledList: ", scheduledList)

	agCopy.Status.PodsScheduled = scheduledList

	if agCopy.Status.TopologyCalculationTime.IsZero() {
		klog.InfoS("Initial Calculation of Topology order...")
		agCopy.Status.TopologyOrder, err = calculateTopologyOrder(agCopy, agCopy.Spec.TopologySortingAlgorithm, agCopy.Spec.Pods, err)
		if err != nil {
			runtime.HandleError(err)
			klog.ErrorS(err, "Error Calculating Topology order", "appGroup", key)
			return err
		}
		agCopy.Status.TopologyCalculationTime = metav1.Time{Time: time.Now()}
	} else if agCopy.Status.TopologyCalculationTime.Sub(ag.CreationTimestamp.Time) > 48*time.Hour {
		klog.InfoS("Calculate Topology order... time over 48 hours")
		agCopy.Status.TopologyOrder, err = calculateTopologyOrder(agCopy, agCopy.Spec.TopologySortingAlgorithm, agCopy.Spec.Pods, err)
		if err != nil {
			runtime.HandleError(err)
			klog.ErrorS(err, "Error Calculating Topology order", "appGroup", key)
			return err
		}
		agCopy.Status.TopologyCalculationTime = metav1.Time{Time: time.Now()}
	}

	err = ctrl.patchAppGroup(ag, agCopy)
	if err == nil {
		ctrl.agQueue.Forget(ag)
	}
	return err
}

func (ctrl *AppGroupController) patchAppGroup(old, new *schedv1alpha1.AppGroup) error {
	if !reflect.DeepEqual(old, new) {
		patch, err := util.CreateMergePatch(old, new)
		if err != nil {
			return err
		}

		_, err = ctrl.agClient.SchedulingV1alpha1().AppGroups(old.Namespace).Patch(context.TODO(), old.Name, types.MergePatchType,
			patch, metav1.PatchOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

// Calculate the correct sequence order for deployment to be used by the TopologicalSort Plugin
func calculateTopologyOrder(agCopy *schedv1alpha1.AppGroup, algorithm string, podList schedv1alpha1.AppGroupPodList, err error) (schedv1alpha1.TopologyList, error) {

	var order []string
	var topologyList schedv1alpha1.TopologyList
	tree := map[string][]string{}

	for _, pod := range podList {
		for _, dependency := range pod.Dependencies {
			tree[pod.PodName] = append(tree[pod.PodName], dependency.PodName)
		}
	}

	klog.V(5).Info("Service Dependency Tree: ", tree)

	// Calculate order based on the specified algorithm
	switch algorithm {
	case util.AppGroupKahnSort:
		klog.V(5).InfoS("Sorting Algorithm identified as KahnSort")
		order, err = util.KahnSort(tree)
		if err != nil {
			klog.ErrorS(err, "KahnSort failed", "AppGroup", klog.KObj(agCopy))
			return topologyList, err
		}
	case util.AppGroupTarjanSort:
		klog.V(5).InfoS("Sorting Algorithm identified as TarjanSort")
		order, err = util.TarjanSort(tree)
		if err != nil {
			klog.ErrorS(err, "TarjanSort failed", "AppGroup", klog.KObj(agCopy))
			return topologyList, err
		}
	case util.AppGroupReverseKahn:
		klog.V(5).InfoS("Sorting Algorithm identified as ReverseKahn")
		order, err = util.ReverseKahn(tree)
		if err != nil {
			klog.ErrorS(err, "ReverseKahn failed", "AppGroup", klog.KObj(agCopy))
			return topologyList, err
		}
	case util.AppGroupReverseTarjan:
		klog.V(5).InfoS("Sorting Algorithm identified as ReverseTarjan")
		order, err = util.ReverseTarjan(tree)
		if err != nil {
			klog.ErrorS(err, "ReverseTarjan failed", "AppGroup", klog.KObj(agCopy))
			return topologyList, err
		}
	case util.AppGroupAlternateKahn:
		klog.V(5).InfoS("Sorting Algorithm identified as AlternateKahn")
		order, err = util.AlternateKahn(tree)
		if err != nil {
			klog.ErrorS(err, "AlternateKahn failed", "AppGroup", klog.KObj(agCopy))
			return topologyList, err
		}
	case util.AppGroupAlternateTarjan:
		klog.V(5).InfoS("Sorting Algorithm identified as AlternateTarjan")
		order, err = util.AlternateTarjan(tree)
		if err != nil {
			klog.ErrorS(err, "AlternateTarjan failed", "AppGroup", klog.KObj(agCopy))
			return topologyList, err
		}
	default: // Default
		klog.V(5).Info("Sorting Algorithm not identified: ", agCopy.Spec.TopologySortingAlgorithm)
		klog.V(5).InfoS("Default: KahnSort Selected...")
		order, err = util.KahnSort(tree)
		if err != nil {
			klog.ErrorS(err, "KahnSort failed", "AppGroup", klog.KObj(agCopy))
			return topologyList, err
		}
	}

	for id, pod := range order {
		index := int32(id + 1)
		topologyList = append(topologyList, schedv1alpha1.TopologyInfo{
			PodName: pod,
			Index:   index,
		})
	}

	// Sort TopologyList by Pod Name
	klog.V(5).Infof("Sort Topology List by pod name... ")
	sort.Sort(util.ByPodName(topologyList))

	klog.V(5).Info("topologyList (After): ", topologyList)
	return topologyList, nil
}
