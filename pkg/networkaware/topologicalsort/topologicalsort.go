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

package topologicalsort

import (
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	pluginConfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	schedLister "sigs.k8s.io/scheduler-plugins/pkg/generated/listers/scheduling/v1alpha1"
	networkAwareUtil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
	"sigs.k8s.io/scheduler-plugins/pkg/qos"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name = "TopologicalSort"
)

// TopologicalSort : scheduler plugin
type TopologicalSort struct {
	handle     framework.Handle
	agLister   *schedLister.AppGroupLister
	namespaces []string
}

// Name : returns name of the plugin.
func (pl *TopologicalSort) Name() string {
	return Name
}

func getArgs(obj runtime.Object) (*pluginConfig.TopologicalSortArgs, error) {
	TopologicalSortArgs, ok := obj.(*pluginConfig.TopologicalSortArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type TopologicalSortArgs, got %T", obj)
	}

	return TopologicalSortArgs, nil
}

// ScoreExtensions : an interface for Score extended functionality
func (pl *TopologicalSort) QueueSortExtensions() framework.QueueSortPlugin {
	return pl
}

// New : create an instance of a TopologicalSort plugin
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	klog.V(4).Infof("Creating new instance of the TopologicalSort plugin")

	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}

	agLister, err := networkAwareUtil.InitAppGroupInformer(&args.MasterOverride, &args.KubeConfigPath)
	if err != nil {
		return nil, err
	}

	pl := &TopologicalSort{
		handle:     handle,
		agLister:   agLister,
		namespaces: args.Namespaces,
	}
	return pl, nil
}

// Less is the function used by the activeQ heap algorithm to sort pods.
// Sort Pods based on their App Group and corresponding service topology.
// Otherwise, follow the strategy of the QueueSort Plugin
func (ts *TopologicalSort) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	p1AppGroup := util.GetAppGroupLabel(pInfo1.Pod)
	p2AppGroup := util.GetAppGroupLabel(pInfo2.Pod)

	if len(p1AppGroup) == 0 || len(p2AppGroup) == 0 { // Follow QoS Sort
		s := &qos.Sort{}
		return s.Less(pInfo1, pInfo2)
	}

	if p1AppGroup == p2AppGroup { // Pods belong to the same App Group
		klog.V(6).Infof("Pods: %v and %v from the same appGroup %v", pInfo1.Pod.Name, pInfo2.Pod.Name, p1AppGroup)
		agName := p1AppGroup
		appGroup, err := findAppGroupTopologicalSort(agName, ts)

		if err != nil {
			klog.ErrorS(err, "Error while returning AppGroup")
			s := &qos.Sort{}
			return s.Less(pInfo1, pInfo2)
		}

		// Binary search to find both order index since topology list is ordered by Pod Name
		var orderP1 = networkAwareUtil.FindPodOrder(appGroup.Status.TopologyOrder, pInfo1.Pod.Name)
		var orderP2 = networkAwareUtil.FindPodOrder(appGroup.Status.TopologyOrder, pInfo2.Pod.Name)

		klog.V(6).Infof("Pod %v order: %v and Pod %v order: %v.", pInfo1.Pod.Name, orderP1, pInfo2.Pod.Name, orderP2)

		// Lower is better, thus invert result!
		return !(orderP1 > orderP2)
	} else { // Pods do not belong to the same App Group: follow the strategy from the QoS plugin
		klog.V(6).Infof("Pod %v and %v do not belong to the same appGroup %v", pInfo1.Pod.Name, pInfo2.Pod.Name, p1AppGroup)
		s := &qos.Sort{}
		return s.Less(pInfo1, pInfo2)
	}
}

func findAppGroupTopologicalSort(agName string, ts *TopologicalSort) (*v1alpha1.AppGroup, error) {
	klog.V(5).Infof("namespaces: %s", ts.namespaces)
	var err error
	for _, namespace := range ts.namespaces {
		klog.V(5).Infof("data.lister: %v", ts.agLister)
		// AppGroup couldn't be placed in several namespaces simultaneously
		lister := ts.agLister
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
