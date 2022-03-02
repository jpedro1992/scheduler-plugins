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

package integration

import (
	"context"
	"fmt"
	"sigs.k8s.io/scheduler-plugins/pkg/networkaware/topologicalsort"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler"
	schedapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	imageutils "k8s.io/kubernetes/test/utils/image"

	schedconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	"sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	"sigs.k8s.io/scheduler-plugins/test/util"
)

func TestTopologicalSortPlugin(t *testing.T) {
	testCtx := &testContext{}
	testCtx.Ctx, testCtx.CancelFn = context.WithCancel(context.Background())

	cs := kubernetes.NewForConfigOrDie(globalKubeConfig)
	extClient := versioned.NewForConfigOrDie(globalKubeConfig)
	testCtx.ClientSet = cs
	testCtx.KubeConfig = globalKubeConfig

	if err := wait.Poll(100*time.Millisecond, 3*time.Second, func() (done bool, err error) {
		groupList, _, err := cs.ServerGroupsAndResources()
		if err != nil {
			return false, nil
		}
		for _, group := range groupList {
			if group.Name == scheduling.GroupName {
				t.Log("The CRD is ready to serve")
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("Timed out waiting for CRD to be ready: %v", err)
	}

	cfg, err := util.NewDefaultSchedulerComponentConfig()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Profiles[0].Plugins.QueueSort = schedapi.PluginSet{
		Enabled:  []schedapi.Plugin{{Name: topologicalsort.Name}},
		Disabled: []schedapi.Plugin{{Name: "*"}},
	}

	ns := fmt.Sprintf("integration-test-%v", string(uuid.NewUUID()))
	createNamespace(t, testCtx, ns)

	cfg.Profiles[0].PluginConfig = append(cfg.Profiles[0].PluginConfig, schedapi.PluginConfig{
		Name: topologicalsort.Name,
		Args: &schedconfig.TopologicalSortArgs{
			Namespaces: []string{ns},
		},
	})

	testCtx = initTestSchedulerWithOptions(
		t,
		testCtx,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{topologicalsort.Name: topologicalsort.New}),
	)
	syncInformerFactory(testCtx)
	go testCtx.Scheduler.Run(testCtx.Ctx)
	t.Log("Init scheduler success")
	defer cleanupTest(t, testCtx)

	// Create a Node.
	nodeName := "fake-node"
	node := st.MakeNode().Name("fake-node").Label("node", nodeName).Obj()
	node.Status.Allocatable = v1.ResourceList{
		v1.ResourcePods:   *resource.NewQuantity(32, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(300, resource.DecimalSI),
	}
	node.Status.Capacity = v1.ResourceList{
		v1.ResourcePods:   *resource.NewQuantity(32, resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(300, resource.DecimalSI),
	}
	node, err = cs.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create Node %q: %v", nodeName, err)
	}

	// Create an AppGroup: basic
	basicAppGroup := v1alpha1.AppGroupWorkloadList{
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace: "default"},
			Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{
				Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}}}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"},
			Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{
				Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}}}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}},
	}

	basicTopologyOrder := v1alpha1.AppGroupTopologyList{
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace: "default"}, Index: 1},
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}, Index: 2},
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}, Index: 3},
	}

	pause := imageutils.GetPauseImageName()
	for _, tt := range []struct {
		name         string
		pods         []*v1.Pod
		appGroups    []*v1alpha1.AppGroup
		expectedPods []string
	}{
		{
			name: "basic AppGroup",
			pods: []*v1.Pod{
				WithContainer(st.MakePod().Namespace(ns).Name("p1-1").Req(map[v1.ResourceName]string{v1.ResourceMemory: "50"}).Priority(
					midPriority).Label(v1alpha1.AppGroupLabel, "basic").Label(v1alpha1.AppGroupSelectorLabel, "P1").ZeroTerminationGracePeriod().Obj(), pause),
				WithContainer(st.MakePod().Namespace(ns).Name("p2-1").Req(map[v1.ResourceName]string{v1.ResourceMemory: "50"}).Priority(
					midPriority).Label(v1alpha1.AppGroupLabel, "basic").Label(v1alpha1.AppGroupSelectorLabel, "P2").ZeroTerminationGracePeriod().Obj(), pause),
				WithContainer(st.MakePod().Namespace(ns).Name("p3-1").Req(map[v1.ResourceName]string{v1.ResourceMemory: "50"}).Priority(
					midPriority).Label(v1alpha1.AppGroupLabel, "basic").Label(v1alpha1.AppGroupSelectorLabel, "P3").ZeroTerminationGracePeriod().Obj(), pause),
			},
			appGroups: []*v1alpha1.AppGroup{
				util.MakeAG("basic", 3, ns, "KahnSort", basicAppGroup, basicTopologyOrder, nil),
			},
			expectedPods: []string{"p1-1", "p2-1", "p3-1"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Start-topologicalSort-test %v", tt.name)
			defer cleanupAppGroups(testCtx.Ctx, extClient, tt.appGroups)
			// create AppGroup
			if err := createAppGroups(testCtx.Ctx, extClient, tt.appGroups); err != nil {
				t.Fatal(err)
			}
			defer cleanupPods(t, testCtx, tt.pods)
			// Create Pods, we will expect them to be scheduled in a reversed order.
			for i := range tt.pods {
				klog.InfoS("Creating pod ", "podName", tt.pods[i].Name)
				if _, err := cs.CoreV1().Pods(tt.pods[i].Namespace).Create(testCtx.Ctx, tt.pods[i], metav1.CreateOptions{}); err != nil {
					t.Fatalf("Failed to create Pod %q: %v", tt.pods[i].Name, err)
				}
			}
			err = wait.Poll(1*time.Second, 120*time.Second, func() (bool, error) {
				for _, v := range tt.expectedPods {
					if !podScheduled(cs, ns, v) {
						return false, nil
					}
				}
				return true, nil
			})
			if err != nil {
				t.Fatalf("%v Waiting expectedPods error: %v", tt.name, err.Error())
			}
			t.Logf("Case %v finished", tt.name)
		})
	}
}
