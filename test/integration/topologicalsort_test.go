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
	schedconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	apiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/scheduler"
	schedapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	testfwk "k8s.io/kubernetes/test/integration/framework"
	testutil "k8s.io/kubernetes/test/integration/util"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling"
	"sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	"sigs.k8s.io/scheduler-plugins/pkg/networkaware/topologicalsort"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/scheduler-plugins/test/util"
)

const (
	// AppGroupLabel is the default label of the AppGroup for the network-aware framework
	AppGroupLabel = "app-group.scheduling.sigs.k8s.io"

	// SelectorLabel is the default selector label for Pods belonging to a given AppGroup (e.g., app = myApp)
	SelectorLabel = "app"
)

func TestTopologicalSortPlugin(t *testing.T) {
	t.Log("Creating API Server...")
	// Start API Server with apiextensions supported.
	server := apiservertesting.StartTestServerOrDie(
		t, apiservertesting.NewDefaultTestServerOptions(),
		[]string{"--disable-admission-plugins=ServiceAccount,TaintNodesByCondition,Priority", "--runtime-config=api/all=true"},
		testfwk.SharedEtcd(),
	)
	testCtx := &testutil.TestContext{}
	testCtx.Ctx, testCtx.CancelFn = context.WithCancel(context.Background())
	testCtx.CloseFn = func() { server.TearDownFn() }

	t.Log("Creating AppGroup CRD...")
	apiExtensionClient := apiextensionsclient.NewForConfigOrDie(server.ClientConfig)
	ctx := testCtx.Ctx
	if _, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, makeAppGroupCRD(), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	server.ClientConfig.ContentType = "application/json"
	testCtx.KubeConfig = server.ClientConfig
	cs := kubernetes.NewForConfigOrDie(testCtx.KubeConfig)
	testCtx.ClientSet = cs
	extClient := versioned.NewForConfigOrDie(testCtx.KubeConfig)

	if err := wait.Poll(100*time.Millisecond, 3*time.Second, func() (done bool, err error) {
		groupList, _, err := cs.ServerGroupsAndResources()
		if err != nil {
			return false, nil
		}
		for _, group := range groupList {
			if group.Name == scheduling.GroupName {
				t.Log("The AppGroup CRD is ready to serve")
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

	cfg.Profiles[0].PluginConfig = append(cfg.Profiles[0].PluginConfig, schedapi.PluginConfig{
		Name: topologicalsort.Name,
		Args: &schedconfig.TopologicalSortArgs{
			Namespaces: []string{"default"},
		},
	})

	ns, err := cs.CoreV1().Namespaces().Create(ctx, &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("integration-test-%v", string(uuid.NewUUID()))}}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create integration test ns: %v", err)
	}

	testCtx = util.InitTestSchedulerWithOptions(
		t,
		testCtx,
		true,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{topologicalsort.Name: topologicalsort.New}),
	)
	t.Log("Init scheduler success")
	defer testutil.CleanupTest(t, testCtx)

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
	node, err = cs.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create Node %q: %v", nodeName, err)
	}

	basicAppGroup := v1alpha1.AppGroupWorkloadList{
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1", APIVersion: "apps/v1", Namespace: "default"},
			Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{
				Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2", APIVersion: "apps/v1", Namespace: "default"}}}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2", APIVersion: "apps/v1", Namespace: "default"},
			Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{
				Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3", APIVersion: "apps/v1", Namespace: "default"}}}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3", APIVersion: "apps/v1", Namespace: "default"}},
	}

	basicTopologyOrder := v1alpha1.AppGroupTopologyList{
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1", APIVersion: "apps/v1", Namespace: "default"}, Index: 1},
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2", APIVersion: "apps/v1", Namespace: "default"}, Index: 2},
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3", APIVersion: "apps/v1", Namespace: "default"}, Index: 3},
	}

	pause := imageutils.GetPauseImageName()
	for _, tt := range []struct {
		name     string
		pods     []*v1.Pod
		appGroup []*v1alpha1.AppGroup
		podNames []string
	}{
		{
			name: "basic Appgroup",
			pods: []*v1.Pod{
				WithContainer(st.MakePod().Namespace(ns.Name).Name("P1-1").Req(map[v1.ResourceName]string{v1.ResourceMemory: "50"}).Priority(
					midPriority).Label(AppGroupLabel, "basic").Label(SelectorLabel, "P1").ZeroTerminationGracePeriod().Obj(), pause),
				WithContainer(st.MakePod().Namespace(ns.Name).Name("P2-1").Req(map[v1.ResourceName]string{v1.ResourceMemory: "50"}).Priority(
					midPriority).Label(AppGroupLabel, "basic").Label(SelectorLabel, "P2").ZeroTerminationGracePeriod().Obj(), pause),
				WithContainer(st.MakePod().Namespace(ns.Name).Name("P3-1").Req(map[v1.ResourceName]string{v1.ResourceMemory: "50"}).Priority(
					midPriority).Label(AppGroupLabel, "basic").Label(SelectorLabel, "P3").ZeroTerminationGracePeriod().Obj(), pause),
			},
			appGroup: []*v1alpha1.AppGroup{
				makeAG("basic", 3, ns.Name, "KahnSort", basicAppGroup, basicTopologyOrder, nil),
			},
			podNames: []string{"P1-1", "P2-1", "P3-1"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Start topologicalSort integration test %v ...", tt.name)
			defer cleanupAppGroups(ctx, extClient, tt.appGroup)
			// create AppGroup
			if err := createAppGroups(ctx, extClient, tt.appGroup); err != nil {
				t.Fatal(err)
			}
			defer testutil.CleanupPods(cs, t, tt.pods)

			// Create Pods
			t.Logf("Start by Creating Pods... ")
			for i := range tt.pods {
				t.Logf("Creating Pod %q", tt.pods[i].Name)
				_, err = cs.CoreV1().Pods(ns.Name).Create(context.TODO(), tt.pods[i], metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Failed to create Pod %q: %v", tt.pods[i].Name, err)
				}
			}

			// Wait for all Pods are in the scheduling queue.
			err = wait.Poll(time.Millisecond*200, wait.ForeverTestTimeout, func() (bool, error) {
				if len(testCtx.Scheduler.SchedulingQueue.PendingPods()) == len(tt.pods) {
					return true, nil
				}
				return false, nil
			})
			if err != nil {
				t.Fatal(err)
			}

			// Expect Pods are popped as in the TopologyOrder defined by the AppGroup.
			for i := len(tt.podNames) - 1; i >= 0; i-- {
				podInfo := testCtx.Scheduler.NextPod()
				if podInfo.Pod.Name != tt.podNames[i] {
					t.Errorf("Expect Pod %q, but got %q", tt.podNames[i], podInfo.Pod.Name)
				} else {
					t.Logf("Pod %q is popped out as expected.", podInfo.Pod.Name)
				}
			}
		})
	}
}

func makeAG(agName string, numMembers int32, namespace string, topologySortingAlgorithm string, workloadList v1alpha1.AppGroupWorkloadList, topologyOrder []v1alpha1.AppGroupTopologyInfo, createTime *metav1.Time) *v1alpha1.AppGroup {
	ag := &v1alpha1.AppGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              agName,
			Namespace:         namespace,
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: v1alpha1.AppGroupSpec{
			NumMembers:               numMembers,
			TopologySortingAlgorithm: topologySortingAlgorithm,
			Workloads:                workloadList,
		},
		Status: v1alpha1.AppGroupStatus{
			RunningWorkloads:  0,
			ScheduleStartTime: metav1.Time{Time: time.Now()},
			TopologyOrder:     topologyOrder,
		},
	}
	if createTime != nil {
		ag.CreationTimestamp = *createTime
	}
	return ag
}

func makeAppGroupCRD() *apiextensionsv1.CustomResourceDefinition {
	var min = 1.0
	var maxNetworkCost = 1000.0
	var minNetworkCost = 0.0

	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "app-group.scheduling.sigs.k8s.io",
			Annotations: map[string]string{
				"api-approved.kubernetes.io": "TO BE DEFINED",
			},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: scheduling.GroupName,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"numMembers": {
										Type:    "integer",
										Minimum: &min,
									},
									"topologySortingAlgorithm": {
										Type: "string",
									},
									"workloads": {
										Type: "object",
										AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
											Schema: &apiextensionsv1.JSONSchemaProps{
												Type: "object",
												Properties: map[string]apiextensionsv1.JSONSchemaProps{
													"workload": {
														Type: "object",
														AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
															Schema: &apiextensionsv1.JSONSchemaProps{
																Type: "object",
																Properties: map[string]apiextensionsv1.JSONSchemaProps{
																	"kind": {
																		Type: "string",
																	},
																	"apiVersion": {
																		Type: "string",
																	},
																	"namespace": {
																		Type: "string",
																	},
																	"name": {
																		Type: "string",
																	},
																},
															},
														},
													},
													"dependencies": {
														Type: "object",
														Properties: map[string]apiextensionsv1.JSONSchemaProps{
															"workload": {
																Type: "object",
																AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
																	Schema: &apiextensionsv1.JSONSchemaProps{
																		Type: "object",
																		Properties: map[string]apiextensionsv1.JSONSchemaProps{
																			"kind": {
																				Type: "string",
																			},
																			"apiVersion": {
																				Type: "string",
																			},
																			"namespace": {
																				Type: "string",
																			},
																			"name": {
																				Type: "string",
																			},
																		},
																	},
																},
															},
															"minBandwidth": {
																Type: "object",
																AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
																	Schema: &apiextensionsv1.JSONSchemaProps{
																		Type: "string",
																	},
																},
															},
															"maxNetworkCost": {
																Type:    "integer",
																Minimum: &minNetworkCost,
																Maximum: &maxNetworkCost,
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}}},
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:       "AppGroup",
				Plural:     "appgroups",
				ShortNames: []string{"ag", "ags"},
			},
		},
	}
}

func createAppGroups(ctx context.Context, client versioned.Interface, appGroups []*v1alpha1.AppGroup) error {
	for _, ag := range appGroups {
		_, err := client.SchedulingV1alpha1().AppGroups(ag.Namespace).Create(ctx, ag, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func cleanupAppGroups(ctx context.Context, client versioned.Interface, appGroups []*v1alpha1.AppGroup) {
	for _, ag := range appGroups {
		client.SchedulingV1alpha1().AppGroups(ag.Namespace).Delete(ctx, ag.Name, metav1.DeleteOptions{})
	}
}
