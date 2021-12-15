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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	testClientSet "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"math"
	"math/rand"
	"reflect"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	fake "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/fake"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
	"testing"
	"time"
)

var _ framework.SharedLister = &testSharedLister{}

type testSharedLister struct {
	nodes       []*v1.Node
	nodeInfos   []*framework.NodeInfo
	nodeInfoMap map[string]*framework.NodeInfo
}

func (f *testSharedLister) NodeInfos() framework.NodeInfoLister {
	return f
}

func (f *testSharedLister) List() ([]*framework.NodeInfo, error) {
	return f.nodeInfos, nil
}

func (f *testSharedLister) HavePodsWithAffinityList() ([]*framework.NodeInfo, error) {
	return nil, nil
}

func (f *testSharedLister) HavePodsWithRequiredAntiAffinityList() ([]*framework.NodeInfo, error) {
	return nil, nil
}

func (f *testSharedLister) Get(nodeName string) (*framework.NodeInfo, error) {
	return f.nodeInfoMap[nodeName], nil
}

func TestNodeNetworkFitPlugin(t *testing.T) {
	// Create AppGroup CRD
	appGroup := &v1alpha1.AppGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "basic", Namespace: "default"},
		Spec: v1alpha1.AppGroupSpec{
			NumMembers:               3,
			TopologySortingAlgorithm: "KahnSort",
			Pods: v1alpha1.AppGroupPodList{
				v1alpha1.AppGroupPod{PodName: "P1", Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{PodName: "P2", MaxNetworkCost: 15}}},
				v1alpha1.AppGroupPod{PodName: "P2", Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{PodName: "P3", MaxNetworkCost: 8}}},
				v1alpha1.AppGroupPod{PodName: "P3"}}},
		Status: v1alpha1.AppGroupStatus{
			RunningPods: 3, PodsScheduled: v1alpha1.ScheduledList{
				v1alpha1.ScheduledInfo{PodName: "P1", ReplicaID: "id1", Hostname: "n-2"},
				v1alpha1.ScheduledInfo{PodName: "P2", ReplicaID: "id2", Hostname: "n-5"},
				v1alpha1.ScheduledInfo{PodName: "P3", ReplicaID: "id3", Hostname: "n-8"},
			}, ScheduleStartTime: metav1.Time{time.Now()}, TopologyCalculationTime: metav1.Time{time.Now()},
			TopologyOrder: v1alpha1.TopologyList{
				v1alpha1.TopologyInfo{PodName: "P1", Index: 1},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 2},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 3}},
		},
	}

	pods:= []*v1.Pod{
		makePodAllocated("P1", "n-2", 0, "basic", nil, nil),
		makePodAllocated("P2", "n-5", 0, "basic", nil, nil),
		makePodAllocated("P3", "n-8", 0, "basic", nil, nil),
	}

	// Create Network Topology CRD
	networkTopology := &v1alpha1.NetworkTopology{
		ObjectMeta: metav1.ObjectMeta{Name: "nt-test", Namespace: "default"},
		Spec: v1alpha1.NetworkTopologySpec{Weights: v1alpha1.WeightList{
			v1alpha1.WeightInfo{Name: "UserDefined", RegionCostList: v1alpha1.CostList{
				v1alpha1.OriginInfo{Origin: "us-west-1", Costs: []v1alpha1.CostInfo{{Destination: "us-east-1", NetworkCost: 20}}},
				v1alpha1.OriginInfo{Origin: "us-east-1", Costs: []v1alpha1.CostInfo{{Destination: "us-west-1", NetworkCost: 20}}}},
				ZoneCostList: v1alpha1.CostList{
					v1alpha1.OriginInfo{Origin: "Z1", Costs: []v1alpha1.CostInfo{{Destination: "Z2", NetworkCost: 5}}},
					v1alpha1.OriginInfo{Origin: "Z2", Costs: []v1alpha1.CostInfo{{Destination: "Z1", NetworkCost: 5}}},
					v1alpha1.OriginInfo{Origin: "Z3", Costs: []v1alpha1.CostInfo{{Destination: "Z4", NetworkCost: 10}}},
					v1alpha1.OriginInfo{Origin: "Z4", Costs: []v1alpha1.CostInfo{{Destination: "Z3", NetworkCost: 10}}},
				}}}},
	}

	// Create Nodes
	nodes := []*v1.Node{
		st.MakeNode().Name("n-1").Label(v1.LabelTopologyRegion, "us-west-1").Label(v1.LabelTopologyZone, "Z1").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
		st.MakeNode().Name("n-2").Label(v1.LabelTopologyRegion, "us-west-1").Label(v1.LabelTopologyZone, "Z1").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
		st.MakeNode().Name("n-3").Label(v1.LabelTopologyRegion, "us-west-1").Label(v1.LabelTopologyZone, "Z2").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
		st.MakeNode().Name("n-4").Label(v1.LabelTopologyRegion, "us-west-1").Label(v1.LabelTopologyZone, "Z2").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
		st.MakeNode().Name("n-5").Label(v1.LabelTopologyRegion, "us-east-1").Label(v1.LabelTopologyZone, "Z3").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
		st.MakeNode().Name("n-6").Label(v1.LabelTopologyRegion, "us-east-1").Label(v1.LabelTopologyZone, "Z3").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
		st.MakeNode().Name("n-7").Label(v1.LabelTopologyRegion, "us-east-1").Label(v1.LabelTopologyZone, "Z4").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
		st.MakeNode().Name("n-8").Label(v1.LabelTopologyRegion, "us-east-1").Label(v1.LabelTopologyZone, "Z4").Capacity(
			map[v1.ResourceName]string{v1.ResourceCPU: "8000m", v1.ResourceMemory: "16Gi"}).Obj(),
	}

	tests := []struct {
		name            string
		agName          string
		appGroup        *v1alpha1.AppGroup
		networkTopology *v1alpha1.NetworkTopology
		pod             *v1.Pod
		pods			[]*v1.Pod
		nodes           []*v1.Node
		nodeToFilter    *v1.Node
		wantStatus      *framework.Status
	}{
		{
			name:            "AppGroup: basic, P1 to allocate, n-1 to filter: n-1 does not meet network requirements",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "basic", nil, nil),
			nodes:           nodes,
			wantStatus:      framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node n-1 does not meet several network requirements from Pod dependencies: OK: 0 NotOK: 1")),
			nodeToFilter:    nodes[0],
			pods:			 pods,
		},
		{
			name:            "AppGroup: basic, P1 to allocate, n-6 to filter: n-6 meets network requirements",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "basic", nil, nil),
			nodes:           nodes,
			wantStatus:      nil,
			nodeToFilter:    nodes[5],
			pods:			 pods,
		},
		{
			name:            "AppGroup: basic, P2 to allocate, n-5 to filter: n-5 does not meet network requirements",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P2", 0, "basic", nil, nil),
			nodes:           nodes,
			wantStatus:      framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node n-5 does not meet several network requirements from Pod dependencies: OK: 0 NotOK: 1")),
			nodeToFilter:    nodes[4],
			pods:			 pods,
		},
		{
			name:            "AppGroup: basic, P2 to allocate, n-7 to filter: n-7 meets network requirements",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P2", 0, "basic", nil, nil),
			nodes:           nodes,
			wantStatus:      nil,
			nodeToFilter:    nodes[6],
			pods:			 pods,
		},
		{
			name:            "AppGroup: basic, P3 to allocate, no dependencies, n-1 to filter: n-1 meets network requirements",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P3", 0, "basic", nil, nil),
			nodes:           nodes,
			wantStatus:      nil,
			nodeToFilter:    nodes[0],
			pods:			 pods,
		},
		{
			name:            "AppGroup: basic, P10 to allocate (Different AppGroup!), n-1 to filter: n-1 meets network requirements",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P10", 0, "", nil, nil),
			nodes:           nodes,
			wantStatus:      nil,
			nodeToFilter:    nodes[0],
			pods:			 pods,
		},
		{
			name:   "AppGroup: basic, P1 to allocate, n-1 to filter, multiple dependencies: n-1 does not meet network requirements",
			agName: "basic",
			appGroup: appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "basic", nil, nil),
			nodes:           nodes,
			wantStatus:      framework.NewStatus(framework.Unschedulable, fmt.Sprintf("Node n-1 does not meet several network requirements from Pod dependencies: OK: 0 NotOK: 1")),
			nodeToFilter:    nodes[0],
			pods:			 pods,
		},
		{
			name:   "AppGroup: basic, P1 to allocate, n-6 to filter, multiple dependencies: n-6 meets network requirements",
			agName: "basic",
			appGroup: appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "basic", nil, nil),
			nodes:           nodes,
			wantStatus:      nil,
			nodeToFilter:    nodes[5],
			pods:			 pods,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// init listers
			fakeClient := fake.NewSimpleClientset()
			fakeAgInformer := schedinformer.NewSharedInformerFactory(fakeClient, 0).Scheduling().V1alpha1().AppGroups()
			fakeNTInformer := schedinformer.NewSharedInformerFactory(fakeClient, 0).Scheduling().V1alpha1().NetworkTopologies()

			// add CRDs
			fakeAgInformer.Informer().GetStore().Add(tt.appGroup)
			fakeNTInformer.Informer().GetStore().Add(tt.networkTopology)

			agLister := fakeAgInformer.Lister()
			ntLister := fakeNTInformer.Lister()

			// create plugin
			ctx := context.Background()
			cs := testClientSet.NewSimpleClientset()

			informerFactory := informers.NewSharedInformerFactory(cs, 0)

			snapshot := newTestSharedLister(nil, nodes)

			podInformer := informerFactory.Core().V1().Pods()
			podLister := podInformer.Lister()
			informerFactory.Start(ctx.Done())

			for _, p := range tt.pods{
				_, err := cs.CoreV1().Pods("default").Create(ctx, p, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Failed to create Pod %q: %v", p.Name, err)
				}
				//t.Logf("Pod %v created  \n", p.Name)
			}

			registeredPlugins := []st.RegisterPluginFunc{
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
			}

			fh, _ := st.NewFramework(registeredPlugins, "default-scheduler", runtime.WithClientSet(cs),
				runtime.WithInformerFactory(informerFactory), runtime.WithSnapshotSharedLister(snapshot))

			pl := &NodeNetworkCostFit{
				handle:     fh,
				agLister:   &agLister,
				podLister:  podLister,
				ntLister:   &ntLister,
				namespaces: []string{"default"},
				weightsName:  "UserDefined",
				ntName:     "nt-test",
			}

			// Without sleep sometimes the pods are not created in the api in time
			time.Sleep(1 * time.Second)

			//t.Logf("Test: %v \n", tt.name)
			t.Logf("PodsScheduled: %v", tt.appGroup.Status.PodsScheduled)

			t.Logf("Pod to schedule: %v / AppGroup: %v", tt.pod.Name, tt.appGroup.Name)
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(tt.nodeToFilter)

			t.Logf("Node: %v", nodeInfo.Node().Name)
			gotStatus := pl.Filter(context.Background(), framework.NewCycleState(), tt.pod, nodeInfo)

			t.Logf("Status: %v", gotStatus)
			if !reflect.DeepEqual(gotStatus, tt.wantStatus) {
				t.Errorf("status does not match: %v, want: %v", gotStatus, tt.wantStatus)
			}
		})
	}
}

func BenchmarkNodeNetworkCostFitPlugin(b *testing.B) {
	// Create AppGroup CRD -> OnlineBoutique
	appGroup := &v1alpha1.AppGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "OnlineBoutique", Namespace: "default"},
		Spec: v1alpha1.AppGroupSpec{NumMembers: 10, TopologySortingAlgorithm: "KahnSort",
			Pods: v1alpha1.AppGroupPodList{v1alpha1.AppGroupPod{PodName: "P1", // frontend
				Dependencies: v1alpha1.DependenciesList{
					v1alpha1.DependenciesInfo{PodName: "P2"},
					v1alpha1.DependenciesInfo{PodName: "P3"},
					v1alpha1.DependenciesInfo{PodName: "P4"},
					v1alpha1.DependenciesInfo{PodName: "P6"},
					v1alpha1.DependenciesInfo{PodName: "P8"},
					v1alpha1.DependenciesInfo{PodName: "P9"},
					v1alpha1.DependenciesInfo{PodName: "P10"},
				}},
				v1alpha1.AppGroupPod{PodName: "P2"}, // cartService
				v1alpha1.AppGroupPod{PodName: "P3"}, // productCatalogService
				v1alpha1.AppGroupPod{PodName: "P4"}, // currencyService
				v1alpha1.AppGroupPod{PodName: "P5"}, // paymentService
				v1alpha1.AppGroupPod{PodName: "P6"}, // shippingService
				v1alpha1.AppGroupPod{PodName: "P7"}, // emailService
				v1alpha1.AppGroupPod{PodName: "P8", // checkoutService
					Dependencies: v1alpha1.DependenciesList{
						v1alpha1.DependenciesInfo{PodName: "P2"},
						v1alpha1.DependenciesInfo{PodName: "P3"},
						v1alpha1.DependenciesInfo{PodName: "P4"},
						v1alpha1.DependenciesInfo{PodName: "P5"},
						v1alpha1.DependenciesInfo{PodName: "P6"},
						v1alpha1.DependenciesInfo{PodName: "P7"},
					}},
				v1alpha1.AppGroupPod{PodName: "P9", // recommendationService
					Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{PodName: "P3"}}},
				v1alpha1.AppGroupPod{PodName: "P10"}, // adService
			},
		},
		Status: v1alpha1.AppGroupStatus{
			ScheduleStartTime:       metav1.Time{time.Now()},
			TopologyCalculationTime: metav1.Time{time.Now()},
			TopologyOrder: v1alpha1.TopologyList{
				v1alpha1.TopologyInfo{PodName: "P1", Index: 1},
				v1alpha1.TopologyInfo{PodName: "P10", Index: 2},
				v1alpha1.TopologyInfo{PodName: "P9", Index: 3},
				v1alpha1.TopologyInfo{PodName: "P8", Index: 4},
				v1alpha1.TopologyInfo{PodName: "P7", Index: 5},
				v1alpha1.TopologyInfo{PodName: "P6", Index: 6},
				v1alpha1.TopologyInfo{PodName: "P5", Index: 7},
				v1alpha1.TopologyInfo{PodName: "P4", Index: 8},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 9},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 10},
			}},
	}

	podNames := []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"}
	regionNames := []string{"R1", "R2", "R3", "R4", "R5"}
	zoneNames := []string{"Z1", "Z2", "Z3", "Z4", "Z5", "Z6", "Z7", "Z8", "Z9", "Z10"}

	pods:= []*v1.Pod{
		makePodAllocated("P1", "n-2", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P2", "n-5", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P3", "n-1", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P4", "n-9", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P5", "n-5", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P6", "n-1", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P7", "n-2", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P8", "n-5", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P9", "n-1", 0, "OnlineBoutique", nil, nil),
		makePodAllocated("P10", "n-2", 0, "OnlineBoutique", nil, nil),
	}

	// Create Network Topology CRD
	networkTopology := &v1alpha1.NetworkTopology{
		ObjectMeta: metav1.ObjectMeta{Name: "nt-test", Namespace: "default"},
		Spec: v1alpha1.NetworkTopologySpec{Weights: v1alpha1.WeightList{
			v1alpha1.WeightInfo{Name: "UserDefined",
				RegionCostList: v1alpha1.CostList{
					v1alpha1.OriginInfo{Origin: "R1", Costs: []v1alpha1.CostInfo{
						{Destination: "R2", NetworkCost: 50},
						{Destination: "R3", NetworkCost: 50},
						{Destination: "R4", NetworkCost: 50},
						{Destination: "R5", NetworkCost: 50}},
					},
					v1alpha1.OriginInfo{Origin: "R2", Costs: []v1alpha1.CostInfo{
						{Destination: "R1", NetworkCost: 50},
						{Destination: "R3", NetworkCost: 50},
						{Destination: "R4", NetworkCost: 50},
						{Destination: "R5", NetworkCost: 50}},
					},
					v1alpha1.OriginInfo{Origin: "R3", Costs: []v1alpha1.CostInfo{
						{Destination: "R1", NetworkCost: 50},
						{Destination: "R2", NetworkCost: 50},
						{Destination: "R4", NetworkCost: 50},
						{Destination: "R5", NetworkCost: 50}},
					},
					v1alpha1.OriginInfo{Origin: "R4", Costs: []v1alpha1.CostInfo{
						{Destination: "R1", NetworkCost: 50},
						{Destination: "R2", NetworkCost: 50},
						{Destination: "R3", NetworkCost: 50},
						{Destination: "R5", NetworkCost: 50}},
					},
					v1alpha1.OriginInfo{Origin: "R5", Costs: []v1alpha1.CostInfo{
						{Destination: "R1", NetworkCost: 50},
						{Destination: "R2", NetworkCost: 50},
						{Destination: "R3", NetworkCost: 50},
						{Destination: "R4", NetworkCost: 50}},
					},
				},
				ZoneCostList: v1alpha1.CostList{
					v1alpha1.OriginInfo{Origin: "Z1", Costs: []v1alpha1.CostInfo{
						{Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10}, {Destination: "Z4", NetworkCost: 10},
						{Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10}, {Destination: "Z7", NetworkCost: 10},
						{Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z2", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10}, {Destination: "Z4", NetworkCost: 10},
						{Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10}, {Destination: "Z7", NetworkCost: 10},
						{Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z3", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z4", NetworkCost: 10},
						{Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10}, {Destination: "Z7", NetworkCost: 10},
						{Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z4", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10},
						{Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10}, {Destination: "Z7", NetworkCost: 10},
						{Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z5", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10},
						{Destination: "Z4", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10}, {Destination: "Z7", NetworkCost: 10},
						{Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z6", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10},
						{Destination: "Z4", NetworkCost: 10}, {Destination: "Z5", NetworkCost: 10}, {Destination: "Z7", NetworkCost: 10},
						{Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z7", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10},
						{Destination: "Z4", NetworkCost: 10}, {Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10},
						{Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z8", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10},
						{Destination: "Z4", NetworkCost: 10}, {Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10},
						{Destination: "Z7", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z9", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10},
						{Destination: "Z4", NetworkCost: 10}, {Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10},
						{Destination: "Z7", NetworkCost: 10}, {Destination: "Z8", NetworkCost: 10}, {Destination: "Z10", NetworkCost: 10}},
					},
					v1alpha1.OriginInfo{Origin: "Z10", Costs: []v1alpha1.CostInfo{
						{Destination: "Z1", NetworkCost: 10}, {Destination: "Z2", NetworkCost: 10}, {Destination: "Z3", NetworkCost: 10},
						{Destination: "Z4", NetworkCost: 10}, {Destination: "Z5", NetworkCost: 10}, {Destination: "Z6", NetworkCost: 10},
						{Destination: "Z7", NetworkCost: 10}, {Destination: "Z8", NetworkCost: 10}, {Destination: "Z9", NetworkCost: 10}},
					},
				},
			}},
		},
	}

	tests := []struct {
		name            string
		nodesNum        int64
		dependenciesNum int32
		agName          string
		podNames        []string
		regionNames     []string
		zoneNames       []string
		appGroup        *v1alpha1.AppGroup
		networkTopology *v1alpha1.NetworkTopology
		pod             *v1.Pod
		pods            []*v1.Pod
	}{
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 10 nodes, 1 pod to allocate",
			nodesNum:        10,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 100 nodes, 1 pod to allocate",
			nodesNum:        100,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 500 nodes, 1 pod to allocate",
			nodesNum:        500,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 1000 nodes, 1 pod to allocate",
			nodesNum:        1000,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 2000 nodes, 1 pod to allocate",
			nodesNum:        2000,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 3000 nodes, 1 pod to allocate",
			nodesNum:        3000,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 5000 nodes, 1 pod to allocate",
			nodesNum:        5000,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
		{
			name:            "AppGroup: OnlineBoutique, 10 pods allocated, 10000 nodes, 1 pod to allocate",
			nodesNum:        10000,
			dependenciesNum: 10,
			agName:          "OnlineBoutique",
			podNames:        podNames,
			regionNames:     regionNames,
			zoneNames:       zoneNames,
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "OnlineBoutique", nil, nil),
			pods:            pods,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// init listers
			fakeClient := fake.NewSimpleClientset()
			fakeAgInformer := schedinformer.NewSharedInformerFactory(fakeClient, 0).Scheduling().V1alpha1().AppGroups()
			fakeNTInformer := schedinformer.NewSharedInformerFactory(fakeClient, 0).Scheduling().V1alpha1().NetworkTopologies()

			// init nodes
			nodes := getNodes(tt.nodesNum, tt.regionNames, tt.zoneNames)

			// Create dependencies
			//tt.appGroup.Status.RunningPods = tt.dependenciesNum
			//tt.appGroup.Status.PodsScheduled = createDependencies(int64(tt.dependenciesNum), tt.podNames, nodes)

			// add CRDs
			agLister := fakeAgInformer.Lister()
			ntLister := fakeNTInformer.Lister()

			_ = fakeAgInformer.Informer().GetStore().Add(tt.appGroup)
			_ = fakeNTInformer.Informer().GetStore().Add(tt.networkTopology)

			// create plugin
			ctx := context.Background()
			cs := testClientSet.NewSimpleClientset()

			informerFactory := informers.NewSharedInformerFactory(cs, 0)

			snapshot := newTestSharedLister(nil, nodes)

			podInformer := informerFactory.Core().V1().Pods()
			podLister := podInformer.Lister()

			informerFactory.Start(ctx.Done())

			for _, p := range tt.pods{
				_, err := cs.CoreV1().Pods("default").Create(ctx, p, metav1.CreateOptions{})
				if err != nil {
					b.Fatalf("Failed to create Pod %q: %v", p.Name, err)
				}
				//b.Logf("Pod %v created  \n", p.Name)
			}

			registeredPlugins := []st.RegisterPluginFunc{
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
			}

			fh, _ := st.NewFramework(registeredPlugins, "default-scheduler", runtime.WithClientSet(cs),
				runtime.WithInformerFactory(informerFactory), runtime.WithSnapshotSharedLister(snapshot))

			pl := &NodeNetworkCostFit{
				handle:     fh,
				agLister:   &agLister,
				podLister:  podLister,
				ntLister:   &ntLister,
				namespaces: []string{"default"},
				weightsName:  "UserDefined",
				ntName:     "nt-test",
			}

			// Without sleep sometimes the pods are not created in the api in time
			time.Sleep(1 * time.Second)

			state := framework.NewCycleState()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				filter := func(i int) {
					nodeInfo := framework.NewNodeInfo()
					nodeInfo.SetNode(nodes[i])
					_ = pl.Filter(ctx, state, tt.pod, nodeInfo)
					// b.Logf("Status: %v", gotStatus)
				}
				Until(ctx, len(nodes), filter)
			}
		})
	}
}

func getNodes(nodesNum int64, regionNames []string, zoneNames []string) (nodes []*v1.Node) {
	nodeResources := map[v1.ResourceName]string{
		v1.ResourceCPU:    "8000m",
		v1.ResourceMemory: "16Gi",
	}
	var i int64
	for i = 0; i < nodesNum; i++ {
		regionId := randomInt(0, len(regionNames))
		zoneId := randomInt(0, len(zoneNames))
		region := regionNames[regionId]
		zone := zoneNames[zoneId]

		nodes = append(nodes, st.MakeNode().Name(
			fmt.Sprintf("n-%v", (i+1))).Label(v1.LabelTopologyRegion, region).Label(v1.LabelTopologyZone, zone).Capacity(nodeResources).Obj())
	}
	return nodes
}

const parallelism = 16

// Copied from k8s internal package
// chunkSizeFor returns a chunk size for the given number of items to use for
// parallel work. The size aims to produce good CPU utilization.
func chunkSizeFor(n int) workqueue.Options {
	s := int(math.Sqrt(float64(n)))
	if r := n/parallelism + 1; s > r {
		s = r
	} else if s < 1 {
		s = 1
	}
	return workqueue.WithChunkSize(s)
}

// Copied from k8s internal package
// Until is a wrapper around workqueue.ParallelizeUntil to use in scheduling algorithms.
func Until(ctx context.Context, pieces int, doWorkPiece workqueue.DoWorkPieceFunc) {
	workqueue.ParallelizeUntil(ctx, parallelism, pieces, doWorkPiece, chunkSizeFor(pieces))
}

func createDependencies(dependenciesNum int64, podNames []string, nodes []*v1.Node) []v1alpha1.ScheduledInfo {
	var list []v1alpha1.ScheduledInfo
	var i int64
	for i = 0; i < dependenciesNum; i++ {
		podIndex := randomInt(0, len(podNames))
		nodeIndex := randomInt(0, len(nodes))
		podName := podNames[podIndex]
		nodeName := nodes[nodeIndex].Name

		list = append(list, v1alpha1.ScheduledInfo{
			PodName:   podName,
			ReplicaID: fmt.Sprint(i + 1),
			Hostname:  nodeName,
		})
	}
	return list
}

func randomInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func newTestSharedLister(pods []*v1.Pod, nodes []*v1.Node) *testSharedLister {
	nodeInfoMap := make(map[string]*framework.NodeInfo)
	nodeInfos := make([]*framework.NodeInfo, 0)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if _, ok := nodeInfoMap[nodeName]; !ok {
			nodeInfoMap[nodeName] = framework.NewNodeInfo()
		}
		nodeInfoMap[nodeName].AddPod(pod)
	}
	for _, node := range nodes {
		if _, ok := nodeInfoMap[node.Name]; !ok {
			nodeInfoMap[node.Name] = framework.NewNodeInfo()
		}
		nodeInfoMap[node.Name].SetNode(node)
	}

	for _, v := range nodeInfoMap {
		nodeInfos = append(nodeInfos, v)
	}

	return &testSharedLister{
		nodes:       nodes,
		nodeInfos:   nodeInfos,
		nodeInfoMap: nodeInfoMap,
	}
}

func makePod(name string, priority int32, appGroup string, requests, limits v1.ResourceList) *v1.Pod {
	label := make(map[string]string)
	label[util.AppGroupLabel] = appGroup
	label[util.DeploymentLabel] = name

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: label,
		},
		Spec: v1.PodSpec{
			Priority: &priority,
			Containers: []v1.Container{
				{
					Name: name,
					Resources: v1.ResourceRequirements{
						Requests: requests,
						Limits:   limits,
					},
				},
			},
		},
	}
}

func makePodAllocated(name string, hostname string, priority int32, appGroup string, requests, limits v1.ResourceList) *v1.Pod {
	label := make(map[string]string)
	label[util.AppGroupLabel] = appGroup
	label[util.DeploymentLabel] = name

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: label,
		},
		Spec: v1.PodSpec{
			NodeName: hostname,
			Priority: &priority,
			Containers: []v1.Container{
				{
					Name: name,
					Resources: v1.ResourceRequirements{
						Requests: requests,
						Limits:   limits,
					},
				},
			},
		},
	}
}
