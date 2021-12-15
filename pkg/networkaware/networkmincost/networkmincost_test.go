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
	"github.com/stretchr/testify/assert"
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

func TestNetworkMinCostPlugin(t *testing.T) {
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
				v1alpha1.ScheduledInfo{PodName: "P3", ReplicaID: "id3", Hostname: "n-1"},
			}, ScheduleStartTime: metav1.Time{time.Now()}, TopologyCalculationTime: metav1.Time{time.Now()},
			TopologyOrder: v1alpha1.TopologyList{
				v1alpha1.TopologyInfo{PodName: "P1", Index: 1},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 2},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 3}},
		},
	}

	podNames := []string{"P1", "P2", "P3"}

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
		name               	string
		agName             	string
		dependenciesNum 	int32
		podNames           	[]string
		pods                []*v1.Pod
		appGroup           	*v1alpha1.AppGroup
		networkTopology    	*v1alpha1.NetworkTopology
		nodes              	[]*v1.Node
		pod               	 *v1.Pod
		want               	*framework.Status
		wantedScoresBefore 	framework.NodeScoreList
		wantedScoresAfter  	framework.NodeScoreList
		nodeToScore        	*v1.Node
	}{
		{
			name:            "AppGroup: basic, P1 to allocate, 8 nodes to score",
			agName:          "basic",
			appGroup:        appGroup,
			dependenciesNum: 5,
			podNames:		 podNames,

			pods: []*v1.Pod{
				makePodAllocated("P1", "n-2", 0, "basic", nil, nil),
				makePodAllocated("P2", "n-5", 0, "basic", nil, nil),
				makePodAllocated("P3", "n-1", 0, "basic", nil, nil),
			},
			networkTopology: networkTopology,
			pod:             makePod("P1", 0, "basic", nil, nil),
			nodes:           nodes,
			want:            nil,
			wantedScoresBefore: framework.NodeScoreList{
				framework.NodeScore{Name: nodes[0].Name, Score: 20},
				framework.NodeScore{Name: nodes[1].Name, Score: 20},
				framework.NodeScore{Name: nodes[2].Name, Score: 20},
				framework.NodeScore{Name: nodes[3].Name, Score: 20},
				framework.NodeScore{Name: nodes[4].Name, Score: 0},
				framework.NodeScore{Name: nodes[5].Name, Score: 1},
				framework.NodeScore{Name: nodes[6].Name, Score: 10},
				framework.NodeScore{Name: nodes[7].Name, Score: 10},
			},
			wantedScoresAfter: framework.NodeScoreList{
				framework.NodeScore{Name: nodes[0].Name, Score: 0},
				framework.NodeScore{Name: nodes[1].Name, Score: 0},
				framework.NodeScore{Name: nodes[2].Name, Score: 0},
				framework.NodeScore{Name: nodes[3].Name, Score: 0},
				framework.NodeScore{Name: nodes[4].Name, Score: 100},
				framework.NodeScore{Name: nodes[5].Name, Score: 95},
				framework.NodeScore{Name: nodes[6].Name, Score: 50},
				framework.NodeScore{Name: nodes[7].Name, Score: 50},
			},
		},
		{
			name:            "AppGroup: basic, P2 to allocate, 8 nodes to score",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pod:             makePod("P2", 0, "basic", nil, nil),
			pods: []*v1.Pod{
				makePodAllocated("P1", "n-2", 0, "basic", nil, nil),
				makePodAllocated("P2", "n-5", 0, "basic", nil, nil),
				makePodAllocated("P3", "n-1", 0, "basic", nil, nil),
			},
			nodes:           nodes,
			want:            nil,
			wantedScoresBefore: framework.NodeScoreList{
				framework.NodeScore{Name: nodes[0].Name, Score: 0},
				framework.NodeScore{Name: nodes[1].Name, Score: 1},
				framework.NodeScore{Name: nodes[2].Name, Score: 5},
				framework.NodeScore{Name: nodes[3].Name, Score: 5},
				framework.NodeScore{Name: nodes[4].Name, Score: 20},
				framework.NodeScore{Name: nodes[5].Name, Score: 20},
				framework.NodeScore{Name: nodes[6].Name, Score: 20},
				framework.NodeScore{Name: nodes[7].Name, Score: 20},
			},
			wantedScoresAfter: framework.NodeScoreList{
				framework.NodeScore{Name: nodes[0].Name, Score: 100},
				framework.NodeScore{Name: nodes[1].Name, Score: 95},
				framework.NodeScore{Name: nodes[2].Name, Score: 75},
				framework.NodeScore{Name: nodes[3].Name, Score: 75},
				framework.NodeScore{Name: nodes[4].Name, Score: 0},
				framework.NodeScore{Name: nodes[5].Name, Score: 0},
				framework.NodeScore{Name: nodes[6].Name, Score: 0},
				framework.NodeScore{Name: nodes[7].Name, Score: 0},
			},
		},
		{
			name:            "AppGroup: basic, P3 to allocate, no dependency, 8 nodes to score",
			agName:          "basic",
			appGroup:        appGroup,
			networkTopology: networkTopology,
			pods: []*v1.Pod{
				makePodAllocated("P1", "n-2", 0, "basic", nil, nil),
				makePodAllocated("P2", "n-5", 0, "basic", nil, nil),
				makePodAllocated("P3", "n-1", 0, "basic", nil, nil),
			},
			pod:             makePod("P3", 0, "basic", nil, nil),
			nodes:           nodes,
			want:            nil,
			wantedScoresBefore: framework.NodeScoreList{
				framework.NodeScore{Name: nodes[0].Name, Score: 0},
				framework.NodeScore{Name: nodes[1].Name, Score: 0},
				framework.NodeScore{Name: nodes[2].Name, Score: 0},
				framework.NodeScore{Name: nodes[3].Name, Score: 0},
				framework.NodeScore{Name: nodes[4].Name, Score: 0},
				framework.NodeScore{Name: nodes[5].Name, Score: 0},
				framework.NodeScore{Name: nodes[6].Name, Score: 0},
				framework.NodeScore{Name: nodes[7].Name, Score: 0},
			},
			wantedScoresAfter: framework.NodeScoreList{
				framework.NodeScore{Name: nodes[0].Name, Score: 0},
				framework.NodeScore{Name: nodes[1].Name, Score: 0},
				framework.NodeScore{Name: nodes[2].Name, Score: 0},
				framework.NodeScore{Name: nodes[3].Name, Score: 0},
				framework.NodeScore{Name: nodes[4].Name, Score: 0},
				framework.NodeScore{Name: nodes[5].Name, Score: 0},
				framework.NodeScore{Name: nodes[6].Name, Score: 0},
				framework.NodeScore{Name: nodes[7].Name, Score: 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// init listers
			fakeClient := fake.NewSimpleClientset()
			fakeAgInformer := schedinformer.NewSharedInformerFactory(fakeClient, 0).Scheduling().V1alpha1().AppGroups()
			fakeNTInformer := schedinformer.NewSharedInformerFactory(fakeClient, 0).Scheduling().V1alpha1().NetworkTopologies()

			// add CRDs
			_ = fakeAgInformer.Informer().GetStore().Add(tt.appGroup)
			_ = fakeNTInformer.Informer().GetStore().Add(tt.networkTopology)

			agLister := fakeAgInformer.Lister()
			ntLister := fakeNTInformer.Lister()

			// create plugin
			//var kubeClient = fake.NewSimpleClientset()
			//kubeClient = fake.NewSimpleClientset(nodes[0], nodes[1], nodes[2], nodes[3], nodes[4], nodes[5], nodes[6], nodes[7])

			ctx := context.Background()
			cs := testClientSet.NewSimpleClientset()

			informerFactory := informers.NewSharedInformerFactory(cs, 0)

			snapshot := newTestSharedLister(nil, tt.nodes)

			podInformer := informerFactory.Core().V1().Pods()
			podLister := podInformer.Lister()
			informerFactory.Start(ctx.Done())

			for _, p := range tt.pods{
				_, err := cs.CoreV1().Pods("default").Create(ctx, p, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Failed to create Pod %q: %v", p.Name, err)
				}
				t.Logf("Pod %v created  \n", p.Name)
			}

			registeredPlugins := []st.RegisterPluginFunc{
				st.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				st.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
			}

			fh, _ := st.NewFramework(registeredPlugins, "default-scheduler", runtime.WithClientSet(cs),
				runtime.WithInformerFactory(informerFactory), runtime.WithSnapshotSharedLister(snapshot))

			pl := &NetworkMinCost{
				handle:      fh,
				agLister:    &agLister,
				podLister:   podLister,
				ntLister:    &ntLister,
				namespaces:  []string{"default"},
				weightsName: "UserDefined",
				ntName:      "nt-test",
			}

			// Without sleep sometimes the pods are not created in the api in time
			time.Sleep(1 * time.Second)

			t.Logf("Test: %v \n", tt.name)
			var scoreList framework.NodeScoreList

			state := framework.NewCycleState()

				//t.Logf("PodsScheduled: %v", tt.appGroup.Status.PodsScheduled)
			for _, n := range nodes {
				score, gotStatus := pl.Score(
					ctx,
					state,
					tt.pod, n.Name)
				t.Logf("Pod: %v; Node: %v; score: %v; status: %v; message: %v \n", tt.pod.Name, n.Name, score, gotStatus.Code().String(), gotStatus.Message())

				nodeScore := framework.NodeScore{
					Name:  n.Name,
					Score: score,
				}
				scoreList = append(scoreList, nodeScore)
			}

			//t.Logf("Score List (Before normalization): %v", scoreList)

			if !reflect.DeepEqual(tt.wantedScoresBefore, scoreList) {
				t.Errorf("[Score] status does not match: %v, want: %v\n", scoreList, tt.wantedScoresBefore)
			}

			pl.NormalizeScore(
				context.Background(),
				framework.NewCycleState(),
				tt.pod,
				scoreList)

			//t.Logf("Score List (After Normalization): %v", scoreList)

			if !reflect.DeepEqual(tt.wantedScoresAfter, scoreList) {
				t.Errorf("[Normalize] status does not match: %v, want: %v\n", scoreList, tt.wantedScoresAfter)
			}
		})
	}

}

func BenchmarkNetworkMinCostPlugin(b *testing.B) {
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
			tt.appGroup.Status.RunningPods = tt.dependenciesNum
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

			pl := &NetworkMinCost{
				handle:      fh,
				agLister:    &agLister,
				podLister:   podLister,
				ntLister:    &ntLister,
				namespaces:  []string{"default"},
				weightsName: "UserDefined",
				ntName:      "nt-test",
			}

			state := framework.NewCycleState()

			// Without sleep sometimes the pods are not created in the api in time
			time.Sleep(1 * time.Second)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				gotList := make(framework.NodeScoreList, len(nodes))
				scoreNode := func(i int) {
					n := nodes[i]
					score, _ := pl.Score(ctx, state, tt.pod, n.Name)
					gotList[i] = framework.NodeScore{Name: n.Name, Score: score}
				}
				Until(ctx, len(nodes), scoreNode)

				//b.Logf("Score List (Before normalization): %v", gotList)
				status := pl.NormalizeScore(ctx, state, tt.pod, gotList)
				assert.True(b, status.IsSuccess())

				//b.Logf("Score List (After normalization): %v", gotList)
			}
		})
	}
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

/*
func calculateRegionWeights(regionNames []string) (v1alpha1.CostList, error) {
	var weightList v1alpha1.CostList

	for _, r1 := range regionNames {
		// init vars
		var costList []v1alpha1.CostInfo
		for _, r2 := range regionNames {
			if r1 != r2 {
				cost := int64(randomInt(1, 100))
				costInfo := v1alpha1.CostInfo{
					Destination:       r2,
					BandwidthCapacity: *resource.NewQuantity(1*1024, resource.DecimalSI),
					NetworkCost:       cost,
				}
				costList = append(costList, costInfo)
			}
		}
		originInfo := v1alpha1.OriginInfo{
			Origin: r1,
			Costs:costList,
		}
		weightList = append(weightList, originInfo)
	}

	// Sort Costs by origin
	sort.Sort(networkAwareUtil.ByOrigin(weightList))
	return weightList, nil
}

func calculateZoneWeights(zoneNames []string) (v1alpha1.CostList, error) {
	var weightList v1alpha1.CostList

	for _, z1 := range zoneNames {
		// init vars
		var costList []v1alpha1.CostInfo
		for _, z2 := range zoneNames {
			if z1 != z2 {
				cost := int64(randomInt(1, 100))
				costInfo := v1alpha1.CostInfo{
					Destination:       z2,
					BandwidthCapacity: *resource.NewQuantity(1 * 250, resource.DecimalSI),
					NetworkCost:       cost,
				}
				costList = append(costList, costInfo)
			}
		}
		originInfo := v1alpha1.OriginInfo{
			Origin: z1,
			Costs:costList,
		}
		weightList = append(weightList, originInfo)
	}

	// Sort Costs by origin
	sort.Sort(networkAwareUtil.ByOrigin(weightList))
	return weightList, nil
}
*/
