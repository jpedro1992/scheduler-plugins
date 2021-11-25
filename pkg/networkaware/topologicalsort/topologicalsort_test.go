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
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/util/rand"
	"math"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	ctrAppGroup "sigs.k8s.io/scheduler-plugins/pkg/controller"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	agfake "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/fake"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

func TestTopologicalSortLess(t *testing.T) {

	ctx := context.TODO()
	tests := []struct {
		name                     string
		namespace                string
		pInfo1                   *framework.QueuedPodInfo
		pInfo2                   *framework.QueuedPodInfo
		want                     bool
		agName                   string
		numMembers               int32
		podNames                 []string
		podPhase                 v1.PodPhase
		podNextPhase             v1.PodPhase
		topologySortingAlgorithm string
		pods                     v1alpha1.AppGroupPodList
		desiredRunningPods       int32
		desiredTopologyOrder     v1alpha1.TopologyList
		appGroupCreateTime       *metav1.Time
	}{
		{
			name:                     "SimpleChain use case, same AppGroup, P1 order lower than P2",
			agName:                   "simpleChain",
			namespace:                "default",
			numMembers:               3,
			podNames:                 []string{"P1", "P2", "P3"},
			desiredRunningPods:       3,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: "KahnSort",
			pInfo1: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P1", 0, "simpleChain", nil, nil)),
			},
			pInfo2: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P2", 0, "simpleChain", nil, nil)),
			},
			pods: v1alpha1.AppGroupPodList{
				v1alpha1.AppGroupPod{PodName: "P1",
					Dependencies: v1alpha1.DependenciesList{
						v1alpha1.DependenciesInfo{PodName: "P2"}}},
				v1alpha1.AppGroupPod{PodName: "P2",
					Dependencies: v1alpha1.DependenciesList{
						v1alpha1.DependenciesInfo{PodName: "P3"}}},
				v1alpha1.AppGroupPod{PodName: "P3"},
			},
			desiredTopologyOrder: v1alpha1.TopologyList{
				v1alpha1.TopologyInfo{PodName: "P1", Index: 1},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 2},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 3}},
			want: true,
		},
		{
			name:                     "OnlineBoutique, same AppGroup, P5 order higher than P1",
			agName:                   "onlineBoutique",
			namespace:                "default",
			numMembers:               10,
			podNames:                 []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"},
			desiredRunningPods:       10,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: "KahnSort",
			pInfo1: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P5", 0, "onlineBoutique", nil, nil)),
			},
			pInfo2: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P1", 0, "onlineBoutique", nil, nil)),
			},
			pods: v1alpha1.AppGroupPodList{
				v1alpha1.AppGroupPod{PodName: "P1", // frontend
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
					Dependencies: v1alpha1.DependenciesList{
						v1alpha1.DependenciesInfo{PodName: "P3"},
					}},
				v1alpha1.AppGroupPod{PodName: "P10"}, // adService
			},
			desiredTopologyOrder: v1alpha1.TopologyList{
				v1alpha1.TopologyInfo{PodName: "P1", Index: 1},
				v1alpha1.TopologyInfo{PodName: "P10", Index: 2},
				v1alpha1.TopologyInfo{PodName: "P9", Index: 3},
				v1alpha1.TopologyInfo{PodName: "P8", Index: 4},
				v1alpha1.TopologyInfo{PodName: "P7", Index: 5},
				v1alpha1.TopologyInfo{PodName: "P6", Index: 6},
				v1alpha1.TopologyInfo{PodName: "P5", Index: 7},
				v1alpha1.TopologyInfo{PodName: "P4", Index: 8},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 9},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 10}},
			want: false,
		},
		{
			name:                     "Simple Chain but pods from different AppGroup... ",
			agName:                   "simpleChain",
			namespace:                "default",
			numMembers:               3,
			podNames:                 []string{"P1", "P2", "P3"},
			desiredRunningPods:       3,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: "TarjanSort",
			pInfo1: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P1", 0, "simpleChain", nil, nil)),
			},
			pInfo2: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P5", 0, "other", nil, nil)),
			},
			pods: v1alpha1.AppGroupPodList{
				v1alpha1.AppGroupPod{PodName: "P1",
					Dependencies: v1alpha1.DependenciesList{
						v1alpha1.DependenciesInfo{PodName: "P2"}}},
				v1alpha1.AppGroupPod{PodName: "P2",
					Dependencies: v1alpha1.DependenciesList{
						v1alpha1.DependenciesInfo{PodName: "P3"}}},
				v1alpha1.AppGroupPod{PodName: "P3"},
			},
			desiredTopologyOrder: v1alpha1.TopologyList{
				v1alpha1.TopologyInfo{PodName: "P1", Index: 1},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 2},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 3}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := makePodsAppGroup(tt.podNames, tt.agName, tt.podPhase)

			var kubeClient = fake.NewSimpleClientset()

			if len(ps) == 3 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2])
			} else if len(ps) == 10 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6], ps[7], ps[8], ps[9])
			}

			ag := makeAG(tt.agName, tt.numMembers, tt.topologySortingAlgorithm, tt.pods, tt.appGroupCreateTime)
			agClient := agfake.NewSimpleClientset(ag)

			informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			agInformerFactory := schedinformer.NewSharedInformerFactory(agClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			nodeInformer := informerFactory.Core().V1().Nodes()
			agInformer := agInformerFactory.Scheduling().V1alpha1().AppGroups()

			ctrl := ctrAppGroup.NewAppGroupController(kubeClient, agInformer, podInformer, nodeInformer, agClient)

			agInformerFactory.Start(ctx.Done())
			informerFactory.Start(ctx.Done())

			agLister := agInformer.Lister()

			go ctrl.Run(1, ctx.Done())
			err := wait.Poll(200*time.Millisecond, 1*time.Second, func() (done bool, err error) {
				ag, err := agClient.SchedulingV1alpha1().AppGroups("default").Get(ctx, tt.agName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if ag.Status.RunningPods == 0 {
					return false, fmt.Errorf("want %v, got %v", tt.desiredRunningPods, ag.Status.RunningPods)
				}
				if ag.Status.TopologyOrder == nil {
					return false, fmt.Errorf("want %v, got %v", tt.desiredTopologyOrder, ag.Status.TopologyOrder)
				}
				ts := &TopologicalSort{
					agLister:   &agLister,
					namespaces: []string{metav1.NamespaceDefault},
				}

				if got := ts.Less(tt.pInfo1, tt.pInfo2); got != tt.want {
					t.Errorf("Less() = %v, want %v", got, tt.want)
				}
				return true, nil
			})
			if err != nil {
				t.Fatal("Unexpected error", err)
			}
		})
	}
}

func BenchmarkTopologicalSortPlugin(b *testing.B) {
	ctx := context.TODO()

	agName := "onlineBoutique"
	podNames := []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"}
	var desiredRunningPods int32 = 10
	var numMembers int32 = 10
	namespace := "default"
	topologySortingAlgorithm := "KahnSort"

	pods := v1alpha1.AppGroupPodList{v1alpha1.AppGroupPod{PodName: "P1", // frontend
		Dependencies: v1alpha1.DependenciesList{
			v1alpha1.DependenciesInfo{PodName: "P2"},
			v1alpha1.DependenciesInfo{PodName: "P3"},
			v1alpha1.DependenciesInfo{PodName: "P4"},
			v1alpha1.DependenciesInfo{PodName: "P6"},
			v1alpha1.DependenciesInfo{PodName: "P8"},
			v1alpha1.DependenciesInfo{PodName: "P9"},
			v1alpha1.DependenciesInfo{PodName: "P10"},
		}},
		v1alpha1.AppGroupPod{PodName: "P2"},
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
		v1alpha1.AppGroupPod{PodName: "P9",
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{PodName: "P3"},
			}},
		v1alpha1.AppGroupPod{PodName: "P10"},
	}

	desiredTopologyOrder := v1alpha1.TopologyList{
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
	}

	tests := []struct {
		name                     string
		namespace                string
		want                     bool
		agName                   string
		numMembers               int32
		podNames                 []string
		podNum                   int
		podPhase                 v1.PodPhase
		podNextPhase             v1.PodPhase
		topologySortingAlgorithm string
		pods                     v1alpha1.AppGroupPodList
		desiredTopologyOrder     v1alpha1.TopologyList
		desiredRunningPods       int32
		appGroupCreateTime       *metav1.Time
	}{
		{
			name:                     "OnlineBoutique AppGroup, 10 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   10,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 100 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   100,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 500 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   500,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 1000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   1000,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 2000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   2000,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 3000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   3000,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 5000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   5000,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 10000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   10000,
			podNames:                 podNames,
			desiredRunningPods:       desiredRunningPods,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			pods:                     pods,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ps := makePodsAppGroup(tt.podNames, tt.agName, tt.podPhase)

			var kubeClient = fake.NewSimpleClientset()

			if len(ps) == 3 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2])
			} else if len(ps) == 10 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6], ps[7], ps[8], ps[9])
			}

			ag := makeAG(tt.agName, tt.numMembers, tt.topologySortingAlgorithm, tt.pods, tt.appGroupCreateTime)
			agClient := agfake.NewSimpleClientset(ag)

			informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			agInformerFactory := schedinformer.NewSharedInformerFactory(agClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			nodeInformer := informerFactory.Core().V1().Nodes()
			agInformer := agInformerFactory.Scheduling().V1alpha1().AppGroups()

			ctrl := ctrAppGroup.NewAppGroupController(kubeClient, agInformer, podInformer, nodeInformer, agClient)

			agInformerFactory.Start(ctx.Done())
			informerFactory.Start(ctx.Done())

			agLister := agInformer.Lister()

			go ctrl.Run(1, ctx.Done())
			err := wait.Poll(200*time.Millisecond, 1*time.Second, func() (done bool, err error) {
				ag, err := agClient.SchedulingV1alpha1().AppGroups("default").Get(ctx, tt.agName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if ag.Status.RunningPods == 0 {
					return false, fmt.Errorf("want %v, got %v", tt.desiredRunningPods, ag.Status.RunningPods)
				}
				if ag.Status.TopologyOrder == nil {
					return false, fmt.Errorf("want %v, got %v", tt.desiredTopologyOrder, ag.Status.TopologyOrder)
				}
				return true, nil
			})

			if err != nil {
				b.Fatal("Unexpected error", err)
			}

			ts := &TopologicalSort{
				agLister:   &agLister,
				namespaces: []string{metav1.NamespaceDefault},
			}

			pInfo1 := getPodInfos(tt.podNum, tt.agName, tt.podNames)
			pInfo2 := getPodInfos(tt.podNum, tt.agName, tt.podNames)

			//b.Logf("len(pInfo1): %v", len(pInfo1))
			//b.Logf("len(pInfo2): %v", len(pInfo2))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sorting := func(i int) {
					_ = ts.Less(pInfo1[i], pInfo2[i])
				}
				Until(ctx, len(pInfo1), sorting)
			}

		})
	}
}

func randomInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func getPodInfos(podsNum int, agName string, podNames []string) (pInfo []*framework.QueuedPodInfo) {
	pInfo = []*framework.QueuedPodInfo{}

	for i := 0; i < podsNum; i++ {
		pInfo = append(pInfo, &framework.QueuedPodInfo{PodInfo: framework.NewPodInfo(makePod(podNames[randomInt(0, len(podNames))], 0, agName, nil, nil))})
	}
	return pInfo
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

func makePodsAppGroup(podNames []string, agName string, phase v1.PodPhase) []*v1.Pod {
	pds := make([]*v1.Pod, 0)
	for _, name := range podNames {
		pod := st.MakePod().Namespace("default").Name(name).Obj()
		pod.Labels = map[string]string{util.AppGroupLabel: agName}
		pod.Status.Phase = phase
		pds = append(pds, pod)
	}
	return pds
}

func makeAG(agName string, numMembers int32, topologySortingAlgorithm string, appGroupPod v1alpha1.AppGroupPodList, createTime *metav1.Time) *v1alpha1.AppGroup {
	ag := &v1alpha1.AppGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              agName,
			Namespace:         "default",
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: v1alpha1.AppGroupSpec{
			NumMembers:               numMembers,
			TopologySortingAlgorithm: topologySortingAlgorithm,
			Pods:                     appGroupPod,
		},
		Status: v1alpha1.AppGroupStatus{
			RunningPods:       0,
			PodsScheduled:     nil,
			ScheduleStartTime: metav1.Time{Time: time.Now()},
			TopologyOrder:     nil,
		},
	}
	if createTime != nil {
		ag.CreationTimestamp = *createTime
	}
	return ag
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
