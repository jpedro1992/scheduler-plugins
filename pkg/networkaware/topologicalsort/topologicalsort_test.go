/*
Copyright 2022 The Kubernetes Authors.

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
	ctr "sigs.k8s.io/scheduler-plugins/pkg/controller"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	agfake "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/fake"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
)

func TestTopologicalSortLess(t *testing.T) {
	basicAppGroup := v1alpha1.AppGroupWorkloadList{
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace:  "default"},
			Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{
				Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}}}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"},
			Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{
				Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}}}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace:  "default"}},
	}

	basicTopologyOrder := v1alpha1.AppGroupTopologyList{
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace: "default"}, Index: 1},
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}, Index: 2},
		v1alpha1.AppGroupTopologyInfo{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}, Index: 3},
	}

	onlineBoutique:= v1alpha1.AppGroupWorkloadList{
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace: "default"}, // frontend
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P8-deployment", Selector: "P8", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P9-deployment", Selector: "P9", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P10-deployment", Selector: "P10",APIVersion: "apps/v1", Namespace:  "default"}},
			},
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}, // cartService
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P11-deployment", Selector: "P11", APIVersion: "apps/v1", Namespace: "default",}},
			},
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}, // productCatalogService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace: "default"}, // currencyService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P5-deployment", Selector: "P5", APIVersion: "apps/v1", Namespace: "default"}, // paymentService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace: "default"}, // shippingService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P7-deployment", Selector: "P7", APIVersion: "apps/v1", Namespace: "default"}, // emailService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P8-deployment", Selector: "P8", APIVersion: "apps/v1", Namespace: "default"}, // checkoutService
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2",APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P5-deployment", Selector: "P5", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P7-deployment", Selector: "P7", APIVersion: "apps/v1", Namespace: "default"}},
			},
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P9-deployment", Selector: "P9", APIVersion: "apps/v1", Namespace: "default"}, // recommendationService
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}},
			}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P10-deployment", Selector: "P10", APIVersion: "apps/v1", Namespace: "default",}, // adService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P11-deployment", Selector: "P11", APIVersion: "apps/v1", Namespace: "default",}, // redis-cart
		},
	}

	onlineBoutiqueTopologyOrder := v1alpha1.AppGroupTopologyList{
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace: "default"}, Index: 1},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P10-deployment", Selector: "P10", APIVersion: "apps/v1", Namespace: "default"}, Index: 2},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P9-deployment", Selector: "P9", APIVersion: "apps/v1", Namespace: "default"}, Index: 3},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P8-deployment", Selector: "P8", APIVersion: "apps/v1", Namespace: "default"}, Index: 4},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P7-deployment", Selector: "P7", APIVersion: "apps/v1", Namespace: "default"}, Index: 5},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace: "default"}, Index: 6},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P5-deployment", Selector: "P5", APIVersion: "apps/v1", Namespace: "default"}, Index: 7},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace: "default"}, Index: 8},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}, Index: 9},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}, Index: 10},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P11-deployment", Selector: "P11", APIVersion: "apps/v1", Namespace: "default"}, Index: 11},
	}

	ctx := context.TODO()
	tests := []struct {
		name                     string
		namespace                string
		pInfo1                   *framework.QueuedPodInfo
		pInfo2                   *framework.QueuedPodInfo
		want                     bool
		agName                   string
		numMembers               int32
		selectors                []string
		deploymentNames          []string
		podPhase                 v1.PodPhase
		podNextPhase             v1.PodPhase
		topologySortingAlgorithm string
		workloadList             v1alpha1.AppGroupWorkloadList
		desiredRunningWorkloads  int32
		desiredTopologyOrder     v1alpha1.AppGroupTopologyList
		appGroupCreateTime       *metav1.Time
	}{
		{
			name:                     	"basic use case, same AppGroup, P1 order lower than P2",
			agName:                   	"basic",
			namespace:                	"default",
			numMembers:               	3,
			selectors:           	  	[]string{"P1", "P2", "P3"},
			deploymentNames:          	[]string{"P1-deployment", "P2-deployment", "P3-deployment"},
			desiredRunningWorkloads:    3,
			podPhase:                 	v1.PodRunning,
			topologySortingAlgorithm: 	"KahnSort",
			pInfo1: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P1", "P1-deployment", 0, "basic", nil, nil)),
			},
			pInfo2: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P2", "P2-deployment", 0, "basic", nil, nil)),
			},
			workloadList: 				basicAppGroup,
			desiredTopologyOrder: 		basicTopologyOrder,
			want: true,
		},
		{
			name:                     	"OnlineBoutique, same AppGroup, P5 order higher than P1",
			agName:                   	"onlineBoutique",
			namespace:                	"default",
			numMembers:               	11,
			selectors: 					[]string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  	[]string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  	11,
			podPhase:                 	v1.PodRunning,
			topologySortingAlgorithm: 	"KahnSort",
			pInfo1: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P5", "P5-deployment", 0, "onlineBoutique", nil, nil)),
			},
			pInfo2: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P1", "P1-deployment", 0, "onlineBoutique", nil, nil)),
			},
			workloadList: 				onlineBoutique,
			desiredTopologyOrder: 		onlineBoutiqueTopologyOrder,
			want: false,
		},
		{
			name:                     	"Simple Chain but pods from different AppGroup... ",
			agName:                   	"basic",
			namespace:                	"default",
			numMembers:               	3,
			selectors:           	  	[]string{"P1", "P2", "P3"},
			deploymentNames:          	[]string{"P1-deployment", "P2-deployment", "P3-deployment"},
			desiredRunningWorkloads:  	3,
			podPhase:                 	v1.PodRunning,
			topologySortingAlgorithm: 	"TarjanSort",
			pInfo1: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P1", "P1-deployment",0, "basic", nil, nil)),
			},
			pInfo2: &framework.QueuedPodInfo{
				PodInfo: framework.NewPodInfo(makePod("P5", "P5-deployment",0, "other", nil, nil)),
			},
			workloadList: 				basicAppGroup,
			desiredTopologyOrder: 		basicTopologyOrder,
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := makePodsAppGroup(tt.deploymentNames, tt.agName, tt.podPhase)

			var kubeClient = fake.NewSimpleClientset()

			if len(ps) == 3 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2])
			} else if len(ps) == 11 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6], ps[7], ps[8], ps[9], ps[10])
			}

			ag := makeAG(tt.agName, tt.numMembers, tt.topologySortingAlgorithm, tt.workloadList, tt.appGroupCreateTime)
			agClient := agfake.NewSimpleClientset(ag)

			informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			agInformerFactory := schedinformer.NewSharedInformerFactory(agClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			agInformer := agInformerFactory.Scheduling().V1alpha1().AppGroups()

			ctrl := ctr.NewAppGroupController(kubeClient, agInformer, podInformer, agClient)

			agInformerFactory.Start(ctx.Done())
			informerFactory.Start(ctx.Done())

			agLister := agInformer.Lister()

			go ctrl.Run(1, ctx.Done())
			err := wait.Poll(200*time.Millisecond, 1*time.Second, func() (done bool, err error) {
				ag, err := agClient.SchedulingV1alpha1().AppGroups("default").Get(ctx, tt.agName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if ag.Status.RunningWorkloads == 0 {
					return false, fmt.Errorf("want %v, got %v", tt.desiredRunningWorkloads, ag.Status.RunningWorkloads)
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
	var desiredRunningWorkloads int32 = 11
	var numMembers int32 = 11
	namespace := "default"
	topologySortingAlgorithm := "KahnSort"

	onlineBoutique:= v1alpha1.AppGroupWorkloadList{
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace: "default"}, // frontend
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P8-deployment", Selector: "P8", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P9-deployment", Selector: "P9", APIVersion: "apps/v1", Namespace:  "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P10-deployment", Selector: "P10",APIVersion: "apps/v1", Namespace:  "default"}},
			},
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}, // cartService
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P11-deployment", Selector: "P11", APIVersion: "apps/v1", Namespace: "default",}},
			},
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}, // productCatalogService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace: "default"}, // currencyService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P5-deployment", Selector: "P5", APIVersion: "apps/v1", Namespace: "default"}, // paymentService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace: "default"}, // shippingService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P7-deployment", Selector: "P7", APIVersion: "apps/v1", Namespace: "default"}, // emailService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P8-deployment", Selector: "P8", APIVersion: "apps/v1", Namespace: "default"}, // checkoutService
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2",APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P5-deployment", Selector: "P5", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace: "default"}},
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P7-deployment", Selector: "P7", APIVersion: "apps/v1", Namespace: "default"}},
			},
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P9-deployment", Selector: "P9", APIVersion: "apps/v1", Namespace: "default"}, // recommendationService
			Dependencies: v1alpha1.DependenciesList{
				v1alpha1.DependenciesInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}},
			}},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P10-deployment", Selector: "P10", APIVersion: "apps/v1", Namespace: "default",}, // adService
		},
		v1alpha1.AppGroupWorkload{
			Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P11-deployment", Selector: "P11", APIVersion: "apps/v1", Namespace: "default",}, // redis-cart
		},
	}

	desiredTopologyOrder := v1alpha1.AppGroupTopologyList{
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P1-deployment", Selector: "P1", APIVersion: "apps/v1", Namespace: "default"}, Index: 1},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P10-deployment", Selector: "P10", APIVersion: "apps/v1", Namespace: "default"}, Index: 2},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P9-deployment", Selector: "P9", APIVersion: "apps/v1", Namespace: "default"}, Index: 3},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P8-deployment", Selector: "P8", APIVersion: "apps/v1", Namespace: "default"}, Index: 4},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P7-deployment", Selector: "P7", APIVersion: "apps/v1", Namespace: "default"}, Index: 5},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P6-deployment", Selector: "P6", APIVersion: "apps/v1", Namespace: "default"}, Index: 6},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P5-deployment", Selector: "P5", APIVersion: "apps/v1", Namespace: "default"}, Index: 7},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P4-deployment", Selector: "P4", APIVersion: "apps/v1", Namespace: "default"}, Index: 8},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P3-deployment", Selector: "P3", APIVersion: "apps/v1", Namespace: "default"}, Index: 9},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P2-deployment", Selector: "P2", APIVersion: "apps/v1", Namespace: "default"}, Index: 10},
		v1alpha1.AppGroupTopologyInfo{Workload: v1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: "P11-deployment", Selector: "P11", APIVersion: "apps/v1", Namespace: "default"}, Index: 11},
	}

	tests := []struct {
		name                     string
		namespace                string
		want                     bool
		agName                   string
		numMembers               int32
		selectors                []string
		deploymentNames          []string
		podNum                   int
		podPhase                 v1.PodPhase
		podNextPhase             v1.PodPhase
		topologySortingAlgorithm string
		workloadList             v1alpha1.AppGroupWorkloadList
		desiredTopologyOrder     v1alpha1.AppGroupTopologyList
		desiredRunningWorkloads  int32
		appGroupCreateTime       *metav1.Time
	}{
		{
			name:                     "OnlineBoutique AppGroup, 10 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   10,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
										"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 100 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   100,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 500 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   500,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 1000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   1000,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 2000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   2000,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 3000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   3000,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 5000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   5000,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
		{
			name:                     "OnlineBoutique AppGroup, 10000 Pods",
			agName:                   agName,
			namespace:                namespace,
			numMembers:               numMembers,
			podNum:                   10000,
			selectors: 				  []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"},
			deploymentNames:		  []string{"P1-deployment", "P2-deployment", "P3-deployment", "P4-deployment", "P5-deployment",
											"P6-deployment", "P7-deployment", "P8-deployment", "P9-deployment", "P10-deployment", "P11-deployment"},
			desiredRunningWorkloads:  desiredRunningWorkloads,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: topologySortingAlgorithm,
			workloadList:             onlineBoutique,
			desiredTopologyOrder:     desiredTopologyOrder,
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ps := makePodsAppGroup(tt.deploymentNames, tt.agName, tt.podPhase)

			var kubeClient = fake.NewSimpleClientset()

			if len(ps) == 3 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2])
			} else if len(ps) == 11 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6], ps[7], ps[8], ps[9], ps[10])
			}

			ag := makeAG(tt.agName, tt.numMembers, tt.topologySortingAlgorithm, tt.workloadList, tt.appGroupCreateTime)
			agClient := agfake.NewSimpleClientset(ag)

			informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			agInformerFactory := schedinformer.NewSharedInformerFactory(agClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			agInformer := agInformerFactory.Scheduling().V1alpha1().AppGroups()

			ctrl := ctr.NewAppGroupController(kubeClient, agInformer, podInformer, agClient)

			agInformerFactory.Start(ctx.Done())
			informerFactory.Start(ctx.Done())

			agLister := agInformer.Lister()

			go ctrl.Run(1, ctx.Done())
			err := wait.Poll(200*time.Millisecond, 1*time.Second, func() (done bool, err error) {
				ag, err := agClient.SchedulingV1alpha1().AppGroups("default").Get(ctx, tt.agName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if ag.Status.RunningWorkloads == 0 {
					return false, fmt.Errorf("want %v, got %v", tt.desiredRunningWorkloads, ag.Status.RunningWorkloads)
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

			pInfo1 := getPodInfos(tt.podNum, tt.agName, tt.selectors, tt.deploymentNames)
			pInfo2 := getPodInfos(tt.podNum, tt.agName, tt.selectors, tt.deploymentNames)

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

func getPodInfos(podsNum int, agName string, selectors []string, podNames []string) (pInfo []*framework.QueuedPodInfo) {
	pInfo = []*framework.QueuedPodInfo{}

	for i := 0; i < podsNum; i++ {
		random := randomInt(0, len(podNames))
		pInfo = append(pInfo, &framework.QueuedPodInfo{PodInfo: framework.NewPodInfo(makePod(selectors[random], podNames[random] , 0, agName, nil, nil))})
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
		pod.Labels = map[string]string{v1alpha1.AppGroupLabel: agName}
		pod.Status.Phase = phase
		pds = append(pds, pod)
	}
	return pds
}

func makeAG(agName string, numMembers int32, topologySortingAlgorithm string, appGroupPod v1alpha1.AppGroupWorkloadList, createTime *metav1.Time) *v1alpha1.AppGroup {
	ag := &v1alpha1.AppGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              agName,
			Namespace:         "default",
			CreationTimestamp: metav1.Time{Time: time.Now()},
		},
		Spec: v1alpha1.AppGroupSpec{
			NumMembers:               numMembers,
			TopologySortingAlgorithm: topologySortingAlgorithm,
			Workloads:                appGroupPod,
		},
		Status: v1alpha1.AppGroupStatus{
			RunningWorkloads:       0,
			ScheduleStartTime: metav1.Time{Time: time.Now()},
			TopologyOrder:     nil,
		},
	}
	if createTime != nil {
		ag.CreationTimestamp = *createTime
	}
	return ag
}

func makePod(selector string, podName string, priority int32, appGroup string, requests, limits v1.ResourceList) *v1.Pod {
	label := make(map[string]string)
	label[v1alpha1.AppGroupLabel] = appGroup
	label[v1alpha1.AppGroupSelectorLabel] = selector

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   podName,
			Labels: label,
		},
		Spec: v1.PodSpec{
			Priority: &priority,
			Containers: []v1.Container{
				{
					Name: podName,
					Resources: v1.ResourceRequirements{
						Requests: requests,
						Limits:   limits,
					},
				},
			},
		},
	}
}
