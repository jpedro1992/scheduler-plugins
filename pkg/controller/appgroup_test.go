package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"
	st "k8s.io/kubernetes/pkg/scheduler/testing"

	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	agfake "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/fake"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

func TestAppGroupController_Run(t *testing.T) {
	ctx := context.TODO()
	//createTime := metav1.Time{Time: time.Now().Add(-72 * time.Hour)}
	cases := []struct {
		name                     string
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
			name:                     "AppGroup running: Simple chain with 3 pods",
			agName:                   "simpleChain",
			numMembers:               3,
			podNames:                 []string{"P1", "P2", "P3"},
			desiredRunningPods:       3,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: "KahnSort",
			pods: v1alpha1.AppGroupPodList{
				v1alpha1.AppGroupPod{PodName: "P1",
					Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{PodName: "P2"}}},
				v1alpha1.AppGroupPod{PodName: "P2",
					Dependencies: v1alpha1.DependenciesList{v1alpha1.DependenciesInfo{PodName: "P3"}}},
				v1alpha1.AppGroupPod{PodName: "P3"},
			},
			desiredTopologyOrder: v1alpha1.TopologyList{
				v1alpha1.TopologyInfo{PodName: "P1", Index: 1},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 2},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 3}},
		},
		{
			name:                     "AppGroup Online Boutique - KahnSort - https://github.com/GoogleCloudPlatform/microservices-demo",
			agName:                   "onlineBoutique",
			numMembers:               10,
			podNames:                 []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"},
			desiredRunningPods:       10,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: "KahnSort",
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
				v1alpha1.TopologyInfo{PodName: "P2", Index: 10},
			},
		},
		{
			name:                     "AppGroup Online Boutique - AlternateKahn - https://github.com/GoogleCloudPlatform/microservices-demo",
			agName:                   "onlineBoutique",
			numMembers:               10,
			podNames:                 []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"},
			desiredRunningPods:       10,
			podPhase:                 v1.PodRunning,
			topologySortingAlgorithm: "AlternateKahn",
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
				v1alpha1.TopologyInfo{PodName: "P10", Index: 3},
				v1alpha1.TopologyInfo{PodName: "P9", Index: 5},
				v1alpha1.TopologyInfo{PodName: "P8", Index: 7},
				v1alpha1.TopologyInfo{PodName: "P7", Index: 9},
				v1alpha1.TopologyInfo{PodName: "P6", Index: 10},
				v1alpha1.TopologyInfo{PodName: "P5", Index: 8},
				v1alpha1.TopologyInfo{PodName: "P4", Index: 6},
				v1alpha1.TopologyInfo{PodName: "P3", Index: 4},
				v1alpha1.TopologyInfo{PodName: "P2", Index: 2},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ps := makePodsAppGroup(c.podNames, c.agName, c.podPhase)

			var kubeClient = fake.NewSimpleClientset()

			if len(ps) == 3 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2])
			} else if len(ps) == 10 {
				kubeClient = fake.NewSimpleClientset(ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6], ps[7], ps[8], ps[9])
			}

			ag := makeAG(c.agName, c.numMembers, c.topologySortingAlgorithm, c.pods, c.appGroupCreateTime)
			agClient := agfake.NewSimpleClientset(ag)

			informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			agInformerFactory := schedinformer.NewSharedInformerFactory(agClient, controller.NoResyncPeriodFunc())
			podInformer := informerFactory.Core().V1().Pods()
			nodeInformer := informerFactory.Core().V1().Nodes()
			agInformer := agInformerFactory.Scheduling().V1alpha1().AppGroups()

			ctrl := NewAppGroupController(kubeClient, agInformer, podInformer, nodeInformer, agClient)

			agInformerFactory.Start(ctx.Done())
			informerFactory.Start(ctx.Done())

			// 0 means not set
			//if len(c.podNextPhase) != 0 {
			//	ps := makePods(c.podNames, c.agName, c.podNextPhase)
			//	for _, p := range ps {
			//		kubeClient.CoreV1().Pods(p.Namespace).UpdateStatus(ctx, p, metav1.UpdateOptions{})
			//	}
			//}
			go ctrl.Run(1, ctx.Done())
			err := wait.Poll(200*time.Millisecond, 1*time.Second, func() (done bool, err error) {
				ag, err := agClient.SchedulingV1alpha1().AppGroups("default").Get(ctx, c.agName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if ag.Status.RunningPods == 0 {
					return false, fmt.Errorf("want %v, got %v", c.desiredRunningPods, ag.Status.RunningPods)
				}
				if ag.Status.TopologyOrder == nil {
					return false, fmt.Errorf("want %v, got %v", c.desiredTopologyOrder, ag.Status.TopologyOrder)
				}
				for _, pod := range ag.Status.TopologyOrder {
					for _, desiredPod := range c.desiredTopologyOrder {
						if desiredPod.PodName == pod.PodName {
							if pod.Index != desiredPod.Index { // Some algorithms might give a different result depending on the service topology
								return false, fmt.Errorf("want %v, got %v", desiredPod.Index, pod.Index)
							}
						}
					}
				}
				return true, nil
			})
			if err != nil {
				t.Fatal("Unexpected error", err)
			}
		})
	}
}

func makePodsAppGroup(podNames []string, agName string, phase v1.PodPhase) []*v1.Pod {
	pds := make([]*v1.Pod, 0)
	i := 0
	for _, name := range podNames {
		pod := st.MakePod().Namespace("default").Name(name + fmt.Sprint(i)).Obj()
		pod.Labels = map[string]string{util.AppGroupLabel: agName, util.DeploymentLabel: name}
		pod.Status.Phase = phase
		pds = append(pds, pod)
		i += i
	}
	return pds
}

/*
func randomInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
*/

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
