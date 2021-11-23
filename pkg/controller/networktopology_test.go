package controller

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/scheduling/v1alpha1"
	ntfake "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned/fake"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	"testing"
	"time"
)

func TestNetworkTopologyController_Run(t *testing.T) {
	ctx := context.TODO()
	//createTime := metav1.Time{Time: time.Now().Add(-72 * time.Hour)}

	// Create Network Topology CRD
	networkTopology := &v1alpha1.NetworkTopology{
		ObjectMeta: metav1.ObjectMeta{Name: "nt-test", Namespace: "default"},
		Spec: v1alpha1.NetworkTopologySpec{ConfigmapName: "netperfMetrics", Weights: v1alpha1.WeightList{
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

	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "netperfMetrics", Namespace: "default"},
		Data: map[string]string{
			"netperf_p90_latency_microseconds.origin.n-1.destination.n-2": "1",
			"netperf_p90_latency_microseconds.origin.n-1.destination.n-3": "5",
			"netperf_p90_latency_microseconds.origin.n-1.destination.n-4": "5",
			"netperf_p90_latency_microseconds.origin.n-1.destination.n-5": "100",
			"netperf_p90_latency_microseconds.origin.n-1.destination.n-6": "100",
			"netperf_p90_latency_microseconds.origin.n-1.destination.n-7": "100",
			"netperf_p90_latency_microseconds.origin.n-1.destination.n-8": "100",
			"netperf_p90_latency_microseconds.origin.n-2.destination.n-1": "1",
			"netperf_p90_latency_microseconds.origin.n-2.destination.n-3": "5",
			"netperf_p90_latency_microseconds.origin.n-2.destination.n-4": "5",
			"netperf_p90_latency_microseconds.origin.n-2.destination.n-5": "100",
			"netperf_p90_latency_microseconds.origin.n-2.destination.n-6": "100",
			"netperf_p90_latency_microseconds.origin.n-2.destination.n-7": "100",
			"netperf_p90_latency_microseconds.origin.n-2.destination.n-8": "100",
			"netperf_p90_latency_microseconds.origin.n-3.destination.n-1": "5",
			"netperf_p90_latency_microseconds.origin.n-3.destination.n-2": "5",
			"netperf_p90_latency_microseconds.origin.n-3.destination.n-4": "1",
			"netperf_p90_latency_microseconds.origin.n-3.destination.n-5": "100",
			"netperf_p90_latency_microseconds.origin.n-3.destination.n-6": "100",
			"netperf_p90_latency_microseconds.origin.n-3.destination.n-7": "100",
			"netperf_p90_latency_microseconds.origin.n-3.destination.n-8": "100",
			"netperf_p90_latency_microseconds.origin.n-4.destination.n-1": "5",
			"netperf_p90_latency_microseconds.origin.n-4.destination.n-2": "5",
			"netperf_p90_latency_microseconds.origin.n-4.destination.n-3": "1",
			"netperf_p90_latency_microseconds.origin.n-4.destination.n-5": "100",
			"netperf_p90_latency_microseconds.origin.n-4.destination.n-6": "100",
			"netperf_p90_latency_microseconds.origin.n-4.destination.n-7": "100",
			"netperf_p90_latency_microseconds.origin.n-4.destination.n-8": "100",
			"netperf_p90_latency_microseconds.origin.n-5.destination.n-1": "100",
			"netperf_p90_latency_microseconds.origin.n-5.destination.n-2": "100",
			"netperf_p90_latency_microseconds.origin.n-5.destination.n-3": "100",
			"netperf_p90_latency_microseconds.origin.n-5.destination.n-4": "100",
			"netperf_p90_latency_microseconds.origin.n-5.destination.n-6": "1",
			"netperf_p90_latency_microseconds.origin.n-5.destination.n-7": "5",
			"netperf_p90_latency_microseconds.origin.n-5.destination.n-8": "5",
			"netperf_p90_latency_microseconds.origin.n-6.destination.n-1": "100",
			"netperf_p90_latency_microseconds.origin.n-6.destination.n-2": "100",
			"netperf_p90_latency_microseconds.origin.n-6.destination.n-3": "100",
			"netperf_p90_latency_microseconds.origin.n-6.destination.n-4": "100",
			"netperf_p90_latency_microseconds.origin.n-6.destination.n-5": "1",
			"netperf_p90_latency_microseconds.origin.n-6.destination.n-7": "5",
			"netperf_p90_latency_microseconds.origin.n-6.destination.n-8": "5",
			"netperf_p90_latency_microseconds.origin.n-7.destination.n-1": "100",
			"netperf_p90_latency_microseconds.origin.n-7.destination.n-2": "100",
			"netperf_p90_latency_microseconds.origin.n-7.destination.n-3": "100",
			"netperf_p90_latency_microseconds.origin.n-7.destination.n-4": "100",
			"netperf_p90_latency_microseconds.origin.n-7.destination.n-5": "5",
			"netperf_p90_latency_microseconds.origin.n-7.destination.n-6": "5",
			"netperf_p90_latency_microseconds.origin.n-7.destination.n-8": "1",
			"netperf_p90_latency_microseconds.origin.n-8.destination.n-1": "100",
			"netperf_p90_latency_microseconds.origin.n-8.destination.n-2": "100",
			"netperf_p90_latency_microseconds.origin.n-8.destination.n-3": "100",
			"netperf_p90_latency_microseconds.origin.n-8.destination.n-4": "100",
			"netperf_p90_latency_microseconds.origin.n-8.destination.n-5": "5",
			"netperf_p90_latency_microseconds.origin.n-8.destination.n-6": "5",
			"netperf_p90_latency_microseconds.origin.n-8.destination.n-7": "1",
		},
		BinaryData: nil,
	}

	cases := []struct {
		name                      string
		ntName                    string
		networkTopology           *v1alpha1.NetworkTopology
		nodes                     []*v1.Node
		networkTopologyCreateTime *metav1.Time
		configMap                 *v1.ConfigMap
		desiredNodeCount          int64
	}{
		{
			name:             "Network Topology controller: 8 nodes",
			ntName:           "nt-test",
			networkTopology:  networkTopology,
			nodes:            nodes,
			configMap:        configMap,
			desiredNodeCount: 8,
		}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			var kubeClient = fake.NewSimpleClientset()

			if len(nodes) == 8 {
				kubeClient = fake.NewSimpleClientset(nodes[0], nodes[1], nodes[2], nodes[3], nodes[4], nodes[5], nodes[6], nodes[7], configMap)
			}

			ntClient := ntfake.NewSimpleClientset(networkTopology)

			informerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())
			ntInformerFactory := schedinformer.NewSharedInformerFactory(ntClient, controller.NoResyncPeriodFunc())
			ntInformer := ntInformerFactory.Scheduling().V1alpha1().NetworkTopologies()
			nodeInformer := informerFactory.Core().V1().Nodes()
			configmapInformer := informerFactory.Core().V1().ConfigMaps()

			ctrl := NewNetworkTopologyController(kubeClient, ntInformer, nodeInformer, configmapInformer, ntClient)

			ntInformerFactory.Start(ctx.Done())
			informerFactory.Start(ctx.Done())

			go ctrl.Run(1, ctx.Done())
			err := wait.Poll(200*time.Millisecond, 1*time.Second, func() (done bool, err error) {
				nt, err := ntClient.SchedulingV1alpha1().NetworkTopologies("default").Get(ctx, c.ntName, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				if nt.Status.NodeCount != c.desiredNodeCount {
					return false, fmt.Errorf("want %v, got %v", c.desiredNodeCount, nt.Status.NodeCount)
				}
				//if nt.Status.Weights == nil {
				//	return false, fmt.Errorf("want %v, got %v", c.desiredWeights, nt.Status.Weights)
				//}

				//for _, w := range nt.Status.Weights {
				//	for _, cost := range w.CostList {
				//	}
				//}
				return true, nil
			})
			if err != nil {
				t.Fatal("Unexpected error", err)
			}
		})
	}
}
