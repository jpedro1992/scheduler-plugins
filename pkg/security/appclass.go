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

package appclass

import (
	"context"
	"fmt"
	appclassv1alpha1 "github.com/diktyo-io/appclass-api/pkg/apis/appclass/v1alpha1"
	agv1alpha1 "github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	klog "k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"math"
	"sigs.k8s.io/controller-runtime/pkg/client"
	pluginconfig "sigs.k8s.io/scheduler-plugins/apis/config"
	networkawareutil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
	"sigs.k8s.io/scheduler-plugins/pkg/security/util"
)

var _ framework.FilterPlugin = &AppClass{}
var _ framework.ScorePlugin = &AppClass{}

const (
	// Name : name of plugin used in the plugin registry and configurations.
	Name = "AppClass"

	// defaultAppClassName : default name of the AppClass CR
	defaultAppClassName = "app-class"

	// preFilterStateKey is the key in CycleState to NetworkOverhead pre-computed data.
	preFilterStateKey = "PreFilter" + Name
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agv1alpha1.AddToScheme(scheme))
	utilruntime.Must(appclassv1alpha1.AddToScheme(scheme))
}

type AppClass struct {
	client.Client
	podLister    corelisters.PodLister
	handle       framework.Handle
	namespaces   []string
	appClassName string
}

// PreFilterState computed at PreFilter and used at Filter and Score.
type PreFilterState struct {
	// boolean that tells the filter and scoring functions to pass the pod since it does not belong to an AppGroup
	scoreEqually bool

	// agName: corresponds to the name of the AppGroup of the pod
	agName string

	// appClass name of the pod
	appClassName string

	// AppGroup CR
	appGroup *agv1alpha1.AppGroup

	// AppClass CR
	appClass *appclassv1alpha1.AppClass

	// Pods already scheduled for a given AppGroup
	scheduledList util.ScheduledList

	// node map for counting affinity classes
	satisfiedMap map[string]int64

	// node map for counting anti-affinity classes
	violatedMap map[string]int64
}

// Clone the preFilter state.
func (ac *PreFilterState) Clone() framework.StateData {
	return ac
}

func (ac *AppClass) Name() string {
	return Name
}

func getArgs(obj runtime.Object) (*pluginconfig.AppClassArgs, error) {
	AppClassArgs, ok := obj.(*pluginconfig.AppClassArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type AppClass, got %T", obj)
	}

	return AppClassArgs, nil
}

func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.V(4).InfoS("Creating new instance of the AppClass plugin")
	args, err := getArgs(obj)
	if err != nil {
		return nil, err
	}
	client, err := client.New(handle.KubeConfig(), client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}

	appClassName := args.AppClassName
	if appClassName == "" {
		appClassName = defaultAppClassName
	}

	ac := &AppClass{
		Client:       client,
		podLister:    handle.SharedInformerFactory().Core().V1().Pods().Lister(),
		handle:       handle,
		namespaces:   args.Namespaces,
		appClassName: args.AppClassName,
	}
	return ac, nil
}

// AppClass plugin
// PreFilter performs the following operations:
// 1. Check if Pod belongs to an AppGroup
// 2. Get AppGroup and AppClass CR -> if not, score equally
// 3. Get AppClass name of the given pod
// 4. Get all deployed pods that have the AppGroup label -> if 0, score equally
// 5. Get AppClass of all deployed pods - if status is empty, take spec. Otherwise, take status part.
// 6. Based on scheduling list check satisfied (same class, affinity) and violated (different class, anti-affinity)
// 7. Update satisfiedMap[nodeName] and violatedMap[nodeName]. It counts the number of satisfied and violated per node.
// 8. Update PreFilter state

// Filter performs the following operations:
// 1. Get PreFilterState
// 2. Check number of violated for that particular node. If higher than 0, then filter node

// Score performs the following operations:
// 1. Get PreFilterState
// 2. If scoreEqually=True, return minScore (Pod does not belong to AppGroup, or no pods are yet deployed.)
// 3. Return satisfied value as score (e.g., 3 deployed pods with the same class, mean the node will have a +3 score.)

// NormalizeScore performs the following operations:
// 1. Normalize scores (between 0 and 100) based on min and max scores

// Open Questions:
// Q1: What to do if pods do not belong to any AppGroup? Just pass all nodes?
// Q2: Do we assume all pods in the AppGroup belong to the same appClass?
//	   Solution: Otherwise, we can add Appgroup ref and workload ref in the
// appClass CR to check this info in the plugin to check pods in the nodes.
// So, different workloads in from an Appgroup can have different classes.

func (ac *AppClass) PreFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// Init PreFilter State
	preFilterState := &PreFilterState{
		scoreEqually: true,
	}
	score := false

	// Write initial status
	state.Write(preFilterStateKey, preFilterState)

	// Check if Pod belongs to an AppGroup
	agName := networkawareutil.GetPodAppGroupLabel(pod)
	if len(agName) == 0 { // Return
		return nil, framework.NewStatus(framework.Success, "Pod does not belong to an AppGroup, return")
	}

	// Get AppGroup CR
	appGroupCR := ac.findAppGroup(agName)

	// Get AppClass CR
	appClassCR := ac.findAppClass(ac.appClassName)

	// Get AppClass Name
	appClassName := util.GetClassName(agName, networkawareutil.GetPodAppGroupSelector(pod), appClassCR)

	// Get all pods that have the AppGroup label
	req, err := labels.NewRequirement(agv1alpha1.AppGroupLabel, selection.Exists, nil)
	if err != nil {
		return nil, framework.NewStatus(framework.Success, "Error while returning pods from appGroup, return")
	}

	// List pods matching that selector
	selector := labels.NewSelector().Add(*req)
	pods, err := ac.podLister.List(selector)
	if err != nil {
		return nil, framework.NewStatus(framework.Success, "Error while returning pods from appGroup, return")
	}

	// Return if pods are not yet allocated for any AppGroup...
	if len(pods) == 0 {
		score = true
		//	return nil, framework.NewStatus(framework.Success, "No pods yet allocated, return")
	}

	// Pods already scheduled: Get Scheduled List (Deployment name, AgName, Workload Name, replicaID, hostname)
	var scheduledList util.ScheduledList
	if len(pods) != 0 {
		scheduledList = util.GetScheduledList(pods, appClassCR)
	}

	// Check if scheduledList is empty...
	if len(scheduledList) == 0 {
		score = true
		//	klog.ErrorS(nil, "Scheduled list is empty, return")
		//	return nil, framework.NewStatus(framework.Success, "Scheduled list is empty, return")
	}

	// Print the scheduled list
	klog.V(6).InfoS("Collecting List of Scheduled Pods...")
	for _, p := range scheduledList {
		klog.V(6).InfoS("Scheduled Pod",
			"PodName", p.Name,
			"AgName", p.AgName,
			"Selector", p.Selector,
			"ReplicaID", p.ReplicaID,
			"Hostname", p.Hostname,
			"ClassName", p.ClassName,
		)
	}

	// Based on scheduling list check satisfied (same class, affinity) and violated (different class, anti-affinity)
	satisfiedMap := make(map[string]int64)
	violatedMap := make(map[string]int64)

	// Get all nodes
	nodeList, err := ac.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("Error getting the nodelist: %v", err))
	}

	for _, nodeInfo := range nodeList {
		nodeName := nodeInfo.Node().Name
		var satisfied, violated int64

		for _, p := range scheduledList {
			if p.Hostname != nodeName || p.ClassName == "" {
				continue
			}

			if p.ClassName == appClassName {
				satisfied++
			} else {
				violated++
			}
		}

		satisfiedMap[nodeName] = satisfied
		violatedMap[nodeName] = violated
	}

	// Print satisfiedMap[nodeName] and violatedMap[nodeName]
	for n, satisfied := range satisfiedMap {
		klog.V(6).Infof("Node=%s, satisfied=%d, violated=%d", n, satisfied, violatedMap[n])
	}

	// Update PreFilter State
	preFilterState = &PreFilterState{
		scoreEqually:  score,
		agName:        agName,
		appClass:      appClassCR,
		appGroup:      appGroupCR,
		scheduledList: scheduledList,
		satisfiedMap:  satisfiedMap,
		violatedMap:   violatedMap,
	}

	state.Write(preFilterStateKey, preFilterState)
	return nil, framework.NewStatus(framework.Success, "PreFilter State updated")
}

func getPreFilterState(cycleState *framework.CycleState) (*PreFilterState, error) {
	no, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	state, ok := no.(*PreFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NetworkOverhead.preFilterState error", no)
	}
	return state, nil
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (ac *AppClass) PreFilterExtensions() framework.PreFilterExtensions {
	return ac
}

// AddPod from pre-computed data in cycleState.
// no current need for the AppClassPlugin plugin
func (ac *AppClass) AddPod(ctx context.Context,
	cycleState *framework.CycleState,
	podToSchedule *corev1.Pod,
	podToAdd *framework.PodInfo,
	nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

// RemovePod from pre-computed data in cycleState.
// no current need for the AppClassPlugin plugin
func (ac *AppClass) RemovePod(ctx context.Context,
	cycleState *framework.CycleState,
	podToSchedule *corev1.Pod,
	podToRemove *framework.PodInfo,
	nodeInfo *framework.NodeInfo) *framework.Status {
	return framework.NewStatus(framework.Success, "")
}

func (ac *AppClass) Filter(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	// Get PreFilterState
	preFilterState, err := getPreFilterState(cycleState)
	if err != nil {
		klog.ErrorS(err, "Failed to read preFilterState from cycleState", "preFilterStateKey", preFilterStateKey)
		return framework.NewStatus(framework.Error, "not eligible due to failed to read from cycleState")
	}

	// Check violated count for this node
	if len(preFilterState.scheduledList) != 0 {
		klog.V(6).InfoS("Checking the number of violated dependencies... ")
		violated := preFilterState.violatedMap[nodeInfo.Node().Name]
		if violated > 0 {
			// Node has conflicting classes → pod cannot be scheduled here
			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Node %v does not meet requirements. Violated: %v", nodeInfo.Node().Name, violated))
		}
	}

	return nil
}

// Score : evaluate score for a node
func (ac *AppClass) Score(ctx context.Context,
	cycleState *framework.CycleState,
	pod *corev1.Pod,
	nodeName string) (int64, *framework.Status) {
	score := framework.MinNodeScore

	// Get PreFilterState
	preFilterState, err := getPreFilterState(cycleState)
	if err != nil {
		klog.ErrorS(err, "Failed to read preFilterState from cycleState", "preFilterStateKey", preFilterStateKey)
		return score, framework.NewStatus(framework.Error, "not eligible due to failed to read from cycleState, return min score")
	}

	// If scoreEqually, return minScore
	if preFilterState.scoreEqually {
		return score, framework.NewStatus(framework.Success, "scoreEqually enabled: minimum score")
	}

	// Return satisfied value as score
	score = preFilterState.satisfiedMap[nodeName]
	klog.V(4).InfoS("Score:", "pod", pod.GetName(), "node", nodeName, "finalScore", score)
	return score, framework.NewStatus(framework.Success, "Satisfied value added as score")
}

// NormalizeScore : normalize scores
func (ac *AppClass) NormalizeScore(ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	scores framework.NodeScoreList) *framework.Status {
	klog.V(4).InfoS("before normalization: ", "scores", scores)

	// Get Min and Max Scores
	minScore, maxScore := getMinMaxScores(scores)

	// Transform the highest to lowest score range to fit the framework's min to max node score range.
	oldRange := maxScore - minScore
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MaxNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - minScore) * newRange / oldRange) + framework.MinNodeScore
		}
	}

	klog.V(4).InfoS("after normalization: ", "scores", scores)
	return nil
}

// ScoreExtensions : an interface for Score extended functionality
func (ac *AppClass) ScoreExtensions() framework.ScoreExtensions {
	return ac
}

// MinMax : get min and max scores from NodeScoreList
func getMinMaxScores(scores framework.NodeScoreList) (int64, int64) {
	var max int64 = math.MinInt64 // Set to min value
	var min int64 = math.MaxInt64 // Set to max value

	for _, nodeScore := range scores {
		if nodeScore.Score > max {
			max = nodeScore.Score
		}
		if nodeScore.Score < min {
			min = nodeScore.Score
		}
	}
	// return min and max scores
	return min, max
}

func (ac *AppClass) findAppClass(acName string) *appclassv1alpha1.AppClass {
	klog.V(6).InfoS("namespaces: %s", ac.namespaces)
	for _, namespace := range ac.namespaces {
		klog.V(6).InfoS("appClass CR", "namespace", namespace, "name", acName)
		// AppClass could not be placed in several namespaces simultaneously
		appClass := &appclassv1alpha1.AppClass{}
		err := ac.Get(context.TODO(), client.ObjectKey{
			Namespace: namespace,
			Name:      acName,
		}, appClass)
		if err != nil {
			klog.V(4).ErrorS(err, "Failed to get AppClass",
				"namespace", namespace,
				"name", acName,
				"groupVersion", appclassv1alpha1.SchemeGroupVersion.String(),
			)
			continue
		}
		if appClass != nil && appClass.GetUID() != "" {
			return appClass
		}
	}
	return nil
}

func (ac *AppClass) findAppGroup(agName string) *agv1alpha1.AppGroup {
	klog.V(6).InfoS("namespaces: %s", ac.namespaces)
	for _, namespace := range ac.namespaces {
		klog.V(6).InfoS("appGroup CR", "namespace", namespace, "name", agName)
		// AppGroup could not be placed in several namespaces simultaneously
		appGroup := &agv1alpha1.AppGroup{}
		err := ac.Get(context.TODO(), client.ObjectKey{
			Namespace: namespace,
			Name:      agName,
		}, appGroup)
		if err != nil {
			klog.V(4).ErrorS(err, "Failed to get AppGroup",
				"namespace", namespace,
				"name", agName,
				"groupVersion", agv1alpha1.SchemeGroupVersion.String(),
			)
			continue
		}
		if appGroup != nil && appGroup.GetUID() != "" {
			return appGroup
		}
	}
	return nil
}
