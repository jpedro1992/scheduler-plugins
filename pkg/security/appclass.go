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
	"encoding/json"
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

	// MAX_ZONE_CLASSES and MAX_SEGMENT_CLASSES Defaults for maximum classes per zone and segment
	defaultMaxZoneClasses    = 2
	defaultMaxSegmentClasses = 1
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agv1alpha1.AddToScheme(scheme))
	utilruntime.Must(appclassv1alpha1.AddToScheme(scheme))
}

type AppClass struct {
	client.Client
	podLister     corelisters.PodLister
	handle        framework.Handle
	namespaces    []string
	appClassName  string
	maxPerZone    int
	maxPerSegment int
}

// PreFilterState computed at PreFilter and used at Filter and Score.
type PreFilterState struct {
	// boolean that tells the filter and scoring functions to pass the pod since it does not belong to an AppGroup
	scoreEqually bool

	// agName: corresponds to the name of the AppGroup of the pod
	agName string

	// workloadClassName of the pod
	workloadClassName string

	// globalClassName of the pod
	globalClassName string

	// AppGroup CR
	appGroup *agv1alpha1.AppGroup

	// AppClass CR
	appClass *appclassv1alpha1.AppClass

	// Pods already scheduled for a given AppGroup
	scheduledList util.ScheduledList

	// node map for counting affinity classes (workload)
	workloadSatisfiedMap map[string]int64

	// node map for counting anti-affinity classes (workload)
	workloadViolatedMap map[string]int64

	// node map for counting affinity classes (global)
	globalSatisfiedMap map[string]int64

	// node map for counting anti-affinity classes (global)
	globalViolatedMap map[string]int64

	// zone map for counting classes in zones
	zoneClassMap map[string]map[string]bool

	// segment map for counting classes in zones
	segmentClassMap map[string]map[string]bool
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
		Client:        client,
		podLister:     handle.SharedInformerFactory().Core().V1().Pods().Lister(),
		handle:        handle,
		namespaces:    args.Namespaces,
		appClassName:  args.AppClassName,
		maxPerZone:    defaultMaxZoneClasses,    // TODO: make it configurable via plugin args
		maxPerSegment: defaultMaxSegmentClasses, // TODO: make it configurable via plugin args
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

	// Get Global AppClass Name
	globalAppClassName := util.GetAGClassName(agName, appClassCR)

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
			"AGClassName", p.AGClassName,
			"ClassName", p.ClassName,
		)
	}

	// Based on scheduling list check satisfied (same class, affinity) and violated (different class, anti-affinity)
	workloadSatisfiedMap := make(map[string]int64)
	workloadViolatedMap := make(map[string]int64)

	globalSatisfiedMap := make(map[string]int64)
	globalViolatedMap := make(map[string]int64)

	// Get all nodes
	nodeList, err := ac.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, framework.NewStatus(framework.Error, fmt.Sprintf("Error getting the nodelist: %v", err))
	}

	zoneClassMap := make(map[string]map[string]bool)
	segmentClassMap := make(map[string]map[string]bool)

	for _, nodeInfo := range nodeList {
		nodeName := nodeInfo.Node().Name
		// retrieve zone and segment labels
		zone := networkawareutil.GetNodeZone(nodeInfo.Node())
		segment := networkawareutil.GetNodeSegment(nodeInfo.Node())
		klog.V(6).InfoS("Node info",
			"name", nodeInfo.Node().Name,
			"zone", zone,
			"segment", segment)

		var satisfied, violated int64
		var globalSatisfied, globalViolated int64
		classSet := make(map[string]bool)
		globalClassSet := make(map[string]bool)

		for _, p := range scheduledList {
			if p.Hostname != nodeName || p.ClassName == "" {
				continue
			}

			// For workload class
			classSet[p.ClassName] = true
			if p.ClassName == appClassName {
				satisfied++
			} else {
				violated++
			}

			// For global class
			globalClassSet[p.AGClassName] = true
			if p.AGClassName == globalAppClassName {
				globalSatisfied++
			} else {
				globalViolated++
			}
		}

		workloadSatisfiedMap[nodeName] = satisfied
		workloadViolatedMap[nodeName] = violated
		globalSatisfiedMap[nodeName] = globalSatisfied
		globalViolatedMap[nodeName] = globalViolated

		// Update zone/segment maps
		if zone != "" {
			if _, ok := zoneClassMap[zone]; !ok {
				zoneClassMap[zone] = make(map[string]bool)
			}
			for c := range classSet {
				zoneClassMap[zone][c] = true
			}
		}
		if segment != "" {
			if _, ok := segmentClassMap[segment]; !ok {
				segmentClassMap[segment] = make(map[string]bool)
			}
			for c := range classSet {
				segmentClassMap[segment][c] = true
			}
		}
	}

	// Print satisfiedMap[nodeName] and violatedMap[nodeName]
	for n, satisfied := range workloadSatisfiedMap {
		klog.V(6).Infof("Node=%s, satisfied=%d, violated=%d", n, satisfied, workloadViolatedMap[n])
	}

	// --- Logging consistency per zone and segment ---
	for zone, classes := range zoneClassMap {
		if len(classes) > 1 {
			klog.V(4).Infof("Zone %s hosts multiple global classes: %v", zone, classes)
		} else {
			klog.V(6).Infof("Zone %s is consistent: %v", zone, classes)
		}
	}

	for segment, classes := range segmentClassMap {
		if len(classes) > 1 {
			klog.V(4).Infof("Segment %s hosts multiple global classes: %v", segment, classes)
		} else {
			klog.V(6).Infof("Segment %s is consistent: %v", segment, classes)
		}
	}

	// Update PreFilter State
	preFilterState = &PreFilterState{
		scoreEqually:         score,
		agName:               agName,
		appClass:             appClassCR,
		appGroup:             appGroupCR,
		workloadClassName:    appClassName,
		globalClassName:      globalAppClassName,
		scheduledList:        scheduledList,
		workloadSatisfiedMap: workloadSatisfiedMap,
		workloadViolatedMap:  workloadViolatedMap,
		globalSatisfiedMap:   globalSatisfiedMap,
		globalViolatedMap:    globalViolatedMap,
		zoneClassMap:         zoneClassMap,
		segmentClassMap:      segmentClassMap,
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

	// Check violated workload classes for this node
	if len(preFilterState.scheduledList) != 0 {
		klog.V(6).InfoS("Checking the number of violated workload classes... ")
		violated := preFilterState.workloadViolatedMap[nodeInfo.Node().Name]
		if violated > 0 {
			// Node has conflicting workload classes → pod cannot be scheduled here
			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Node %v does not meet requirements. Workload Violated: %v", nodeInfo.Node().Name, violated))
		}
	}

	// Check violated global classes for this node
	if len(preFilterState.scheduledList) != 0 {
		klog.V(6).InfoS("Checking the number of violated global classes... ")
		violated := preFilterState.globalViolatedMap[nodeInfo.Node().Name]
		if violated > 0 {
			// Node has conflicting global classes → pod cannot be scheduled here
			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Node %v does not meet requirements. Global Violated: %v", nodeInfo.Node().Name, violated))
		}
	}

	// Zone-level check - workload classes
	zone := networkawareutil.GetNodeZone(nodeInfo.Node())
	if zone != "" { // check if zone exists
		if classMap, ok := preFilterState.zoneClassMap[zone]; ok && len(classMap) > ac.maxPerZone {
			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Zone %v hosts multiple global classes: %v", zone, classMap))
		}
	}

	// Segment-level check - workload classes
	segment := networkawareutil.GetNodeSegment(nodeInfo.Node())
	if segment != "" { // check if segment exists
		if classMap, ok := preFilterState.segmentClassMap[segment]; ok && len(classMap) > ac.maxPerSegment {
			return framework.NewStatus(framework.Unschedulable,
				fmt.Sprintf("Segment %v hosts multiple global classes: %v", segment, classMap))
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

	// Add workload and global satisfied to score
	workloadScore := preFilterState.workloadSatisfiedMap[nodeName]
	globalScore := preFilterState.globalSatisfiedMap[nodeName]
	score = workloadScore + globalScore

	klog.V(4).InfoS("Score:",
		"pod", pod.GetName(),
		"node", nodeName,
		"workloadScore", workloadScore,
		"globalScore", globalScore,
		"finalScore", score)

	return score, framework.NewStatus(framework.Success, "Satisfied values added as score")
}

// NormalizeScore : normalize scores
func (ac *AppClass) NormalizeScore(ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	scores framework.NodeScoreList) *framework.Status {
	before, _ := json.MarshalIndent(scores, "", "  ")
	klog.V(4).Infof("%s:%s", "Before normalization", string(before))

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

	after, _ := json.MarshalIndent(scores, "", "  ")
	klog.V(4).Infof("%s:%s", "After normalization", string(after))
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
