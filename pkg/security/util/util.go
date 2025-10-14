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

package util

import (
	appclassv1alpha1 "github.com/diktyo-io/appclass-api/pkg/apis/appclass/v1alpha1"
	v1 "k8s.io/api/core/v1"
	networkawareutil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
)

// ScheduledInfo : struct for scheduled pods
type ScheduledInfo struct {
	// Pod Name
	Name string

	// Pod AppGroup Name
	AgName string

	// AGClassName is the Global className of the AppGroup
	AGClassName string

	// className of Pod from AppClass CR
	ClassName string

	// Pod AppGroup Workload Selector
	Selector string

	// Replica ID
	ReplicaID string

	// Hostname
	Hostname string
}

type ScheduledList []ScheduledInfo

// GetScheduledList : get Pods already scheduled in the cluster for that specific AppGroup
func GetScheduledList(pods []*v1.Pod, appClassCR *appclassv1alpha1.AppClass) ScheduledList {
	// scheduledList: Deployment name, replicaID, hostname
	scheduledList := ScheduledList{}

	for _, p := range pods {
		if len(p.Spec.NodeName) != 0 {
			scheduledInfo := ScheduledInfo{
				Name:     p.Name,
				AgName:   networkawareutil.GetPodAppGroupLabel(p),
				Selector: networkawareutil.GetPodAppGroupSelector(p),
				AGClassName: GetAGClassName(networkawareutil.GetPodAppGroupLabel(p),
					appClassCR),
				ClassName: GetClassName(networkawareutil.GetPodAppGroupLabel(p),
					networkawareutil.GetPodAppGroupSelector(p),
					appClassCR),
				ReplicaID: string(p.GetUID()),
				Hostname:  p.Spec.NodeName,
			}
			scheduledList = append(scheduledList, scheduledInfo)
		}
	}
	// Return the scheduledList
	return scheduledList
}

func GetClassName(agName, selector string, appClassCR *appclassv1alpha1.AppClass) string {
	// Favor status if available
	if len(appClassCR.Status.ApplicationClasses) > 0 {
		for _, class := range appClassCR.Status.ApplicationClasses {
			for _, appInfo := range class.AppInfos {
				if agName == appInfo.AppGroup {
					// check each workload
					for _, wl := range appInfo.AppGroupWorkloads {
						if selector == wl {
							return class.Name
						}
					}
				}
			}
		}
	}

	// Fallback to spec
	for _, class := range appClassCR.Spec.ApplicationClasses {
		for _, workload := range class.AppGroupWorkloads {
			if agName == workload.AppGroup {
				for _, wl := range workload.AppGroupWorkloads {
					if selector == wl {
						return class.Name
					}
				}
			}
		}
	}

	return ""
}

func GetAGClassName(agName string, appClassCR *appclassv1alpha1.AppClass) string {
	// Prefer the Status section if available
	if appClassCR.Status.GlobalClassification.AppGroups != nil {
		for _, ag := range appClassCR.Status.GlobalClassification.AppGroups {
			if ag.Name == agName {
				return ag.Class
			}
		}
	}

	// Fallback to Spec if Status is not populated
	if appClassCR.Spec.GlobalClassification.AppGroups != nil {
		for _, ag := range appClassCR.Spec.GlobalClassification.AppGroups {
			if ag.AppGroup == agName {
				return ag.Class
			}
		}
	}
	return ""
}
