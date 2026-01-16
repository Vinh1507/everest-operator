// everest-operator
// Copyright (C) 2022 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package altinity

import (
	"context"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type applier struct {
	*Provider
	ctx context.Context //nolint:containedctx
}

func (p *applier) ResetDefaults() error {
	p.ClickHouseInstallation.Spec = defaultSpec()
	return nil
}

func (p *applier) Paused(paused bool) {
	if paused {
		p.ClickHouseInstallation.Spec.Stop = types.NewStringBool(true)
	} else {
		p.ClickHouseInstallation.Spec.Stop = types.NewStringBool(false)
	}
}

func (p *applier) AllowUnsafeConfig() {
}

func (p *applier) Metadata() error {
	if p.ClickHouseInstallation.GetDeletionTimestamp().IsZero() {
		for _, f := range []string{
			finalizerDeleteCHIPVC,
			finalizerDeleteCHISSL,
		} {
			controllerutil.AddFinalizer(p.ClickHouseInstallation, f)
		}
	}
	return nil
}

func (p *applier) Engine() error {
	chi := p.ClickHouseInstallation
	database := p.DB

	if chi.Spec.Configuration == nil || len(chi.Spec.Configuration.Clusters) == 0 {
		chi.Spec = defaultSpec()
	}

	if chi.Spec.Templates == nil {
		chi.Spec.Templates = &chiv1.Templates{}
	}

	if chi.Spec.Defaults == nil {
		chi.Spec.Defaults = &chiv1.Defaults{
			Templates: chiv1.NewTemplatesList(),
		}
	} else if chi.Spec.Defaults.Templates == nil {
		chi.Spec.Defaults.Templates = chiv1.NewTemplatesList()
	}

	// Layout
	chi.Spec.Configuration.Clusters[0].Layout = &chiv1.ChiClusterLayout{
		ShardsCount:   int(database.Spec.Engine.Replicas),
		ReplicasCount: 1,
	}

	// Resources
	resources := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	if !database.Spec.Engine.Resources.CPU.IsZero() {
		resources.Requests[corev1.ResourceCPU] = database.Spec.Engine.Resources.CPU
		resources.Limits[corev1.ResourceCPU] = database.Spec.Engine.Resources.CPU
	}

	if !database.Spec.Engine.Resources.Memory.IsZero() {
		resources.Requests[corev1.ResourceMemory] = database.Spec.Engine.Resources.Memory
		resources.Limits[corev1.ResourceMemory] = database.Spec.Engine.Resources.Memory
	}

	// podTemplate := chiv1.PodTemplate{
	// 	Name: "pod-template",
	// 	Spec: corev1.PodSpec{
	// 		Containers: []corev1.Container{
	// 			{
	// 				Name:      "clickhouse",
	// 				Resources: *resources,
	// 			},
	// 		},
	// 	},
	// }

	// Storage
	volumeClaimTemplate := chiv1.VolumeClaimTemplate{
		Name: "storage-template",
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			StorageClassName: database.Spec.Engine.Storage.Class,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: database.Spec.Engine.Storage.Size,
				},
			},
		},
	}

	// chi.Spec.Templates.PodTemplates = []chiv1.PodTemplate{podTemplate}
	chi.Spec.Templates.VolumeClaimTemplates = []chiv1.VolumeClaimTemplate{volumeClaimTemplate}

	// chi.Spec.Defaults.Templates.PodTemplate = podTemplate.Name
	chi.Spec.Defaults.Templates.DataVolumeClaimTemplate = volumeClaimTemplate.Name

	return nil
}

func (p *applier) EngineFeatures() error {
	// Nothing to do here for PG
	return nil
}

func (p *applier) Proxy() error {
	return nil
}

func (p *applier) Backup() error {
	return nil
}

func (p *applier) DataSource() error {
	return nil
}

func (p *applier) DataImport() error {
	return nil
}

func (p *applier) Monitoring() error {
	return nil
}

func (p *applier) PodSchedulingPolicy() error {
	return nil
}

// getPMMImagePullPolicy returns the PMM image pull policy to be used for the DB cluster.
// The logic is as follows:
// 1. If this is a new DB cluster, use PullIfNotPresent.
// 2. If this is an existing DB cluster and PMM was enabled before, use the current image pull policy to prevent changes in spec.
// 3. If this is an existing DB cluster and PMM was not enabled before, use PullIfNotPresent.
func (p *applier) getPMMImagePullPolicy() corev1.PullPolicy {
	return corev1.PullIfNotPresent
}

func defaultSpec() chiv1.ChiSpec {
	return chiv1.ChiSpec{
		Defaults: &chiv1.Defaults{
			Templates: chiv1.NewTemplatesList(),
		},
		Templates: &chiv1.Templates{},
		Configuration: &chiv1.Configuration{
			Clusters: []*chiv1.Cluster{
				{
					Name: "default",
					Layout: &chiv1.ChiClusterLayout{
						ShardsCount:   1,
						ReplicasCount: 1,
					},
				},
			},
			// Profiles: map[string]chiv1.ChiProfile{
			// 	"default": {
			// 		Settings: map[string]string{
			// 			"max_memory_usage":       "10000000000",
			// 			"use_uncompressed_cache": "0",
			// 			"load_balancing":         "random",
			// 		},
			// 	},
			// },
			// Quotas: map[string]chiv1.ChiQuota{
			// 	"default": {
			// 		Intervals: []chiv1.ChiQuotaInterval{
			// 			{
			// 				Duration: 3600,
			// 			},
			// 		},
			// 	},
			// },
		},
	}
}
