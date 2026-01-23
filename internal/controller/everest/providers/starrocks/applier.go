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

package starrocks

import (
	"context"
	"fmt"
	"strconv"

	starrocksv1 "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	corev1 "k8s.io/api/core/v1"
	_ "k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type applier struct {
	*Provider
	ctx context.Context //nolint:containedctx
}

// ResetDefaults resets the spec to default values.
func (a *applier) ResetDefaults() error {
	a.StarRocksCluster.Spec = defaultSpec()
	return nil
}

// Metadata applies metadata settings.
func (a *applier) Metadata() error {
	if a.StarRocksCluster.GetDeletionTimestamp().IsZero() {
		controllerutil.AddFinalizer(a.StarRocksCluster, finalizerDeleteStarRocksCluster)
	}
	return nil
}

// Paused sets the pause state.
func (a *applier) Paused(paused bool) {
	// StarRocks operator doesn't have a direct pause field in the spec
	// This would need to be implemented based on StarRocks operator capabilities
	// For now, we'll leave it empty
}

// AllowUnsafeConfig allows unsafe configurations.
func (a *applier) AllowUnsafeConfig() {
	// StarRocks doesn't have AllowUnsafeConfig field
	// This can be implemented if needed based on specific requirements
}

// Engine applies engine configuration.
func (a *applier) Engine() error {
	db := a.DB
	engineVersion := a.dbEngineVersionOrDefault()

	custom := db.Spec.Custom

	// Configure Frontend (FE) - query coordination layer
	if a.StarRocksCluster.Spec.StarRocksFeSpec == nil {
		a.StarRocksCluster.Spec.StarRocksFeSpec = &starrocksv1.StarRocksFeSpec{}
	}

	feSpec := a.StarRocksCluster.Spec.StarRocksFeSpec

	// Set FE replicas (typically 1 or 3 for HA)
	// For now, we'll use 1 replica as minimum
	feReplicas := int32(1)
	if _, found := custom["frontend.replicas"]; found {
		replicas, err := strconv.ParseInt(custom["frontend.replicas"], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid int32 for key frontend.replicas: %w", err)
		}
		feReplicas = int32(replicas)
	} else if db.Spec.Engine.Replicas > 1 {
		// For HA, we might want odd number of FE nodes
		feReplicas = int32(3)
	}
	feSpec.Replicas = &feReplicas

	// Set FE image
	feSpec.Image = "starrocks/fe-ubuntu:" + engineVersion

	// Set FE resources
	feResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	// Apply CPU and Memory from Everest spec to FE
	if !db.Spec.Engine.Resources.CPU.IsZero() {
		feResources.Requests[corev1.ResourceCPU] = db.Spec.Engine.Resources.CPU
		feResources.Limits[corev1.ResourceCPU] = db.Spec.Engine.Resources.CPU
	}
	if !db.Spec.Engine.Resources.Memory.IsZero() {
		feResources.Requests[corev1.ResourceMemory] = db.Spec.Engine.Resources.Memory
		feResources.Limits[corev1.ResourceMemory] = db.Spec.Engine.Resources.Memory
	}

	feSpec.ResourceRequirements = feResources

	// Configure Backend (BE) - data storage and query execution
	if a.StarRocksCluster.Spec.StarRocksBeSpec == nil {
		a.StarRocksCluster.Spec.StarRocksBeSpec = &starrocksv1.StarRocksBeSpec{}
	}

	beSpec := a.StarRocksCluster.Spec.StarRocksBeSpec

	// Set BE replicas from Everest spec
	beReplicas := db.Spec.Engine.Replicas
	beSpec.Replicas = &beReplicas

	// Set BE image
	beSpec.Image = "starrocks/be-ubuntu:" + engineVersion

	// Set BE resources
	beResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	// Apply CPU and Memory from Everest spec to BE
	if !db.Spec.Engine.Resources.CPU.IsZero() {
		beResources.Requests[corev1.ResourceCPU] = db.Spec.Engine.Resources.CPU
		beResources.Limits[corev1.ResourceCPU] = db.Spec.Engine.Resources.CPU
	}
	if !db.Spec.Engine.Resources.Memory.IsZero() {
		beResources.Requests[corev1.ResourceMemory] = db.Spec.Engine.Resources.Memory
		beResources.Limits[corev1.ResourceMemory] = db.Spec.Engine.Resources.Memory
	}

	beSpec.ResourceRequirements = beResources

	// Configure BE storage
	if beSpec.StorageVolumes == nil {
		beSpec.StorageVolumes = []starrocksv1.StorageVolume{}
	}

	// Add or update data storage volume
	storageSize := db.Spec.Engine.Storage.Size
	storageClassName := ""
	if db.Spec.Engine.Storage.Class != nil {
		storageClassName = *db.Spec.Engine.Storage.Class
	}

	dataVolume := starrocksv1.StorageVolume{
		Name:             "data",
		StorageClassName: &storageClassName,
		StorageSize:      storageSize.String(),
		MountPath:        "/opt/starrocks/be/storage",
	}

	// Replace or add the data volume
	found := false
	for i, vol := range beSpec.StorageVolumes {
		if vol.Name == "data" {
			beSpec.StorageVolumes[i] = dataVolume
			found = true
			break
		}
	}
	if !found {
		beSpec.StorageVolumes = append(beSpec.StorageVolumes, dataVolume)
	}

	return nil
}

// EngineFeatures applies engine-specific features.
func (a *applier) EngineFeatures() error {
	// StarRocks-specific features can be implemented here
	// For example: CN (Compute Node) configuration for auto-scaling
	return nil
}

// Proxy applies proxy configuration.
func (a *applier) Proxy() error {
	// StarRocks uses FE for query routing, no separate proxy like HAProxy
	// This can be left empty unless you want to configure external load balancers
	return nil
}

// DataSource applies data source configuration.
func (a *applier) DataSource() error {
	// TODO: Implement data source configuration if needed for StarRocks
	return nil
}

// DataImport applies data import configuration.
func (a *applier) DataImport() error {
	// TODO: Implement data import logic for StarRocks
	return nil
}

// Monitoring applies monitoring configuration.
func (a *applier) Monitoring() error {
	// TODO: Implement monitoring integration (e.g., PMM, Prometheus)
	// StarRocks can export metrics to Prometheus
	return nil
}

// PodSchedulingPolicy applies pod scheduling policy.
func (a *applier) PodSchedulingPolicy() error {
	// TODO: Implement affinity/anti-affinity rules based on PodSchedulingPolicy
	return nil
}

// Backup applies backup configuration.
func (a *applier) Backup() error {
	// TODO: Implement backup configuration if StarRocks operator supports it
	return nil
}
