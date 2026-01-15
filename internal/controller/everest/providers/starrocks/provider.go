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

// Package starrocks contains the provider for StarRocks.
package starrocks

import (
	"context"

	starrocksv1 "github.com/StarRocks/starrocks-kubernetes-operator/pkg/apis/starrocks/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	"github.com/percona/everest-operator/internal/controller/everest/version"
)

const (
	finalizerDeleteStarRocksCluster = "starrocks.com/delete-starrocks-cluster"
)

// Provider is a provider for StarRocks.
type Provider struct {
	providers.ProviderOptions
	*starrocksv1.StarRocksCluster

	// currentStarRocksClusterSpec holds the current StarRocks spec.
	currentStarRocksClusterSpec starrocksv1.StarRocksClusterSpec

	clusterType     consts.ClusterType
	operatorVersion *version.Version
}

// New returns a new provider for StarRocks.
func New(
	ctx context.Context,
	opts providers.ProviderOptions,
) (*Provider, error) {
	starrocksCluster := &starrocksv1.StarRocksCluster{}
	client := opts.C
	err := client.Get(
		ctx,
		types.NamespacedName{Name: opts.DB.GetName(), Namespace: opts.DB.GetNamespace()},
		starrocksCluster)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}

	dbEngine, err := common.GetDatabaseEngine(ctx, client, consts.StarRocksDeploymentName, opts.DB.GetNamespace())
	if err != nil {
		return nil, err
	}
	opts.DBEngine = dbEngine

	// Get operator version.
	v, err := common.GetOperatorVersion(ctx, opts.C, types.NamespacedName{
		Name:      consts.StarRocksDeploymentName,
		Namespace: consts.StarRocksOperatorNamespace,
	})
	if err != nil {
		return nil, err
	}

	currentSpec := starrocksCluster.Spec
	starrocksCluster.Spec = defaultSpec()

	p := &Provider{
		StarRocksCluster:            starrocksCluster,
		ProviderOptions:             opts,
		operatorVersion:             v,
		currentStarRocksClusterSpec: currentSpec,
	}

	// Get cluster type.
	ct, err := common.GetClusterType(ctx, p.C)
	if err != nil {
		return nil, err
	}
	p.clusterType = ct

	if err := p.ensureDefaults(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// Apply returns the applier for StarRocks.
//
//nolint:ireturn
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	return &applier{
		Provider: p,
		ctx:      ctx,
	}
}

func (p *Provider) dbEngineVersionOrDefault() string {
	engineVersion := p.DB.Spec.Engine.Version
	if engineVersion == "" {
		engineVersion = p.DBEngine.BestEngineVersion()
	}
	return engineVersion
}

func (p *Provider) ensureDefaults(ctx context.Context) error {
	// TODO: Add any default configuration for StarRocks
	return nil
}

// Status returns the status of the StarRocks cluster.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	// Get the StarRocks cluster status
	starrocksCluster := &starrocksv1.StarRocksCluster{}
	err := p.C.Get(ctx, types.NamespacedName{
		Name:      p.DB.GetName(),
		Namespace: p.DB.GetNamespace(),
	}, starrocksCluster)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return everestv1alpha1.DatabaseClusterStatus{
				Status: everestv1alpha1.AppStateNew,
			}, false, nil
		}
		return everestv1alpha1.DatabaseClusterStatus{}, false, err
	}

	// Map StarRocks status to Everest status
	status := everestv1alpha1.DatabaseClusterStatus{
		Status: mapStarRocksPhaseToAppState(starrocksCluster),
	}

	// Consider ready when phase is Running
	ready := status.Status == everestv1alpha1.AppStateReady

	return status, ready, nil
}

// Cleanup performs cleanup operations when deleting a StarRocks cluster.
func (p *Provider) Cleanup(ctx context.Context, db *everestv1alpha1.DatabaseCluster) (bool, error) {
	// Delete the StarRocks cluster
	starrocksCluster := &starrocksv1.StarRocksCluster{}
	err := p.C.Get(ctx, types.NamespacedName{
		Name:      db.GetName(),
		Namespace: db.GetNamespace(),
	}, starrocksCluster)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}

	if starrocksCluster.GetDeletionTimestamp().IsZero() {
		err = p.C.Delete(ctx, starrocksCluster)
		if err != nil && !k8serrors.IsNotFound(err) {
			return false, err
		}
	}

	return false, nil
}

// DBObject returns the underlying database object.
func (p *Provider) DBObject() client.Object {
	p.StarRocksCluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   consts.SRAPIGroup,
		Version: p.operatorVersion.ToK8sVersion(),
		Kind:    consts.StarRocksClusterKind,
	})
	return p.StarRocksCluster
}

// RunPreReconcileHook runs the pre-reconcile hook.
func (p *Provider) RunPreReconcileHook(ctx context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}

// SetGroupVersionKind sets the GVK for StarRocksCluster.
func (p *Provider) SetGroupVersionKind(gvk metav1.GroupVersionKind) {
	p.StarRocksCluster.SetGroupVersionKind(schema.GroupVersionKind(gvk))
}

// defaultSpec returns the default spec for StarRocks cluster.
func defaultSpec() starrocksv1.StarRocksClusterSpec {
	return starrocksv1.StarRocksClusterSpec{}
}

// mapStarRocksPhaseToAppState maps StarRocks phase to Everest AppState.
func mapStarRocksPhaseToAppState(cluster *starrocksv1.StarRocksCluster) everestv1alpha1.AppState {
	if cluster.Status.StarRocksFeStatus == nil {
		return everestv1alpha1.AppStateInit
	}
	return everestv1alpha1.AppStateReady
}
