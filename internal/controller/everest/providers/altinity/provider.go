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
	"strings"

	chiv1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
)

// Provider is a provider for Percona PostgreSQL.
type Provider struct {
	*chiv1.ClickHouseInstallation
	providers.ProviderOptions
	clusterType    consts.ClusterType
	currentCHISpec chiv1.ChiSpec
}

const (
	finalizerDeleteCHIPVC = "percona.com/delete-pvc"
	finalizerDeleteCHISSL = "percona.com/delete-ssl"

	conditionTypeReady = "Ready"
)

// New returns a new provider for Percona PostgreSQL.
func New(
	ctx context.Context,
	opts providers.ProviderOptions,
) (*Provider, error) {
	client := opts.C
	chi := &chiv1.ClickHouseInstallation{}
	err := client.Get(ctx, types.NamespacedName{Name: opts.DB.GetName(), Namespace: opts.DB.GetNamespace()}, chi)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}

	// dbEngine, err := common.GetDatabaseEngine(ctx, client, consts.chiDeploymentName, opts.DB.GetNamespace())
	// if err != nil {
	// 	return nil, err
	// }
	// opts.DBEngine = dbEngine

	currentCHISpec := chi.Spec

	p := &Provider{
		ClickHouseInstallation: chi,
		ProviderOptions:        opts,
		currentCHISpec:         currentCHISpec,
	}
	ct, err := common.GetClusterType(ctx, p.C)
	if err != nil {
		return nil, err
	}
	p.clusterType = ct
	return p, nil
}

// Apply returns the chi applier.
//
//nolint:ireturn
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	return &applier{
		Provider: p,
		ctx:      ctx,
	}
}

// +kubebuilder:rbac:groups=postgres-operator.crunchydata.com,resources=postgresclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch

// Status builds the DatabaseCluster Status based on the current state of the CHI.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	chi := p.ClickHouseInstallation
	if chi == nil || chi.GetUID() == "" || chi.Status == nil || chi.Status.Status == "" {
		return everestv1alpha1.DatabaseClusterStatus{
			Status: everestv1alpha1.AppStateCreating,
		}, false, nil
	}

	status := p.DB.Status
	status.Status = statusToAppState(chi.Status.Status).WithCreatingState()
	status.Hostname = strings.Join(chi.Status.Endpoints, ",")
	status.Size = int32(chi.Status.HostsCount)
	status.Details = common.StatusAsPlainTextOrEmptyString(chi.Status)

	// Calculate Ready pods
	podList := &corev1.PodList{}
	labelSelector := client.MatchingLabels{
		"clickhouse.altinity.com/chi": chi.Name,
	}
	if err := p.C.List(ctx, podList, labelSelector, client.InNamespace(chi.Namespace)); err != nil {
		return status, false, err
	}

	readyCount := 0
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					readyCount++
					break
				}
			}
		}
	}
	status.Ready = int32(readyCount)

	if status.Status == everestv1alpha1.AppStateReady {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionTrue,
			Reason:             "ClusterReady",
			Message:            "Cluster is ready",
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: p.DB.GetGeneration(),
		})
	} else {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             "ClusterNotReady",
			Message:            "Cluster is not ready",
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: p.DB.GetGeneration(),
		})
	}

	return status, true, nil
}

func statusToAppState(status string) everestv1alpha1.AppState {
	switch status {
	case chiv1.StatusCompleted:
		return everestv1alpha1.AppStateReady
	case chiv1.StatusInProgress:
		return everestv1alpha1.AppStateInit
	case chiv1.StatusTerminating:
		return everestv1alpha1.AppStateDeleting
	default:
		return everestv1alpha1.AppStateUnknown
	}
}

// Cleanup runs the cleanup routines and returns true if the cleanup is done.
func (p *Provider) Cleanup(ctx context.Context, database *everestv1alpha1.DatabaseCluster) (bool, error) {
	// Even though we no longer set the DBBackupCleanupFinalizer, we still need
	// to handle the cleanup to ensure backward compatibility.
	done, err := common.HandleDBBackupsCleanup(ctx, p.C, database)
	if err != nil || !done {
		return done, err
	}
	return common.HandleUpstreamClusterCleanup(ctx, p.C, database, &chiv1.ClickHouseInstallation{})
}

// DBObject returns the ClickHouseInstallation object.
//
//nolint:ireturn
func (p *Provider) DBObject() client.Object {
	p.ClickHouseInstallation.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   consts.CHIAPIGroup,
		Version: "v1",
		Kind:    consts.ClickHouseInstallationKind,
	})
	return p.ClickHouseInstallation
}

// RunPreReconcileHook runs the pre-reconcile hook for the PG provider.
func (p *Provider) RunPreReconcileHook(_ context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}
