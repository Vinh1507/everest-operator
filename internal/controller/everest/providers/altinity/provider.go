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
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
	chi.Spec = defaultSpec()

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
	return everestv1alpha1.DatabaseClusterStatus{}, true, nil
	// c := p.C
	pg := p.ClickHouseInstallation

	status := p.DB.Status
	// prevStatus := status
	status.Status = everestv1alpha1.AppState(pg.Status.Status).WithCreatingState()
	// status.Hostname = pg.Status.Host
	status.Ready = int32(pg.Status.HostsCount)
	status.Size = int32(pg.Status.HostsCount)
	// status.Port = pg.GetStatus()
	// status.CRVersion = pg.Spec.CRVersion
	status.Details = common.StatusAsPlainTextOrEmptyString(pg.Status)

	// // If a restore is running for this database, set the database status to restoring
	// if restoring, err := common.IsDatabaseClusterRestoreRunning(ctx, c, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
	// 	return status, false, err
	// } else if restoring {
	// 	status.Status = everestv1alpha1.AppStateRestoring
	// }

	// if ok, err := isPVCResizing(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
	// 	return status, false, err
	// } else if ok {
	// 	status.Status = everestv1alpha1.AppStateResizingVolumes
	// }

	// // If the PVC resize is currently in progress, or just finished, we need to
	// // check if it failed in order to set or clear the error condition.
	// if status.Status == everestv1alpha1.AppStateResizingVolumes ||
	// 	prevStatus.Status == everestv1alpha1.AppStateResizingVolumes {
	// 	meta.RemoveStatusCondition(&status.Conditions, everestv1alpha1.ConditionTypeVolumeResizeFailed)
	// 	if failed, condMessage, err := common.VerifyPVCResizeFailure(ctx, p.C, p.DB.GetName(), p.DB.GetNamespace()); err != nil {
	// 		return status, false, err
	// 	} else if failed {
	// 		// XXX: If a PVC resize failed, the DB operator will revert the
	// 		// spec to the previous one and unset the annotation we use to
	// 		// detect that a PVC resize is in progress. This means that we
	// 		// would move away from the ResizingVolumes state until the next
	// 		// reconcile loop where the PVC resize will be retried. To avoid
	// 		// having the state change back and forth, we keep the state as
	// 		// ResizingVolumes until the PVC resize is successful.
	// 		status.Status = everestv1alpha1.AppStateResizingVolumes
	// 		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
	// 			Type:               everestv1alpha1.ConditionTypeVolumeResizeFailed,
	// 			Status:             metav1.ConditionTrue,
	// 			Reason:             everestv1alpha1.ReasonVolumeResizeFailed,
	// 			Message:            condMessage,
	// 			LastTransitionTime: metav1.Now(),
	// 			ObservedGeneration: p.DB.GetGeneration(),
	// 		})
	// 	}
	// }

	// if upgrading, err := p.isDatabaseUpgrading(ctx); err != nil {
	// 	return status, false, err
	// } else if upgrading {
	// 	status.Status = everestv1alpha1.AppStateUpgrading
	// }

	// recCRVer, err := common.GetRecommendedCRVersion(ctx, p.C, consts.CHIDeploymentName, p.DB)
	// if err != nil && !k8serrors.IsNotFound(err) {
	// 	return status, false, err
	// }
	// status.RecommendedCRVersion = recCRVer

	return status, true, nil
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
