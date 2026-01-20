package cassandra

import (
	"context"
	"fmt"
	"strings"

	cassv1 "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/consts"
	"github.com/percona/everest-operator/internal/controller/everest/common"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Provider struct {
	providers.ProviderOptions
	*cassv1.CassandraDatacenter
	clusterType consts.ClusterType
}

const (
	finalizerDeleteCassPVC = "percona.com/delete-pvc"
	finalizerDeleteCassSSL = "percona.com/delete-ssl"
)

func New(
	ctx context.Context,
	opts providers.ProviderOptions,
) (*Provider, error) {
	client := opts.C
	cassDc := &cassv1.CassandraDatacenter{}
	err := client.Get(ctx, types.NamespacedName{Name: opts.DB.GetName(), Namespace: opts.DB.GetNamespace()}, cassDc)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}

	opts.DBEngine, err = common.GetDatabaseEngine(ctx, client, consts.CassandraDeploymentName, opts.DB.GetNamespace())
	if err != nil {
		return nil, err
	}

	// Get operator version.
	_, err = common.GetOperatorVersion(ctx, opts.C, types.NamespacedName{
		Name:      consts.CassandraDeploymentName,
		Namespace: consts.CassandraDeploymentName,
	})
	if err != nil {
		return nil, err
	}

	p := &Provider{
		CassandraDatacenter: cassDc,
		ProviderOptions:     opts,
	}

	if p.Spec.ClusterName == "" {
		p.Spec = defaultSpec()
	}

	return p, nil
}

// Apply returns the applier for Cassandra.
//
//nolint:ireturn
func (p *Provider) Apply(ctx context.Context) everestv1alpha1.Applier {
	return &applier{
		Provider: p,
		ctx:      ctx,
	}
}

// Status builds the DatabaseCluster Status based on the current state of the CassandraDatacenter.
func (p *Provider) Status(ctx context.Context) (everestv1alpha1.DatabaseClusterStatus, bool, error) {
	status := p.DB.Status
	cass := p.CassandraDatacenter

	// TODO: Proper mapping of Cassandra status to Everest status
	status.Status = everestv1alpha1.AppState(cass.Status.CassandraOperatorProgress).WithCreatingState()
	if cass.Status.CassandraOperatorProgress == "Ready" {
		status.Status = everestv1alpha1.AppStateReady
	}

	status.Hostname = fmt.Sprintf("%s-%s-service.%s", cass.Spec.ClusterName, cass.Name, cass.Namespace)
	// status.Port = 9042 // generic CQL port

	status.Size = cass.Spec.Size
	status.Ready = int32(len(cass.Status.NodeStatuses))

	conds := make([]string, 0, len(cass.Status.Conditions))
	for _, c := range cass.Status.Conditions {
		conds = append(conds, fmt.Sprintf("%v", c.Type))
	}
	status.Message = strings.Join(conds, ";")

	status.Conditions = make([]metav1.Condition, 0, len(cass.Status.Conditions))
	for _, c := range cass.Status.Conditions {
		reason := c.Reason
		if reason == "" {
			reason = string(c.Type)
		}
		status.Conditions = append(status.Conditions, metav1.Condition{
			Type:               string(c.Type),
			Status:             metav1.ConditionStatus(c.Status),
			LastTransitionTime: c.LastTransitionTime,
			Reason:             reason,
			Message:            c.Message,
		})
	}

	return status, true, nil
}

// RunPreReconcileHook runs the pre-reconcile hook.
func (p *Provider) RunPreReconcileHook(ctx context.Context) (providers.HookResult, error) {
	return providers.HookResult{}, nil
}

// Cleanup runs the cleanup routines and returns true if the cleanup is done.
func (p *Provider) Cleanup(ctx context.Context, database *everestv1alpha1.DatabaseCluster) (bool, error) {
	// Even though we no longer set the DBBackupCleanupFinalizer, we still need
	// to handle the cleanup to ensure backward compatibility.
	done, err := common.HandleDBBackupsCleanup(ctx, p.C, database)
	if err != nil || !done {
		return done, err
	}
	return common.HandleUpstreamClusterCleanup(ctx, p.C, database, &cassv1.CassandraDatacenter{})
}

// DBObject returns the CassandraDatacenter object.
//
//nolint:ireturn
func (p *Provider) DBObject() client.Object {
	p.CassandraDatacenter.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   consts.CassAPIGroup,
		Version: "v1beta1", // Use constant or variable
		Kind:    consts.CassandraDatacenterKind,
	})
	return p.CassandraDatacenter
}
