package everest

import (
	"context"
	"fmt"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"

	// engine providers
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	"github.com/percona/everest-operator/internal/controller/everest/providers/altinity"
	"github.com/percona/everest-operator/internal/controller/everest/providers/cassandra"
	"github.com/percona/everest-operator/internal/controller/everest/providers/pg"
	"github.com/percona/everest-operator/internal/controller/everest/providers/psmdb"
	"github.com/percona/everest-operator/internal/controller/everest/providers/pxc"
	"github.com/percona/everest-operator/internal/controller/everest/providers/starrocks"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewDBProvider creates a provider based on dbCluster.Spec.Engine.Type.
func NewDBProvider(
	ctx context.Context,
	c client.Client,
	dbCluster *everestv1alpha1.DatabaseCluster,
) (dbProvider, error) {
	if dbCluster == nil {
		return nil, fmt.Errorf("dbCluster is nil")
	}

	opts := providers.ProviderOptions{
		C:  c,
		DB: dbCluster,
	}

	switch dbCluster.Spec.Engine.Type {
	case everestv1alpha1.DatabaseEnginePXC:
		return pxc.New(ctx, opts)

	case everestv1alpha1.DatabaseEnginePostgresql:
		return pg.New(ctx, opts)

	case everestv1alpha1.DatabaseEnginePSMDB:
		return psmdb.New(ctx, opts)

	case everestv1alpha1.DatabaseEngineStarRocks:
		return starrocks.New(ctx, opts)

	case everestv1alpha1.DatabaseEngineClickhouse:
		return altinity.New(ctx, opts)

	case everestv1alpha1.DatabaseEngineCassandra:
		return cassandra.New(ctx, opts)

	default:
		return nil, fmt.Errorf("unsupported engine type %q", dbCluster.Spec.Engine.Type)
	}
}
