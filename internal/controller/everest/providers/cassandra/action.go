package cassandra

import (
	"context"

	"github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type opsHandler struct {
	*Provider
	ctx        context.Context //nolint:containedctx
	opsRequest v1alpha1.OpsRequest
}

// HandleOpsRequestStatus implements providers.OperationHandler.
func (o *opsHandler) HandleOpsRequestStatus(ctx context.Context) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	panic("unimplemented")
}

// HandleBackup implements providers.OperationHandler.
func (o *opsHandler) HandleBackup(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	panic("unimplemented")
}

// HandleCustom implements providers.OperationHandler.
func (o *opsHandler) HandleCustom(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	panic("unimplemented")
}

// HandleExpose implements providers.OperationHandler.
func (o *opsHandler) HandleExpose(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	panic("unimplemented")
}

// HandleRestore implements providers.OperationHandler.
func (o *opsHandler) HandleRestore(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	panic("unimplemented")
}

// HandleSwitchover implements providers.OperationHandler.
func (o *opsHandler) HandleSwitchover(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	panic("unimplemented")
}

// RunPreReconcileHook implements providers.OperationHandler.
// Subtle: this method shadows the method (*Provider).RunPreReconcileHook of opsHandler.Provider.
func (o *opsHandler) RunPreReconcileHook(ctx context.Context, opsRequest *v1alpha1.OpsRequest) (providers.HookResult, error) {
	panic("unimplemented")
}

//nolint:ireturn
func (p *Provider) GetOpsHandler(ctx context.Context, opsRequest v1alpha1.OpsRequest) providers.OperationHandler {
	return &opsHandler{
		Provider:   p,
		ctx:        ctx,
		opsRequest: opsRequest,
	}
}
