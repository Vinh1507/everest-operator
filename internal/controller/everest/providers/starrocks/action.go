package starrocks

import (
	"context"
	"fmt"

	"github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type opsHandler struct {
	*Provider
	ctx        context.Context //nolint:containedctx
	opsRequest v1alpha1.OpsRequest
}

//nolint:ireturn
func (p *Provider) GetOpsHandler(ctx context.Context, opsRequest v1alpha1.OpsRequest) providers.OperationHandler {
	return &opsHandler{
		Provider:   p,
		ctx:        ctx,
		opsRequest: opsRequest,
	}
}

// HandleBackup implements providers.OperationHandler.
func (o *opsHandler) HandleBackup(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
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
	return providers.HookResult{}, nil
}

func (o *opsHandler) HandleCustom(
	opsRequest *v1alpha1.OpsRequest,
) ([]client.Object, error) {

	params := opsRequest.Spec.Params

	action, ok := params["action"]
	if !ok || action == "" {
		return nil, fmt.Errorf("missing param: action")
	}

	db := o.DB.DeepCopy()

	switch action {

	case "UpdateFrontendReplicas":
		replicas, ok := params["replicas"]
		if !ok || replicas == "" {
			return nil, fmt.Errorf("missing param: replicas")
		}

		if db.Spec.Custom == nil {
			db.Spec.Custom = map[string]string{}
		}

		db.Spec.Custom["frontend.replicas"] = replicas

		return []client.Object{db}, nil

	default:
		return nil, fmt.Errorf("unsupported action: %s", action)
	}
}

// HandleOpsRequestStatus implements providers.OperationHandler.
func (o *opsHandler) HandleOpsRequestStatus(ctx context.Context) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	status := v1alpha1.OpsRequestClusterStatus{}
	dbStatus := o.DB.Status

	totalReplicas := int32(0)
	readyReplicas := int32(0)

	if &dbStatus.Ready != nil {
		readyReplicas = dbStatus.Ready
	}
	if &dbStatus.Size != nil {
		totalReplicas = dbStatus.Size
	}

	statusReady := false

	if totalReplicas == 0 {
		status.Phase = v1alpha1.OpsRequestPhasePending
		status.Message = "Waiting for replicas to be initialized"
		status.Reason = "ReplicasNotInitialized"
		status.Progress = 0
	} else if readyReplicas < totalReplicas {
		status.Phase = v1alpha1.OpsRequestPhaseProcessing
		status.Message = fmt.Sprintf("Replicas updating: %d/%d ready", readyReplicas, totalReplicas)
		status.Reason = "ReplicasUpdating"
		status.Progress = int32(float64(readyReplicas) / float64(totalReplicas) * 100)
	} else {
		status.Phase = v1alpha1.OpsRequestPhaseSucceeded
		status.Message = fmt.Sprintf("Operation completed successfully: %d/%d replicas ready", readyReplicas, totalReplicas)
		status.Reason = "OperationSucceeded"
		status.Progress = 100
		statusReady = true

	}
	if status.Output == nil {
		status.Output = make(map[string]string)
	}
	status.Output["readyReplicas"] = fmt.Sprintf("%d", readyReplicas)
	status.Output["totalReplicas"] = fmt.Sprintf("%d", totalReplicas)

	return status, statusReady, nil
}
