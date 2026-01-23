package providers

import (
	"context"
	"time"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OperationHandler handles database operations
type OperationHandler interface {
	// Pre-reconcile hook - called before processing operation
	// Returns HookResult to control requeue behavior
	RunPreReconcileHook(ctx context.Context, opsRequest *everestv1alpha1.OpsRequest) (HookResult, error)

	// Operation handlers
	// Returns:
	// - *DatabaseCluster: updated cluster (nil to skip cluster update)
	// - []client.Object: additional objects to create/update (e.g., backup CR)
	// - HookResult: controls requeue behavior
	// - error: operation error

	// HandleStart(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	// HandleStop(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	// HandleRestart(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	// HandleVerticalScaling(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	// HandleHorizontalScaling(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	// HandleVolumeExpansion(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	// HandleReconfiguring(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	// HandleUpgrade(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	HandleSwitchover(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	HandleBackup(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	HandleRestore(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	HandleExpose(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)
	HandleCustom(opsRequest *everestv1alpha1.OpsRequest) ([]client.Object, error)

	// HandleOpsRequestStatus updates the OpsRequest status based on the operation type and cluster state
	// This is called after successfully applying changes
	HandleOpsRequestStatus(ctx context.Context) (everestv1alpha1.OpsRequestClusterStatus, bool, error)
}

// Helper functions for common HookResults

// NoRequeue returns a HookResult with no requeue
func NoRequeue() HookResult {
	return HookResult{
		Requeue:      false,
		RequeueAfter: 0,
		Message:      "",
	}
}

// Requeue returns a HookResult that triggers requeue
func Requeue(after time.Duration, message string) HookResult {
	return HookResult{
		Requeue:      true,
		RequeueAfter: after,
		Message:      message,
	}
}
