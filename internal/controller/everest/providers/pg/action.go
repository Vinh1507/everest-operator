package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/percona/everest-operator/api/everest/v1alpha1"
	"github.com/percona/everest-operator/internal/controller/everest/providers"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// PostgreSQL Operator annotations
	TriggerSwitchoverAnnotation = "postgres-operator.crunchydata.com/trigger-switchover"

	// Everest tracking annotations
	LastSwitchoverAnnotation = "everest.percona.com/last-switchover-requested-at"
	RestartAnnotation        = "everest.percona.com/restart-at"

	// PostgreSQL Operator labels
	PGOClusterLabel  = "postgres-operator.crunchydata.com/cluster"
	PGOInstanceLabel = "postgres-operator.crunchydata.com/instance"
	PGORoleLabel     = "postgres-operator.crunchydata.com/role"

	// Role values
	RoleMaster  = "master"
	RoleReplica = "replica"

	// Requeue intervals
	SwitchoverCheckInterval = 10 * time.Second
	DefaultCheckInterval    = 15 * time.Second

	// Timeout for switchover
	SwitchoverTimeout = 5 * time.Minute
)

type opsHandler struct {
	*Provider
	ctx        context.Context //nolint:containedctx
	opsRequest v1alpha1.OpsRequest
}

func (p *Provider) GetOpsHandler(ctx context.Context, opsRequest v1alpha1.OpsRequest) providers.OperationHandler {
	return &opsHandler{
		Provider:   p,
		ctx:        ctx,
		opsRequest: opsRequest,
	}
}

// HandleOpsRequestStatus implements providers.OperationHandler.
func (o *opsHandler) HandleOpsRequestStatus(ctx context.Context) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	logger := log.FromContext(ctx)
	status := v1alpha1.OpsRequestClusterStatus{}

	// Get current database cluster status
	dbStatus, statusReady, err := o.Provider.Status(ctx)
	if err != nil {
		logger.Error(err, "Failed to get database cluster status")
		return status, false, err
	}

	// Initialize output map
	if status.Output == nil {
		status.Output = make(map[string]string)
	}

	// Handle different operation types
	switch o.opsRequest.Spec.Type {
	case v1alpha1.ActionSwitchover:
		return o.handleSwitchoverStatus(ctx, dbStatus, statusReady)

	case v1alpha1.ActionStart, v1alpha1.ActionStop:
		return o.handleStartStopStatus(ctx, dbStatus, statusReady)

	case v1alpha1.ActionRestart:
		return o.handleRestartStatus(ctx, dbStatus, statusReady)

	case v1alpha1.ActionHorizontalScaling, v1alpha1.ActionVerticalScaling, v1alpha1.ActionVolumeExpansion:
		return o.handleScalingStatus(ctx, dbStatus, statusReady)

	case v1alpha1.ActionUpgrade:
		return o.handleUpgradeStatus(ctx, dbStatus, statusReady)

	default:
		// For other operations, use database status directly
		status.Phase = v1alpha1.OpsRequestPhase(dbStatus.Status)
		status.Message = dbStatus.Message
		status.Progress = o.calculateProgress(dbStatus, statusReady)
		return status, statusReady, nil
	}
}

// handleSwitchoverStatus checks switchover operation status
func (o *opsHandler) handleSwitchoverStatus(
	ctx context.Context,
	dbStatus v1alpha1.DatabaseClusterStatus,
	statusReady bool,
) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	logger := log.FromContext(ctx)
	status := v1alpha1.OpsRequestClusterStatus{
		Output: make(map[string]string),
	}

	// Get switchover configuration from spec
	switchoverEnabled := false
	targetInstance := ""

	if o.DB.Spec.Custom != nil {
		if enabled, ok := o.DB.Spec.Custom["patroni.switchover.enabled"]; ok {
			switchoverEnabled = enabled == "true"
		}
		if target, ok := o.DB.Spec.Custom["patroni.switchover.targetInstance"]; ok {
			targetInstance = target
		}
	}

	// Get annotations
	triggerAnnotation := o.DB.Annotations[TriggerSwitchoverAnnotation]
	lastSwitchover := o.DB.Annotations[LastSwitchoverAnnotation]

	if lastSwitchover != "" {
		status.Output["lastSwitchoverAt"] = lastSwitchover
	}
	if triggerAnnotation != "" {
		status.Output["triggerAnnotation"] = triggerAnnotation
	}
	status.Output["switchoverEnabled"] = fmt.Sprintf("%v", switchoverEnabled)

	// Check if switchover was configured
	if targetInstance == "" {
		// Get from OpsRequest params if not in DB spec
		if target, ok := o.opsRequest.Spec.Params["targetInstance"]; ok {
			targetInstance = target
		} else {
			status.Phase = v1alpha1.OpsRequestPhaseFailed
			status.Reason = "MissingTargetInstance"
			status.Message = "Target instance not specified"
			status.Progress = 0
			return status, false, nil
		}
	}

	status.Output["targetInstance"] = targetInstance

	// Get current primary instance from pods
	currentPrimary, err := o.getCurrentPrimaryInstance(ctx)
	if err != nil {
		logger.Error(err, "Failed to get current primary instance")
		// Don't fail, just continue with empty primary
		currentPrimary = ""
	}

	if currentPrimary != "" {
		status.Output["currentPrimary"] = currentPrimary
	}

	// Check timeout
	// if lastSwitchover != "" {
	// 	switchoverTime, err := time.Parse(time.RFC3339, lastSwitchover)
	// 	if err == nil && time.Since(switchoverTime) > SwitchoverTimeout {
	// 		if currentPrimary != targetInstance {
	// 			status.Phase = v1alpha1.OpsRequestPhaseFailed
	// 			status.Reason = "SwitchoverTimeout"
	// 			status.Message = fmt.Sprintf("Switchover timed out after %v. Current primary: %s, target: %s",
	// 				SwitchoverTimeout, currentPrimary, targetInstance)
	// 			status.Progress = 0

	// 			logger.Info("Switchover timed out, will disable on next reconcile",
	// 				"targetInstance", targetInstance,
	// 				"currentPrimary", currentPrimary)

	// 			return status, false, nil
	// 		}
	// 	}
	// }

	// Check if switchover completed successfully
	if currentPrimary == targetInstance {
		if statusReady && dbStatus.Status == v1alpha1.AppStateReady {
			status.Phase = v1alpha1.OpsRequestPhaseSucceeded
			status.Reason = "SwitchoverCompleted"
			status.Message = fmt.Sprintf("Switchover completed successfully. New primary: %s", currentPrimary)
			status.Progress = 100

			logger.Info("Switchover completed successfully, will disable on next reconcile",
				"targetInstance", targetInstance,
				"currentPrimary", currentPrimary)

			return status, true, nil
		}

		// Primary changed but cluster not fully ready yet
		status.Phase = v1alpha1.OpsRequestPhaseProcessing
		status.Reason = "WaitingForClusterReady"
		status.Message = fmt.Sprintf("Primary switched to %s, waiting for cluster to be ready (status: %s)",
			currentPrimary, dbStatus.Status)
		status.Progress = 90
		return status, false, nil
	}

	// Primary hasn't changed yet
	if statusReady && dbStatus.Status == v1alpha1.AppStateReady {
		// Cluster is ready but primary hasn't changed
		status.Phase = v1alpha1.OpsRequestPhaseProcessing
		status.Reason = "WaitingForPrimaryChange"
		status.Message = fmt.Sprintf("Waiting for primary to switch from %s to %s",
			currentPrimary, targetInstance)
		status.Progress = 70
		return status, false, nil
	}

	// Cluster not ready - switchover in progress
	if dbStatus.Status == v1alpha1.AppStateInit ||
		dbStatus.Status == v1alpha1.AppStatePaused ||
		dbStatus.Status == v1alpha1.AppStateUpgrading {
		status.Phase = v1alpha1.OpsRequestPhaseProcessing
		status.Reason = "SwitchoverInProgress"
		status.Message = fmt.Sprintf("Switchover in progress. Cluster status: %s", dbStatus.Status)
		status.Progress = 50
		return status, false, nil
	}

	// Check for errors
	if dbStatus.Status == v1alpha1.AppStateError {
		status.Phase = v1alpha1.OpsRequestPhaseFailed
		status.Reason = "SwitchoverFailed"
		status.Message = fmt.Sprintf("Switchover failed. Cluster in error state: %s", dbStatus.Message)
		status.Progress = 0

		logger.Info("Switchover failed, will disable on next reconcile",
			"error", dbStatus.Message)

		return status, false, nil
	}

	// Unknown state
	status.Phase = v1alpha1.OpsRequestPhaseProcessing
	status.Reason = "CheckingStatus"
	status.Message = fmt.Sprintf("Checking switchover status. Cluster status: %s", dbStatus.Status)
	status.Progress = 30
	return status, false, nil
}

// getCurrentPrimaryInstance returns the current primary instance name by querying pods
func (o *opsHandler) getCurrentPrimaryInstance(ctx context.Context) (string, error) {
	logger := log.FromContext(ctx)

	// List pods with PGO cluster label and master role
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(o.DB.Namespace),
		client.MatchingLabels{
			PGOClusterLabel: o.DB.Name,
			PGORoleLabel:    RoleMaster,
		},
	}

	if err := o.C.List(ctx, podList, listOpts...); err != nil {
		return "", fmt.Errorf("failed to list pods: %w", err)
	}

	if len(podList.Items) == 0 {
		logger.V(1).Info("No master pod found yet", "cluster", o.DB.Name)
		return "", nil
	}

	if len(podList.Items) > 1 {
		logger.Info("Multiple master pods found, using first one",
			"cluster", o.DB.Name,
			"count", len(podList.Items))
	}

	// Get the instance name from the label
	masterPod := podList.Items[0]
	instanceName := masterPod.Labels[PGOInstanceLabel]

	if instanceName == "" {
		// Fallback to pod name if label not found
		instanceName = masterPod.Name
	}

	logger.V(1).Info("Current primary instance",
		"cluster", o.DB.Name,
		"instance", instanceName,
		"pod", masterPod.Name)

	return instanceName, nil
}

// shouldDisableSwitchover checks if switchover should be disabled
func (o *opsHandler) shouldDisableSwitchover(ctx context.Context) (bool, string) {
	logger := log.FromContext(ctx)

	// Check if switchover is currently enabled
	if o.DB.Spec.Custom == nil {
		return false, ""
	}

	enabled, ok := o.DB.Spec.Custom["patroni.switchover.enabled"]
	if !ok || enabled != "true" {
		return false, ""
	}

	targetInstance, ok := o.DB.Spec.Custom["patroni.switchover.targetInstance"]
	if !ok || targetInstance == "" {
		return false, ""
	}

	// Get current primary
	currentPrimary, err := o.getCurrentPrimaryInstance(ctx)
	if err != nil {
		logger.Error(err, "Failed to get current primary for disable check")
		return false, ""
	}

	// Check if switchover completed (primary matches target)
	dbStatus, statusReady, err := o.Provider.Status(ctx)
	if err == nil && statusReady && dbStatus.Status == v1alpha1.AppStateReady {
		if currentPrimary == targetInstance {
			return true, fmt.Sprintf("Switchover completed: primary is now %s", currentPrimary)
		}
	}

	// Check if switchover timed out
	// lastSwitchover := o.DB.Annotations[LastSwitchoverAnnotation]
	// if lastSwitchover != "" {
	// 	switchoverTime, err := time.Parse(time.RFC3339, lastSwitchover)
	// 	if err == nil && time.Since(switchoverTime) > SwitchoverTimeout {
	// 		if currentPrimary != targetInstance {
	// 			return true, fmt.Sprintf("Switchover timed out: primary still %s, target was %s",
	// 				currentPrimary, targetInstance)
	// 		}
	// 	}
	// }

	// Check if cluster is in error state
	if err == nil && dbStatus.Status == v1alpha1.AppStateError {
		return true, fmt.Sprintf("Cluster in error state: %s", dbStatus.Message)
	}

	return false, ""
}

// handleStartStopStatus checks start/stop operation status
func (o *opsHandler) handleStartStopStatus(
	ctx context.Context,
	dbStatus v1alpha1.DatabaseClusterStatus,
	statusReady bool,
) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	status := v1alpha1.OpsRequestClusterStatus{
		Output: make(map[string]string),
	}

	isPaused := o.DB.Spec.Paused

	if o.opsRequest.Spec.Type == v1alpha1.ActionStop {
		// Expecting cluster to be paused
		if isPaused && dbStatus.Status == v1alpha1.AppStatePaused {
			status.Phase = v1alpha1.OpsRequestPhaseSucceeded
			status.Reason = "ClusterStopped"
			status.Message = "Cluster successfully stopped"
			status.Progress = 100
			return status, true, nil
		}

		status.Phase = v1alpha1.OpsRequestPhaseProcessing
		status.Reason = "StoppingCluster"
		status.Message = "Stopping cluster..."
		status.Progress = 50
		return status, false, nil
	}

	// ActionStart - expecting cluster to be running
	if !isPaused && statusReady && dbStatus.Status == v1alpha1.AppStateReady {
		status.Phase = v1alpha1.OpsRequestPhaseSucceeded
		status.Reason = "ClusterStarted"
		status.Message = "Cluster successfully started"
		status.Progress = 100
		return status, true, nil
	}

	status.Phase = v1alpha1.OpsRequestPhaseProcessing
	status.Reason = "StartingCluster"
	status.Message = fmt.Sprintf("Starting cluster. Status: %s", dbStatus.Status)
	status.Progress = 50
	return status, false, nil
}

// handleRestartStatus checks restart operation status
func (o *opsHandler) handleRestartStatus(
	ctx context.Context,
	dbStatus v1alpha1.DatabaseClusterStatus,
	statusReady bool,
) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	status := v1alpha1.OpsRequestClusterStatus{
		Output: make(map[string]string),
	}

	restartAt := o.DB.Annotations[RestartAnnotation]
	if restartAt != "" {
		status.Output["restartRequestedAt"] = restartAt
	}

	// Check if cluster is ready after restart
	if statusReady && dbStatus.Status == v1alpha1.AppStateReady {
		status.Phase = v1alpha1.OpsRequestPhaseSucceeded
		status.Reason = "RestartCompleted"
		status.Message = "Cluster restart completed successfully"
		status.Progress = 100
		return status, true, nil
	}

	// Cluster restarting
	status.Phase = v1alpha1.OpsRequestPhaseProcessing
	status.Reason = "RestartInProgress"
	status.Message = fmt.Sprintf("Cluster restart in progress. Status: %s", dbStatus.Status)
	status.Progress = 50
	return status, false, nil
}

// handleScalingStatus checks scaling operation status
func (o *opsHandler) handleScalingStatus(
	ctx context.Context,
	dbStatus v1alpha1.DatabaseClusterStatus,
	statusReady bool,
) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	status := v1alpha1.OpsRequestClusterStatus{
		Output: make(map[string]string),
	}

	// Add current replicas info
	status.Output["currentReplicas"] = fmt.Sprintf("%d", dbStatus.Size)
	status.Output["readyReplicas"] = fmt.Sprintf("%d", dbStatus.Ready)

	if statusReady && dbStatus.Status == v1alpha1.AppStateReady {
		// Verify scaling completed
		desiredReplicas := o.DB.Spec.Engine.Replicas

		if dbStatus.Size == desiredReplicas {
			status.Phase = v1alpha1.OpsRequestPhaseSucceeded
			status.Reason = "ScalingCompleted"
			status.Message = fmt.Sprintf("Scaling completed. Replicas: %d/%d",
				dbStatus.Size, desiredReplicas)
			status.Progress = 100
			return status, true, nil
		}
	}

	status.Phase = v1alpha1.OpsRequestPhaseProcessing
	status.Reason = "ScalingInProgress"
	status.Message = "Scaling operation in progress"
	status.Progress = o.calculateProgress(dbStatus, statusReady)
	return status, false, nil
}

// handleUpgradeStatus checks upgrade operation status
func (o *opsHandler) handleUpgradeStatus(
	ctx context.Context,
	dbStatus v1alpha1.DatabaseClusterStatus,
	statusReady bool,
) (v1alpha1.OpsRequestClusterStatus, bool, error) {
	status := v1alpha1.OpsRequestClusterStatus{
		Output: make(map[string]string),
	}

	targetVersion := o.opsRequest.Spec.Params["version"]
	status.Output["targetVersion"] = targetVersion

	if statusReady && dbStatus.Status == v1alpha1.AppStateReady {
		// Check if version matches
		if dbStatus.CRVersion == targetVersion {
			status.Phase = v1alpha1.OpsRequestPhaseSucceeded
			status.Reason = "UpgradeCompleted"
			status.Message = fmt.Sprintf("Upgrade completed to version %s", targetVersion)
			status.Progress = 100
			status.Output["currentVersion"] = dbStatus.CRVersion
			return status, true, nil
		}
	}

	status.Phase = v1alpha1.OpsRequestPhaseProcessing
	status.Reason = "UpgradeInProgress"
	status.Message = fmt.Sprintf("Upgrading to version %s", targetVersion)
	status.Progress = o.calculateProgress(dbStatus, statusReady)
	status.Output["currentVersion"] = dbStatus.CRVersion

	return status, false, nil
}

// calculateProgress calculates operation progress based on cluster status
func (o *opsHandler) calculateProgress(dbStatus v1alpha1.DatabaseClusterStatus, statusReady bool) int32 {
	if statusReady && dbStatus.Status == v1alpha1.AppStateReady {
		return 100
	}

	// Calculate based on ready replicas
	if dbStatus.Size > 0 {
		return int32(float64(dbStatus.Ready) / float64(dbStatus.Size) * 100)
	}

	// Default progress based on status
	switch dbStatus.Status {
	case v1alpha1.AppStateNew:
		return 10
	case v1alpha1.AppStateInit:
		return 30
	case v1alpha1.AppStateUpgrading:
		return 50
	case v1alpha1.AppStatePaused:
		return 100
	case v1alpha1.AppStateError:
		return 0
	default:
		return 25
	}
}

// RunPreReconcileHook implements providers.OperationHandler.
func (o *opsHandler) RunPreReconcileHook(ctx context.Context, opsRequest *v1alpha1.OpsRequest) (providers.HookResult, error) {
	logger := log.FromContext(ctx)

	// Check if switchover is in progress
	if o.isSwitchoverInProgress() {
		logger.Info("Switchover in progress, requeuing",
			"cluster", o.DB.Name,
			"lastSwitchover", o.DB.Annotations[LastSwitchoverAnnotation])

		return providers.HookResult{
			Requeue:      true,
			RequeueAfter: SwitchoverCheckInterval,
			Message:      "Switchover operation in progress, waiting for completion",
		}, nil
	}

	// Check if restart is in progress
	if o.isRestartInProgress() {
		logger.Info("Restart in progress, requeuing",
			"cluster", o.DB.Name,
			"restartAt", o.DB.Annotations[RestartAnnotation])

		return providers.HookResult{
			Requeue:      true,
			RequeueAfter: DefaultCheckInterval,
			Message:      "Restart operation in progress, waiting for completion",
		}, nil
	}

	// No blocking operations in progress
	return providers.HookResult{}, nil
}

// isSwitchoverInProgress checks if a switchover operation is currently in progress
func (o *opsHandler) isSwitchoverInProgress() bool {
	// Check if switchover is enabled in spec
	if o.DB.Spec.Custom == nil {
		return false
	}

	enabled, ok := o.DB.Spec.Custom["patroni.switchover.enabled"]
	if !ok || enabled != "true" {
		return false
	}

	// Check if cluster is in a transitional state
	dbStatus, statusReady, err := o.Provider.Status(o.ctx)
	if err != nil {
		return false
	}

	// If cluster is not ready, switchover might be in progress
	if !statusReady {
		return true
	}

	// If cluster status is updating/initializing, switchover might be in progress
	if dbStatus.Status == v1alpha1.AppStateUpgrading ||
		dbStatus.Status == v1alpha1.AppStateInit {
		return true
	}

	// Check if switchover was requested recently (within timeout)
	// lastSwitchover := o.DB.Annotations[LastSwitchoverAnnotation]
	// if lastSwitchover != "" {
	// 	switchoverTime, err := time.Parse(time.RFC3339, lastSwitchover)
	// 	if err == nil && time.Since(switchoverTime) < SwitchoverTimeout {
	// 		// Recent switchover request and cluster not fully stable
	// 		if !statusReady || dbStatus.Status != v1alpha1.AppStateReady {
	// 			return true
	// 		}
	// 	}
	// }

	return false
}

// isRestartInProgress checks if a restart operation is currently in progress
func (o *opsHandler) isRestartInProgress() bool {
	restartAt := o.DB.Annotations[RestartAnnotation]
	if restartAt == "" {
		return false
	}

	// Check if cluster is ready
	dbStatus, statusReady, err := o.Provider.Status(o.ctx)
	if err != nil {
		return false
	}

	// If cluster not ready after restart annotation, consider it in progress
	if !statusReady {
		return true
	}

	// Check if status indicates restart
	if dbStatus.Status == v1alpha1.AppStateUpgrading ||
		dbStatus.Status == v1alpha1.AppStateInit {
		return true
	}

	return false
}

// HandleBackup implements providers.OperationHandler.
func (o *opsHandler) HandleBackup(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	// TODO: Implement backup handling for PostgreSQL
	return nil, fmt.Errorf("backup operation not yet implemented for PostgreSQL")
}

// HandleCustom implements providers.OperationHandler.
func (o *opsHandler) HandleCustom(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	// TODO: Implement custom operations
	return nil, fmt.Errorf("custom operation not yet implemented for PostgreSQL")
}

// HandleExpose implements providers.OperationHandler.
func (o *opsHandler) HandleExpose(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	// TODO: Implement expose handling
	return nil, fmt.Errorf("expose operation not yet implemented for PostgreSQL")
}

// HandleRestore implements providers.OperationHandler.
func (o *opsHandler) HandleRestore(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	// TODO: Implement restore handling
	return nil, fmt.Errorf("restore operation not yet implemented for PostgreSQL")
}

// HandleSwitchover handles switchover operation
func (o *opsHandler) HandleSwitchover(opsRequest *v1alpha1.OpsRequest) ([]client.Object, error) {
	logger := log.FromContext(o.ctx)

	params := opsRequest.Spec.Params
	targetInstance, ok := params["targetInstance"]
	if !ok || targetInstance == "" {
		return nil, fmt.Errorf("missing param: targetInstance")
	}

	cluster := o.DB.DeepCopy()

	// Ensure annotations map exists
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}

	// Update custom spec to trigger switchover in underlying PG cluster
	if cluster.Spec.Custom == nil {
		cluster.Spec.Custom = make(map[string]string)
	}

	// Check if we should disable switchover
	shouldDisable, reason := o.shouldDisableSwitchover(o.ctx)
	if shouldDisable {
		logger.Info("Disabling switchover",
			"cluster", o.DB.Name,
			"reason", reason)

		// Disable switchover
		cluster.Spec.Custom["patroni.switchover.enabled"] = "false"
		// Keep targetInstance for reference

		return []client.Object{cluster}, nil
	}

	// Enable switchover if not already enabled or target changed
	currentTarget := ""
	if o.DB.Spec.Custom != nil {
		currentTarget = o.DB.Spec.Custom["patroni.switchover.targetInstance"]
	}

	fmt.Println(">>>>>>>>> currentTarget != targetInstance", currentTarget, targetInstance)

	// Only update if target changed or not enabled
	if currentTarget != targetInstance {
		logger.Info("Enabling/updating switchover configuration",
			"cluster", o.DB.Name,
			"targetInstance", targetInstance,
			"previousTarget", currentTarget)

		// Set patroni switchover configuration
		cluster.Spec.Custom["patroni.switchover.enabled"] = "true"
		cluster.Spec.Custom["patroni.switchover.targetInstance"] = targetInstance

		// Set annotations to track and trigger switchover
		now := metav1.Now().UTC().Format(time.RFC3339)
		cluster.Annotations[TriggerSwitchoverAnnotation] = now
		cluster.Annotations[LastSwitchoverAnnotation] = now
	} else {
		logger.V(1).Info("Switchover already configured, no changes needed",
			"cluster", o.DB.Name,
			"targetInstance", targetInstance)
	}

	return []client.Object{cluster}, nil
}
