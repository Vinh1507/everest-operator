// internal/controller/everest/opsrequest_controller.go
package everest

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// OpsRequestReconciler reconciles a OpsRequest object
type OpsRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	opsRequestFinalizer = "ops.everest.percona.com/finalizer"
	defaultTimeout      = 30 * time.Minute
)

// +kubebuilder:rbac:groups=everest.percona.com,resources=opsrequests,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=everest.percona.com,resources=opsrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=everest.percona.com,resources=opsrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups=everest.percona.com,resources=databaseclusters,verbs=get;list;watch;update;patch

// Reconcile is part of the main kubernetes reconciliation loop
func (r *OpsRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch OpsRequest
	opsRequest := &everestv1alpha1.OpsRequest{}
	if err := r.Get(ctx, req.NamespacedName, opsRequest); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion
	if !opsRequest.GetDeletionTimestamp().IsZero() {
		return r.handleDeletion(ctx, opsRequest)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(opsRequest, opsRequestFinalizer) {
		controllerutil.AddFinalizer(opsRequest, opsRequestFinalizer)
		if err := r.Update(ctx, opsRequest); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Skip if already completed
	if opsRequest.IsComplete() {
		logger.Info("OpsRequest already completed", "phase", opsRequest.Status.Phase)
		return ctrl.Result{}, nil
	}

	// Initialize status if needed
	if opsRequest.Status.Phase == "" {
		opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhasePending
		opsRequest.Status.ObservedGeneration = opsRequest.Generation
		opsRequest.Status.StartTime = &metav1.Time{Time: time.Now()}
		if err := r.updateStatus(ctx, opsRequest); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check timeout
	if r.isTimedOut(opsRequest) {
		logger.Info("OpsRequest timed out")
		return r.markAsFailed(ctx, opsRequest, "Timeout", "Operation timed out")
	}

	return r.processOperation(ctx, opsRequest)
}

// processOperation processes the OpsRequest by iterating through all clusters
func (r *OpsRequestReconciler) processOperation(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
) (result ctrl.Result, err error) {
	logger := log.FromContext(ctx)
	defer func() {
		syncResult, syncErr := r.syncOperationStatus(ctx, opsRequest)
		if syncErr != nil {
			result = ctrl.Result{}
			err = syncErr
			return
		}
		result = syncResult
		if err != nil && !errors.IsNotFound(err) {
			return
		}
		err = nil
	}()

	// Update phase to Processing
	opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseProcessing
	opsRequest.Status.Message = "Processing operation"
	if err := r.updateStatus(ctx, opsRequest); err != nil {
		return ctrl.Result{}, err
	}

	if len(opsRequest.Spec.Clusters) == 0 {
		logger.Info("No clusters specified in OpsRequest")
		return r.markAsFailed(ctx, opsRequest, "NoClusters", "No clusters specified in the request")
	}

	if opsRequest.Status.ClusterStatus == nil {
		opsRequest.Status.ClusterStatus = make([]everestv1alpha1.OpsRequestClusterStatus, 0, len(opsRequest.Spec.Clusters))
	}

	for _, clusterRef := range opsRequest.Spec.Clusters {
		targetNamespace := clusterRef.Namespace
		if targetNamespace == "" {
			targetNamespace = opsRequest.GetTargetNamespace()
		}

		logger.Info("Processing cluster",
			"name", clusterRef.Name,
			"namespace", targetNamespace,
			"type", opsRequest.Spec.Type)

		dbCluster := &everestv1alpha1.DatabaseCluster{}
		clusterKey := types.NamespacedName{
			Name:      clusterRef.Name,
			Namespace: targetNamespace,
		}

		if err := r.Get(ctx, clusterKey, dbCluster); err != nil {
			if errors.IsNotFound(err) {
				logger.Error(err, "DatabaseCluster not found",
					"name", clusterRef.Name,
					"namespace", targetNamespace)
				// Mark cluster status as failed immediately
				r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
					everestv1alpha1.OpsRequestPhaseFailed,
					"ClusterNotFound",
					fmt.Sprintf("DatabaseCluster not found: %v", err),
					0, nil)
				continue
			}
			logger.Error(err, "Failed to get DatabaseCluster")
			return ctrl.Result{}, err
		}

		provider, err := NewDBProvider(ctx, r.Client, dbCluster)
		if err != nil {
			logger.Error(err, "Failed to create provider")
			r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
				everestv1alpha1.OpsRequestPhaseFailed,
				"ProviderError",
				fmt.Sprintf("Failed to create provider: %v", err),
				0, nil)
			continue
		}

		opsHandler := provider.GetOpsHandler(ctx, *opsRequest)

		hookResult, err := opsHandler.RunPreReconcileHook(ctx, opsRequest)
		if err != nil {
			logger.Error(err, "Pre-reconcile hook failed")
			r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
				everestv1alpha1.OpsRequestPhaseFailed,
				"PreHookFailed",
				fmt.Sprintf("Pre-reconcile hook failed: %v", err),
				0, nil)
			continue
		}

		if hookResult.Requeue {
			logger.Info("Requeue requested by pre-reconcile hook",
				"cluster", clusterRef.Name,
				"message", hookResult.Message,
				"after", hookResult.RequeueAfter)

			opsRequest.Status.Message = hookResult.Message
			if err := r.updateStatus(ctx, opsRequest); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{
				Requeue:      true,
				RequeueAfter: hookResult.RequeueAfter,
			}, nil
		}

		// Apply the operation to the cluster
		err = r.applyOperationToCluster(ctx, opsRequest, dbCluster, provider)
		if err != nil {
			logger.Error(err, "Failed to apply operation to cluster",
				"cluster", clusterRef.Name,
				"operation", opsRequest.Spec.Type)
			r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
				everestv1alpha1.OpsRequestPhaseFailed,
				"ApplyOperationFailed",
				fmt.Sprintf("Failed to apply operation: %v", err),
				0, nil)
			continue
		}

		// Mark cluster as processing (actual status will be updated in defer)
		r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
			everestv1alpha1.OpsRequestPhaseProcessing,
			"OperationApplied",
			"Operation successfully applied, waiting for completion",
			0, nil)

		logger.Info("Successfully applied operation to cluster",
			"name", clusterRef.Name,
			"operation", opsRequest.Spec.Type)
	}
	return ctrl.Result{}, nil
}

// syncOperationStatus synchronizes the operation status by checking DatabaseCluster status
func (r *OpsRequestReconciler) syncOperationStatus(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if opsRequest.IsComplete() {
		logger.Info("OpsRequest already completed, skipping sync")
		return ctrl.Result{}, nil
	}

	if len(opsRequest.Spec.Clusters) == 0 {
		logger.Info("No clusters to sync")
		return ctrl.Result{}, nil
	}

	if opsRequest.Status.ClusterStatus == nil {
		opsRequest.Status.ClusterStatus = make([]everestv1alpha1.OpsRequestClusterStatus, 0, len(opsRequest.Spec.Clusters))
	}

	allSucceeded := true
	anyFailed := false
	anyProcessing := false
	totalProgress := int32(0)
	clustersProcessed := 0

	for _, clusterRef := range opsRequest.Spec.Clusters {
		targetNamespace := clusterRef.Namespace
		if targetNamespace == "" {
			targetNamespace = opsRequest.GetTargetNamespace()
		}

		existingStatus := r.findClusterStatus(opsRequest, clusterRef.Name, targetNamespace)

		if existingStatus != nil && existingStatus.Phase == everestv1alpha1.OpsRequestPhaseFailed {
			logger.Info("Cluster already marked as failed, skipping sync",
				"cluster", clusterRef.Name)
			anyFailed = true
			allSucceeded = false
			totalProgress += existingStatus.Progress
			clustersProcessed++
			continue
		}

		// Skip clusters that already succeeded
		if existingStatus != nil && existingStatus.Phase == everestv1alpha1.OpsRequestPhaseSucceeded {
			logger.Info("Cluster already succeeded, skipping sync",
				"cluster", clusterRef.Name)
			totalProgress += 100
			clustersProcessed++
			continue
		}

		dbCluster := &everestv1alpha1.DatabaseCluster{}
		clusterKey := types.NamespacedName{
			Name:      clusterRef.Name,
			Namespace: targetNamespace,
		}

		if err := r.Get(ctx, clusterKey, dbCluster); err != nil {
			logger.Error(err, "Failed to get DatabaseCluster for status sync",
				"cluster", clusterRef.Name)

			r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
				everestv1alpha1.OpsRequestPhaseFailed,
				"ClusterNotFound",
				fmt.Sprintf("Failed to get cluster: %v", err),
				0, nil)
			anyFailed = true
			allSucceeded = false
			clustersProcessed++
			continue
		}

		provider, err := NewDBProvider(ctx, r.Client, dbCluster)
		if err != nil {
			logger.Error(err, "Failed to create provider for status sync",
				"cluster", clusterRef.Name)

			r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
				everestv1alpha1.OpsRequestPhaseFailed,
				"ProviderError",
				fmt.Sprintf("Failed to create provider: %v", err),
				0, nil)
			anyFailed = true
			allSucceeded = false
			clustersProcessed++
			continue
		}

		opsHandler := provider.GetOpsHandler(ctx, *opsRequest)
		clusterStatus, statusDone, err := opsHandler.HandleOpsRequestStatus(ctx)
		if err != nil {
			logger.Error(err, "Failed to get operation status",
				"cluster", clusterRef.Name)

			r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
				everestv1alpha1.OpsRequestPhaseFailed,
				"StatusError",
				fmt.Sprintf("Failed to get status: %v", err),
				0, nil)
			anyFailed = true
			allSucceeded = false
			clustersProcessed++
			continue
		}

		clusterPhase := clusterStatus.Phase
		if statusDone {
			clusterPhase = everestv1alpha1.OpsRequestPhaseSucceeded
			logger.Info("Cluster operation completed successfully",
				"cluster", clusterRef.Name)
		} else {
			if clusterPhase == "" || clusterPhase == everestv1alpha1.OpsRequestPhasePending {
				clusterPhase = everestv1alpha1.OpsRequestPhaseProcessing
			}
			logger.Info("Cluster operation still in progress",
				"cluster", clusterRef.Name,
				"phase", clusterPhase,
				"progress", clusterStatus.Progress)
		}

		// Update cluster status
		r.updateOpsRequestClusterStatus(opsRequest, clusterRef.Name, targetNamespace,
			clusterPhase,
			clusterStatus.Reason,
			clusterStatus.Message,
			clusterStatus.Progress,
			clusterStatus.Output)

		// Track overall status
		totalProgress += clusterStatus.Progress
		clustersProcessed++

		switch clusterPhase {
		case everestv1alpha1.OpsRequestPhaseSucceeded:
			// Cluster succeeded
		case everestv1alpha1.OpsRequestPhaseFailed:
			anyFailed = true
			allSucceeded = false
		case everestv1alpha1.OpsRequestPhaseProcessing, everestv1alpha1.OpsRequestPhasePending:
			anyProcessing = true
			allSucceeded = false
		default:
			allSucceeded = false
		}
	}

	// Calculate overall progress
	if clustersProcessed > 0 {
		opsRequest.Status.Progress = totalProgress / int32(clustersProcessed)
	}

	// Determine overall phase and return value
	var result ctrl.Result

	if allSucceeded {
		// All clusters succeeded
		opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseSucceeded
		opsRequest.Status.Message = fmt.Sprintf("Successfully completed operation on %d clusters",
			len(opsRequest.Spec.Clusters))
		opsRequest.Status.Progress = 100

		if opsRequest.Status.CompletionTime == nil {
			now := metav1.Now()
			opsRequest.Status.CompletionTime = &now
		}

		meta.SetStatusCondition(&opsRequest.Status.Conditions, metav1.Condition{
			Type:               "Succeeded",
			Status:             metav1.ConditionTrue,
			Reason:             "OperationCompleted",
			Message:            opsRequest.Status.Message,
			ObservedGeneration: opsRequest.Generation,
		})

		logger.Info("All operations completed successfully",
			"totalClusters", len(opsRequest.Spec.Clusters))
		result = ctrl.Result{} // No requeue needed

	} else if anyFailed {
		succeededCount := 0
		failedCount := 0
		processingCount := 0

		for _, cs := range opsRequest.Status.ClusterStatus {
			switch cs.Phase {
			case everestv1alpha1.OpsRequestPhaseSucceeded:
				succeededCount++
			case everestv1alpha1.OpsRequestPhaseFailed:
				failedCount++
			case everestv1alpha1.OpsRequestPhaseProcessing, everestv1alpha1.OpsRequestPhasePending:
				processingCount++
			}
		}

		if processingCount > 0 {
			opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseProcessing
			opsRequest.Status.Message = fmt.Sprintf("Processing: %d succeeded, %d processing, %d failed",
				succeededCount, processingCount, failedCount)

			logger.Info("Some operations still processing with failures",
				"succeeded", succeededCount,
				"processing", processingCount,
				"failed", failedCount)

			result = ctrl.Result{
				Requeue:      true,
				RequeueAfter: 30 * time.Second,
			}
		} else {
			opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseFailed
			opsRequest.Status.Reason = "ClusterOperationFailed"
			opsRequest.Status.Message = fmt.Sprintf("Operation failed: %d succeeded, %d failed out of %d clusters",
				succeededCount, failedCount, len(opsRequest.Spec.Clusters))

			if opsRequest.Status.CompletionTime == nil {
				now := metav1.Now()
				opsRequest.Status.CompletionTime = &now
			}

			meta.SetStatusCondition(&opsRequest.Status.Conditions, metav1.Condition{
				Type:               "Failed",
				Status:             metav1.ConditionTrue,
				Reason:             "ClusterOperationFailed",
				Message:            opsRequest.Status.Message,
				ObservedGeneration: opsRequest.Generation,
			})

			logger.Info("Operations completed with failures",
				"succeeded", succeededCount,
				"failed", failedCount)
			result = ctrl.Result{} // No requeue
		}

	} else if anyProcessing {
		processingCount := 0
		succeededCount := 0
		for _, cs := range opsRequest.Status.ClusterStatus {
			if cs.Phase == everestv1alpha1.OpsRequestPhaseSucceeded {
				succeededCount++
			} else if cs.Phase == everestv1alpha1.OpsRequestPhaseProcessing ||
				cs.Phase == everestv1alpha1.OpsRequestPhasePending {
				processingCount++
			}
		}

		opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseProcessing
		opsRequest.Status.Message = fmt.Sprintf("Processing operation: %d/%d completed (progress: %d%%)",
			succeededCount, len(opsRequest.Spec.Clusters), opsRequest.Status.Progress)

		logger.Info("Operations still processing",
			"succeeded", succeededCount,
			"processing", processingCount,
			"progress", opsRequest.Status.Progress)

		result = ctrl.Result{
			Requeue:      true,
			RequeueAfter: 30 * time.Second,
		}
	} else {
		logger.Info("No clusters in recognizable state, marking as failed")
		opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseFailed
		opsRequest.Status.Reason = "UnknownState"
		opsRequest.Status.Message = "All clusters in unknown state"

		if opsRequest.Status.CompletionTime == nil {
			now := metav1.Now()
			opsRequest.Status.CompletionTime = &now
		}

		result = ctrl.Result{} // No requeue
	}

	if err := r.updateStatus(ctx, opsRequest); err != nil {
		logger.Error(err, "Failed to update OpsRequest status")
		return ctrl.Result{}, err
	}

	return result, nil
}

func (r *OpsRequestReconciler) applyOperationToCluster(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
	dbCluster *everestv1alpha1.DatabaseCluster,
	provider dbProvider,
) error {
	logger := log.FromContext(ctx)

	var (
		additionalObjs []client.Object
		err            error
	)

	switch opsRequest.Spec.Type {
	case everestv1alpha1.ActionStart:
		additionalObjs, err = r.handleStartAction(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionStop:
		additionalObjs, err = r.handleStopAction(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionRestart:
		additionalObjs, err = r.handleRestartAction(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionSwitchover:
		additionalObjs, err = r.handleSwitchover(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionVerticalScaling:
		additionalObjs, err = r.handleVerticalScaling(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionHorizontalScaling:
		additionalObjs, err = r.handleHorizontalScaling(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionVolumeExpansion:
		additionalObjs, err = r.handleVolumeExpansion(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionReconfiguring:
		additionalObjs, err = r.handleReconfiguring(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionUpgrade:
		additionalObjs, err = r.handleUpgrade(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionBackup:
		additionalObjs, err = r.handleBackup(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionRestore:
		additionalObjs, err = r.handleRestore(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionExpose:
		additionalObjs, err = r.handleExpose(ctx, dbCluster, opsRequest, provider)

	case everestv1alpha1.ActionCustom:
		additionalObjs, err = r.handleCustom(ctx, dbCluster, opsRequest, provider)

	default:
		return fmt.Errorf("unsupported operation type: %s", opsRequest.Spec.Type)
	}

	if err != nil {
		return err
	}

	for i := range additionalObjs {
		obj := additionalObjs[i]

		key := client.ObjectKeyFromObject(obj)
		objGVK := obj.GetObjectKind().GroupVersionKind()

		logger.Info("Applying object",
			"kind", objGVK.Kind,
			"name", key.Name,
			"namespace", key.Namespace)

		desired := obj.DeepCopyObject().(client.Object)

		_, err := controllerutil.CreateOrPatch(ctx, r.Client, obj, func() error {
			obj.SetLabels(desired.GetLabels())
			obj.SetAnnotations(desired.GetAnnotations())

			if db, ok := obj.(*everestv1alpha1.DatabaseCluster); ok {
				desiredDB := desired.(*everestv1alpha1.DatabaseCluster)
				db.Spec = desiredDB.Spec
			}

			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to apply %s/%s: %w", objGVK.Kind, key.Name, err)
		}

		logger.Info("Successfully applied object",
			"kind", objGVK.Kind,
			"name", key.Name)
	}

	return nil
}

// findClusterStatus finds existing cluster status in the OpsRequest
func (r *OpsRequestReconciler) findClusterStatus(
	opsRequest *everestv1alpha1.OpsRequest,
	clusterName, clusterNamespace string,
) *everestv1alpha1.OpsRequestClusterStatus {
	for i := range opsRequest.Status.ClusterStatus {
		if opsRequest.Status.ClusterStatus[i].ClusterName == clusterName &&
			opsRequest.Status.ClusterStatus[i].ClusterNamespace == clusterNamespace {
			return &opsRequest.Status.ClusterStatus[i]
		}
	}
	return nil
}

// updateOpsRequestClusterStatus updates or adds cluster status in the OpsRequest
func (r *OpsRequestReconciler) updateOpsRequestClusterStatus(
	opsRequest *everestv1alpha1.OpsRequest,
	clusterName, clusterNamespace string,
	phase everestv1alpha1.OpsRequestPhase,
	reason, message string,
	progress int32,
	output map[string]string,
) {
	// Find existing cluster status
	found := false
	for i := range opsRequest.Status.ClusterStatus {
		if opsRequest.Status.ClusterStatus[i].ClusterName == clusterName &&
			opsRequest.Status.ClusterStatus[i].ClusterNamespace == clusterNamespace {
			// Update existing
			opsRequest.Status.ClusterStatus[i].Phase = phase
			opsRequest.Status.ClusterStatus[i].Reason = reason
			opsRequest.Status.ClusterStatus[i].Message = message
			opsRequest.Status.ClusterStatus[i].Progress = progress
			opsRequest.Status.ClusterStatus[i].Output = output
			found = true
			break
		}
	}

	// Add new cluster status if not found
	if !found {
		opsRequest.Status.ClusterStatus = append(opsRequest.Status.ClusterStatus,
			everestv1alpha1.OpsRequestClusterStatus{
				ClusterName:      clusterName,
				ClusterNamespace: clusterNamespace,
				Phase:            phase,
				Reason:           reason,
				Message:          message,
				Progress:         progress,
				Output:           output,
			})
	}
}

func (r *OpsRequestReconciler) createOrPatchObject(
	ctx context.Context,
	obj client.Object,
	mutateFn controllerutil.MutateFn,
) error {
	_, err := controllerutil.CreateOrPatch(ctx, r.Client, obj, mutateFn)
	return err
}

func (r *OpsRequestReconciler) handleStartAction(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	updatedCluster := cluster.DeepCopy()
	updatedCluster.Spec.Paused = false
	return []client.Object{updatedCluster}, nil
}

func (r *OpsRequestReconciler) handleStopAction(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	updatedCluster := cluster.DeepCopy()
	updatedCluster.Spec.Paused = true
	return []client.Object{updatedCluster}, nil
}

func (r *OpsRequestReconciler) handleRestartAction(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	updatedCluster := cluster.DeepCopy()
	if updatedCluster.Annotations == nil {
		updatedCluster.Annotations = make(map[string]string)
	}
	updatedCluster.Annotations["ops.everest.percona.com/restart"] = time.Now().Format(time.RFC3339)
	return []client.Object{updatedCluster}, nil
}

func (r *OpsRequestReconciler) handleSwitchover(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	opsHandler := provider.GetOpsHandler(ctx, *opsRequest)
	return opsHandler.HandleSwitchover(opsRequest)
}

func (r *OpsRequestReconciler) handleVerticalScaling(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	params := opsRequest.Spec.Params
	if params == nil {
		return nil, fmt.Errorf("missing params")
	}

	cpuStr, hasCPU := params["cpu"]
	memStr, hasMem := params["memory"]

	if (!hasCPU || cpuStr == "") && (!hasMem || memStr == "") {
		return nil, fmt.Errorf("missing param: cpu and/or memory")
	}

	updatedCluster := cluster.DeepCopy()

	// Update CPU if provided
	if hasCPU && cpuStr != "" {
		cpuQty, err := resource.ParseQuantity(cpuStr)
		if err != nil {
			return nil, fmt.Errorf("invalid cpu=%q: %w", cpuStr, err)
		}
		updatedCluster.Spec.Engine.Resources.CPU = cpuQty
	}

	// Update Memory if provided
	if hasMem && memStr != "" {
		memQty, err := resource.ParseQuantity(memStr)
		if err != nil {
			return nil, fmt.Errorf("invalid memory=%q: %w", memStr, err)
		}
		updatedCluster.Spec.Engine.Resources.Memory = memQty
	}

	return []client.Object{updatedCluster}, nil
}

func (r *OpsRequestReconciler) handleHorizontalScaling(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	replicasStr, ok := opsRequest.Spec.Params["replicas"]
	if !ok {
		return nil, fmt.Errorf("missing param: replicas")
	}

	replicasInt, err := strconv.Atoi(replicasStr)
	if err != nil {
		return nil, fmt.Errorf("invalid replicas=%q: %w", replicasStr, err)
	}
	if replicasInt < 0 {
		return nil, fmt.Errorf("replicas must be >= 0, got %d", replicasInt)
	}

	updatedCluster := cluster.DeepCopy()
	updatedCluster.Spec.Engine.Replicas = int32(replicasInt)
	return []client.Object{updatedCluster}, nil
}

func (r *OpsRequestReconciler) handleVolumeExpansion(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	// Default implementation
	sizeStr, ok := opsRequest.Spec.Params["size"]
	if !ok || sizeStr == "" {
		return nil, fmt.Errorf("missing param: size")
	}

	newQty, err := resource.ParseQuantity(sizeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid size=%q: %w", sizeStr, err)
	}

	oldQty := cluster.Spec.Engine.Storage.Size

	// Do not allow shrinking
	if newQty.Cmp(oldQty) < 0 {
		return nil, fmt.Errorf(
			"volume shrink is not allowed: requested=%s current=%s",
			newQty.String(), oldQty.String(),
		)
	}

	// If equal => no-op
	if newQty.Cmp(oldQty) == 0 {
		return nil, nil
	}

	updatedCluster := cluster.DeepCopy()
	updatedCluster.Spec.Engine.Storage.Size = newQty
	return []client.Object{updatedCluster}, nil
}

func (r *OpsRequestReconciler) handleReconfiguring(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	// Default: not implemented
	return nil, fmt.Errorf("reconfiguring is not implemented for this engine")
}

func (r *OpsRequestReconciler) handleUpgrade(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	version, ok := opsRequest.Spec.Params["version"]
	if !ok || version == "" {
		return nil, fmt.Errorf("missing param: version")
	}

	updatedCluster := cluster.DeepCopy()
	updatedCluster.Spec.Engine.Version = version
	return []client.Object{updatedCluster}, nil
}

func (r *OpsRequestReconciler) handleBackup(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	opsHandler := provider.GetOpsHandler(ctx, *opsRequest)
	return opsHandler.HandleBackup(opsRequest)
}

func (r *OpsRequestReconciler) handleRestore(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	opsHandler := provider.GetOpsHandler(ctx, *opsRequest)
	return opsHandler.HandleRestore(opsRequest)
}

func (r *OpsRequestReconciler) handleExpose(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	opsHandler := provider.GetOpsHandler(ctx, *opsRequest)
	return opsHandler.HandleExpose(opsRequest)
}

func (r *OpsRequestReconciler) handleCustom(
	ctx context.Context,
	cluster *everestv1alpha1.DatabaseCluster,
	opsRequest *everestv1alpha1.OpsRequest,
	provider dbProvider,
) ([]client.Object, error) {
	opsHandler := provider.GetOpsHandler(ctx, *opsRequest)
	return opsHandler.HandleCustom(opsRequest)
}

// applyObject applies a Kubernetes object (create or update)
func (r *OpsRequestReconciler) applyObject(ctx context.Context, obj client.Object) error {
	// Try to get the object
	key := client.ObjectKeyFromObject(obj)
	existing := obj.DeepCopyObject().(client.Object)

	err := r.Get(ctx, key, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			// Create if not found
			return r.Create(ctx, obj)
		}
		return err
	}

	// Update if exists
	obj.SetResourceVersion(existing.GetResourceVersion())
	return r.Update(ctx, obj)
}

// Status update methods
func (r *OpsRequestReconciler) markAsSucceeded(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
	message string,
) (ctrl.Result, error) {
	opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseSucceeded
	opsRequest.Status.Message = message
	opsRequest.Status.Progress = 100
	opsRequest.Status.CompletionTime = &metav1.Time{Time: time.Now()}

	meta.SetStatusCondition(&opsRequest.Status.Conditions, metav1.Condition{
		Type:               "Succeeded",
		Status:             metav1.ConditionTrue,
		Reason:             "OperationCompleted",
		Message:            message,
		ObservedGeneration: opsRequest.Generation,
	})

	if err := r.updateStatus(ctx, opsRequest); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *OpsRequestReconciler) markAsPartiallySucceeded(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
	message string,
) (ctrl.Result, error) {
	opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseFailed
	opsRequest.Status.Reason = "PartialFailure"
	opsRequest.Status.Message = message
	opsRequest.Status.CompletionTime = &metav1.Time{Time: time.Now()}

	meta.SetStatusCondition(&opsRequest.Status.Conditions, metav1.Condition{
		Type:               "PartiallySucceeded",
		Status:             metav1.ConditionTrue,
		Reason:             "PartialFailure",
		Message:            message,
		ObservedGeneration: opsRequest.Generation,
	})

	if err := r.updateStatus(ctx, opsRequest); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *OpsRequestReconciler) markAsFailed(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
	reason, message string,
) (ctrl.Result, error) {
	opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseFailed
	opsRequest.Status.Reason = reason
	opsRequest.Status.Message = message
	opsRequest.Status.CompletionTime = &metav1.Time{Time: time.Now()}

	meta.SetStatusCondition(&opsRequest.Status.Conditions, metav1.Condition{
		Type:               "Failed",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: opsRequest.Generation,
	})

	if err := r.updateStatus(ctx, opsRequest); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *OpsRequestReconciler) updateStatus(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &everestv1alpha1.OpsRequest{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(opsRequest), latest); err != nil {
			return err
		}

		latest.Status = opsRequest.Status
		return r.Status().Update(ctx, latest)
	})
}

func (r *OpsRequestReconciler) handleDeletion(
	ctx context.Context,
	opsRequest *everestv1alpha1.OpsRequest,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(opsRequest, opsRequestFinalizer) {
		return ctrl.Result{}, nil
	}

	// Cancel operation if still processing
	if opsRequest.IsProcessing() {
		opsRequest.Status.Phase = everestv1alpha1.OpsRequestPhaseCanceled
		opsRequest.Status.Message = "Operation canceled by deletion"
		opsRequest.Status.CompletionTime = &metav1.Time{Time: time.Now()}

		if err := r.updateStatus(ctx, opsRequest); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(opsRequest, opsRequestFinalizer)
	if err := r.Update(ctx, opsRequest); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *OpsRequestReconciler) isTimedOut(opsRequest *everestv1alpha1.OpsRequest) bool {
	if opsRequest.Status.StartTime == nil {
		return false
	}

	timeout := defaultTimeout
	if opsRequest.Spec.Timeout != "" {
		if d, err := time.ParseDuration(opsRequest.Spec.Timeout); err == nil {
			timeout = d
		}
	}

	return time.Since(opsRequest.Status.StartTime.Time) > timeout
}

// SetupWithManager sets up the controller with the Manager.
func (r *OpsRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&everestv1alpha1.OpsRequest{}).
		WithEventFilter(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
		)).
		Named("opsrequest").
		Complete(r)
}
