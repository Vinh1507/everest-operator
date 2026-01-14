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

package everest

import (
	"context"
	"fmt"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	everestv1alpha1 "github.com/percona/everest-operator/api/everest/v1alpha1"
)

// ClusterGroupReconciler reconciles a ClusterGroup object
type ClusterGroupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=everest.percona.com,resources=clustergroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=everest.percona.com,resources=clustergroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=everest.percona.com,resources=clustergroups/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ClusterGroup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *ClusterGroupReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {

	// log := logf.FromContext(ctx)
	fmt.Println(">>>>> GROUP RECONCILE")

	var group everestv1alpha1.ClusterGroup
	if err := r.Get(ctx, req.NamespacedName, &group); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Nếu pause thì bỏ qua
	if group.Spec.Paused {
		return ctrl.Result{}, nil
	}

	// 1. List các cluster con theo label + namespace
	var clusterList everestv1alpha1.DatabaseClusterList // ⚠️ đổi sang type cluster thật của bạn
	if err := r.List(
		ctx,
		&clusterList,
		client.InNamespace(group.Namespace),
		client.MatchingLabels{
			"everest.com/group-name": group.Name,
		},
	); err != nil {
		return ctrl.Result{}, err
	}

	// Map spec clusters để check name nhanh
	specMap := make(map[string]everestv1alpha1.ClusterReference)
	for _, c := range group.Spec.Clusters {
		specMap[c.Name] = c
	}

	var (
		total    = len(group.Spec.Clusters)
		ready    = 0
		statuses []everestv1alpha1.ClusterStatus
	)

	// 2. Đồng bộ status từng cluster con
	for _, c := range clusterList.Items {

		spec, ok := specMap[c.Name]
		if !ok {
			continue
		}

		cs := everestv1alpha1.ClusterStatus{
			Name:               c.Name,
			Type:               spec.Type,
			Generation:         c.Generation,
			ObservedGeneration: c.Status.ObservedGeneration,
			// Phase:              c.Status.Phase,
			Ready: c.Status.Ready > 0,
		}

		if cs.Ready {
			ready++
		}

		statuses = append(statuses, cs)
	}

	// 3. Xác định phase của group
	phase := "Pending"
	switch {
	case ready == 0:
		phase = "Pending"
	case ready < total:
		phase = "PartiallyReady"
	case ready == total && total > 0:
		phase = "Ready"
	}

	// 4. Chỉ update status khi CÓ THAY ĐỔI
	changed :=
		group.Status.ReadyClusters != ready ||
			group.Status.TotalClusters != total ||
			group.Status.Phase != phase ||
			!reflect.DeepEqual(group.Status.Clusters, statuses) ||
			group.Status.ObservedGeneration != group.Generation

	if !changed {
		return ctrl.Result{}, nil
	}

	now := metav1.Now()

	group.Status.TotalClusters = total
	group.Status.ReadyClusters = ready
	group.Status.Phase = phase
	group.Status.Clusters = statuses
	group.Status.ObservedGeneration = group.Generation
	group.Status.LastReconcileTime = &now

	if err := r.Status().Update(ctx, &group); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("ClusterGroup").
		For(&everestv1alpha1.ClusterGroup{}).
		Watches(
			&everestv1alpha1.DatabaseCluster{},
			handler.EnqueueRequestsFromMapFunc(
				func(ctx context.Context, obj client.Object) []reconcile.Request {
					cluster, ok := obj.(*everestv1alpha1.DatabaseCluster)
					if !ok {
						return nil
					}

					if cluster.Spec.GroupName == "" {
						return nil
					}

					return []reconcile.Request{{
						NamespacedName: types.NamespacedName{
							Name:      cluster.Spec.GroupName,
							Namespace: cluster.Namespace,
						},
					}}
				},
			),
		).
		Complete(r)
}
