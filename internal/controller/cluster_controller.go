/*
Copyright 2026 The OtterScale Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"cmp"
	"context"
	"errors"
	"slices"

	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	"github.com/otterscale/fleet-operator/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterReconciler reconciles a Cluster object.
// It ensures that the Talos secrets bundle and talosconfig client configuration
// are created and kept in sync, and tracks the overall cluster readiness state
// based on Machine conditions.
//
// The controller is intentionally kept thin: it orchestrates the reconciliation flow,
// while the actual resource synchronization logic resides in internal/cluster/.
type ClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Version  string
	Recorder events.EventRecorder
}

// RBAC permissions required by the controller:
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=machines,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is the main loop for the Cluster controller.
// Flow: Fetch -> Reconcile Secrets -> Reconcile Talosconfig -> Update Status.
//
// Deletion is handled by Kubernetes garbage collection: Secret resources
// are created with OwnerReferences pointing to the Cluster, so they are
// automatically cascade-deleted when the Cluster is removed.
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName(req.Name)
	ctx = log.IntoContext(ctx, logger)

	var cl fleetv1alpha1.Cluster
	if err := r.Get(ctx, req.NamespacedName, &cl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err := r.reconcileResources(ctx, &cl); err != nil {
		return r.handleReconcileError(ctx, &cl, err)
	}

	if err := r.updateStatus(ctx, &cl); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileResources orchestrates the domain-level resource sync.
func (r *ClusterReconciler) reconcileResources(ctx context.Context, cl *fleetv1alpha1.Cluster) error {
	bundle, err := cluster.ReconcileSecrets(ctx, r.Client, r.Scheme, cl, r.Version)
	if err != nil {
		return err
	}

	endpoints, err := r.collectControlPlaneEndpoints(ctx, cl)
	if err != nil {
		return err
	}

	return cluster.ReconcileTalosconfig(ctx, r.Client, r.Scheme, cl, bundle, endpoints, r.Version)
}

// collectControlPlaneEndpoints returns IP addresses from all control plane
// Machines that have addresses available, used to populate the talosconfig.
func (r *ClusterReconciler) collectControlPlaneEndpoints(ctx context.Context, cl *fleetv1alpha1.Cluster) ([]string, error) {
	var machines fleetv1alpha1.MachineList
	if err := r.List(ctx, &machines); err != nil {
		return nil, err
	}

	var endpoints []string
	for _, m := range machines.Items {
		if m.Spec.ClusterRef != cl.Name {
			continue
		}
		if m.Spec.Role != fleetv1alpha1.MachineRoleControlPlane {
			continue
		}
		for _, addr := range m.Status.Addresses {
			endpoints = append(endpoints, addr.Address)
		}
	}

	slices.Sort(endpoints)
	return endpoints, nil
}

// handleReconcileError categorizes errors and updates status accordingly.
func (r *ClusterReconciler) handleReconcileError(ctx context.Context, cl *fleetv1alpha1.Cluster, err error) (ctrl.Result, error) {
	var ice *cluster.InvalidConfigError
	if errors.As(err, &ice) {
		r.setReadyConditionFalse(ctx, cl, "InvalidConfig", err.Error())
		r.Recorder.Eventf(cl, nil, corev1.EventTypeWarning, "InvalidConfig", "Reconcile", err.Error())
		return ctrl.Result{}, nil
	}

	r.setReadyConditionFalse(ctx, cl, "ReconcileError", err.Error())
	r.Recorder.Eventf(cl, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", err.Error())
	return ctrl.Result{}, err
}

// setReadyConditionFalse patches the Ready condition to False.
func (r *ClusterReconciler) setReadyConditionFalse(ctx context.Context, cl *fleetv1alpha1.Cluster, reason, message string) {
	logger := log.FromContext(ctx)
	patch := client.MergeFrom(cl.DeepCopy())
	meta.SetStatusCondition(&cl.Status.Conditions, metav1.Condition{
		Type:               cluster.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cl.Generation,
	})
	cl.Status.ObservedGeneration = cl.Generation

	if err := r.Status().Patch(ctx, cl, patch); err != nil {
		logger.Error(err, "Failed to patch Ready=False status condition", "reason", reason)
	}
}

// updateStatus calculates the observed status from Machines and patches the Cluster.
func (r *ClusterReconciler) updateStatus(ctx context.Context, cl *fleetv1alpha1.Cluster) error {
	newStatus := cl.Status.DeepCopy()
	newStatus.ObservedGeneration = cl.Generation

	newStatus.SecretsRef = &fleetv1alpha1.ResourceReference{
		Name:      cl.Name + cluster.SecretsSuffix,
		Namespace: cluster.SecretsNamespace,
	}
	newStatus.TalosconfigRef = &fleetv1alpha1.ResourceReference{
		Name:      cl.Name + cluster.TalosconfigSuffix,
		Namespace: cluster.SecretsNamespace,
	}

	var machines fleetv1alpha1.MachineList
	if err := r.List(ctx, &machines); err != nil {
		return err
	}

	cpTotal := 0
	cpReady := 0
	anyProvisioning := false
	anyBootstrapped := false

	for _, m := range machines.Items {
		if m.Spec.ClusterRef != cl.Name || m.Spec.Role != fleetv1alpha1.MachineRoleControlPlane {
			continue
		}
		cpTotal++
		if m.Status.Ready {
			cpReady++
		}
		if m.Status.Phase == fleetv1alpha1.MachinePhaseProvisioning || m.Status.Phase == fleetv1alpha1.MachinePhaseBootstrapping {
			anyProvisioning = true
		}
		if m.Status.BootstrapReady {
			anyBootstrapped = true
		}
	}

	newStatus.ControlPlaneReady = cpTotal > 0 && cpReady == cpTotal
	newStatus.Initialized = anyBootstrapped

	switch {
	case cpTotal == 0:
		newStatus.Phase = fleetv1alpha1.ClusterPhasePending
	case anyProvisioning:
		newStatus.Phase = fleetv1alpha1.ClusterPhaseProvisioning
	case newStatus.ControlPlaneReady:
		newStatus.Phase = fleetv1alpha1.ClusterPhaseReady
	case anyBootstrapped:
		newStatus.Phase = fleetv1alpha1.ClusterPhaseProvisioned
	default:
		newStatus.Phase = fleetv1alpha1.ClusterPhaseProvisioning
	}

	meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               cluster.ConditionSecretsReady,
		Status:             metav1.ConditionTrue,
		Reason:             "SecretsAvailable",
		Message:            "Talos secrets bundle is available",
		ObservedGeneration: cl.Generation,
	})

	meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               cluster.ConditionTalosconfigReady,
		Status:             metav1.ConditionTrue,
		Reason:             "TalosconfigAvailable",
		Message:            "Talosconfig client configuration is available",
		ObservedGeneration: cl.Generation,
	})

	readyStatus := metav1.ConditionFalse
	readyReason := "NotReady"
	readyMessage := "Control plane is not ready"
	if newStatus.ControlPlaneReady {
		readyStatus = metav1.ConditionTrue
		readyReason = "ControlPlaneReady"
		readyMessage = "All control plane nodes are ready"
	}
	meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               cluster.ConditionTypeReady,
		Status:             readyStatus,
		Reason:             readyReason,
		Message:            readyMessage,
		ObservedGeneration: cl.Generation,
	})

	slices.SortFunc(newStatus.Conditions, func(a, b metav1.Condition) int {
		return cmp.Compare(a.Type, b.Type)
	})

	if !equality.Semantic.DeepEqual(cl.Status, *newStatus) {
		patch := client.MergeFrom(cl.DeepCopy())
		cl.Status = *newStatus
		if err := r.Status().Patch(ctx, cl, patch); err != nil {
			return err
		}
		log.FromContext(ctx).Info("Cluster status updated", "phase", cl.Status.Phase)
		r.Recorder.Eventf(cl, nil, corev1.EventTypeNormal, "Reconciled", "Reconcile",
			"Cluster status updated to %s", cl.Status.Phase)
	}

	return nil
}

// SetupWithManager registers the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fleetv1alpha1.Cluster{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Owns(&corev1.Secret{}).
		Watches(
			&fleetv1alpha1.Machine{},
			handler.EnqueueRequestsFromMapFunc(r.mapMachineToCluster),
		).
		Named("cluster").
		Complete(r)
}

// mapMachineToCluster maps Machine events to the owning Cluster.
func (r *ClusterReconciler) mapMachineToCluster(_ context.Context, obj client.Object) []reconcile.Request {
	m, ok := obj.(*fleetv1alpha1.Machine)
	if !ok {
		return nil
	}

	if m.Spec.ClusterRef == "" {
		return nil
	}

	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Name: m.Spec.ClusterRef}},
	}
}
