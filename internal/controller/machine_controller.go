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
	"time"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	"github.com/otterscale/fleet-operator/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MachineReconciler reconciles a Machine object.
// It generates Talos machine configs, patches BareMetalHost resources with the
// Talos image and bootstrap data, and triggers the initial Talos bootstrap on
// the designated bootstrap node.
//
// Unlike the tenant-operator (which uses pure OwnerReference GC), the Machine
// controller requires a finalizer because BareMetalHost is an external resource
// that must be explicitly cleaned up on deletion.
type MachineReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Version      string
	Recorder     events.EventRecorder
	Bootstrapper cluster.Bootstrapper
}

// RBAC permissions required by the controller:
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=machines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=machines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=machines/finalizers,verbs=update
// +kubebuilder:rbac:groups=fleet.otterscale.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=metal3.io,resources=baremetalhosts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is the main loop for the Machine controller.
// Flow: Fetch -> Fetch Cluster -> Handle Deletion -> Reconcile Resources ->
// Reconcile Bootstrap -> Update Status.
func (r *MachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName(req.Name)
	ctx = log.IntoContext(ctx, logger)

	var m fleetv1alpha1.Machine
	if err := r.Get(ctx, req.NamespacedName, &m); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var cl fleetv1alpha1.Cluster
	if err := r.Get(ctx, types.NamespacedName{Name: m.Spec.ClusterRef}, &cl); err != nil {
		if apierrors.IsNotFound(err) {
			r.setReadyConditionFalse(ctx, &m, "ClusterNotFound", "Referenced Cluster does not exist")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	if !m.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &m)
	}

	if !controllerutil.ContainsFinalizer(&m, cluster.FinalizerMachineCleanup) {
		controllerutil.AddFinalizer(&m, cluster.FinalizerMachineCleanup)
		if err := r.Update(ctx, &m); err != nil {
			return ctrl.Result{}, err
		}
	}

	if m.Spec.Role == fleetv1alpha1.MachineRoleWorker && m.Spec.Bootstrap {
		return r.handleReconcileError(ctx, &m, &cluster.InvalidConfigError{
			Field:   "bootstrap",
			Message: "worker nodes cannot be bootstrap nodes",
		})
	}

	if err := r.reconcileResources(ctx, &cl, &m); err != nil {
		return r.handleReconcileError(ctx, &m, err)
	}

	result, err := r.reconcileBootstrap(ctx, &cl, &m)
	if err != nil {
		return r.handleReconcileError(ctx, &m, err)
	}

	if err := r.updateStatus(ctx, &cl, &m); err != nil {
		return ctrl.Result{}, err
	}

	return result, nil
}

// reconcileDelete handles Machine deletion by deprovisioning the BareMetalHost
// and removing the finalizer.
func (r *MachineReconciler) reconcileDelete(ctx context.Context, m *fleetv1alpha1.Machine) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(m, cluster.FinalizerMachineCleanup) {
		logger.Info("Deprovisioning BareMetalHost for Machine deletion")

		if err := cluster.DeprovisionBareMetalHost(ctx, r.Client, m); err != nil {
			return ctrl.Result{}, err
		}

		controllerutil.RemoveFinalizer(m, cluster.FinalizerMachineCleanup)
		if err := r.Update(ctx, m); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// reconcileResources orchestrates machine config generation and BMH patching.
func (r *MachineReconciler) reconcileResources(ctx context.Context, cl *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine) error {
	bundle, err := cluster.ReconcileSecrets(ctx, r.Client, r.Scheme, cl, r.Version)
	if err != nil {
		return err
	}

	if err := cluster.ReconcileMachineConfig(ctx, r.Client, r.Scheme, cl, m, bundle, r.Version); err != nil {
		return err
	}

	return cluster.ReconcileBareMetalHost(ctx, r.Client, cl, m)
}

// reconcileBootstrap checks the BareMetalHost provisioning state and triggers
// Talos bootstrap when the infrastructure is ready.
func (r *MachineReconciler) reconcileBootstrap(ctx context.Context, cl *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine) (ctrl.Result, error) {
	bmhState, err := cluster.GetBareMetalHostState(ctx, r.Client, m)
	if err != nil {
		var bmhErr *cluster.BMHNotReadyError
		if errors.As(err, &bmhErr) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	if len(bmhState.Addresses) > 0 {
		m.Status.Addresses = bmhState.Addresses
	}

	if !bmhState.IsProvisioned() {
		log.FromContext(ctx).V(1).Info("BareMetalHost not yet provisioned",
			"state", bmhState.ProvisioningState, "poweredOn", bmhState.PoweredOn)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	m.Status.InfrastructureReady = true

	if m.Spec.Bootstrap && !cl.Status.Initialized {
		if len(m.Status.Addresses) == 0 {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		if err := cluster.TriggerBootstrap(ctx, r.Client, r.Bootstrapper, cl, m); err != nil {
			return ctrl.Result{}, err
		}

		m.Status.BootstrapReady = true
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "Bootstrapped", "Bootstrap",
			"Talos bootstrap completed on node %s", m.Name)
	} else if !m.Spec.Bootstrap {
		// Non-bootstrap nodes are considered bootstrap-ready once infrastructure
		// is provisioned; they auto-join via the Talos join config.
		m.Status.BootstrapReady = true
	}

	return ctrl.Result{}, nil
}

// handleReconcileError categorizes errors and updates status accordingly.
func (r *MachineReconciler) handleReconcileError(ctx context.Context, m *fleetv1alpha1.Machine, err error) (ctrl.Result, error) {
	var ice *cluster.InvalidConfigError
	if errors.As(err, &ice) {
		r.setReadyConditionFalse(ctx, m, "InvalidConfig", err.Error())
		r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "InvalidConfig", "Reconcile", err.Error())
		return ctrl.Result{}, nil
	}

	var bmhErr *cluster.BMHNotReadyError
	if errors.As(err, &bmhErr) {
		r.setReadyConditionFalse(ctx, m, "InfrastructureNotReady", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if apierrors.IsConflict(err) {
		log.FromContext(ctx).V(1).Info("Conflict detected, requeuing", "error", err)
		return ctrl.Result{Requeue: true}, nil
	}

	r.setReadyConditionFalse(ctx, m, "ReconcileError", err.Error())
	r.Recorder.Eventf(m, nil, corev1.EventTypeWarning, "ReconcileError", "Reconcile", err.Error())
	return ctrl.Result{}, err
}

// setReadyConditionFalse patches the Ready condition to False.
func (r *MachineReconciler) setReadyConditionFalse(ctx context.Context, m *fleetv1alpha1.Machine, reason, message string) {
	logger := log.FromContext(ctx)
	patch := client.MergeFrom(m.DeepCopy())
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
		Type:               cluster.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: m.Generation,
	})
	m.Status.ObservedGeneration = m.Generation

	if err := r.Status().Patch(ctx, m, patch); err != nil {
		logger.Error(err, "Failed to patch Ready=False status condition", "reason", reason)
	}
}

// updateStatus calculates the observed status and patches the Machine.
func (r *MachineReconciler) updateStatus(ctx context.Context, cl *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine) error {
	newStatus := m.Status.DeepCopy()
	newStatus.ObservedGeneration = m.Generation

	newStatus.BootstrapDataSecretRef = &fleetv1alpha1.ResourceReference{
		Name:      m.Name + cluster.BootstrapDataSuffix,
		Namespace: cluster.SecretsNamespace,
	}

	switch {
	case m.DeletionTimestamp != nil:
		newStatus.Phase = fleetv1alpha1.MachinePhaseDeleting
	case newStatus.BootstrapReady && newStatus.InfrastructureReady:
		newStatus.Phase = fleetv1alpha1.MachinePhaseRunning
		newStatus.Ready = true
	case newStatus.InfrastructureReady && m.Spec.Bootstrap && !newStatus.BootstrapReady:
		newStatus.Phase = fleetv1alpha1.MachinePhaseBootstrapping
	case newStatus.InfrastructureReady:
		newStatus.Phase = fleetv1alpha1.MachinePhaseProvisioned
	default:
		newStatus.Phase = fleetv1alpha1.MachinePhaseProvisioning
	}

	infraStatus := metav1.ConditionFalse
	infraReason := "Provisioning"
	infraMessage := "BareMetalHost is being provisioned"
	if newStatus.InfrastructureReady {
		infraStatus = metav1.ConditionTrue
		infraReason = "Provisioned"
		infraMessage = "BareMetalHost provisioning is complete"
	}
	meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               cluster.ConditionInfrastructureReady,
		Status:             infraStatus,
		Reason:             infraReason,
		Message:            infraMessage,
		ObservedGeneration: m.Generation,
	})

	bootstrapStatus := metav1.ConditionFalse
	bootstrapReason := "Pending"
	bootstrapMessage := "Talos bootstrap has not completed"
	if newStatus.BootstrapReady {
		bootstrapStatus = metav1.ConditionTrue
		bootstrapReason = "Bootstrapped"
		bootstrapMessage = "Talos bootstrap is complete"
	}
	meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               cluster.ConditionBootstrapReady,
		Status:             bootstrapStatus,
		Reason:             bootstrapReason,
		Message:            bootstrapMessage,
		ObservedGeneration: m.Generation,
	})

	readyStatus := metav1.ConditionFalse
	readyReason := "NotReady"
	readyMessage := "Machine is not fully ready"
	if newStatus.Ready {
		readyStatus = metav1.ConditionTrue
		readyReason = "Ready"
		readyMessage = "Machine is fully operational"
	}
	meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{
		Type:               cluster.ConditionTypeReady,
		Status:             readyStatus,
		Reason:             readyReason,
		Message:            readyMessage,
		ObservedGeneration: m.Generation,
	})

	slices.SortFunc(newStatus.Conditions, func(a, b metav1.Condition) int {
		return cmp.Compare(a.Type, b.Type)
	})

	if !equality.Semantic.DeepEqual(m.Status, *newStatus) {
		patch := client.MergeFrom(m.DeepCopy())
		m.Status = *newStatus
		if err := r.Status().Patch(ctx, m, patch); err != nil {
			return err
		}
		log.FromContext(ctx).Info("Machine status updated",
			"phase", m.Status.Phase, "ready", m.Status.Ready)
		r.Recorder.Eventf(m, nil, corev1.EventTypeNormal, "StatusUpdated", "Reconcile",
			"Machine phase is %s", m.Status.Phase)
	}

	return nil
}

// SetupWithManager registers the controller with the Manager.
func (r *MachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fleetv1alpha1.Machine{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Owns(&corev1.Secret{}).
		Watches(
			&metal3api.BareMetalHost{},
			handler.EnqueueRequestsFromMapFunc(r.mapBMHToMachine),
		).
		Named("machine").
		Complete(r)
}

// mapBMHToMachine maps BareMetalHost events to the Machine that references it.
func (r *MachineReconciler) mapBMHToMachine(ctx context.Context, obj client.Object) []reconcile.Request {
	bmh, ok := obj.(*metal3api.BareMetalHost)
	if !ok {
		return nil
	}

	var machines fleetv1alpha1.MachineList
	if err := r.List(ctx, &machines); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list Machines for BMH mapping")
		return nil
	}

	var requests []reconcile.Request
	for _, m := range machines.Items {
		if m.Spec.BareMetalHostRef.Name == bmh.Name && m.Spec.BareMetalHostRef.Namespace == bmh.Namespace {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: m.Name},
			})
		}
	}

	return requests
}
