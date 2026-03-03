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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	"github.com/otterscale/fleet-operator/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("ClusterReconciler", func() {
	const (
		clusterName = "test-cluster"
		timeout     = 10 * time.Second
		interval    = 250 * time.Millisecond
	)

	var (
		reconciler *ClusterReconciler
		cl         *fleetv1alpha1.Cluster
		ns         *corev1.Namespace
	)

	BeforeEach(func() {
		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: cluster.SecretsNamespace},
		}
		_ = k8sClient.Create(ctx, ns)

		reconciler = &ClusterReconciler{
			Client:   k8sClient,
			Scheme:   scheme.Scheme,
			Version:  "test",
			Recorder: &fakeRecorder{},
		}

		cl = &fleetv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: clusterName,
			},
			Spec: fleetv1alpha1.ClusterSpec{
				ControlPlaneEndpoint: fleetv1alpha1.Endpoint{
					Host: "10.0.0.1",
					Port: 6443,
				},
				TalosVersion:      "v1.9",
				KubernetesVersion: "v1.32.0",
				TalosImage: fleetv1alpha1.ImageSpec{
					URL:      "http://example.com/talos.raw",
					Checksum: "abc123",
					Format:   "raw",
				},
				ControlPlaneConfig: fleetv1alpha1.TalosConfigSpec{
					GenerateType: "controlplane",
				},
			},
		}
	})

	JustBeforeEach(func() {
		Expect(k8sClient.Create(ctx, cl)).To(Succeed())
	})

	AfterEach(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, cl))).To(Succeed())

		secretsList := &corev1.SecretList{}
		Expect(k8sClient.List(ctx, secretsList, client.InNamespace(cluster.SecretsNamespace))).To(Succeed())
		for i := range secretsList.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &secretsList.Items[i]))).To(Succeed())
		}
	})

	It("should create Talos secrets and talosconfig Secrets", func() {
		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(ctrl.Result{}))

		secretsSecret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: cluster.SecretsNamespace,
			Name:      clusterName + cluster.SecretsSuffix,
		}, secretsSecret)).To(Succeed())
		Expect(secretsSecret.Data).To(HaveKey("bundle"))

		tcSecret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: cluster.SecretsNamespace,
			Name:      clusterName + cluster.TalosconfigSuffix,
		}, tcSecret)).To(Succeed())
		Expect(tcSecret.Data).To(HaveKey("talosconfig"))
	})

	It("should set status phase to Pending when no Machines exist", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName},
		})
		Expect(err).NotTo(HaveOccurred())

		updated := &fleetv1alpha1.Cluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName}, updated)).To(Succeed())
		Expect(updated.Status.Phase).To(Equal(fleetv1alpha1.ClusterPhasePending))
		Expect(updated.Status.ControlPlaneReady).To(BeFalse())
		Expect(updated.Status.SecretsRef).NotTo(BeNil())
		Expect(updated.Status.TalosconfigRef).NotTo(BeNil())
	})

	It("should be idempotent on repeated reconciliation", func() {
		result1, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName},
		})
		Expect(err).NotTo(HaveOccurred())

		result2, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result2).To(Equal(result1))
	})

	It("should ignore deleted Cluster", func() {
		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "nonexistent"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(ctrl.Result{}))
	})

	It("should track worker counts in status when worker Machines exist", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName},
		})
		Expect(err).NotTo(HaveOccurred())

		worker := &fleetv1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-worker-status",
			},
			Spec: fleetv1alpha1.MachineSpec{
				ClusterRef: clusterName,
				Role:       fleetv1alpha1.MachineRoleWorker,
				BareMetalHostRef: fleetv1alpha1.BareMetalHostReference{
					Name:      "some-bmh",
					Namespace: "default",
				},
			},
		}
		Expect(k8sClient.Create(ctx, worker)).To(Succeed())
		defer func() {
			_ = client.IgnoreNotFound(k8sClient.Delete(ctx, worker))
		}()

		_, err = reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName},
		})
		Expect(err).NotTo(HaveOccurred())

		updated := &fleetv1alpha1.Cluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName}, updated)).To(Succeed())
		Expect(updated.Status.TotalWorkers).To(Equal(int32(1)))
		Expect(updated.Status.ReadyWorkers).To(Equal(int32(0)))
	})
})

type fakeRecorder struct{}

func (f *fakeRecorder) Eventf(_ runtime.Object, _ runtime.Object, _ string, _, _ string, _ string, _ ...interface{}) {
}
