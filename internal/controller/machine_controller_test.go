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
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	"github.com/otterscale/fleet-operator/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// mockBootstrapper is a test double for the Talos bootstrap API.
type mockBootstrapper struct {
	bootstrapCalled bool
	bootstrapErr    error
	isBootstrapped  bool
	checkErr        error
}

func (m *mockBootstrapper) Bootstrap(_ context.Context, _ []string, _ []byte) error {
	m.bootstrapCalled = true
	return m.bootstrapErr
}

func (m *mockBootstrapper) IsBootstrapped(_ context.Context, _ []string, _ []byte) (bool, error) {
	return m.isBootstrapped, m.checkErr
}

var _ = Describe("MachineReconciler", func() {
	const (
		clusterName = "test-machine-cluster"
		machineName = "test-machine"
		bmhName     = "test-bmh"
		bmhNS       = "default"
		timeout     = 10 * time.Second
		interval    = 250 * time.Millisecond
	)

	var (
		reconciler     *MachineReconciler
		clReconciler   *ClusterReconciler
		cl             *fleetv1alpha1.Cluster
		m              *fleetv1alpha1.Machine
		bmh            *metal3api.BareMetalHost
		bootstrapper   *mockBootstrapper
		ns             *corev1.Namespace
	)

	BeforeEach(func() {
		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: cluster.SecretsNamespace},
		}
		_ = k8sClient.Create(ctx, ns)

		bootstrapper = &mockBootstrapper{}

		reconciler = &MachineReconciler{
			Client:       k8sClient,
			Scheme:       scheme.Scheme,
			Version:      "test",
			Recorder:     &fakeRecorder{},
			Bootstrapper: bootstrapper,
		}

		clReconciler = &ClusterReconciler{
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
					URL:          "http://example.com/talos.raw",
					Checksum:     "abc123",
					ChecksumType: "sha256",
					Format:       "raw",
				},
				ControlPlaneConfig: fleetv1alpha1.TalosConfigSpec{
					GenerateType: "controlplane",
				},
			},
		}

		bmh = &metal3api.BareMetalHost{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bmhName,
				Namespace: bmhNS,
			},
			Spec: metal3api.BareMetalHostSpec{
				BMC: metal3api.BMCDetails{
					Address:         "ipmi://192.168.1.1",
					CredentialsName: "bmc-creds",
				},
				BootMACAddress: "00:11:22:33:44:55",
			},
		}

		m = &fleetv1alpha1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name: machineName,
			},
			Spec: fleetv1alpha1.MachineSpec{
				ClusterRef: clusterName,
				Role:       fleetv1alpha1.MachineRoleControlPlane,
				BareMetalHostRef: fleetv1alpha1.BareMetalHostReference{
					Name:      bmhName,
					Namespace: bmhNS,
				},
				Bootstrap: true,
			},
		}
	})

	JustBeforeEach(func() {
		Expect(k8sClient.Create(ctx, cl)).To(Succeed())

		_, err := clReconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: clusterName},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Create(ctx, bmh)).To(Succeed())
		Expect(k8sClient.Create(ctx, m)).To(Succeed())
	})

	AfterEach(func() {
		if m != nil {
			updated := &fleetv1alpha1.Machine{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: machineName}, updated); err == nil {
				controllerutil.RemoveFinalizer(updated, cluster.FinalizerMachineCleanup)
				_ = k8sClient.Update(ctx, updated)
				_ = k8sClient.Delete(ctx, updated)
			}
		}
		_ = client.IgnoreNotFound(k8sClient.Delete(ctx, bmh))
		_ = client.IgnoreNotFound(k8sClient.Delete(ctx, cl))

		secretsList := &corev1.SecretList{}
		_ = k8sClient.List(ctx, secretsList, client.InNamespace(cluster.SecretsNamespace))
		for i := range secretsList.Items {
			_ = client.IgnoreNotFound(k8sClient.Delete(ctx, &secretsList.Items[i]))
		}
	})

	It("should add finalizer and create bootstrap data Secret", func() {
		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: machineName},
		})
		Expect(err).NotTo(HaveOccurred())
		_ = result

		updated := &fleetv1alpha1.Machine{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: machineName}, updated)).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(updated, cluster.FinalizerMachineCleanup)).To(BeTrue())

		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: cluster.SecretsNamespace,
			Name:      machineName + cluster.BootstrapDataSuffix,
		}, secret)).To(Succeed())
		Expect(secret.Data).To(HaveKey("value"))
	})

	It("should patch BareMetalHost with Talos image", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: machineName},
		})
		Expect(err).NotTo(HaveOccurred())

		updatedBMH := &metal3api.BareMetalHost{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      bmhName,
			Namespace: bmhNS,
		}, updatedBMH)).To(Succeed())

		Expect(updatedBMH.Spec.Image).NotTo(BeNil())
		Expect(updatedBMH.Spec.Image.URL).To(Equal("http://example.com/talos.raw"))
		Expect(updatedBMH.Spec.Online).To(BeTrue())
		Expect(updatedBMH.Spec.UserData).NotTo(BeNil())
	})

	It("should handle deletion by deprovisioning BMH and removing finalizer", func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: machineName},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Delete(ctx, m)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: machineName},
		})
		Expect(err).NotTo(HaveOccurred())

		updatedBMH := &metal3api.BareMetalHost{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name:      bmhName,
			Namespace: bmhNS,
		}, updatedBMH)).To(Succeed())
		Expect(updatedBMH.Spec.Image).To(BeNil())
		Expect(updatedBMH.Spec.Online).To(BeFalse())
	})

	It("should ignore already-deleted Machine", func() {
		result, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: "nonexistent-machine"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(ctrl.Result{}))
	})
})

var _ runtime.Object // suppress unused import

func init() {
	// ensure fakeRecorder is used in both test files
	_ = &fakeRecorder{}
}
