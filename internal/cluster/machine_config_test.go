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

package cluster

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
)

var _ = Describe("effectiveTalosConfig", func() {
	var cluster *fleetv1alpha1.Cluster

	BeforeEach(func() {
		cluster = &fleetv1alpha1.Cluster{
			Spec: fleetv1alpha1.ClusterSpec{
				ControlPlaneConfig: fleetv1alpha1.TalosConfigSpec{
					GenerateType: "controlplane",
				},
				WorkerConfig: fleetv1alpha1.TalosConfigSpec{
					GenerateType: "worker",
				},
			},
		}
	})

	It("should return Machine-level override when set", func() {
		m := &fleetv1alpha1.Machine{
			Spec: fleetv1alpha1.MachineSpec{
				Role: fleetv1alpha1.MachineRoleControlPlane,
				TalosConfig: &fleetv1alpha1.TalosConfigSpec{
					GenerateType: "none",
					Data:         "custom-data",
				},
			},
		}
		cfg := effectiveTalosConfig(cluster, m)
		Expect(cfg.GenerateType).To(Equal("none"))
		Expect(cfg.Data).To(Equal("custom-data"))
	})

	It("should fall back to ControlPlaneConfig for control plane role", func() {
		m := &fleetv1alpha1.Machine{
			Spec: fleetv1alpha1.MachineSpec{
				Role: fleetv1alpha1.MachineRoleControlPlane,
			},
		}
		cfg := effectiveTalosConfig(cluster, m)
		Expect(cfg.GenerateType).To(Equal("controlplane"))
	})

	It("should fall back to WorkerConfig for worker role", func() {
		m := &fleetv1alpha1.Machine{
			Spec: fleetv1alpha1.MachineSpec{
				Role: fleetv1alpha1.MachineRoleWorker,
			},
		}
		cfg := effectiveTalosConfig(cluster, m)
		Expect(cfg.GenerateType).To(Equal("worker"))
	})

	It("should default GenerateType to controlplane when ControlPlaneConfig has empty GenerateType", func() {
		cluster.Spec.ControlPlaneConfig.GenerateType = ""
		m := &fleetv1alpha1.Machine{
			Spec: fleetv1alpha1.MachineSpec{
				Role: fleetv1alpha1.MachineRoleControlPlane,
			},
		}
		cfg := effectiveTalosConfig(cluster, m)
		Expect(cfg.GenerateType).To(Equal("controlplane"))
	})

	It("should default GenerateType to worker when WorkerConfig has empty GenerateType", func() {
		cluster.Spec.WorkerConfig.GenerateType = ""
		m := &fleetv1alpha1.Machine{
			Spec: fleetv1alpha1.MachineSpec{
				Role: fleetv1alpha1.MachineRoleWorker,
			},
		}
		cfg := effectiveTalosConfig(cluster, m)
		Expect(cfg.GenerateType).To(Equal("worker"))
	})

	It("should prefer Machine-level TalosConfig even for worker", func() {
		m := &fleetv1alpha1.Machine{
			Spec: fleetv1alpha1.MachineSpec{
				Role: fleetv1alpha1.MachineRoleWorker,
				TalosConfig: &fleetv1alpha1.TalosConfigSpec{
					GenerateType: "none",
					Data:         "worker-custom",
				},
			},
		}
		cfg := effectiveTalosConfig(cluster, m)
		Expect(cfg.GenerateType).To(Equal("none"))
		Expect(cfg.Data).To(Equal("worker-custom"))
	})

	It("should not mutate the original Cluster config", func() {
		cluster.Spec.WorkerConfig.GenerateType = ""
		m := &fleetv1alpha1.Machine{
			Spec: fleetv1alpha1.MachineSpec{
				Role: fleetv1alpha1.MachineRoleWorker,
			},
		}
		cfg := effectiveTalosConfig(cluster, m)
		Expect(cfg.GenerateType).To(Equal("worker"))
		Expect(cluster.Spec.WorkerConfig.GenerateType).To(Equal(""),
			"original Cluster config should not be mutated")
	})
})

var _ = Describe("applyConfigPatches", func() {
	It("should return an error indicating patches are not yet supported", func() {
		patches := []fleetv1alpha1.ConfigPatch{
			{Op: "add", Path: "/machine/network"},
		}
		_, err := applyConfigPatches("some-yaml", patches, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not yet supported"))
		Expect(err.Error()).To(ContainSubstring("1 patches"))
	})
})
