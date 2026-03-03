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

	"github.com/otterscale/fleet-operator/internal/labels"
)

var _ = Describe("Helpers", func() {
	Describe("InvalidConfigError", func() {
		It("should format error message correctly", func() {
			err := &InvalidConfigError{Field: "talosVersion", Message: "invalid format"}
			Expect(err.Error()).To(Equal("invalid talosVersion config: invalid format"))
		})
	})

	Describe("BMHNotReadyError", func() {
		It("should format error message correctly", func() {
			err := &BMHNotReadyError{State: "provisioning", Message: "still writing image"}
			Expect(err.Error()).To(Equal("BareMetalHost not ready (state=provisioning): still writing image"))
		})
	})

	Describe("LabelsForCluster", func() {
		It("should return standard labels with cluster name", func() {
			l := LabelsForCluster("my-cluster", "v1.0.0")
			Expect(l).To(HaveKeyWithValue(labels.Name, "my-cluster"))
			Expect(l).To(HaveKeyWithValue(labels.Component, "cluster"))
			Expect(l).To(HaveKeyWithValue(labels.PartOf, labels.System))
			Expect(l).To(HaveKeyWithValue(labels.ManagedBy, labels.Operator))
			Expect(l).To(HaveKeyWithValue(labels.Version, "v1.0.0"))
		})
	})

	Describe("LabelsForMachine", func() {
		It("should return standard labels with machine and cluster names", func() {
			l := LabelsForMachine("my-machine", "my-cluster", "controlplane", "v1.0.0")
			Expect(l).To(HaveKeyWithValue(labels.Name, "my-machine"))
			Expect(l).To(HaveKeyWithValue(labels.Component, "machine"))
			Expect(l).To(HaveKeyWithValue("fleet.otterscale.io/cluster", "my-cluster"))
			Expect(l).To(HaveKeyWithValue(LabelRole, "controlplane"))
		})

		It("should include worker role label for worker machines", func() {
			l := LabelsForMachine("my-worker", "my-cluster", "worker", "v1.0.0")
			Expect(l).To(HaveKeyWithValue(LabelRole, "worker"))
		})

		It("should omit version label when version is empty", func() {
			l := LabelsForMachine("m", "c", "worker", "")
			Expect(l).NotTo(HaveKey(labels.Version))
		})

		It("should omit role label when role is empty", func() {
			l := LabelsForMachine("m", "c", "", "v1.0.0")
			Expect(l).NotTo(HaveKey(LabelRole))
		})
	})
})
