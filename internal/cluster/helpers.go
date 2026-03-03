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
	"fmt"

	"github.com/otterscale/fleet-operator/internal/labels"
)

const (
	ConditionTypeReady              = "Ready"
	ConditionSecretsReady           = "SecretsReady"
	ConditionTalosconfigReady       = "TalosconfigReady"
	ConditionInfrastructureReady    = "InfrastructureReady"
	ConditionBootstrapReady         = "BootstrapReady"
	ConditionControlPlaneReady      = "ControlPlaneReady"
	ConditionControlPlaneInitialized = "ControlPlaneInitialized"
	ConditionWorkersReady           = "WorkersReady"

	// FinalizerMachineCleanup is applied to Machine resources to ensure
	// BareMetalHost is deprovisioned before the Machine object is removed.
	FinalizerMachineCleanup = "fleet.otterscale.io/machine-cleanup"

	// SecretsSuffix is appended to the Cluster name for the secrets bundle Secret.
	SecretsSuffix = "-talos"

	// TalosconfigSuffix is appended to the Cluster name for the talosconfig Secret.
	TalosconfigSuffix = "-talosconfig"

	// BootstrapDataSuffix is appended to the Machine name for the bootstrap data Secret.
	BootstrapDataSuffix = "-bootstrap-data"

	// SecretsNamespace is the namespace where cluster secrets are stored.
	SecretsNamespace = "otterscale-system"
)

// InvalidConfigError is a permanent error indicating that the Talos
// configuration is invalid and cannot be used.
type InvalidConfigError struct {
	Field   string
	Message string
}

func (e *InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid %s config: %s", e.Field, e.Message)
}

// BMHNotReadyError indicates the BareMetalHost has not reached the expected
// provisioning state. The controller should requeue and check again later.
type BMHNotReadyError struct {
	State   string
	Message string
}

func (e *BMHNotReadyError) Error() string {
	return fmt.Sprintf("BareMetalHost not ready (state=%s): %s", e.State, e.Message)
}

// LabelsForCluster returns the standard K8s recommended labels for cluster-owned resources.
func LabelsForCluster(clusterName, version string) map[string]string {
	return labels.Standard(clusterName, "cluster", version)
}

// LabelRole is the label key for the Machine role (controlplane or worker).
const LabelRole = "fleet.otterscale.io/role"

// LabelsForMachine returns the standard K8s recommended labels for machine-owned resources.
func LabelsForMachine(machineName, clusterName, role, version string) map[string]string {
	m := labels.Standard(machineName, "machine", version)
	m["fleet.otterscale.io/cluster"] = clusterName
	if role != "" {
		m[LabelRole] = role
	}
	return m
}
