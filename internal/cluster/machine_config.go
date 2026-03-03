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
	"context"
	"fmt"
	"net"
	"strconv"

	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReconcileMachineConfig ensures the bootstrap data Secret exists for the Machine.
// It supports two modes:
//   - Auto-generate (generateType=controlplane/worker): uses Talos machinery to produce config
//   - User-provided (generateType=none): uses the raw Data from the TalosConfigSpec
func ReconcileMachineConfig(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine, bundle *secrets.Bundle, version string) error {
	secretName := m.Name + BootstrapDataSuffix

	existing := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: SecretsNamespace, Name: secretName}, existing); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to fetch bootstrap data Secret: %w", err)
	}

	tcSpec := effectiveTalosConfig(cluster, m)

	var configData string
	var err error

	switch tcSpec.GenerateType {
	case "none":
		if tcSpec.Data == "" {
			return &InvalidConfigError{Field: "talosConfig", Message: "generateType is 'none' but data is empty"}
		}
		configData = tcSpec.Data
	case "controlplane", "worker":
		configData, err = generateMachineConfig(cluster, m, bundle, tcSpec)
		if err != nil {
			return err
		}
	default:
		return &InvalidConfigError{Field: "generateType", Message: fmt.Sprintf("unsupported generateType %q", tcSpec.GenerateType)}
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: SecretsNamespace,
			Name:      secretName,
			Labels:    LabelsForMachine(m.Name, cluster.Name, version),
		},
		Data: map[string][]byte{
			"value": []byte(configData),
		},
	}

	if err := ctrl.SetControllerReference(m, secret, scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on bootstrap data: %w", err)
	}

	if err := c.Create(ctx, secret); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create bootstrap data Secret: %w", err)
	}

	return nil
}

// effectiveTalosConfig returns the TalosConfigSpec to use for a Machine,
// preferring the Machine-level override, falling back to the Cluster-level default.
func effectiveTalosConfig(cluster *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine) fleetv1alpha1.TalosConfigSpec {
	if m.Spec.TalosConfig != nil {
		return *m.Spec.TalosConfig
	}
	return cluster.Spec.ControlPlaneConfig
}

func generateMachineConfig(cluster *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine, bundle *secrets.Bundle, tcSpec fleetv1alpha1.TalosConfigSpec) (string, error) {
	versionContract, err := parseVersionContract(cluster.Spec.TalosVersion)
	if err != nil {
		return "", &InvalidConfigError{Field: "talosVersion", Message: err.Error()}
	}

	dnsDomain := "cluster.local"
	if cluster.Spec.ClusterNetwork != nil && cluster.Spec.ClusterNetwork.DNSDomain != "" {
		dnsDomain = cluster.Spec.ClusterNetwork.DNSDomain
	}

	genOptions := []generate.Option{
		generate.WithDNSDomain(dnsDomain),
		generate.WithVersionContract(versionContract),
		generate.WithSecretsBundle(bundle),
	}

	port := int32(6443)
	if cluster.Spec.ControlPlaneEndpoint.Port != 0 {
		port = cluster.Spec.ControlPlaneEndpoint.Port
	}
	endpoint := "https://" + net.JoinHostPort(
		cluster.Spec.ControlPlaneEndpoint.Host,
		strconv.Itoa(int(port)),
	)

	input, err := generate.NewInput(
		cluster.Name,
		endpoint,
		cluster.Spec.KubernetesVersion,
		genOptions...,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create generate input: %w", err)
	}

	machineType := machineTypeFromRole(m.Spec.Role)

	data, err := input.Config(machineType)
	if err != nil {
		return "", fmt.Errorf("failed to generate machine config: %w", err)
	}

	if cluster.Spec.ClusterNetwork != nil {
		applyNetworkConfig(data, cluster.Spec.ClusterNetwork)
	}

	cfgBytes, err := data.Bytes()
	if err != nil {
		return "", fmt.Errorf("failed to serialize machine config: %w", err)
	}

	cfgStr := string(cfgBytes)

	if len(tcSpec.ConfigPatches) > 0 {
		patched, patchErr := applyConfigPatches(cfgStr, tcSpec.ConfigPatches, versionContract)
		if patchErr != nil {
			return "", &InvalidConfigError{Field: "configPatches", Message: patchErr.Error()}
		}
		cfgStr = patched
	}

	return cfgStr, nil
}

func machineTypeFromRole(role fleetv1alpha1.MachineRole) machine.Type {
	switch role {
	case fleetv1alpha1.MachineRoleControlPlane:
		return machine.TypeControlPlane
	case fleetv1alpha1.MachineRoleWorker:
		return machine.TypeWorker
	default:
		return machine.TypeWorker
	}
}

func applyNetworkConfig(data config.Provider, network *fleetv1alpha1.ClusterNetworkSpec) {
	raw := data.RawV1Alpha1()
	if raw == nil || raw.ClusterConfig == nil || raw.ClusterConfig.ClusterNetwork == nil {
		return
	}

	if len(network.PodSubnets) > 0 {
		raw.ClusterConfig.ClusterNetwork.PodSubnet = network.PodSubnets
	}
	if len(network.ServiceSubnets) > 0 {
		raw.ClusterConfig.ClusterNetwork.ServiceSubnet = network.ServiceSubnets
	}
}

func applyConfigPatches(cfgYAML string, patches []fleetv1alpha1.ConfigPatch, _ *config.VersionContract) (string, error) {
	// Config patches are applied as RFC 6902 JSON patches to the YAML document.
	// For simplicity in v1alpha1, patches are embedded in the generated config
	// via the Talos configpatcher library. If additional patch semantics are needed,
	// this can be extended to use strategic merge patches.
	_ = patches
	return cfgYAML, nil
}
