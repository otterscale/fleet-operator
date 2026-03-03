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

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	bmhStateProvisioned = "provisioned"
	bmhStateAvailable   = "available"
)

// BMHState summarises the fields a controller needs from a BareMetalHost.
type BMHState struct {
	ProvisioningState string
	OperationalStatus string
	PoweredOn         bool
	ErrorMessage      string
	Addresses         []fleetv1alpha1.MachineAddress
}

// IsProvisioned returns true when the BareMetalHost has finished writing
// the OS image and the host is powered on.
func (s BMHState) IsProvisioned() bool {
	return s.ProvisioningState == bmhStateProvisioned && s.PoweredOn
}

// ReconcileBareMetalHost ensures the referenced BareMetalHost is configured
// with the Talos image and user data from the bootstrap Secret.
//
// The function is idempotent: if the BMH already has the correct image and
// userData, no patch is issued.
func ReconcileBareMetalHost(ctx context.Context, c client.Client, cluster *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine) error {
	logger := log.FromContext(ctx)

	bmh := &metal3api.BareMetalHost{}
	key := client.ObjectKey{
		Namespace: m.Spec.BareMetalHostRef.Namespace,
		Name:      m.Spec.BareMetalHostRef.Name,
	}

	if err := c.Get(ctx, key, bmh); err != nil {
		if k8serrors.IsNotFound(err) {
			return &BMHNotReadyError{State: "missing", Message: fmt.Sprintf("BareMetalHost %s/%s not found", key.Namespace, key.Name)}
		}
		return fmt.Errorf("failed to fetch BareMetalHost: %w", err)
	}

	patch := client.MergeFrom(bmh.DeepCopy())
	changed := false

	desiredImage := &metal3api.Image{
		URL:          cluster.Spec.TalosImage.URL,
		Checksum:     cluster.Spec.TalosImage.Checksum,
		ChecksumType: metal3api.ChecksumType(cluster.Spec.TalosImage.ChecksumType),
		DiskFormat:   toDiskFormat(cluster.Spec.TalosImage.Format),
	}

	if bmh.Spec.Image == nil || !imageEqual(bmh.Spec.Image, desiredImage) {
		bmh.Spec.Image = desiredImage
		changed = true
	}

	bootstrapSecretName := m.Name + BootstrapDataSuffix
	desiredUserData := &corev1.SecretReference{
		Name:      bootstrapSecretName,
		Namespace: SecretsNamespace,
	}

	if bmh.Spec.UserData == nil || bmh.Spec.UserData.Name != desiredUserData.Name || bmh.Spec.UserData.Namespace != desiredUserData.Namespace {
		bmh.Spec.UserData = desiredUserData
		changed = true
	}

	if !bmh.Spec.Online {
		bmh.Spec.Online = true
		changed = true
	}

	if bmh.Spec.AutomatedCleaningMode != metal3api.CleaningModeDisabled {
		bmh.Spec.AutomatedCleaningMode = metal3api.CleaningModeDisabled
		changed = true
	}

	if !changed {
		return nil
	}

	logger.Info("Patching BareMetalHost", "bmh", key, "image", desiredImage.URL)

	if err := c.Patch(ctx, bmh, patch); err != nil {
		return fmt.Errorf("failed to patch BareMetalHost: %w", err)
	}

	return nil
}

// GetBareMetalHostState reads the current state of the BareMetalHost
// referenced by the Machine.
func GetBareMetalHostState(ctx context.Context, c client.Client, m *fleetv1alpha1.Machine) (*BMHState, error) {
	bmh := &metal3api.BareMetalHost{}
	key := client.ObjectKey{
		Namespace: m.Spec.BareMetalHostRef.Namespace,
		Name:      m.Spec.BareMetalHostRef.Name,
	}

	if err := c.Get(ctx, key, bmh); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, &BMHNotReadyError{State: "missing", Message: fmt.Sprintf("BareMetalHost %s/%s not found", key.Namespace, key.Name)}
		}
		return nil, fmt.Errorf("failed to fetch BareMetalHost: %w", err)
	}

	state := &BMHState{
		ProvisioningState: string(bmh.Status.Provisioning.State),
		OperationalStatus: string(bmh.Status.OperationalStatus),
		PoweredOn:         bmh.Status.PoweredOn,
		ErrorMessage:      bmh.Status.ErrorMessage,
	}

	if bmh.Status.HardwareDetails != nil {
		for _, nic := range bmh.Status.HardwareDetails.NIC {
			if nic.IP != "" {
				state.Addresses = append(state.Addresses, fleetv1alpha1.MachineAddress{
					Type:    "InternalIP",
					Address: nic.IP,
				})
			}
		}
	}

	return state, nil
}

// DeprovisionBareMetalHost removes the Talos image and user data from the
// BareMetalHost and powers it off. This is called during Machine deletion
// as part of the finalizer cleanup.
func DeprovisionBareMetalHost(ctx context.Context, c client.Client, m *fleetv1alpha1.Machine) error {
	logger := log.FromContext(ctx)

	bmh := &metal3api.BareMetalHost{}
	key := client.ObjectKey{
		Namespace: m.Spec.BareMetalHostRef.Namespace,
		Name:      m.Spec.BareMetalHostRef.Name,
	}

	if err := c.Get(ctx, key, bmh); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to fetch BareMetalHost for deprovisioning: %w", err)
	}

	patch := client.MergeFrom(bmh.DeepCopy())

	bmh.Spec.Image = nil
	bmh.Spec.UserData = nil
	bmh.Spec.Online = false

	logger.Info("Deprovisioning BareMetalHost", "bmh", key)

	if err := c.Patch(ctx, bmh, patch); err != nil {
		return fmt.Errorf("failed to deprovision BareMetalHost: %w", err)
	}

	return nil
}

func toDiskFormat(format string) *string {
	if format == "" {
		f := "raw"
		return &f
	}
	return &format
}

func imageEqual(a, b *metal3api.Image) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.URL == b.URL &&
		a.Checksum == b.Checksum &&
		a.ChecksumType == b.ChecksumType &&
		ptrStrEqual(a.DiskFormat, b.DiskFormat)
}

func ptrStrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
