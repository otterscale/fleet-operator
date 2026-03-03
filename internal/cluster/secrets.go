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
	"time"

	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ReconcileSecrets ensures the Talos secrets bundle Secret exists for the Cluster.
// If the Secret does not exist, a new secrets bundle is generated using the Talos
// machinery library. The Secret is owned by the Cluster via OwnerReference.
func ReconcileSecrets(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *fleetv1alpha1.Cluster, version string) (*secrets.Bundle, error) {
	secretName := cluster.Name + SecretsSuffix

	existing := &corev1.Secret{}
	err := c.Get(ctx, client.ObjectKey{Namespace: SecretsNamespace, Name: secretName}, existing)

	if err == nil {
		return loadSecretsBundle(existing)
	}

	if !k8serrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to fetch secrets bundle: %w", err)
	}

	versionContract, err := parseVersionContract(cluster.Spec.TalosVersion)
	if err != nil {
		return nil, &InvalidConfigError{Field: "talosVersion", Message: err.Error()}
	}

	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), versionContract)
	if err != nil {
		return nil, fmt.Errorf("failed to generate secrets bundle: %w", err)
	}

	if err := writeSecretsBundle(ctx, c, scheme, cluster, secretName, bundle, version); err != nil {
		if k8serrors.IsAlreadyExists(err) {
			if err := c.Get(ctx, client.ObjectKey{Namespace: SecretsNamespace, Name: secretName}, existing); err != nil {
				return nil, fmt.Errorf("failed to fetch secrets bundle after conflict: %w", err)
			}
			return loadSecretsBundle(existing)
		}
		return nil, fmt.Errorf("failed to write secrets bundle: %w", err)
	}

	return bundle, nil
}

func loadSecretsBundle(secret *corev1.Secret) (*secrets.Bundle, error) {
	bundle := &secrets.Bundle{
		Clock: secrets.NewFixedClock(time.Now()),
	}

	data, ok := secret.Data["bundle"]
	if !ok {
		return nil, fmt.Errorf("secrets Secret missing 'bundle' key")
	}

	if err := yaml.Unmarshal(data, bundle); err != nil {
		return nil, fmt.Errorf("failed to unmarshal secrets bundle: %w", err)
	}

	return bundle, nil
}

func writeSecretsBundle(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *fleetv1alpha1.Cluster, name string, bundle *secrets.Bundle, version string) error {
	data, err := yaml.Marshal(bundle)
	if err != nil {
		return fmt.Errorf("failed to marshal secrets bundle: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: SecretsNamespace,
			Name:      name,
			Labels:    LabelsForCluster(cluster.Name, version),
		},
		Data: map[string][]byte{
			"bundle": data,
		},
	}

	if err := ctrl.SetControllerReference(cluster, secret, scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on secrets bundle: %w", err)
	}

	return c.Create(ctx, secret)
}

func parseVersionContract(talosVersion string) (*config.VersionContract, error) {
	vc, err := config.ParseContractFromVersion(talosVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid talos version %q: %w", talosVersion, err)
	}
	return vc, nil
}
