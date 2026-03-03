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

	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ReconcileTalosconfig ensures the talosconfig client configuration Secret exists.
// The talosconfig allows administrators to connect to nodes via the Talos API.
// Endpoints are populated from Machine addresses as they become available.
func ReconcileTalosconfig(ctx context.Context, c client.Client, scheme *runtime.Scheme, cluster *fleetv1alpha1.Cluster, bundle *secrets.Bundle, endpoints []string, version string) error {
	secretName := cluster.Name + TalosconfigSuffix
	logger := log.FromContext(ctx)

	tcYAML, err := genTalosconfigFile(cluster.Name, bundle, endpoints)
	if err != nil {
		return fmt.Errorf("failed to generate talosconfig: %w", err)
	}

	existing := &corev1.Secret{}
	err = c.Get(ctx, client.ObjectKey{Namespace: SecretsNamespace, Name: secretName}, existing)

	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to fetch talosconfig Secret: %w", err)
	}

	if k8serrors.IsNotFound(err) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: SecretsNamespace,
				Name:      secretName,
				Labels:    LabelsForCluster(cluster.Name, version),
			},
			Data: map[string][]byte{
				"talosconfig": []byte(tcYAML),
			},
		}
		if err := ctrl.SetControllerReference(cluster, secret, scheme); err != nil {
			return fmt.Errorf("failed to set owner reference on talosconfig: %w", err)
		}
		return c.Create(ctx, secret)
	}

	patch := client.MergeFrom(existing.DeepCopy())
	existing.Data["talosconfig"] = []byte(tcYAML)
	if err := c.Patch(ctx, existing, patch); err != nil {
		if k8serrors.IsConflict(err) {
			logger.V(1).Info("Conflict updating talosconfig Secret, will retry")
			return err
		}
		return fmt.Errorf("failed to update talosconfig Secret: %w", err)
	}

	return nil
}

func genTalosconfigFile(clusterName string, bundle *secrets.Bundle, endpoints []string) (string, error) {
	in, err := generate.NewInput(
		clusterName,
		"https://localhost",
		"",
		generate.WithSecretsBundle(bundle),
		generate.WithEndpointList(endpoints),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create generate input for talosconfig: %w", err)
	}

	tc, err := in.Talosconfig()
	if err != nil {
		return "", fmt.Errorf("failed to generate talosconfig: %w", err)
	}

	data, err := yaml.Marshal(tc)
	if err != nil {
		return "", fmt.Errorf("failed to marshal talosconfig: %w", err)
	}

	return string(data), nil
}
