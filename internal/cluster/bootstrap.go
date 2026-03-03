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
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	fleetv1alpha1 "github.com/otterscale/api/fleet/v1alpha1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	talosconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Bootstrapper abstracts the Talos bootstrap API so it can be replaced in tests.
type Bootstrapper interface {
	// Bootstrap initializes etcd on the target node. It is a no-op if etcd
	// is already running.
	Bootstrap(ctx context.Context, addresses []string, talosconfigData []byte) error

	// IsBootstrapped checks whether any of the given addresses already have
	// an etcd member directory.
	IsBootstrapped(ctx context.Context, addresses []string, talosconfigData []byte) (bool, error)
}

// TalosBootstrapper implements Bootstrapper using the real Talos gRPC API.
type TalosBootstrapper struct{}

// Bootstrap connects to the Talos API on the given addresses and initializes etcd.
func (TalosBootstrapper) Bootstrap(ctx context.Context, addresses []string, talosconfigData []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tc, err := talosconfig.FromBytes(talosconfigData)
	if err != nil {
		return fmt.Errorf("failed to parse talosconfig: %w", err)
	}

	c, err := talosclient.New(ctx, talosclient.WithEndpoints(addresses...), talosclient.WithConfig(tc))
	if err != nil {
		return fmt.Errorf("failed to create Talos client: %w", err)
	}
	defer c.Close() //nolint:errcheck

	sort.Strings(addresses)

	if err := c.Bootstrap(talosclient.WithNodes(ctx, addresses[0]), &machineapi.BootstrapRequest{}); err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		return fmt.Errorf("failed to bootstrap node %s: %w", addresses[0], err)
	}

	return nil
}

// IsBootstrapped checks whether etcd data exists on any of the given nodes.
func (TalosBootstrapper) IsBootstrapped(ctx context.Context, addresses []string, talosconfigData []byte) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	tc, err := talosconfig.FromBytes(talosconfigData)
	if err != nil {
		return false, fmt.Errorf("failed to parse talosconfig: %w", err)
	}

	c, err := talosclient.New(ctx, talosclient.WithEndpoints(addresses...), talosclient.WithConfig(tc))
	if err != nil {
		return false, fmt.Errorf("failed to create Talos client: %w", err)
	}
	defer c.Close() //nolint:errcheck

	list, err := c.LS(talosclient.WithNodes(ctx, addresses...), &machineapi.ListRequest{Root: "/var/lib/etcd/member"})
	if err != nil {
		return false, fmt.Errorf("failed to list etcd directory: %w", err)
	}

	for {
		info, err := list.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || talosclient.StatusCode(err) == codes.Canceled {
				break
			}
			return false, err
		}
		if info.Metadata.Error == "" {
			return true, nil
		}
	}

	return false, nil
}

// TriggerBootstrap reads the talosconfig Secret and calls Bootstrap on the
// target Machine's addresses. It is a no-op if the cluster is already bootstrapped.
func TriggerBootstrap(ctx context.Context, c client.Client, bootstrapper Bootstrapper, cluster *fleetv1alpha1.Cluster, m *fleetv1alpha1.Machine) error {
	logger := log.FromContext(ctx)

	if len(m.Status.Addresses) == 0 {
		return &BMHNotReadyError{State: "no-addresses", Message: "Machine has no addresses for bootstrap"}
	}

	tcSecret := &corev1.Secret{}
	tcKey := client.ObjectKey{Namespace: SecretsNamespace, Name: cluster.Name + TalosconfigSuffix}

	if err := c.Get(ctx, tcKey, tcSecret); err != nil {
		return fmt.Errorf("failed to fetch talosconfig Secret for bootstrap: %w", err)
	}

	tcData, ok := tcSecret.Data["talosconfig"]
	if !ok {
		return fmt.Errorf("talosconfig Secret missing 'talosconfig' key")
	}

	addresses := make([]string, 0, len(m.Status.Addresses))
	for _, addr := range m.Status.Addresses {
		addresses = append(addresses, addr.Address)
	}

	bootstrapped, err := bootstrapper.IsBootstrapped(ctx, addresses, tcData)
	if err != nil {
		logger.V(1).Info("Failed to check bootstrap status, will attempt bootstrap", "error", err)
	}

	if bootstrapped {
		logger.Info("Cluster already bootstrapped, skipping")
		return nil
	}

	logger.Info("Triggering Talos bootstrap", "node", addresses[0])

	return bootstrapper.Bootstrap(ctx, addresses, tcData)
}
