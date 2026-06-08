// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ocm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	arohcpv1alpha1 "github.com/openshift-online/ocm-sdk-go/arohcp/v1alpha1"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/utils"
)

// nodePoolUpdatableConfig is the canonical representation of node pool properties
// hashed by NodePoolUpdatableConfigHash and applied to Cluster Service by
// applyNodePoolUpdatableConfig (via BuildCSNodePool). Add or remove fields here
// and update NodePoolUpdatableConfigFromProperties plus applyNodePoolUpdatableConfig
// in the same change.
//
// The digest is stored on the node pool as ClusterServiceUpdatableConfigHash and
// compared by the update dispatch controller: a mismatch triggers a CS PATCH and
// hash replacement. It is also stamped when a node pool create operation succeeds.
//
// Changing this struct has deploy-time effects:
//   - Adding or removing a field changes the digest for every node pool, causing a
//     one-time hash mismatch and CS PATCH for each installed pool on rollout unless
//     hashes are migrated separately.
//   - Reordering struct fields or json tags changes the marshaled JSON and therefore
//     the digest; treat field order as part of the stable contract.
//
// A field may be included here before customers can update it via the ARM API, as
// long as Cosmos only changes through create (or other controlled paths) until the
// API is opened. Do not include a field in the hash while omitting it from
// applyNodePoolUpdatableConfig if Cosmos can change independently.
//
// Note: This does not necessarily include all the fields that can be updated via the CS API, just the ones
// that are considered during an ARM NodePool update call and processed by the CS NodePool update controller.
type nodePoolUpdatableConfig struct {
	Labels                  map[string]string        `json:"labels,omitempty"`
	AutoScaling             *api.NodePoolAutoScaling `json:"autoScaling,omitempty"`
	Replicas                *int32                   `json:"replicas,omitempty"`
	Taints                  []api.Taint              `json:"taints,omitempty"`
	NodeDrainTimeoutMinutes *int32                   `json:"nodeDrainTimeoutMinutes,omitempty"`
}

// NodePoolUpdatableConfigFromProperties extracts the canonical updatable node pool
// configuration from internal API properties.
func NodePoolUpdatableConfigFromProperties(properties api.HCPOpenShiftClusterNodePoolProperties) *nodePoolUpdatableConfig {
	config := &nodePoolUpdatableConfig{
		// TODO should we sort the labels and taints? If we do, that might change the hash for existing node pools
		Labels:                  properties.Labels,
		Taints:                  properties.Taints,
		NodeDrainTimeoutMinutes: properties.NodeDrainTimeoutMinutes,
	}

	if properties.AutoScaling != nil {
		autoScaling := *properties.AutoScaling
		config.AutoScaling = &autoScaling
	} else {
		replicas := properties.Replicas
		config.Replicas = &replicas
	}

	return config
}

// NodePoolUpdatableConfigHash returns a stable SHA-256 hex digest of
// nodePoolUpdatableConfig built from the node pool properties. See that type for
// change implications.
func NodePoolUpdatableConfigHash(nodePool *api.HCPOpenShiftClusterNodePool) (string, error) {
	config := NodePoolUpdatableConfigFromProperties(nodePool.Properties)

	raw, err := json.Marshal(config)
	if err != nil {
		return "", utils.TrackError(fmt.Errorf("failed to marshal node pool updatable config: %w", err))
	}

	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func applyNodePoolUpdatableConfig(nodePoolBuilder *arohcpv1alpha1.NodePoolBuilder, config *nodePoolUpdatableConfig) {
	nodePoolBuilder.Labels(config.Labels)

	if config.AutoScaling != nil {
		nodePoolBuilder.Autoscaling(arohcpv1alpha1.NewNodePoolAutoscaling().
			MinReplica(int(config.AutoScaling.Min)).
			MaxReplica(int(config.AutoScaling.Max)))
	} else if config.Replicas != nil {
		nodePoolBuilder.Replicas(int(*config.Replicas))
	}

	if config.Taints != nil {
		taintBuilders := make([]*arohcpv1alpha1.TaintBuilder, 0, len(config.Taints))
		for _, t := range config.Taints {
			taintBuilders = append(taintBuilders, arohcpv1alpha1.NewTaint().
				Effect(string(t.Effect)).
				Key(t.Key).
				Value(t.Value))
		}
		nodePoolBuilder.Taints(taintBuilders...)
	}

	if config.NodeDrainTimeoutMinutes != nil {
		nodePoolBuilder.NodeDrainGracePeriod(arohcpv1alpha1.NewValue().
			Unit(csNodeDrainGracePeriodUnit).
			Value(float64(*config.NodeDrainTimeoutMinutes)))
	}
}
