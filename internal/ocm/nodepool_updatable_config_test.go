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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"github.com/Azure/ARO-HCP/internal/api"
)

func TestNodePoolUpdatableConfigHash(t *testing.T) {
	base := &api.HCPOpenShiftClusterNodePool{
		Properties: api.HCPOpenShiftClusterNodePoolProperties{
			Replicas: 3,
			Labels:   map[string]string{"role": "worker"},
		},
	}

	hash1, err := NodePoolUpdatableConfigHash(base)
	require.NoError(t, err)
	require.NotEmpty(t, hash1)

	hash2, err := NodePoolUpdatableConfigHash(base)
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2)

	withTaintOrder := &api.HCPOpenShiftClusterNodePool{
		Properties: api.HCPOpenShiftClusterNodePoolProperties{
			Replicas: 1,
			Taints: []api.Taint{
				{Key: "b", Value: "v2", Effect: api.EffectNoSchedule},
				{Key: "a", Value: "v1", Effect: api.EffectNoExecute},
			},
		},
	}
	reordered := &api.HCPOpenShiftClusterNodePool{
		Properties: api.HCPOpenShiftClusterNodePoolProperties{
			Replicas: 1,
			Taints: []api.Taint{
				{Key: "a", Value: "v1", Effect: api.EffectNoExecute},
				{Key: "b", Value: "v2", Effect: api.EffectNoSchedule},
			},
		},
	}

	hashA, err := NodePoolUpdatableConfigHash(withTaintOrder)
	require.NoError(t, err)
	hashB, err := NodePoolUpdatableConfigHash(reordered)
	require.NoError(t, err)
	assert.Equal(t, hashA, hashB)

	autoscaling := &api.HCPOpenShiftClusterNodePool{
		Properties: api.HCPOpenShiftClusterNodePoolProperties{
			Replicas:    99,
			AutoScaling: &api.NodePoolAutoScaling{Min: 2, Max: 5},
		},
	}
	autoscalingHash, err := NodePoolUpdatableConfigHash(autoscaling)
	require.NoError(t, err)

	fixedReplicas := &api.HCPOpenShiftClusterNodePool{
		Properties: api.HCPOpenShiftClusterNodePoolProperties{
			Replicas: 99,
		},
	}
	fixedReplicasHash, err := NodePoolUpdatableConfigHash(fixedReplicas)
	require.NoError(t, err)
	assert.NotEqual(t, autoscalingHash, fixedReplicasHash)

	withDrainTimeout := &api.HCPOpenShiftClusterNodePool{
		Properties: api.HCPOpenShiftClusterNodePoolProperties{
			Replicas:                1,
			NodeDrainTimeoutMinutes: ptr.To(int32(30)),
		},
	}
	drainHash, err := NodePoolUpdatableConfigHash(withDrainTimeout)
	require.NoError(t, err)
	assert.NotEqual(t, fixedReplicasHash, drainHash)
}
