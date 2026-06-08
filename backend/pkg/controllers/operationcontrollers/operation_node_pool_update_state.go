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

package operationcontrollers

import (
	"context"
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/openshift/hypershift/api/hypershift/v1beta1"

	"github.com/Azure/ARO-HCP/backend/pkg/maestrohelpers"
	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/arm"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
	"github.com/Azure/ARO-HCP/internal/utils"
)

func (c *operationNodePoolUpdate) cosmosOperationState(
	ctx context.Context,
	operation *api.Operation,
) (*operationState, error) {
	if operation.ExternalID == nil || operation.ExternalID.Parent == nil {
		return nil, utils.TrackError(fmt.Errorf("operation external ID has no parent cluster"))
	}

	clusterName := operation.ExternalID.Parent.Name
	nodePoolName := operation.ExternalID.Name

	nodePool, err := c.resourcesDBClient.HCPClusters(
		operation.ExternalID.SubscriptionID,
		operation.ExternalID.ResourceGroupName,
	).NodePools(clusterName).Get(ctx, nodePoolName)
	if database.IsNotFoundError(err) {
		return newOperationState(arm.ProvisioningStateUpdating, "node pool not found in cosmos"), nil
	}
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to get node pool from cosmos: %w", err))
	}

	csID := nodePool.ServiceProviderProperties.ClusterServiceID
	if csID == nil || len(csID.String()) == 0 {
		return newOperationState(arm.ProvisioningStateUpdating, "node pool clusterServiceID is empty"), nil
	}

	return newOperationState(arm.ProvisioningStateSucceeded, ""), nil
}

func (c *operationNodePoolUpdate) managementClusterOperationState(
	ctx context.Context,
	operation *api.Operation,
) (*operationState, error) {
	logger := utils.LoggerFromContext(ctx)

	if operation.ExternalID == nil || operation.ExternalID.Parent == nil {
		return nil, utils.TrackError(fmt.Errorf("operation external ID has no parent cluster"))
	}

	clusterName := operation.ExternalID.Parent.Name
	nodePoolName := operation.ExternalID.Name

	nodePool, err := c.resourcesDBClient.HCPClusters(operation.ExternalID.SubscriptionID, operation.ExternalID.ResourceGroupName).NodePools(clusterName).Get(ctx, nodePoolName)
	if err != nil {
		return nil, utils.TrackError(fmt.Errorf("failed to get node pool from cosmos: %w", err))
	}

	readDesire, err := c.readDesireLister.GetForNodePool(
		ctx,
		operation.ExternalID.SubscriptionID,
		operation.ExternalID.ResourceGroupName,
		clusterName,
		nodePoolName,
		maestrohelpers.ReadDesireNameReadonlyNodePool,
	)
	if database.IsNotFoundError(err) {
		return newOperationState(arm.ProvisioningStateUpdating, "ReadDesire for NodePool has not been created yet"), nil
	}
	if err != nil {
		return nil, utils.TrackError(err)
	}
	if !meta.IsStatusConditionTrue(readDesire.Status.Conditions, kubeapplier.ConditionTypeSuccessful) {
		message := "ReadDesire has not yet successfully observed the NodePool"
		if successfulCondition := meta.FindStatusCondition(readDesire.Status.Conditions, kubeapplier.ConditionTypeSuccessful); successfulCondition != nil {
			message = fmt.Sprintf("ReadDesire is not successful: %s: %s", successfulCondition.Reason, successfulCondition.Message)
		}
		logger.Info("ReadDesire is not successful", "readDesire.Status.Conditions", readDesire.Status.Conditions)
		return newOperationState(arm.ProvisioningStateUpdating, message), nil
	}

	observedNodePool, err := maestrohelpers.NodePoolFromReadDesire(readDesire)
	if err != nil {
		return nil, utils.TrackError(err)
	}
	if observedNodePool == nil {
		return newOperationState(arm.ProvisioningStateUpdating, "ReadDesire has no NodePool kube content"), nil
	}

	if c.conditionIsTrue(observedNodePool.Status.Conditions, v1beta1.NodePoolUpdatingConfigConditionType) {
		message := "management cluster NodePool config update in progress"
		if updatingConfig := c.findCondition(observedNodePool.Status.Conditions, v1beta1.NodePoolUpdatingConfigConditionType); updatingConfig != nil {
			message = fmt.Sprintf("management cluster NodePool config update in progress: %s: %s", updatingConfig.Reason, updatingConfig.Message)
		}
		logger.Info("management cluster NodePool is updating config", "nodePool.Status.Conditions", observedNodePool.Status.Conditions)
		return newOperationState(arm.ProvisioningStateUpdating, message), nil
	}

	if matches, message := c.specMatchesDesired(nodePool.Properties, observedNodePool.Spec); !matches {
		logger.Info("management cluster NodePool spec does not match desired configuration", "message", message)
		return newOperationState(arm.ProvisioningStateUpdating, message), nil
	}

	if matches, message := c.statusMatchesDesired(nodePool.Properties, observedNodePool.Status); !matches {
		logger.Info("management cluster NodePool status does not match desired configuration", "message", message)
		return newOperationState(arm.ProvisioningStateUpdating, message), nil
	}

	return newOperationState(arm.ProvisioningStateSucceeded, ""), nil
}

// specMatchesDesired compares Cosmos desired properties against the Hypershift
// NodePool spec fields that BuildCSNodePool sends on update: scaling, labels,
// taints, and nodeDrainTimeoutMinutes.
func (c *operationNodePoolUpdate) specMatchesDesired(desired api.HCPOpenShiftClusterNodePoolProperties, observed v1beta1.NodePoolSpec) (bool, string) {
	if matches, message := c.scalingSpecMatchesDesired(desired, observed); !matches {
		return false, message
	}
	if matches, message := c.labelsSpecMatchesDesired(desired.Labels, observed.NodeLabels); !matches {
		return false, message
	}
	if matches, message := c.taintsSpecMatchesDesired(desired.Taints, observed.Taints); !matches {
		return false, message
	}
	if matches, message := c.nodeDrainTimeoutSpecMatchesDesired(desired.NodeDrainTimeoutMinutes, observed.NodeDrainTimeout); !matches {
		return false, message
	}
	return true, ""
}

func (c *operationNodePoolUpdate) scalingSpecMatchesDesired(desired api.HCPOpenShiftClusterNodePoolProperties, observed v1beta1.NodePoolSpec) (bool, string) {
	if desired.AutoScaling != nil {
		if observed.AutoScaling == nil {
			return false, "management cluster NodePool has no autoscaling configuration"
		}
		if observed.AutoScaling.Min == nil {
			return false, "management cluster NodePool autoscaling min is unset"
		}
		if *observed.AutoScaling.Min != desired.AutoScaling.Min {
			return false, fmt.Sprintf("management cluster NodePool autoscaling min is %d, want %d", *observed.AutoScaling.Min, desired.AutoScaling.Min)
		}
		if observed.AutoScaling.Max != desired.AutoScaling.Max {
			return false, fmt.Sprintf("management cluster NodePool autoscaling max is %d, want %d", observed.AutoScaling.Max, desired.AutoScaling.Max)
		}
		if observed.Replicas != nil {
			return false, "management cluster NodePool has replicas set while autoscaling is enabled"
		}
		return true, ""
	}

	if observed.AutoScaling != nil {
		return false, "management cluster NodePool still has autoscaling configuration"
	}

	observedReplicas := int32(0)
	if observed.Replicas != nil {
		observedReplicas = *observed.Replicas
	}
	if observedReplicas != desired.Replicas {
		return false, fmt.Sprintf("management cluster NodePool replicas is %d, want %d", observedReplicas, desired.Replicas)
	}
	return true, ""
}

func (c *operationNodePoolUpdate) labelsSpecMatchesDesired(desired map[string]string, observed map[string]string) (bool, string) {
	if len(desired) == 0 && len(observed) == 0 {
		return true, ""
	}
	if !maps.Equal(desired, observed) {
		return false, "management cluster NodePool nodeLabels do not match desired labels"
	}
	return true, ""
}

func (c *operationNodePoolUpdate) taintsSpecMatchesDesired(desired []api.Taint, observed []v1beta1.Taint) (bool, string) {
	if len(desired) == 0 && len(observed) == 0 {
		return true, ""
	}
	if len(desired) != len(observed) {
		return false, fmt.Sprintf("management cluster NodePool has %d taints, want %d", len(observed), len(desired))
	}

	for i := range desired {
		if c.apiTaintToHypershift(desired[i]) != observed[i] {
			return false, "management cluster NodePool taints do not match desired taints"
		}
	}
	return true, ""
}

func (c *operationNodePoolUpdate) nodeDrainTimeoutSpecMatchesDesired(desiredMinutes *int32, observed *metav1.Duration) (bool, string) {
	if desiredMinutes == nil {
		return true, ""
	}

	want := time.Duration(*desiredMinutes) * time.Minute
	if observed == nil {
		return false, fmt.Sprintf("management cluster NodePool nodeDrainTimeout is unset, want %s", want)
	}
	if observed.Duration != want {
		return false, fmt.Sprintf("management cluster NodePool nodeDrainTimeout is %s, want %s", observed.Duration, want)
	}
	return true, ""
}

func (c *operationNodePoolUpdate) apiTaintToHypershift(t api.Taint) v1beta1.Taint {
	return v1beta1.Taint{
		Key:    t.Key,
		Value:  t.Value,
		Effect: corev1.TaintEffect(t.Effect),
	}
}

// statusMatchesDesired compares replica counts, autoscaling readiness, and
// rollout readiness. Hypershift does not surface labels or taints in NodePool
// status; those are spec-only. nodeDrainTimeout rollout is reflected via
// UpdatingConfig, which the caller checks before invoking this function.
func (c *operationNodePoolUpdate) statusMatchesDesired(desired api.HCPOpenShiftClusterNodePoolProperties, observed v1beta1.NodePoolStatus) (bool, string) {
	if desired.AutoScaling != nil {
		replicas := observed.Replicas
		if replicas < desired.AutoScaling.Min {
			return false, fmt.Sprintf(
				"management cluster NodePool status replicas is %d, want at least %d",
				replicas, desired.AutoScaling.Min,
			)
		}
		if replicas > desired.AutoScaling.Max {
			return false, fmt.Sprintf(
				"management cluster NodePool status replicas is %d, want at most %d",
				replicas, desired.AutoScaling.Max,
			)
		}
		if !c.conditionIsTrue(observed.Conditions, v1beta1.NodePoolAutoscalingEnabledConditionType) {
			message := "management cluster NodePool autoscaling is not enabled"
			if autoscalingCondition := c.findCondition(observed.Conditions, v1beta1.NodePoolAutoscalingEnabledConditionType); autoscalingCondition != nil {
				message = fmt.Sprintf("management cluster NodePool autoscaling is not enabled: %s: %s", autoscalingCondition.Reason, autoscalingCondition.Message)
			}
			return false, message
		}
	} else if observed.Replicas != desired.Replicas {
		return false, fmt.Sprintf(
			"management cluster NodePool status replicas is %d, want %d",
			observed.Replicas, desired.Replicas,
		)
	}

	if !c.conditionIsTrue(observed.Conditions, v1beta1.NodePoolReadyConditionType) {
		message := "management cluster NodePool is not ready"
		if readyCondition := c.findCondition(observed.Conditions, v1beta1.NodePoolReadyConditionType); readyCondition != nil {
			message = fmt.Sprintf("management cluster NodePool is not ready: %s: %s", readyCondition.Reason, readyCondition.Message)
		}
		return false, message
	}

	return true, ""
}

func (c *operationNodePoolUpdate) conditionIsTrue(conditions []v1beta1.NodePoolCondition, conditionType string) bool {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return conditions[i].Status == corev1.ConditionTrue
		}
	}
	return false
}

func (c *operationNodePoolUpdate) findCondition(conditions []v1beta1.NodePoolCondition, conditionType string) *v1beta1.NodePoolCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}
