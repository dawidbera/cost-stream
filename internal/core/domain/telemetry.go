// Package domain contains pure business domain entities, value objects, and domain errors.
package domain

import (
	"time"
)

// TelemetryRecord represents a single telemetry event capturing resource utilization and estimated cost.
type TelemetryRecord struct {
	// EventID uniquely identifies this telemetry record across all clusters.
	EventID string
	// TenantID specifies the organizational tenant owning the resource.
	TenantID string
	// ClusterID specifies the Kubernetes or compute cluster name.
	ClusterID string
	// Namespace indicates the workload's Kubernetes namespace.
	Namespace string
	// WorkloadName specifies the deployment, statefulset, or pod name.
	WorkloadName string
	// CostCenter represents the billing department or cost allocation tag.
	CostCenter string
	// CPUCoresUsed is the measured CPU utilization in core units.
	CPUCoresUsed float64
	// MemoryBytesUsed is the measured RAM consumption in bytes.
	MemoryBytesUsed int64
	// EstimatedHourlyCostUSD is the computed run rate in US dollars per hour.
	EstimatedHourlyCostUSD float64
	// Timestamp denotes when the telemetry sample was captured at the edge.
	Timestamp time.Time
	// Labels contains arbitrary key-value metadata tags associated with the workload.
	Labels map[string]string
}

// Validate checks whether the telemetry record conforms to domain business constraints.
// It returns ErrInvalidRecord if required identifiers are empty or numeric measurements are negative.
func (r *TelemetryRecord) Validate() error {
	if r.EventID == "" || r.TenantID == "" || r.ClusterID == "" {
		return ErrInvalidRecord
	}
	if r.CPUCoresUsed < 0 || r.MemoryBytesUsed < 0 || r.EstimatedHourlyCostUSD < 0 {
		return ErrInvalidRecord
	}
	return nil
}

// CostSummary contains aggregated cost and resource utilization metrics for a given tenant and cost center.
type CostSummary struct {
	// TenantID specifies the organizational tenant identifier.
	TenantID string
	// CostCenter represents the billing department identifier.
	CostCenter string
	// TotalCostUSD is the cumulative estimated expenditure in US dollars over the queried time window.
	TotalCostUSD float64
	// TotalCPUHours represents the cumulative CPU consumption in core-hours.
	TotalCPUHours float64
	// TotalGBHours represents the cumulative RAM consumption in gigabyte-hours.
	TotalGBHours float64
	// TotalSamples is the count of distinct telemetry samples aggregated.
	TotalSamples int64
}
