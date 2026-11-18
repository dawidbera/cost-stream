package domain_test

import (
	"testing"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
)

// TestTelemetryRecord_Validate tests table-driven validation scenarios for domain.TelemetryRecord.
func TestTelemetryRecord_Validate(t *testing.T) {
	validRecord := domain.TelemetryRecord{
		EventID:                "evt-12345",
		TenantID:               "tenant-alpha",
		ClusterID:              "k8s-prod-eu1",
		Namespace:              "billing",
		WorkloadName:           "payment-service",
		CostCenter:             "finops-core",
		CPUCoresUsed:           1.5,
		MemoryBytesUsed:        1024 * 1024 * 512,
		EstimatedHourlyCostUSD: 0.125,
		Timestamp:              time.Now(),
		Labels:                 map[string]string{"env": "prod"},
	}

	tests := []struct {
		name    string
		modify  func(r *domain.TelemetryRecord)
		wantErr bool
	}{
		{
			name:    "valid telemetry record",
			modify:  func(r *domain.TelemetryRecord) {},
			wantErr: false,
		},
		{
			name: "missing EventID",
			modify: func(r *domain.TelemetryRecord) {
				r.EventID = ""
			},
			wantErr: true,
		},
		{
			name: "missing TenantID",
			modify: func(r *domain.TelemetryRecord) {
				r.TenantID = ""
			},
			wantErr: true,
		},
		{
			name: "missing ClusterID",
			modify: func(r *domain.TelemetryRecord) {
				r.ClusterID = ""
			},
			wantErr: true,
		},
		{
			name: "negative CPUCoresUsed",
			modify: func(r *domain.TelemetryRecord) {
				r.CPUCoresUsed = -0.5
			},
			wantErr: true,
		},
		{
			name: "negative MemoryBytesUsed",
			modify: func(r *domain.TelemetryRecord) {
				r.MemoryBytesUsed = -1
			},
			wantErr: true,
		},
		{
			name: "negative EstimatedHourlyCostUSD",
			modify: func(r *domain.TelemetryRecord) {
				r.EstimatedHourlyCostUSD = -0.01
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := validRecord
			tt.modify(&rec)
			err := rec.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
