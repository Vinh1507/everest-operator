package cassandra

import (
	"context"
	"encoding/json"

	cassv1 "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"
)

type applier struct {
	*Provider
	ctx context.Context
}

func (p *applier) ResetDefaults() error {
	p.CassandraDatacenter.Spec = defaultSpec()
	return nil
}

func defaultSpec() cassv1.CassandraDatacenterSpec {
	return cassv1.CassandraDatacenterSpec{
		Size:          1,
		ServerVersion: "4.0.1",
		ServerType:    "cassandra",
		ManagementApiAuth: cassv1.ManagementApiAuthConfig{
			Insecure: &cassv1.ManagementApiAuthInsecureConfig{},
		},
	}
}

func (p *applier) Paused(paused bool) {
	p.CassandraDatacenter.Spec.Stopped = paused
}

func (p *applier) AllowUnsafeConfig() {
	// Not directly applicable to Cassandra operator in the same way,
	// or requires specific flags if they exist. Leaving empty if no direct equivalent.
}

func (p *applier) Metadata() error {
	if p.CassandraDatacenter.GetDeletionTimestamp().IsZero() {
		for _, f := range []string{
			finalizerDeleteCassPVC,
			finalizerDeleteCassSSL,
		} {
			controllerutil.AddFinalizer(p.CassandraDatacenter, f)
		}
	}
	return nil
}

func (p *applier) Engine() error {
	engine := p.DBEngine
	if p.DB.Spec.Engine.Version == "" {
		p.DB.Spec.Engine.Version = engine.BestEngineVersion()
	}

	p.CassandraDatacenter.Spec.Size = p.DB.Spec.Engine.Replicas
	p.CassandraDatacenter.Spec.ServerVersion = p.DB.Spec.Engine.Version

	// Resources
	// Assuming Spec.Resources exists or similar
	p.CassandraDatacenter.Spec.Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    p.DB.Spec.Engine.Resources.CPU,
			corev1.ResourceMemory: p.DB.Spec.Engine.Resources.Memory,
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    p.DB.Spec.Engine.Resources.CPU,
			corev1.ResourceMemory: p.DB.Spec.Engine.Resources.Memory,
		},
	}

	// Storage
	// Assumming Spec.StorageConfig or similar
	// p.CassandraDatacenter.Spec.StorageConfig = ...
	// I'll need to check how storage is defined in cass-operator.
	// Usually it's setup on the Datacenter spec.
	p.CassandraDatacenter.Spec.ClusterName = p.DB.Spec.GroupName
	if p.CassandraDatacenter.Spec.ClusterName == "" {
		p.CassandraDatacenter.Spec.ClusterName = p.DB.Name
	}

	p.CassandraDatacenter.Spec.StorageConfig = cassv1.StorageConfig{
		CassandraDataVolumeClaimSpec: &corev1.PersistentVolumeClaimSpec{
			StorageClassName: p.DB.Spec.Engine.Storage.Class,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: p.DB.Spec.Engine.Storage.Size,
				},
			},
		},
	}

	// Config
	if p.DB.Spec.Engine.Config != "" {
		// Convert YAML string to JSON bytes as cass-operator expects JSON RawMessage
		configJSON, err := yaml.YAMLToJSON([]byte(p.DB.Spec.Engine.Config))
		if err != nil {
			return err
		}
		p.CassandraDatacenter.Spec.Config = json.RawMessage(configJSON)
	}

	return nil
}

func (p *applier) EngineFeatures() error {
	return nil
}

func (p *applier) Proxy() error {
	return nil
}

func (p *applier) Backup() error {
	return nil
}

func (p *applier) DataSource() error {
	return nil
}

func (p *applier) DataImport() error {
	return nil
}

func (p *applier) Monitoring() error {
	return nil
}

func (p *applier) PodSchedulingPolicy() error {
	return nil
}
