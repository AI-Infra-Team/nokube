# NoKube

Kubernetes YAML-driven development environment

## Overview

NoKube is a continuous delivery system based on Kubernetes YAML, supporting:

- **Single CD source**: Kubernetes YAML
- **Internal network development/debugging** (no K8s permissions): Uses Ray + DIND to simulate container execution, automatically converting YAML to Ray tasks
- **Multi-cluster management**: Unified management of Ray and Kubernetes clusters
- **Real-time monitoring**: Complete monitoring dashboard and alerting system
- **Intelligent deployment**: Support for multiple deployment types and auto-scaling

## 🚀 New Features

### Enhanced Cluster Management
- **Cluster scaling**: `nokube cluster scale <name> --workers <count>`
- **Cluster restart**: `nokube cluster restart <name>`
- **Health check**: `nokube cluster health <name>`
- **Configuration management**: `nokube cluster config <name> --format yaml`
- **Configuration import**: `nokube cluster import-config <name> <config_file>`

### Enhanced Deployment Management
- **Deployment scaling**: `nokube deploy scale <deployment> <replicas>`
- **Deployment rollback**: `nokube deploy rollback <deployment> <version>`
- **Health check**: `nokube deploy health <deployment>`
- **Log viewing**: `nokube deploy logs <deployment> --lines <count>`
- **Multiple deployment types**: Ray Job, Service, Workflow, Dataset

### Monitoring System
- **Real-time dashboard**: `nokube monitor dashboard`
- **System monitoring**: `nokube monitor watch --interval <seconds>`
- **Health check**: `nokube monitor health`
- **Metrics export**: `nokube monitor export-metrics <file>`

## Directory Structure

```
nokube/
├── README.md                    # Project documentation
├── LICENSE                      # MIT License
├── pyproject.toml              # Project configuration (uv)
├── MANIFEST.in                 # Package file manifest
├── src/                        # Source code directory
│   ├── cli.py                  # Command-line interface
│   ├── deployer.py             # Deployer
│   ├── etcd_manager.py         # etcd cluster manager
│   ├── yaml_converter.py       # YAML converter
│   ├── ray_cluster_manager.py  # Ray cluster manager
│   ├── cluster_manager.py      # Cluster manager base class
│   └── monitor.py              # Monitoring system
├── templates/                   # Configuration templates
│   ├── ray-cluster.yaml        # Ray cluster template
│   └── k8s-cluster.yaml        # Kubernetes cluster template
├── examples/                    # Examples directory
│   ├── cluster-status/          # Cluster status monitoring example
│   ├── git-watcher/            # Git repository monitoring example
│   └── monitoring-demo/        # Monitoring demo application
└── yaml/                       # YAML examples
    └── example-app.yaml
```

## Quick Start

### Global NoKube Installation

#### Method 1: Global installation using uv (recommended)

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Global installation of nokube
sudo uv pip install -e . --system

# Verify installation
nokube --help
```

#### Method 2: Global installation using pip

```bash
# Global installation of nokube
pip install -e .

# Verify installation
nokube --help
```

#### Method 3: Install from source to system Python

```bash
# Clone repository
git clone https://github.com/nokube/nokube.git
cd nokube

# Install to system Python
sudo pip install -e .

# Verify installation
nokube --help
```

### Development Environment Setup

If you want to use in development environment:

```bash
# Create virtual environment using uv
uv sync

# Activate virtual environment
source .venv/bin/activate

# Install development dependencies
uv sync --extra dev

# Verify installation
nokube --help
```

## Configuration Management

NoKube uses global configuration files to manage etcd connections and other settings. **NoKube focuses on etcd as the sole storage backend.**

### Configuration File Locations

NoKube searches for configuration files in the following order:

1. `~/.nokube/config.yaml` (user configuration, recommended)
2. `./.nokube/config.yaml` (project configuration)

### Configuration File Example

```yaml
# etcd cluster configuration (required)
etcd:
  # etcd server addresses, supports multiple addresses separated by commas
  # [Required]
  # Default: localhost:2379
  hosts: "localhost:2379"
  # etcd username (optional)
  username: "nokube_user"
  # etcd password (optional)
  password: "nokube_password"

# Ray cluster default configuration
ray:
  # Ray dashboard default port
  default_dashboard_port: 8265
  # Ray cluster default maximum worker nodes
  default_max_workers: 10

# Kubernetes cluster configuration
kubernetes:
  # Kubernetes configuration file path
  config_file: "~/.kube/config"
  # Kubernetes default namespace
  default_namespace: "default"

# Logging configuration
logging:
  # Log level (DEBUG, INFO, WARNING, ERROR)
  level: "INFO"
  # Log format
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

### etcd Configuration

NoKube requires etcd as a storage backend. If you don't have etcd service:

1. **Start etcd container**:
   ```bash
   docker run -d --name etcd -p 2379:2379 \
     quay.io/coreos/etcd:latest \
     etcd --advertise-client-urls http://0.0.0.0:2379 \
     --listen-client-urls http://0.0.0.0:2379
   ```

2. **Create configuration file**:
   ```bash
   mkdir -p ~/.nokube
   # Then edit ~/.nokube/config.yaml file, refer to the example above
   ```

3. **Use authenticated etcd**:
   Add `username` and `password` fields in the configuration file.

## Command Line Interface

### Basic Commands

```bash
# View help
nokube --help

# View information
nokube info
```

### Ray Cluster Management

```bash
# Start Ray cluster (using configuration file)
nokube ray start --config ray-config.yaml

# Stop Ray cluster
nokube ray stop --config ray-config.yaml

# View Ray cluster status
nokube ray status --config ray-config.yaml
```

### Deployment Management

```bash
# Deploy application
nokube deploy apply <yaml_file> --target ray

# Scale deployment
nokube deploy scale <deployment> <replicas> --target ray

# Rollback deployment
nokube deploy rollback <deployment> <version> --target ray

# Check deployment health
nokube deploy health <deployment> --target ray

# View deployment logs
nokube deploy logs <deployment> --target ray --lines 100

# View deployment status
nokube deploy status --target ray

# Delete deployment
nokube deploy delete <deployment_name> --target ray
```

### Cluster Management

```bash
# List all clusters
nokube cluster list

# Create or update cluster (requires configuration file)
nokube cluster new-or-update <name> <config_file>

# View cluster details
nokube cluster get <name>

# Check cluster health
nokube cluster health <name>

# Scale cluster
nokube cluster scale <name> --workers <count>

# Restart cluster
nokube cluster restart <name>

# View cluster configuration
nokube cluster config <name> --format yaml

# Import cluster configuration
nokube cluster import-config <name> <config_file>

# Set cluster status
nokube cluster status <name> <status>

# Export cluster configuration
nokube cluster export <yaml_file>

# Delete cluster
nokube cluster delete <name>
```

### Monitoring Management

```bash
# Start monitoring dashboard
nokube monitor dashboard

# Real-time system monitoring
nokube monitor watch --interval 5

# System health check
nokube monitor health

# Export system metrics
nokube monitor export-metrics metrics.json
```

## Cluster Management

NoKube uses etcd to store cluster configuration information, supporting the following cluster types:

### Supported Cluster Types

- **Ray**: Distributed computing cluster (supports remote deployment)
- **Kubernetes**: Production K8s cluster

### Cluster Configuration Examples

#### Ray Cluster Configuration

```yaml
# ray-config.yaml
ray_spec:
  dashboard_port: 8265
  max_workers: 10

# Node list
nodes:
  # Head node
  - ssh_url: "prod-server-1:22"
    name: "ray-head-prod"
    role: "head"
    ray_config:
      port: 10001
      dashboard_port: 8265
      num_cpus: 8
      object_store_memory: 2000000000
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/head"
    users:
      - userid: "prod-admin"
        password: "prod123"
  
  # Worker node
  - ssh_url: "prod-server-2:22"
    name: "ray-worker-1"
    role: "worker"
    ray_config:
      num_cpus: 16
      object_store_memory: 4000000000
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/worker-1"
    users:
      - userid: "worker-user"
        password: "worker123"
```

#### Remote Deployment Features

- **SSH connection**: Connect to remote nodes via SSH
- **Automatic deployment**: Automatically upload remote execution libraries and start Ray processes
- **Multi-node support**: Support for head node and multiple worker nodes
- **Status monitoring**: Real-time monitoring of node status
- **Configuration management**: Unified configuration file management

#### Usage Steps

1. **Prepare remote nodes**:
   ```bash
   # Install Ray on remote nodes
   pip install ray[serve]
   ```

2. **Configure SSH connection**:
   ```bash
   # Configure SSH key authentication
   ssh-copy-id user@remote-server
   ```

3. **Create cluster configuration**:
   ```yaml
   # Edit ray-config.yaml
   nodes:
     - ssh_url: "remote-server:22"
       name: "ray-head"
       role: "head"
       ray_config:
         port: 10001
         dashboard_port: 8265
   ```

4. **Start cluster**:
   ```bash
   nokube ray start --config ray-config.yaml
   ```

5. **View status**:
   ```bash
   nokube ray status --config ray-config.yaml
   ```

## Monitoring System

### Real-time Monitoring Dashboard

Start complete monitoring dashboard including:

- **System metrics**: CPU, memory, disk usage
- **Cluster status**: Ray cluster running status
- **Deployment status**: Application deployment health status
- **Alert information**: Real-time alerts and notifications

```bash
nokube monitor dashboard
```

### System Health Check

Quick system health check:

```bash
nokube monitor health
```

### Real-time Monitoring

Simple real-time monitoring mode:

```bash
nokube monitor watch --interval 10
```

## Deployment Types

### Ray Job
For batch processing tasks, supports scheduled execution and resource limits.

```yaml
- name: "data-processing-job"
  type: "ray_job"
  entrypoint: "python data_processor.py"
  runtime_env:
    pip: ["pandas", "numpy"]
  schedule: "0 2 * * *"  # Execute daily at 2 AM
```

### Ray Service
For online services, supports load balancing and health checks.

```yaml
- name: "ml-inference-service"
  type: "ray_service"
  import_path: "ml_service:MLInferenceService"
  replicas: 3
  health_check:
    enabled: true
    endpoint: "/health"
```

### Ray Workflow
For complex workflows, supports state management and fault tolerance.

```yaml
- name: "ml-training-workflow"
  type: "ray_workflow"
  workflow_id: "ml-training-pipeline"
  entrypoint: "python ml_training_workflow.py"
```

### Ray Dataset
For data processing pipelines, supports large-scale data transformation.

```yaml
- name: "data-pipeline"
  type: "ray_dataset"
  data_source: "s3://nokube-demo/data/"
  format: "parquet"
```

## Example Applications

### Monitoring Demo

Complete monitoring and deployment demo application:

```bash
# Enter demo directory
cd examples/monitoring-demo

# Deploy demo application
nokube deploy apply app.yaml --target ray

# Start monitoring dashboard
nokube monitor dashboard
```

### Git Repository Monitoring

Automatically monitor Git repository changes and deploy:

```bash
# Enter Git monitoring example
cd examples/git-watcher

# Start monitoring
python start_watcher.py
```

## Best Practices

### 1. Cluster Design
- Choose appropriate cluster types based on workloads
- Properly configure resource limits and requests
- Use node affinity and anti-affinity to optimize scheduling

### 2. Monitoring Strategy
- Set reasonable alert thresholds
- Configure multiple notification methods
- Regularly check and optimize monitoring rules

### 3. Deployment Management
- Use rolling updates to avoid service interruption
- Configure health checks and automatic recovery
- Implement blue-green deployment or canary releases

### 4. Security Considerations
- Enable RBAC and network policies
- Use secret management to store sensitive information
- Regularly audit and update security configurations

## Troubleshooting

### Common Issues

1. **Cluster startup failure**
   ```bash
   # Check cluster status
   nokube cluster health ray-demo
   
   # View cluster logs
   nokube cluster logs ray-demo
   ```

2. **Deployment failure**
   ```bash
   # Check deployment status
   nokube deploy status --target ray
   
   # View deployment logs
   nokube deploy logs deployment-name --target ray
   ```

3. **Monitoring data anomalies**
   ```bash
   # Check monitoring system
   nokube monitor health
   
   # Export metrics for analysis
   nokube monitor export-metrics debug.json
   ```

### Debugging Tips

- Use `--verbose` parameter for detailed logs
- Check system resource usage
- Verify network connections and firewall settings
- Check if cluster and deployment configurations are correct

## Contributing Guidelines

Contributions of code and documentation are welcome!

1. Fork the project
2. Create feature branch
3. Submit changes
4. Create Pull Request

## License

MIT License - see [LICENSE](LICENSE) file for details. 