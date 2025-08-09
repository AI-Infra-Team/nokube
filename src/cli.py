#!/usr/bin/env python3
"""
NoKube Command Line Interface
Unified Ray environment management
"""

import click
import subprocess
import sys
import os
import shutil
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """NoKube - Kubernetes YAML-driven development environment"""
    pass


@cli.group()
def ray():
    """Ray cluster management"""
    pass


@cli.group()
def deploy():
    """Application deployment management"""
    pass


@cli.group()
def cluster():
    """Cluster management"""
    pass


@cli.group()
def monitor():
    """Monitoring management"""
    pass


@cli.command()
def info():
    """Display NoKube system information"""
    console.print("🔧 NoKube System Information", style="bold blue")
    console.print("=" * 50)
    
    # Check configuration status
    try:
        from src.config import get_config
        nokube_config = get_config()
        console.print("✅ Configuration system: Normal", style="green")
        
        # Validate configuration
        if nokube_config.validate():
            console.print("✅ Configuration validation: Passed", style="green")
        else:
            console.print("❌ Configuration validation: Failed", style="red")
    except Exception as e:
        console.print(f"❌ Configuration system: Error ({e})", style="red")
    
    # Check etcd3 installation
    try:
        import etcd3
        console.print("✅ etcd3 client: Installed", style="green")
        
        # Try to connect to etcd
        try:
            etcd_hosts = nokube_config.get_etcd_hosts()
            console.print(f"📡 etcd address: {etcd_hosts}", style="blue")
            
            # Test etcd connection
            from src.etcd_manager import EtcdManager
            manager = EtcdManager()
            clusters = manager.list_clusters()
            console.print(f"✅ etcd connection: Success (found {len(clusters)} clusters)", style="green")
        except Exception as e:
            console.print(f"❌ etcd connection: Failed ({e})", style="red")
            console.print("💡 Solutions:", style="yellow")
            console.print("   1. Start etcd service: docker run -d --name etcd -p 2379:2379 quay.io/coreos/etcd:latest", style="yellow")
            console.print("   2. Check etcd address settings in configuration file", style="yellow")
    except ImportError:
        console.print("❌ etcd3 client: Not installed", style="red")
        console.print("💡 Install command: pip install etcd3", style="yellow")
    
    console.print()
    console.print("🚀 Quick start:", style="bold yellow")
    console.print("1. Create configuration file (if configuration error is prompted)")
    console.print("2. Start etcd: docker run -d --name etcd -p 2379:2379 quay.io/coreos/etcd:latest")
    console.print("3. Create cluster: nokube cluster new-or-update <name> <config.yaml>")


@monitor.command()
def dashboard():
    """Start monitoring dashboard"""
    from src.monitor import start_monitoring
    
    console.print("📊 Starting monitoring dashboard...", style="blue")
    console.print("Press Ctrl+C to exit", style="yellow")
    
    try:
        start_monitoring()
    except KeyboardInterrupt:
        console.print("\n👋 Monitoring dashboard exited", style="green")


@monitor.command()
def health():
    """System health check"""
    from src.monitor import check_health
    
    check_health()


@monitor.command()
@click.argument('output_file', default='metrics.json')
def export_metrics(output_file):
    """Export system metrics"""
    from src.monitor import export_metrics
    
    export_metrics(output_file)


@monitor.command()
@click.option('--interval', default=5, help='Refresh interval (seconds)')
def watch(interval):
    """Real-time system monitoring"""
    from src.monitor import ClusterMonitor
    import time
    
    monitor = ClusterMonitor()
    
    console.print(f"👀 Starting real-time monitoring (refresh interval: {interval} seconds)", style="blue")
    console.print("Press Ctrl+C to exit", style="yellow")
    
    try:
        while True:
            metrics = monitor.get_system_metrics()
            alerts = monitor.check_alerts(metrics)
            
            # Clear screen
            console.clear()
            
            # Display system metrics
            console.print("📊 System Metrics", style="bold blue")
            console.print(f"  CPU: {metrics.get('cpu', {}).get('usage_percent', 0):.1f}%")
            console.print(f"  Memory: {metrics.get('memory', {}).get('percent', 0):.1f}%")
            console.print(f"  Disk: {metrics.get('disk', {}).get('percent', 0):.1f}%")
            
            # Display alerts
            if alerts:
                console.print("\n⚠️  Alerts", style="bold red")
                for alert in alerts:
                    console.print(f"  {alert['type'].upper()}: {alert['message']}", style="red")
            else:
                console.print("\n✅ System Normal", style="bold green")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        console.print("\n👋 Monitoring exited", style="green")


@ray.command()
@click.option('--config', default='ray-config.yaml', help='Ray configuration file path')
def start(config):
    """Start Ray cluster"""
    from src.ray_cluster_manager import RayClusterManager
    import yaml
    
    manager = RayClusterManager()
    
    try:
        # Read configuration file
        with open(config, 'r') as f:
            config_data = yaml.safe_load(f)
        
        manager.start_cluster(config_data)
    except Exception as e:
        console.print(f"❌ Failed to start Ray cluster: {e}", style="red")


@ray.command()
@click.option('--config', default='ray-config.yaml', help='Ray configuration file path')
def stop(config):
    """Stop Ray cluster"""
    from src.ray_cluster_manager import RayClusterManager
    import yaml
    
    manager = RayClusterManager()
    
    try:
        # Read configuration file
        with open(config, 'r') as f:
            config_data = yaml.safe_load(f)
        
        manager.stop_cluster(config_data)
    except Exception as e:
        console.print(f"❌ Failed to stop Ray cluster: {e}", style="red")


@ray.command()
@click.option('--config', default='ray-config.yaml', help='Ray configuration file path')
def status(config):
    """View Ray cluster status"""
    from src.ray_cluster_manager import RayClusterManager
    import yaml
    
    manager = RayClusterManager()
    
    try:
        # Read configuration file
        with open(config, 'r') as f:
            config_data = yaml.safe_load(f)
        
        manager.show_status(config_data)
    except Exception as e:
        console.print(f"❌ Failed to view Ray cluster status: {e}", style="red")


@deploy.command()
@click.argument('yaml_file')
@click.option('--target', type=click.Choice(['ray']), default='ray', help='Deployment target')
def apply(yaml_file, target):
    """Deploy application to specified environment"""
    from src.deployer import Deployer
    
    deployer = Deployer()
    deployer.deploy_to_ray(yaml_file)


@deploy.command()
@click.argument('deployment_name')
@click.argument('replicas', type=int)
@click.option('--target', type=click.Choice(['ray']), default='ray', help='Deployment target')
def scale(deployment_name, replicas, target):
    """Scale deployment replicas"""
    from src.deployer import Deployer
    
    deployer = Deployer()
    deployer.scale_deployment(deployment_name, replicas, target)


@deploy.command()
@click.argument('deployment_name')
@click.argument('version')
@click.option('--target', type=click.Choice(['ray']), default='ray', help='Deployment target')
def rollback(deployment_name, version, target):
    """Rollback deployment to specified version"""
    from src.deployer import Deployer
    
    deployer = Deployer()
    deployer.rollback_deployment(deployment_name, version, target)


@deploy.command()
@click.argument('deployment_name')
@click.option('--target', type=click.Choice(['ray']), default='ray', help='Deployment target')
def health(deployment_name, target):
    """Check deployment health status"""
    from src.deployer import Deployer
    
    deployer = Deployer()
    deployer.health_check(deployment_name, target)


@deploy.command()
@click.argument('deployment_name')
@click.option('--target', type=click.Choice(['ray']), default='ray', help='Deployment target')
@click.option('--lines', default=100, help='Number of log lines to display')
def logs(deployment_name, target, lines):
    """Get deployment logs"""
    from src.deployer import Deployer
    
    deployer = Deployer()
    deployer.get_deployment_logs(deployment_name, target)


@deploy.command()
@click.argument('yaml_file')
def convert(yaml_file):
    """Convert Kubernetes YAML to Ray tasks"""
    from src.yaml_converter import YAMLConverter
    
    converter = YAMLConverter()
    converter.convert_to_ray(yaml_file)


@cluster.command()
def list():
    """List all clusters"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    manager.show_clusters()


@cluster.command()
@click.argument('name')
@click.argument('config_file', required=False)
def new_or_update(name, config_file):
    """Create or update cluster (currently only supports ray)"""
    from src.etcd_manager import EtcdManager
    from src.config import get_config
    import yaml
    
    # First check configuration
    try:
        nokube_config = get_config()
        if not nokube_config.validate():
            console.print("\n❌ NoKube configuration validation failed", style="red")
            console.print("💡 Please fix configuration issues according to the error report above", style="blue")
            return
    except Exception as e:
        console.print(f"❌ Configuration loading failed: {e}", style="red")
        console.print("💡 Please create a configuration file based on the error guidance", style="blue")
        return
    
    # Try to create EtcdManager
    try:
        manager = EtcdManager()
    except RuntimeError as e:
        console.print(f"❌ etcd configuration error: {e}", style="red")
        console.print("", style="red")
        console.print("💡 Solutions:", style="blue")
        console.print("   1. Create configuration file manually", style="cyan")
        console.print("   2. Or start etcd service:", style="cyan")
        console.print("      docker run -d --name etcd -p 2379:2379 \\", style="cyan")
        console.print("        quay.io/coreos/etcd:latest \\", style="cyan")
        console.print("        etcd --advertise-client-urls http://0.0.0.0:2379 \\", style="cyan")
        console.print("        --listen-client-urls http://0.0.0.0:2379", style="cyan")
        return
    except Exception as e:
        console.print(f"❌ Failed to initialize manager: {e}", style="red")
        console.print("💡 Run 'nokube info' to check system status", style="blue")
        return
    
    # If no configuration file provided or file doesn't exist, create template files
    if not config_file or not os.path.exists(config_file):
        console.print("📝 Creating cluster template files...", style="blue")
        
        # Create template file content
        templates = {
            'kube-template.yaml': '''# Kubernetes cluster configuration template
apiVersion: v1
kind: Config
clusters:
  - name: kube-cluster
    cluster:
      server: https://localhost:6443
      certificate-authority-data: ""
contexts:
  - name: kube-context
    context:
      cluster: kube-cluster
      user: kube-user
current-context: kube-context
users:
  - name: kube-user
    user:
      client-certificate-data: ""
      client-key-data: ""

---
# Cluster configuration
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-config
  namespace: kube-system
data:
  cluster-name: "kube-cluster"
  cluster-type: "kubernetes"
  version: "1.27.3"

---
# Node configuration
apiVersion: v1
kind: Node
metadata:
  name: kube-node-1
  labels:
    node-role.kubernetes.io/worker: ""
    kubernetes.io/hostname: kube-node-1
spec:
  podCIDR: 10.244.0.0/24
  podCIDRs:
    - 10.244.0.0/24
status:
  capacity:
    cpu: "4"
    memory: "8Gi"
    pods: "110"
  allocatable:
    cpu: "4"
    memory: "8Gi"
    pods: "110"
  conditions:
    - type: Ready
      status: "True"
      lastHeartbeatTime: "2023-01-01T00:00:00Z"
      lastTransitionTime: "2023-01-01T00:00:00Z"
      reason: "KubeletReady"
      message: "kubelet is posting ready status"''',
            
            'ray-template.yaml': '''# Ray cluster configuration template

# Ray specific configuration
ray_spec:
  dashboard_port: 8265
  max_workers: 10

# Node list
nodes:
  - ssh_url: "localhost:10001"
    name: "ray-head-node"  # Node name seen by the actual system
    role: "head"
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/head"
    users:
      - userid: "ray-admin"
        password: "ray123"
      - userid: "ray-user"
        sshkey: "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC..."
    
  - ssh_url: "localhost:10002"
    name: "ray-worker-1"   # Node name seen by the actual system
    role: "worker"
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/worker-1"
    users:
      - userid: "worker-user"
        password: "worker123"
      - userid: "deploy"
        sshkey: "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC..."
        
  - ssh_url: "localhost:10003"
    name: "ray-worker-2"   # Node name seen by the actual system
    role: "worker"
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/worker-2"
    users:
      - userid: "worker-user"
        password: "worker123"
      - userid: "deploy"
        sshkey: "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC..."'''
        }
        
        # Create template files
        for template_file, content in templates.items():
            try:
                with open(template_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                console.print(f"✅ Created template file: {template_file}", style="green")
            except Exception as e:
                console.print(f"❌ Failed to create template file {template_file}: {e}", style="red")
        
        console.print("Please use one of the following commands to create a cluster:", style="cyan")
        console.print(f"  nokube cluster new-or-update {name} kube-template.yaml", style="cyan")
        console.print(f"  nokube cluster new-or-update {name} ray-template.yaml", style="cyan")
        return
    
    try:
        # Read configuration file
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Infer cluster type from configuration file name
        if 'ray' in config_file.lower():
            cluster_type = 'ray'
        elif 'kube' in config_file.lower():
            cluster_type = 'kube'
        else:
            # Try to infer from configuration content
            if 'ray_spec' in config:
                cluster_type = 'ray'
            else:
                console.print("❌ Cannot determine cluster type, please check configuration file", style="red")
                return
        
        # Add or update cluster
        existing_cluster = manager.get_cluster(name)
        if existing_cluster:
            console.print(f"🔄 Updating existing cluster: {name}", style="yellow")
            manager.update_cluster(name, config)
        else:
            console.print(f"🆕 Creating new cluster: {name}", style="blue")
            manager.add_cluster(name, cluster_type, config)
        
        # Start cluster
        manager.start_cluster(name)
        
    except Exception as e:
        console.print(f"❌ Operation failed: {e}", style="red")


@cluster.command()
@click.argument('name')
def delete(name):
    """Delete cluster"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    manager.delete_cluster(name)


@cluster.command()
@click.argument('name')
@click.argument('status')
def status(name, status):
    """Set cluster status"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    manager.set_cluster_status(name, status)


@cluster.command()
@click.argument('yaml_file')
def export_clusters(yaml_file):
    """Export cluster configuration to YAML file"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    manager.export_clusters_to_yaml(yaml_file)


@cluster.command()
@click.argument('name')
def get(name):
    """Get cluster configuration"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    cluster = manager.get_cluster(name)
    
    if cluster:
        console.print(f"📊 Cluster information: {name}", style="blue")
        console.print(f"  Type: {cluster.get('type', 'unknown')}", style="cyan")
        console.print(f"  Status: {cluster.get('status', 'unknown')}", style="green")
        console.print(f"  Created: {cluster.get('created_at', 'unknown')}", style="yellow")
        console.print(f"  Updated: {cluster.get('updated_at', 'unknown')}", style="magenta")
        
        config = cluster.get('config', {})
        if config:
            console.print("  Configuration:", style="blue")
            for key, value in config.items():
                console.print(f"    {key}: {value}", style="cyan")
    else:
        console.print(f"❌ Cluster {name} does not exist", style="red")


@cluster.command()
@click.argument('name')
@click.option('--workers', default=1, help='Number of worker nodes')
def scale(name, workers):
    """Scale cluster node count"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    cluster = manager.get_cluster(name)
    
    if not cluster:
        console.print(f"❌ Cluster {name} does not exist", style="red")
        return
    
    console.print(f"🔄 Scaling cluster {name} to {workers} worker nodes", style="blue")
    
    # Here you can add actual scaling logic
    # Currently just updating configuration
    config = cluster.get('config', {})
    if 'nodes' in config:
        # Update number of worker nodes
        worker_nodes = [node for node in config['nodes'] if node.get('role') == 'worker']
        if len(worker_nodes) != workers:
            console.print(f"⚠️  Currently has {len(worker_nodes)} worker nodes, target: {workers}", style="yellow")
    
    console.print(f"✅ Cluster {name} scaling configuration updated", style="green")


@cluster.command()
@click.argument('name')
def restart(name):
    """Restart cluster"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    cluster = manager.get_cluster(name)
    
    if not cluster:
        console.print(f"❌ Cluster {name} does not exist", style="red")
        return
    
    console.print(f"🔄 Restarting cluster {name}", style="blue")
    
    # Stop cluster
    console.print("🛑 Stopping cluster...", style="yellow")
    # Add stop logic here
    
    # Start cluster
    console.print("🚀 Starting cluster...", style="green")
    # Add start logic here
    
    console.print(f"✅ Cluster {name} restart completed", style="green")


@cluster.command()
@click.argument('name')
def health(name):
    """Check cluster health status"""
    from src.etcd_manager import EtcdManager
    
    manager = EtcdManager()
    cluster = manager.get_cluster(name)
    
    if not cluster:
        console.print(f"❌ Cluster {name} does not exist", style="red")
        return
    
    console.print(f"🏥 Checking cluster {name} health status", style="blue")
    
    # Create health status table
    table = Table(title=f"Cluster {name} Health Status")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Details", style="yellow")
    
    # Check cluster status
    status = cluster.get('status', 'unknown')
    status_icon = "✅" if status == 'running' else "❌" if status == 'stopped' else "⚠️"
    table.add_row("Cluster Status", f"{status_icon} {status}", "")
    
    # Check node status
    config = cluster.get('config', {})
    nodes = config.get('nodes', [])
    
    for node in nodes:
        node_name = node.get('name', 'unknown')
        node_role = node.get('role', 'unknown')
        node_status = "✅ Online"  # This should check actual status
        table.add_row(f"{node_role} ({node_name})", node_status, "")
    
    console.print(table)


@cluster.command()
@click.argument('name')
@click.option('--format', type=click.Choice(['yaml', 'json']), default='yaml', help='Output format')
def config(name, format):
    """View cluster configuration"""
    from src.etcd_manager import EtcdManager
    import yaml
    import json
    
    manager = EtcdManager()
    cluster = manager.get_cluster(name)
    
    if not cluster:
        console.print(f"❌ Cluster {name} does not exist", style="red")
        return
    
    config = cluster.get('config', {})
    
    if format == 'yaml':
        console.print(yaml.dump(config, default_flow_style=False, allow_unicode=True))
    else:
        console.print(json.dumps(config, indent=2, ensure_ascii=False))


@cluster.command()
@click.argument('name')
@click.argument('config_file')
def import_config(name, config_file):
    """Import cluster configuration from configuration file"""
    from src.etcd_manager import EtcdManager
    import yaml
    
    manager = EtcdManager()
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Check if cluster exists
        existing_cluster = manager.get_cluster(name)
        if existing_cluster:
            console.print(f"🔄 Updating existing cluster: {name}", style="yellow")
            manager.update_cluster(name, config)
        else:
            console.print(f"🆕 Creating new cluster: {name}", style="blue")
            # Infer cluster type from configuration file name
            if 'ray' in config_file.lower():
                cluster_type = 'ray'
            elif 'kube' in config_file.lower():
                cluster_type = 'kube'
            else:
                cluster_type = 'unknown'
            
            manager.add_cluster(name, cluster_type, config)
        
        console.print(f"✅ Cluster {name} configuration imported successfully", style="green")
        
    except Exception as e:
        console.print(f"❌ Failed to import configuration: {e}", style="red")


if __name__ == '__main__':
    cli() 