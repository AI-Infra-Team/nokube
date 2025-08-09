#!/usr/bin/env python3
"""
NoKube 监控系统
提供集群和应用监控功能
"""

import time
import psutil
import subprocess
import json
import yaml
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout
from rich.text import Text

console = Console()


class ClusterMonitor:
    """集群监控器"""
    
    def __init__(self):
        self.metrics = {}
        self.alerts = []
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """获取系统指标"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'timestamp': datetime.now().isoformat(),
                'cpu': {
                    'usage_percent': cpu_percent,
                    'count': psutil.cpu_count(),
                    'frequency': psutil.cpu_freq()._asdict() if psutil.cpu_freq() else {}
                },
                'memory': {
                    'total': memory.total,
                    'available': memory.available,
                    'used': memory.used,
                    'percent': memory.percent
                },
                'disk': {
                    'total': disk.total,
                    'used': disk.used,
                    'free': disk.free,
                    'percent': (disk.used / disk.total) * 100
                },
                'network': self._get_network_metrics()
            }
        except Exception as e:
            console.print(f"❌ 获取系统指标失败: {e}", style="red")
            return {}
    
    def _get_network_metrics(self) -> Dict[str, Any]:
        """获取网络指标"""
        try:
            net_io = psutil.net_io_counters()
            return {
                'bytes_sent': net_io.bytes_sent,
                'bytes_recv': net_io.bytes_recv,
                'packets_sent': net_io.packets_sent,
                'packets_recv': net_io.packets_recv
            }
        except Exception:
            return {}
    
    def get_ray_metrics(self) -> Dict[str, Any]:
        """获取 Ray 集群指标"""
        try:
            # 检查 Ray 是否运行
            result = subprocess.run(['ray', 'status'], capture_output=True, text=True)
            
            if result.returncode == 0:
                # 解析 Ray 状态
                lines = result.stdout.split('\n')
                metrics = {
                    'status': 'running',
                    'nodes': 0,
                    'resources': {}
                }
                
                for line in lines:
                    if 'node' in line.lower():
                        metrics['nodes'] += 1
                    elif 'cpu' in line.lower():
                        # 解析 CPU 信息
                        pass
                    elif 'memory' in line.lower():
                        # 解析内存信息
                        pass
                
                return metrics
            else:
                return {'status': 'stopped'}
                
        except Exception as e:
            console.print(f"❌ 获取 Ray 指标失败: {e}", style="red")
            return {'status': 'error'}
    
    def check_alerts(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """检查告警条件"""
        alerts = []
        
        # CPU 告警
        if metrics.get('cpu', {}).get('usage_percent', 0) > 80:
            alerts.append({
                'type': 'warning',
                'component': 'cpu',
                'message': f"CPU 使用率过高: {metrics['cpu']['usage_percent']}%",
                'timestamp': datetime.now().isoformat()
            })
        
        # 内存告警
        if metrics.get('memory', {}).get('percent', 0) > 85:
            alerts.append({
                'type': 'critical',
                'component': 'memory',
                'message': f"内存使用率过高: {metrics['memory']['percent']}%",
                'timestamp': datetime.now().isoformat()
            })
        
        # 磁盘告警
        if metrics.get('disk', {}).get('percent', 0) > 90:
            alerts.append({
                'type': 'critical',
                'component': 'disk',
                'message': f"磁盘使用率过高: {metrics['disk']['percent']}%",
                'timestamp': datetime.now().isoformat()
            })
        
        return alerts


class DeploymentMonitor:
    """部署监控器"""
    
    def __init__(self):
        self.deployments = {}
    
    def get_deployment_status(self, deployment_name: str) -> Dict[str, Any]:
        """获取部署状态"""
        try:
            # 使用 ray serve 获取服务状态
            cmd = ['ray', 'serve', 'status', deployment_name]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                return {
                    'name': deployment_name,
                    'status': 'running',
                    'replicas': 1,  # 需要解析输出
                    'health': 'healthy'
                }
            else:
                return {
                    'name': deployment_name,
                    'status': 'stopped',
                    'replicas': 0,
                    'health': 'unhealthy'
                }
                
        except Exception as e:
            console.print(f"❌ 获取部署状态失败: {e}", style="red")
            return {
                'name': deployment_name,
                'status': 'error',
                'replicas': 0,
                'health': 'unknown'
            }
    
    def get_deployment_logs(self, deployment_name: str, lines: int = 100) -> List[str]:
        """获取部署日志"""
        try:
            cmd = ['ray', 'serve', 'logs', deployment_name, '--lines', str(lines)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                return result.stdout.split('\n')
            else:
                return []
                
        except Exception as e:
            console.print(f"❌ 获取部署日志失败: {e}", style="red")
            return []
    
    def get_deployment_metrics(self, deployment_name: str) -> Dict[str, Any]:
        """获取部署指标"""
        try:
            # 这里可以集成 Prometheus 或其他监控系统
            return {
                'name': deployment_name,
                'requests_per_second': 0,
                'response_time': 0,
                'error_rate': 0,
                'cpu_usage': 0,
                'memory_usage': 0
            }
        except Exception as e:
            console.print(f"❌ 获取部署指标失败: {e}", style="red")
            return {}


class MonitoringDashboard:
    """监控仪表板"""
    
    def __init__(self):
        self.cluster_monitor = ClusterMonitor()
        self.deployment_monitor = DeploymentMonitor()
        self.refresh_interval = 5  # 秒
    
    def create_dashboard(self):
        """创建监控仪表板"""
        layout = Layout()
        
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3)
        )
        
        layout["main"].split_row(
            Layout(name="system"),
            Layout(name="cluster"),
            Layout(name="deployments")
        )
        
        with Live(layout, refresh_per_second=1, screen=True):
            while True:
                try:
                    # 更新系统指标
                    system_metrics = self.cluster_monitor.get_system_metrics()
                    layout["system"].update(self._create_system_panel(system_metrics))
                    
                    # 更新集群指标
                    cluster_metrics = self.cluster_monitor.get_ray_metrics()
                    layout["cluster"].update(self._create_cluster_panel(cluster_metrics))
                    
                    # 更新部署指标
                    layout["deployments"].update(self._create_deployments_panel())
                    
                    # 更新头部
                    layout["header"].update(self._create_header_panel())
                    
                    # 更新底部
                    alerts = self.cluster_monitor.check_alerts(system_metrics)
                    layout["footer"].update(self._create_alerts_panel(alerts))
                    
                    time.sleep(self.refresh_interval)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    console.print(f"❌ 仪表板更新失败: {e}", style="red")
                    time.sleep(self.refresh_interval)
    
    def _create_header_panel(self) -> Panel:
        """创建头部面板"""
        title = Text("NoKube 监控仪表板", style="bold blue")
        subtitle = Text(f"更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim")
        
        content = Layout()
        content.split_column(
            Layout(title),
            Layout(subtitle)
        )
        
        return Panel(content, title="监控仪表板", border_style="blue")
    
    def _create_system_panel(self, metrics: Dict[str, Any]) -> Panel:
        """创建系统指标面板"""
        if not metrics:
            return Panel("无法获取系统指标", title="系统指标", border_style="red")
        
        table = Table(title="系统指标")
        table.add_column("指标", style="cyan")
        table.add_column("值", style="green")
        table.add_column("状态", style="yellow")
        
        # CPU
        cpu_percent = metrics.get('cpu', {}).get('usage_percent', 0)
        cpu_status = "🟢" if cpu_percent < 80 else "🟡" if cpu_percent < 90 else "🔴"
        table.add_row("CPU 使用率", f"{cpu_percent:.1f}%", cpu_status)
        
        # 内存
        memory_percent = metrics.get('memory', {}).get('percent', 0)
        memory_status = "🟢" if memory_percent < 80 else "🟡" if memory_percent < 90 else "🔴"
        table.add_row("内存使用率", f"{memory_percent:.1f}%", memory_status)
        
        # 磁盘
        disk_percent = metrics.get('disk', {}).get('percent', 0)
        disk_status = "🟢" if disk_percent < 80 else "🟡" if disk_percent < 90 else "🔴"
        table.add_row("磁盘使用率", f"{disk_percent:.1f}%", disk_status)
        
        return Panel(table, title="系统指标", border_style="green")
    
    def _create_cluster_panel(self, metrics: Dict[str, Any]) -> Panel:
        """创建集群指标面板"""
        table = Table(title="集群指标")
        table.add_column("指标", style="cyan")
        table.add_column("值", style="green")
        table.add_column("状态", style="yellow")
        
        status = metrics.get('status', 'unknown')
        status_icon = "🟢" if status == 'running' else "🔴"
        table.add_row("集群状态", status, status_icon)
        
        nodes = metrics.get('nodes', 0)
        table.add_row("节点数量", str(nodes), "🟢")
        
        return Panel(table, title="集群指标", border_style="blue")
    
    def _create_deployments_panel(self) -> Panel:
        """创建部署指标面板"""
        table = Table(title="部署状态")
        table.add_column("部署", style="cyan")
        table.add_column("状态", style="green")
        table.add_column("副本", style="yellow")
        table.add_column("健康", style="magenta")
        
        # 这里可以获取所有部署
        deployments = ["example-service"]  # 示例
        
        for deployment in deployments:
            status = self.deployment_monitor.get_deployment_status(deployment)
            health_icon = "🟢" if status['health'] == 'healthy' else "🔴"
            table.add_row(
                status['name'],
                status['status'],
                str(status['replicas']),
                health_icon
            )
        
        return Panel(table, title="部署状态", border_style="magenta")
    
    def _create_alerts_panel(self, alerts: List[Dict[str, Any]]) -> Panel:
        """创建告警面板"""
        if not alerts:
            return Panel("暂无告警", title="告警", border_style="green")
        
        table = Table(title="告警")
        table.add_column("时间", style="cyan")
        table.add_column("级别", style="red")
        table.add_column("组件", style="yellow")
        table.add_column("消息", style="white")
        
        for alert in alerts:
            level_icon = "🔴" if alert['type'] == 'critical' else "🟡"
            table.add_row(
                alert['timestamp'][:19],
                f"{level_icon} {alert['type']}",
                alert['component'],
                alert['message']
            )
        
        return Panel(table, title="告警", border_style="red")


def start_monitoring():
    """启动监控"""
    dashboard = MonitoringDashboard()
    dashboard.create_dashboard()


def export_metrics(output_file: str):
    """导出指标数据"""
    monitor = ClusterMonitor()
    metrics = monitor.get_system_metrics()
    
    try:
        with open(output_file, 'w') as f:
            json.dump(metrics, f, indent=2, default=str)
        console.print(f"✅ 指标已导出到: {output_file}", style="green")
    except Exception as e:
        console.print(f"❌ 导出指标失败: {e}", style="red")


def check_health():
    """健康检查"""
    monitor = ClusterMonitor()
    metrics = monitor.get_system_metrics()
    alerts = monitor.check_alerts(metrics)
    
    if alerts:
        console.print("⚠️  发现告警:", style="yellow")
        for alert in alerts:
            console.print(f"  {alert['type'].upper()}: {alert['message']}", style="red")
        return False
    else:
        console.print("✅ 系统健康", style="green")
        return True 