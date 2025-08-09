#!/usr/bin/env python3
"""
应用部署器
统一管理应用部署到 Ray 环境
"""

import subprocess
import yaml
import os
from typing import Dict, Any, List
from rich.console import Console
from rich.table import Table

console = Console()


class Deployer:
    """应用部署器"""
    
    def __init__(self):
        self.ray_manager = None
    
    def deploy_to_ray(self, yaml_file):
        """部署应用到 Ray 集群"""
        console.print(f"🚀 部署应用到 Ray 集群: {yaml_file}", style="blue")
        
        try:
            # 读取 YAML 文件
            with open(yaml_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if not config:
                console.print("❌ 配置文件为空", style="red")
                return False
            
            # 解析部署配置
            deployment_type = config.get('type', 'unknown')
            
            if deployment_type == 'ray_job':
                return self._deploy_ray_job(config)
            elif deployment_type == 'ray_service':
                return self._deploy_ray_service(config)
            elif deployment_type == 'ray_actor':
                return self._deploy_ray_actor(config)
            elif deployment_type == 'ray_workflow':
                return self._deploy_ray_workflow(config)
            elif deployment_type == 'ray_dataset':
                return self._deploy_ray_dataset(config)
            else:
                console.print(f"❌ 不支持的部署类型: {deployment_type}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 部署失败: {e}", style="red")
            return False
    
    def _deploy_ray_job(self, config: Dict[str, Any]) -> bool:
        """部署 Ray Job"""
        try:
            job_name = config.get('name', 'default-job')
            entrypoint = config.get('entrypoint', '')
            runtime_env = config.get('runtime_env', {})
            
            console.print(f"📦 部署 Ray Job: {job_name}", style="blue")
            
            # 构建 ray job submit 命令
            cmd = ['ray', 'job', 'submit', '--job-name', job_name]
            
            if runtime_env:
                # 处理运行时环境
                if 'pip' in runtime_env:
                    cmd.extend(['--runtime-env', f'pip={runtime_env["pip"]}'])
                if 'env_vars' in runtime_env:
                    for key, value in runtime_env['env_vars'].items():
                        cmd.extend(['--env', f'{key}={value}'])
            
            cmd.append(entrypoint)
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ Ray Job {job_name} 部署成功", style="green")
                console.print(result.stdout, style="cyan")
                return True
            else:
                console.print(f"❌ Ray Job 部署失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 部署 Ray Job 失败: {e}", style="red")
            return False
    
    def _deploy_ray_service(self, config: Dict[str, Any]) -> bool:
        """部署 Ray Service"""
        try:
            service_name = config.get('name', 'default-service')
            import_path = config.get('import_path', '')
            runtime_env = config.get('runtime_env', {})
            
            console.print(f"🔧 部署 Ray Service: {service_name}", style="blue")
            
            # 构建 ray service 命令
            cmd = ['ray', 'service', 'start', '--name', service_name]
            
            if import_path:
                cmd.extend(['--import-path', import_path])
            
            if runtime_env:
                # 处理运行时环境
                if 'pip' in runtime_env:
                    cmd.extend(['--runtime-env', f'pip={runtime_env["pip"]}'])
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ Ray Service {service_name} 部署成功", style="green")
                console.print(result.stdout, style="cyan")
                return True
            else:
                console.print(f"❌ Ray Service 部署失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 部署 Ray Service 失败: {e}", style="red")
            return False
    
    def _deploy_ray_actor(self, config: Dict[str, Any]) -> bool:
        """部署 Ray Actor"""
        try:
            actor_name = config.get('name', 'default-actor')
            class_name = config.get('class', '')
            num_replicas = config.get('num_replicas', 1)
            
            console.print(f"🎭 部署 Ray Actor: {actor_name}", style="blue")
            
            # 这里需要根据具体的 Ray Actor 部署方式来实现
            # 可能需要使用 Ray Serve 或其他方式
            console.print("⚠️  Ray Actor 部署功能待实现", style="yellow")
            return False
                
        except Exception as e:
            console.print(f"❌ 部署 Ray Actor 失败: {e}", style="red")
            return False
    
    def _deploy_ray_workflow(self, config: Dict[str, Any]) -> bool:
        """部署 Ray Workflow"""
        try:
            workflow_name = config.get('name', 'default-workflow')
            workflow_id = config.get('workflow_id', '')
            entrypoint = config.get('entrypoint', '')
            runtime_env = config.get('runtime_env', {})
            
            console.print(f"🔄 部署 Ray Workflow: {workflow_name}", style="blue")
            
            # 构建 ray workflow 命令
            cmd = ['ray', 'workflow', 'submit', '--workflow-id', workflow_id]
            
            if runtime_env:
                # 处理运行时环境
                if 'pip' in runtime_env:
                    cmd.extend(['--runtime-env', f'pip={runtime_env["pip"]}'])
                if 'env_vars' in runtime_env:
                    for key, value in runtime_env['env_vars'].items():
                        cmd.extend(['--env', f'{key}={value}'])
            
            cmd.append(entrypoint)
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ Ray Workflow {workflow_name} 部署成功", style="green")
                console.print(result.stdout, style="cyan")
                return True
            else:
                console.print(f"❌ Ray Workflow 部署失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 部署 Ray Workflow 失败: {e}", style="red")
            return False
    
    def _deploy_ray_dataset(self, config: Dict[str, Any]) -> bool:
        """部署 Ray Dataset"""
        try:
            dataset_name = config.get('name', 'default-dataset')
            data_source = config.get('data_source', '')
            format_type = config.get('format', 'parquet')
            
            console.print(f"📊 部署 Ray Dataset: {dataset_name}", style="blue")
            
            # 构建 ray dataset 命令
            cmd = ['ray', 'data', 'read', format_type, data_source]
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ Ray Dataset {dataset_name} 部署成功", style="green")
                console.print(result.stdout, style="cyan")
                return True
            else:
                console.print(f"❌ Ray Dataset 部署失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 部署 Ray Dataset 失败: {e}", style="red")
            return False
    
    def scale_deployment(self, deployment_name: str, replicas: int, target='ray'):
        """扩展部署副本数"""
        console.print(f"📈 扩展部署 {deployment_name} 到 {replicas} 个副本", style="blue")
        
        try:
            if target == 'ray':
                return self._scale_ray_deployment(deployment_name, replicas)
            else:
                console.print(f"❌ 不支持的目标类型: {target}", style="red")
                return False
        except Exception as e:
            console.print(f"❌ 扩展部署失败: {e}", style="red")
            return False
    
    def _scale_ray_deployment(self, deployment_name: str, replicas: int) -> bool:
        """扩展 Ray 部署"""
        try:
            # 使用 ray serve 扩展服务
            cmd = ['ray', 'serve', 'scale', deployment_name, str(replicas)]
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ 部署 {deployment_name} 扩展成功", style="green")
                return True
            else:
                console.print(f"❌ 扩展部署失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 扩展 Ray 部署失败: {e}", style="red")
            return False
    
    def rollback_deployment(self, deployment_name: str, version: str, target='ray'):
        """回滚部署到指定版本"""
        console.print(f"🔄 回滚部署 {deployment_name} 到版本 {version}", style="blue")
        
        try:
            if target == 'ray':
                return self._rollback_ray_deployment(deployment_name, version)
            else:
                console.print(f"❌ 不支持的目标类型: {target}", style="red")
                return False
        except Exception as e:
            console.print(f"❌ 回滚部署失败: {e}", style="red")
            return False
    
    def _rollback_ray_deployment(self, deployment_name: str, version: str) -> bool:
        """回滚 Ray 部署"""
        try:
            # 使用 ray serve 回滚服务
            cmd = ['ray', 'serve', 'rollback', deployment_name, version]
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ 部署 {deployment_name} 回滚成功", style="green")
                return True
            else:
                console.print(f"❌ 回滚部署失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 回滚 Ray 部署失败: {e}", style="red")
            return False
    
    def health_check(self, deployment_name: str, target='ray'):
        """检查部署健康状态"""
        console.print(f"🏥 检查部署 {deployment_name} 健康状态", style="blue")
        
        try:
            if target == 'ray':
                return self._health_check_ray_deployment(deployment_name)
            else:
                console.print(f"❌ 不支持的目标类型: {target}", style="red")
                return False
        except Exception as e:
            console.print(f"❌ 健康检查失败: {e}", style="red")
            return False
    
    def _health_check_ray_deployment(self, deployment_name: str) -> bool:
        """检查 Ray 部署健康状态"""
        try:
            # 使用 ray serve 检查服务状态
            cmd = ['ray', 'serve', 'status', deployment_name]
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ 部署 {deployment_name} 健康状态检查完成", style="green")
                console.print(result.stdout, style="cyan")
                return True
            else:
                console.print(f"❌ 健康检查失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 检查 Ray 部署健康状态失败: {e}", style="red")
            return False
    
    def get_deployment_logs(self, deployment_name: str, target='ray'):
        """获取部署日志"""
        console.print(f"📋 获取部署 {deployment_name} 日志", style="blue")
        
        try:
            if target == 'ray':
                return self._get_ray_deployment_logs(deployment_name)
            else:
                console.print(f"❌ 不支持的目标类型: {target}", style="red")
                return False
        except Exception as e:
            console.print(f"❌ 获取日志失败: {e}", style="red")
            return False
    
    def _get_ray_deployment_logs(self, deployment_name: str) -> bool:
        """获取 Ray 部署日志"""
        try:
            # 使用 ray serve 获取服务日志
            cmd = ['ray', 'serve', 'logs', deployment_name]
            
            console.print(f"执行命令: {' '.join(cmd)}", style="cyan")
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                console.print(f"✅ 部署 {deployment_name} 日志获取完成", style="green")
                console.print(result.stdout, style="cyan")
                return True
            else:
                console.print(f"❌ 获取日志失败: {result.stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 获取 Ray 部署日志失败: {e}", style="red")
            return False
    
    def show_deployment_status(self, target='ray'):
        """显示部署状态"""
        if target == 'ray':
            self._show_ray_status()
    
    def _show_ray_status(self):
        """显示 Ray 部署状态"""
        console.print("📊 Ray 部署状态", style="blue")
        
        try:
            # 检查 Ray 集群状态
            result = subprocess.run(['ray', 'status'], capture_output=True, text=True)
            if result.returncode == 0:
                console.print("Ray 集群状态:", style="green")
                console.print(result.stdout, style="cyan")
            else:
                console.print("❌ 无法获取 Ray 集群状态", style="red")
            
            # 检查 Ray Jobs
            result = subprocess.run(['ray', 'job', 'list'], capture_output=True, text=True)
            if result.returncode == 0:
                console.print("Ray Jobs:", style="green")
                console.print(result.stdout, style="cyan")
            
            # 检查 Ray Services
            result = subprocess.run(['ray', 'service', 'list'], capture_output=True, text=True)
            if result.returncode == 0:
                console.print("Ray Services:", style="green")
                console.print(result.stdout, style="cyan")
                
        except Exception as e:
            console.print(f"❌ 获取状态失败: {e}", style="red")
    
    def cleanup(self, target='ray'):
        """清理部署"""
        if target == 'ray':
            self._cleanup_ray()
    
    def _cleanup_ray(self):
        """清理 Ray 部署"""
        console.print("🧹 清理 Ray 部署...", style="blue")
        
        try:
            # 停止所有 Ray Jobs
            result = subprocess.run(['ray', 'job', 'stop', '--all'], capture_output=True, text=True)
            if result.returncode == 0:
                console.print("✅ 已停止所有 Ray Jobs", style="green")
            
            # 停止所有 Ray Services
            result = subprocess.run(['ray', 'service', 'stop', '--all'], capture_output=True, text=True)
            if result.returncode == 0:
                console.print("✅ 已停止所有 Ray Services", style="green")
            
            console.print("✅ Ray 部署已清理", style="green")
            
        except Exception as e:
            console.print(f"❌ 清理失败: {e}", style="red")


if __name__ == '__main__':
    import sys
    
    deployer = Deployer()
    
    if len(sys.argv) < 2:
        console.print("用法: python deployer.py <yaml_file>", style="red")
        sys.exit(1)
    
    yaml_file = sys.argv[1]
    deployer.deploy_to_ray(yaml_file) 