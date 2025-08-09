#!/usr/bin/env python3
"""
应用部署器
统一管理应用部署到 Ray 环境
"""

import subprocess
import yaml
import os
from pathlib import Path
from typing import Dict, Any, List
import uuid
import re
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
            # 读取 YAML 文件（支持多文档）
            with open(yaml_file, 'r', encoding='utf-8') as f:
                documents = list(yaml.safe_load_all(f))

            if not documents:
                console.print("❌ 配置文件为空", style="red")
                return False

            # 仅识别包含 nokube_cluster_name 的 NoKube 配置文档
            allowed_types = {"ray_job", "ray_service", "ray_actor", "ray_workflow", "ray_dataset"}
            selected = [d for d in documents if isinstance(d, dict) and d.get('nokube_cluster_name')]

            if not selected:
                # 友好提示：用户很可能传入了 Kubernetes 多资源清单
                has_k8s = any(isinstance(d, dict) and d.get('kind') for d in documents)
                if has_k8s:
                    console.print(
                        "⚠️ 检测到 Kubernetes 资源清单，本命令期望的是 NoKube 部署配置（顶层包含 nokube_cluster_name）",
                        style="yellow",
                    )
                    console.print(
                        "👉 请选择其一：\n"
                        "   - 使用 kubectl 部署 K8s 清单：kubectl apply -f <yaml>\n"
                        "   - 将 K8s 清单转换为 Ray 任务：nokube deploy convert <yaml>",
                        style="cyan",
                    )
                else:
                    console.print("❌ 未找到可识别的 NoKube 配置（缺少 nokube_cluster_name）", style="red")
                return False

            # 如存在多个 NoKube 文档，默认取第一个
            if len(selected) > 1:
                console.print(f"⚠️ 检测到 {len(selected)} 个 NoKube 配置文档，默认使用第一个", style="yellow")

            config = selected[0]

            # 解析部署配置
            # 新模式：固定 Actor 控制器部署（无需指定部署类型）
            # 仅要求包含 nokube_cluster_name；其余文档按 K8s 资源处理
            try:
                from src.ray_kube_controller import DeploymentActor, PodActor  # type: ignore
                import ray
            except Exception as e:
                console.print(f"❌ 运行依赖加载失败: {e}", style="red")
                return False

            console.print("🚀 启动 Ray 模拟：Deployment -> Pod -> Container", style="blue")
            try:
                project_root = Path(__file__).resolve().parent.parent
                # 使用固定 namespace，确保 detached actor 可被后续运行复用
                ray.init(
                    ignore_reinit_error=True,
                    runtime_env={"working_dir": str(project_root)},
                    namespace="nokube",
                )
            except Exception:
                pass

            # 从 K8s 文档中提取 Deployments 和 Pods
            deployments = [d for d in documents if isinstance(d, dict) and d.get('kind') == 'Deployment']
            pods = [d for d in documents if isinstance(d, dict) and d.get('kind') == 'Pod']

            futures = []
            for d in deployments:
                meta = d.get('metadata', {})
                ns = meta.get('namespace', 'default')
                name = meta.get('name', 'deployment')
                spec = d.get('spec', {})
                replicas = spec.get('replicas', 1)
                tmpl_spec = spec.get('template', {}).get('spec', {})
                containers = tmpl_spec.get('containers', [])
                # 幂等：同名 DeploymentActor 复用并更新规格；不存在则创建
                safe = lambda s: re.sub(r"[^a-zA-Z0-9_-]", "-", s or "")
                dep_actor_name = f"deploy-{safe(ns)}-{safe(name)}"
                try:
                    from ray import get_actor
                    actor = get_actor(dep_actor_name, namespace="nokube")
                    # 为保证正确性：先关闭旧的 deployment 再重建
                    try:
                        ray.get(actor.stop.remote(), timeout=10)
                    except Exception:
                        pass
                    try:
                        ray.kill(actor, no_restart=True)
                    except Exception:
                        pass
                except Exception:
                    pass

                # 重建新的 detached DeploymentActor（同名覆盖）
                actor = DeploymentActor.options(
                    name=dep_actor_name,
                    lifetime="detached",
                    namespace="nokube",
                ).remote(
                    name, ns, replicas, containers
                )
                futures.append(actor.start.remote())
                try:
                    actor.run.remote()
                except Exception:
                    pass

            for p in pods:
                meta = p.get('metadata', {})
                ns = meta.get('namespace', 'default')
                name = meta.get('name', 'pod')
                spec = p.get('spec', {})
                containers = spec.get('containers', [])
                actor = PodActor.options(name=None).remote(name, ns, containers)
                futures.append(actor.start.remote())

            # 避免长时间阻塞：仅等待最先完成的 start，然后尽快返回
            results = []
            if futures:
                ready, pending = ray.wait(futures, num_returns=len(futures), timeout=10)
                for ref in ready:
                    try:
                        results.append(ray.get(ref))
                    except Exception as e:
                        results.append({"error": str(e)})
            console.print(f"✅ 启动完成: {len(results)} 项", style="green")
            # 打印详细结果，包含 pod/namespace/containers 等信息
            try:
                import json
                console.print(json.dumps(results, ensure_ascii=False, indent=2))
            except Exception:
                console.print(str(results))
            return True

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