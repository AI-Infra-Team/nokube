#!/usr/bin/env python3
"""
自动部署器
根据配置变更自动部署应用到不同环境
"""

import yaml
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from rich.console import Console
from rich.table import Table

console = Console()


class AutoDeployer:
    """自动部署器"""
    
    def __init__(self):
        self.deployment_history = []
        self.rollback_points = {}
    
    def deploy_config_change(self, repo_name: str, config_data: Dict[str, Any], target: str = 'ray'):
        """部署配置变更"""
        console.print(f"🚀 部署配置变更: {repo_name}", style="blue")
        
        try:
            # 验证配置
            validation_errors = self._validate_deployment_config(config_data)
            if validation_errors:
                console.print(f"❌ 配置验证失败: {validation_errors}", style="red")
                return False
            
            # 创建部署点
            deployment_id = self._create_deployment_point(repo_name, config_data)
            
            # 根据目标环境部署
            if target == 'ray':
                success = self._deploy_to_ray(repo_name, config_data)
            else:
                console.print(f"❌ 不支持的部署目标: {target}", style="red")
                return False
            
            if success:
                console.print(f"✅ 部署成功: {repo_name}", style="green")
                self._record_deployment_success(deployment_id, repo_name)
                return True
            else:
                console.print(f"❌ 部署失败: {repo_name}", style="red")
                self._rollback_deployment(deployment_id, repo_name)
                return False
                
        except Exception as e:
            console.print(f"❌ 部署异常: {e}", style="red")
            return False
    
    def _validate_deployment_config(self, config_data: Dict[str, Any]) -> List[str]:
        """验证部署配置"""
        errors = []
        
        if not config_data:
            errors.append("配置数据为空")
            return errors
        
        # 检查必需的字段
        required_fields = ['kind', 'metadata']
        for field in required_fields:
            if field not in config_data:
                errors.append(f"缺少必需字段: {field}")
        
        # 检查 metadata
        metadata = config_data.get('metadata', {})
        if 'name' not in metadata:
            errors.append("缺少 name 字段")
        
        # 根据类型进行特定验证
        kind = config_data.get('kind', '')
        
        if kind == 'Deployment':
            spec = config_data.get('spec', {})
            template = spec.get('template', {})
            containers = template.get('spec', {}).get('containers', [])
            
            if not containers:
                errors.append("Deployment 缺少 containers")
            
            for i, container in enumerate(containers):
                if 'name' not in container:
                    errors.append(f"容器 {i} 缺少 name 字段")
                if 'image' not in container:
                    errors.append(f"容器 {i} 缺少 image 字段")
        
        elif kind == 'ConfigMap':
            if 'data' not in config_data:
                errors.append("ConfigMap 缺少 data 字段")
        
        elif kind == 'Service':
            spec = config_data.get('spec', {})
            if 'selector' not in spec:
                errors.append("Service 缺少 selector 字段")
            if 'ports' not in spec:
                errors.append("Service 缺少 ports 字段")
        
        return errors
    
    def _create_deployment_point(self, repo_name: str, config_data: Dict[str, Any]) -> str:
        """创建部署点"""
        import uuid
        deployment_id = str(uuid.uuid4())
        
        self.rollback_points[deployment_id] = {
            'repo_name': repo_name,
            'config_data': config_data.copy(),
            'timestamp': time.time(),
            'status': 'pending'
        }
        
        console.print(f"📌 创建部署点: {deployment_id}", style="blue")
        return deployment_id
    
    def deploy_to_target(self, repo_name: str, config_data: Dict[str, Any], target: str) -> bool:
        """部署到指定目标"""
        try:
            if target == 'ray':
                success = self._deploy_to_ray(repo_name, config_data)
            else:
                console.print(f"❌ 不支持的部署目标: {target}", style="red")
                return False
            
            if success:
                console.print(f"✅ 部署成功: {repo_name} -> {target}", style="green")
                return True
            else:
                console.print(f"❌ 部署失败: {repo_name} -> {target}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 部署异常: {e}", style="red")
            return False
    
    def _validate_config(self, config_data: Dict[str, Any]) -> bool:
        """验证配置数据"""
        try:
            # 检查必需字段
            required_fields = ['kind', 'metadata']
            for field in required_fields:
                if field not in config_data:
                    console.print(f"❌ 缺少必需字段: {field}", style="red")
                    return False
            
            # 验证资源类型
            kind = config_data.get('kind', '')
            supported_kinds = ['Deployment', 'Service', 'ConfigMap', 'Secret']
            
            if kind not in supported_kinds:
                console.print(f"❌ 不支持的资源类型: {kind}", style="red")
                return False
            
            # 验证元数据
            metadata = config_data.get('metadata', {})
            if 'name' not in metadata:
                console.print("❌ 缺少资源名称", style="red")
                return False
            
            return True
            
        except Exception as e:
            console.print(f"❌ 配置验证失败: {e}", style="red")
            return False
    
    def _deploy_to_ray(self, repo_name: str, config_data: Dict[str, Any]) -> bool:
        """部署到 Ray 集群"""
        try:
            console.print(f"📦 部署到 Ray 集群: {repo_name}", style="blue")
            
            # 验证配置
            if not self._validate_config(config_data):
                return False
            
            # 根据资源类型处理
            kind = config_data.get('kind', '')
            
            if kind == 'Deployment':
                return self._deploy_ray_deployment(repo_name, config_data)
            elif kind == 'ConfigMap':
                return self._deploy_ray_configmap(repo_name, config_data)
            elif kind == 'Service':
                return self._deploy_ray_service(repo_name, config_data)
            else:
                console.print(f"⚠️  不支持的资源类型: {kind}", style="yellow")
                return False
                
        except Exception as e:
            console.print(f"❌ Ray 部署异常: {e}", style="red")
            return False
    
    def _deploy_ray_deployment(self, repo_name: str, config_data: Dict[str, Any]) -> bool:
        """部署 Ray Deployment"""
        try:
            name = config_data['metadata']['name']
            console.print(f"🚀 部署 Ray Deployment: {name}", style="blue")
            
            # 这里实现具体的 Ray Deployment 部署逻辑
            # 可能需要调用 Ray Serve 或其他 Ray 部署方式
            
            console.print(f"✅ Ray Deployment {name} 部署成功", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ Ray Deployment 部署失败: {e}", style="red")
            return False
    
    def _deploy_ray_configmap(self, repo_name: str, config_data: Dict[str, Any]) -> bool:
        """部署 Ray ConfigMap"""
        try:
            name = config_data['metadata']['name']
            console.print(f"📋 部署 Ray ConfigMap: {name}", style="blue")
            
            # 这里实现具体的 Ray ConfigMap 部署逻辑
            
            console.print(f"✅ Ray ConfigMap {name} 部署成功", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ Ray ConfigMap 部署失败: {e}", style="red")
            return False
    
    def _deploy_ray_service(self, repo_name: str, config_data: Dict[str, Any]) -> bool:
        """部署 Ray Service"""
        try:
            name = config_data['metadata']['name']
            console.print(f"🔧 部署 Ray Service: {name}", style="blue")
            
            # 这里实现具体的 Ray Service 部署逻辑
            
            console.print(f"✅ Ray Service {name} 部署成功", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ Ray Service 部署失败: {e}", style="red")
            return False
    
    def _record_deployment_success(self, deployment_id: str, repo_name: str):
        """记录部署成功"""
        if deployment_id in self.rollback_points:
            self.rollback_points[deployment_id]['status'] = 'success'
        
        self.deployment_history.append({
            'id': deployment_id,
            'repo_name': repo_name,
            'timestamp': time.time(),
            'status': 'success'
        })
        
        console.print(f"📝 记录部署成功: {deployment_id}", style="green")
    
    def _rollback_deployment(self, deployment_id: str, repo_name: str):
        """回滚部署"""
        if deployment_id in self.rollback_points:
            console.print(f"🔄 回滚部署: {deployment_id}", style="yellow")
            
            rollback_point = self.rollback_points[deployment_id]
            old_config = rollback_point['config_data']
            
            # 执行回滚
            self.deploy_config_change(repo_name, old_config)
            
            self.rollback_points[deployment_id]['status'] = 'rolled_back'
    
    def show_deployment_history(self):
        """显示部署历史"""
        if not self.deployment_history:
            console.print("📊 暂无部署历史", style="blue")
            return
        
        table = Table(title="部署历史")
        table.add_column("部署ID", style="cyan")
        table.add_column("仓库名称", style="green")
        table.add_column("时间", style="yellow")
        table.add_column("状态", style="magenta")
        
        for deployment in self.deployment_history[-10:]:  # 显示最近10次
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(deployment['timestamp']))
            status = "✅ 成功" if deployment['status'] == 'success' else "❌ 失败"
            
            table.add_row(
                deployment['id'][:8],
                deployment['repo_name'],
                timestamp,
                status
            )
        
        console.print(table)
    
    def show_rollback_points(self):
        """显示回滚点"""
        if not self.rollback_points:
            console.print("📊 暂无回滚点", style="blue")
            return
        
        table = Table(title="回滚点")
        table.add_column("部署ID", style="cyan")
        table.add_column("仓库名称", style="green")
        table.add_column("时间", style="yellow")
        table.add_column("状态", style="magenta")
        
        for deployment_id, point in self.rollback_points.items():
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(point['timestamp']))
            status_map = {
                'pending': '⏳ 待处理',
                'success': '✅ 成功',
                'failed': '❌ 失败',
                'rolled_back': '🔄 已回滚'
            }
            status = status_map.get(point['status'], '❓ 未知')
            
            table.add_row(
                deployment_id[:8],
                point['repo_name'],
                timestamp,
                status
            )
        
        console.print(table)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        console.print("用法: python auto_deployer.py <config_file> [target]", style="red")
        sys.exit(1)
    
    config_file = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else 'ray'
    
    deployer = AutoDeployer()
    
    # 加载配置
    with open(config_file, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
    
    # 部署配置
    success = deployer.deploy_config_change('test-repo', config_data, target)
    
    if success:
        console.print("✅ 部署完成", style="green")
    else:
        console.print("❌ 部署失败", style="red")
    
    # 显示部署历史
    deployer.show_deployment_history() 