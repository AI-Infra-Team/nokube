#!/usr/bin/env python3
"""
YAML 配置扫描器
扫描 Git 仓库中的 YAML 配置文件并解析变更
"""

import yaml
import os
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any
from rich.console import Console
from rich.table import Table

console = Console()


class ConfigScanner:
    """YAML 配置扫描器"""
    
    def __init__(self):
        self.supported_kinds = ['ConfigMap', 'Secret', 'Deployment', 'Service', 'Ingress']
    
    def scan_config_file(self, file_path: str) -> Dict[str, Any]:
        """扫描配置文件"""
        if not os.path.exists(file_path):
            console.print(f"❌ 配置文件不存在: {file_path}", style="red")
            return {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 解析 YAML
            documents = list(yaml.safe_load_all(content))
            
            configs = {}
            
            for doc in documents:
                if doc and 'kind' in doc:
                    kind = doc['kind']
                    if kind in self.supported_kinds:
                        config_key = f"{kind}_{doc['metadata']['name']}"
                        configs[config_key] = {
                            'kind': kind,
                            'metadata': doc['metadata'],
                            'spec': doc.get('spec', {}),
                            'data': doc.get('data', {}),
                            'hash': hashlib.md5(content.encode()).hexdigest()
                        }
            
            console.print(f"✅ 扫描到 {len(configs)} 个配置", style="green")
            return configs
            
        except Exception as e:
            console.print(f"❌ 扫描配置文件失败: {e}", style="red")
            return {}
    
    def compare_configs(self, old_configs: Dict, new_configs: Dict) -> Dict[str, Any]:
        """比较配置变更"""
        changes = {
            'added': [],
            'modified': [],
            'deleted': [],
            'unchanged': []
        }
        
        # 检查新增和修改的配置
        for key, new_config in new_configs.items():
            if key not in old_configs:
                changes['added'].append({
                    'key': key,
                    'config': new_config
                })
            elif old_configs[key]['hash'] != new_config['hash']:
                changes['modified'].append({
                    'key': key,
                    'old_config': old_configs[key],
                    'new_config': new_config
                })
            else:
                changes['unchanged'].append(key)
        
        # 检查删除的配置
        for key in old_configs:
            if key not in new_configs:
                changes['deleted'].append({
                    'key': key,
                    'config': old_configs[key]
                })
        
        return changes
    
    def scan_configmap(self, config_data: Dict) -> Dict[str, Any]:
        """扫描 ConfigMap 配置"""
        if config_data['kind'] != 'ConfigMap':
            return {}
        
        data = config_data.get('data', {})
        metadata = config_data.get('metadata', {})
        
        return {
            'name': metadata.get('name', 'unknown'),
            'namespace': metadata.get('namespace', 'default'),
            'data': data,
            'data_count': len(data),
            'keys': list(data.keys())
        }
    
    def scan_secret(self, secret_data: Dict) -> Dict[str, Any]:
        """扫描 Secret 配置"""
        if secret_data['kind'] != 'Secret':
            return {}
        
        data = secret_data.get('data', {})
        metadata = secret_data.get('metadata', {})
        
        return {
            'name': metadata.get('name', 'unknown'),
            'namespace': metadata.get('namespace', 'default'),
            'data_count': len(data),
            'keys': list(data.keys()),
            'type': secret_data.get('type', 'Opaque')
        }
    
    def scan_deployment(self, deployment_data: Dict) -> Dict[str, Any]:
        """扫描 Deployment 配置"""
        if deployment_data['kind'] != 'Deployment':
            return {}
        
        spec = deployment_data.get('spec', {})
        metadata = deployment_data.get('metadata', {})
        
        containers = spec.get('template', {}).get('spec', {}).get('containers', [])
        
        return {
            'name': metadata.get('name', 'unknown'),
            'namespace': metadata.get('namespace', 'default'),
            'replicas': spec.get('replicas', 1),
            'containers': [
                {
                    'name': container.get('name', 'unknown'),
                    'image': container.get('image', ''),
                    'ports': container.get('ports', []),
                    'env': container.get('env', [])
                }
                for container in containers
            ]
        }
    
    def extract_env_vars(self, config_data: Dict) -> List[str]:
        """提取环境变量"""
        env_vars = []
        
        if config_data['kind'] == 'ConfigMap':
            data = config_data.get('data', {})
            for key, value in data.items():
                env_vars.append(f"{key}={value}")
        
        elif config_data['kind'] == 'Deployment':
            spec = config_data.get('spec', {})
            containers = spec.get('template', {}).get('spec', {}).get('containers', [])
            
            for container in containers:
                env_list = container.get('env', [])
                for env in env_list:
                    name = env.get('name', '')
                    value = env.get('value', '')
                    if name and value:
                        env_vars.append(f"{name}={value}")
        
        return env_vars
    
    def validate_config(self, config_data: Dict) -> List[str]:
        """验证配置"""
        errors = []
        
        if 'metadata' not in config_data:
            errors.append("缺少 metadata 字段")
        
        if 'kind' not in config_data:
            errors.append("缺少 kind 字段")
        
        metadata = config_data.get('metadata', {})
        if 'name' not in metadata:
            errors.append("缺少 name 字段")
        
        # 根据类型进行特定验证
        kind = config_data.get('kind', '')
        
        if kind == 'ConfigMap':
            if 'data' not in config_data:
                errors.append("ConfigMap 缺少 data 字段")
        
        elif kind == 'Secret':
            if 'data' not in config_data:
                errors.append("Secret 缺少 data 字段")
        
        elif kind == 'Deployment':
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
        
        return errors
    
    def show_changes(self, changes: Dict[str, Any]):
        """显示配置变更"""
        console.print("📊 配置变更报告", style="blue")
        
        if changes['added']:
            console.print(f"➕ 新增配置 ({len(changes['added'])}):", style="green")
            for item in changes['added']:
                console.print(f"  - {item['key']}: {item['config']['kind']}")
        
        if changes['modified']:
            console.print(f"🔄 修改配置 ({len(changes['modified'])}):", style="yellow")
            for item in changes['modified']:
                console.print(f"  - {item['key']}: {item['new_config']['kind']}")
        
        if changes['deleted']:
            console.print(f"🗑️  删除配置 ({len(changes['deleted'])}):", style="red")
            for item in changes['deleted']:
                console.print(f"  - {item['key']}: {item['config']['kind']}")
        
        if changes['unchanged']:
            console.print(f"✅ 未变更配置 ({len(changes['unchanged'])}):", style="cyan")
            for key in changes['unchanged']:
                console.print(f"  - {key}")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        console.print("用法: python config_scanner.py <config_file>", style="red")
        sys.exit(1)
    
    config_file = sys.argv[1]
    scanner = ConfigScanner()
    
    # 扫描配置文件
    configs = scanner.scan_config_file(config_file)
    
    # 显示扫描结果
    table = Table(title="配置扫描结果")
    table.add_column("配置名称", style="cyan")
    table.add_column("类型", style="green")
    table.add_column("命名空间", style="yellow")
    table.add_column("状态", style="magenta")
    
    for key, config in configs.items():
        kind = config['kind']
        metadata = config['metadata']
        name = metadata.get('name', 'unknown')
        namespace = metadata.get('namespace', 'default')
        
        # 验证配置
        errors = scanner.validate_config(config)
        status = "✅ 有效" if not errors else f"❌ {len(errors)} 个错误"
        
        table.add_row(name, kind, namespace, status)
    
    console.print(table) 