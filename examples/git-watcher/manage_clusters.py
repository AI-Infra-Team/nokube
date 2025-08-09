#!/usr/bin/env python3
"""
集群管理脚本
用于管理 NoKube 集群配置
"""

import sys
import os
import yaml
import json
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))

from etcd_manager import EtcdManager
from rich.console import Console
from rich.table import Table

console = Console()


# 集群管理示例
# 演示如何使用 NoKube 管理多个集群

import yaml
from src.etcd_manager import EtcdManager

def create_sample_clusters():
    """创建示例集群配置"""
    
    # Ray 生产环境集群
    ray_prod_config = {
        'name': 'ray-prod',
        'type': 'ray',
        'config': {
            'head_port': 10001,
            'dashboard_port': 8265,
            'max_workers': 20,
            'nodes': [
                {
                    'ssh_url': 'prod-head:10001',
                    'name': 'ray-prod-head',
                    'role': 'head',
                    'storage': {
                        'type': 'local',
                        'path': '/opt/nokube/data/ray/prod/head'
                    },
                    'users': [
                        {
                            'userid': 'prod-admin',
                            'password': 'prod123'
                        }
                    ]
                },
                {
                    'ssh_url': 'prod-worker-1:10002',
                    'name': 'ray-prod-worker-1',
                    'role': 'worker',
                    'storage': {
                        'type': 'local',
                        'path': '/opt/nokube/data/ray/prod/worker-1'
                    },
                    'users': [
                        {
                            'userid': 'worker-user',
                            'password': 'worker123'
                        }
                    ]
                }
            ]
        },
        'status': 'inactive',
        'created_at': '2023-01-01T00:00:00Z'
    }
    
    # Ray 测试环境集群
    ray_staging_config = {
        'name': 'ray-staging',
        'type': 'ray',
        'config': {
            'head_port': 10003,
            'dashboard_port': 8266,
            'max_workers': 10,
            'nodes': [
                {
                    'ssh_url': 'staging-head:10003',
                    'name': 'ray-staging-head',
                    'role': 'head',
                    'storage': {
                        'type': 'local',
                        'path': '/opt/nokube/data/ray/staging/head'
                    },
                    'users': [
                        {
                            'userid': 'staging-admin',
                            'password': 'staging123'
                        }
                    ]
                }
            ]
        },
        'status': 'inactive',
        'created_at': '2023-01-01T00:00:00Z'
    }
    
    return [ray_prod_config, ray_staging_config]

def setup_clusters():
    """设置集群"""
    manager = EtcdManager()
    
    # 创建示例集群
    clusters = create_sample_clusters()
    
    for cluster in clusters:
        name = cluster['name']
        cluster_type = cluster['type']
        config = cluster['config']
        
        print(f"创建集群: {name} ({cluster_type})")
        success = manager.add_cluster(name, cluster_type, config)
        
        if success:
            print(f"✅ 集群 {name} 创建成功")
        else:
            print(f"❌ 集群 {name} 创建失败")

def export_clusters():
    """导出集群配置"""
    manager = EtcdManager()
    
    # 导出到 YAML 文件
    export_file = 'exported_clusters.yaml'
    success = manager.export_clusters_to_yaml(export_file)
    
    if success:
        print(f"✅ 集群配置已导出到: {export_file}")
    else:
        print(f"❌ 导出失败")

if __name__ == '__main__':
    print("🚀 开始设置集群...")
    setup_clusters()
    
    print("\n📊 当前集群列表:")
    manager = EtcdManager()
    manager.show_clusters()
    
    print("\n📤 导出集群配置...")
    export_clusters() 