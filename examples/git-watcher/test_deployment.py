#!/usr/bin/env python3
"""
测试部署脚本
用于测试 Git 仓库监控和自动部署功能
"""

import sys
import os
import yaml
import tempfile
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))

from config_scanner import ConfigScanner
from auto_deployer import AutoDeployer
from rich.console import Console
from rich.table import Table

console = Console()


def create_test_config():
    """创建测试配置"""
    test_config = {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {
            'name': 'test-app',
            'namespace': 'default'
        },
        'spec': {
            'replicas': 2,
            'selector': {
                'matchLabels': {
                    'app': 'test-app'
                }
            },
            'template': {
                'metadata': {
                    'labels': {
                        'app': 'test-app'
                    }
                },
                'spec': {
                    'containers': [
                        {
                            'name': 'test-app',
                            'image': 'nginx:alpine',
                            'ports': [
                                {
                                    'containerPort': 80
                                }
                            ],
                            'env': [
                                {
                                    'name': 'APP_ENV',
                                    'value': 'test'
                                },
                                {
                                    'name': 'APP_VERSION',
                                    'value': '1.0.0'
                                }
                            ]
                        }
                    ]
                }
            }
        }
    }
    
    return test_config


def create_test_configmap():
    """创建测试 ConfigMap"""
    test_configmap = {
        'apiVersion': 'v1',
        'kind': 'ConfigMap',
        'metadata': {
            'name': 'test-config',
            'namespace': 'default'
        },
        'data': {
            'app.config': 'test-configuration',
            'database.url': 'localhost:5432',
            'redis.host': 'localhost:6379'
        }
    }
    
    return test_configmap


def test_config_scanner():
    """测试配置扫描器"""
    console.print("🧪 测试配置扫描器", style="blue")
    
    scanner = ConfigScanner()
    
    # 创建测试配置
    test_config = create_test_config()
    test_configmap = create_test_configmap()
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump([test_config, test_configmap], f)
        temp_file = f.name
    
    try:
        # 扫描配置
        configs = scanner.scan_config_file(temp_file)
        
        # 显示结果
        table = Table(title="配置扫描测试结果")
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
        
        # 测试配置比较
        old_configs = {}
        changes = scanner.compare_configs(old_configs, configs)
        scanner.show_changes(changes)
        
        return True
        
    finally:
        # 清理临时文件
        os.unlink(temp_file)


def test_auto_deployer():
    """测试自动部署器"""
    console.print("🧪 测试自动部署器", style="blue")
    
    deployer = AutoDeployer()
    
    # 创建测试配置
    test_config = create_test_config()
    test_configmap = create_test_configmap()
    
    # 测试部署验证
    console.print("📋 测试配置验证", style="blue")
    
    validation_errors = deployer._validate_deployment_config(test_config)
    if validation_errors:
        console.print(f"❌ 配置验证失败: {validation_errors}", style="red")
        return False
    else:
        console.print("✅ 配置验证通过", style="green")
    
    # 测试部署点创建
    console.print("📌 测试部署点创建", style="blue")
    
    deployment_id = deployer._create_deployment_point('test-repo', test_config)
    console.print(f"✅ 部署点创建成功: {deployment_id}", style="green")
    
    # 显示部署历史
    deployer.show_deployment_history()
    deployer.show_rollback_points()
    
    return True


def test_integration():
    """测试集成功能"""
    console.print("🧪 测试集成功能", style="blue")
    
    # 创建测试配置
    test_config = create_test_config()
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        temp_file = f.name
    
    try:
        # 测试配置扫描
        scanner = ConfigScanner()
        configs = scanner.scan_config_file(temp_file)
        
        if not configs:
            console.print("❌ 配置扫描失败", style="red")
            return False
        
        # 测试自动部署
        deployer = AutoDeployer()
        
        # 注意：这里不实际部署，只是测试流程
        console.print("⚠️  跳过实际部署（测试模式）", style="yellow")
        
        # 验证配置
        validation_errors = deployer._validate_deployment_config(test_config)
        if validation_errors:
            console.print(f"❌ 集成测试失败: {validation_errors}", style="red")
            return False
        
        console.print("✅ 集成测试通过", style="green")
        return True
        
    finally:
        # 清理临时文件
        os.unlink(temp_file)


def main():
    """主函数"""
    console.print("🚀 开始测试 Git 仓库监控和自动部署功能", style="blue")
    
    tests = [
        ("配置扫描器", test_config_scanner),
        ("自动部署器", test_auto_deployer),
        ("集成测试", test_integration)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        console.print(f"\n🧪 运行测试: {test_name}", style="blue")
        try:
            success = test_func()
            results.append((test_name, success))
            if success:
                console.print(f"✅ {test_name} 测试通过", style="green")
            else:
                console.print(f"❌ {test_name} 测试失败", style="red")
        except Exception as e:
            console.print(f"❌ {test_name} 测试异常: {e}", style="red")
            results.append((test_name, False))
    
    # 显示测试结果
    console.print("\n📊 测试结果汇总", style="blue")
    
    table = Table(title="测试结果")
    table.add_column("测试名称", style="cyan")
    table.add_column("结果", style="green")
    
    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        table.add_row(test_name, status)
    
    console.print(table)
    
    # 统计结果
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    console.print(f"\n📈 测试统计: {passed}/{total} 通过", style="blue")
    
    if passed == total:
        console.print("🎉 所有测试通过！", style="green")
        return 0
    else:
        console.print("⚠️  部分测试失败", style="yellow")
        return 1


if __name__ == '__main__':
    sys.exit(main()) 