"""
YAML 转换器
将 Kubernetes YAML 转换为 Ray 任务
"""

import yaml
import json
from pathlib import Path
from rich.console import Console
from rich.table import Table

console = Console()


class YAMLConverter:
    """Kubernetes YAML 到 Ray 任务转换器"""
    
    def __init__(self):
        self.supported_kinds = ['Deployment', 'Service', 'Pod', 'ConfigMap', 'Secret']
    
    def convert_to_ray(self, yaml_file):
        """将 Kubernetes YAML 转换为 Ray 任务"""
        console.print(f"🔄 转换 YAML 文件: {yaml_file}", style="blue")
        
        if not Path(yaml_file).exists():
            console.print(f"❌ 文件不存在: {yaml_file}", style="red")
            return None
        
        try:
            # 读取 YAML 文件
            with open(yaml_file, 'r', encoding='utf-8') as f:
                yaml_content = f.read()
            
            # 解析 YAML
            documents = list(yaml.safe_load_all(yaml_content))
            
            ray_tasks = []
            
            for doc in documents:
                if doc and 'kind' in doc:
                    kind = doc['kind']
                    if kind in self.supported_kinds:
                        task = self._convert_resource(doc)
                        if task:
                            ray_tasks.append(task)
            
            if ray_tasks:
                console.print(f"✅ 成功转换 {len(ray_tasks)} 个资源", style="green")
                return ray_tasks
            else:
                console.print("⚠️  没有找到可转换的资源", style="yellow")
                return None
                
        except Exception as e:
            console.print(f"❌ 转换 YAML 时发生错误: {e}", style="red")
            return None
    
    def _convert_resource(self, resource):
        """转换单个 Kubernetes 资源"""
        kind = resource['kind']
        metadata = resource.get('metadata', {})
        name = metadata.get('name', 'unknown')
        namespace = metadata.get('namespace', 'default')
        
        console.print(f"  📦 转换 {kind}: {name}", style="cyan")
        
        if kind == 'Deployment':
            return self._convert_deployment(resource)
        elif kind == 'Service':
            return self._convert_service(resource)
        elif kind == 'Pod':
            return self._convert_pod(resource)
        elif kind == 'ConfigMap':
            return self._convert_configmap(resource)
        elif kind == 'Secret':
            return self._convert_secret(resource)
        else:
            console.print(f"  ⚠️  不支持的资源类型: {kind}", style="yellow")
            return None
    
    def _convert_deployment(self, deployment):
        """转换 Deployment 为 Ray 任务"""
        spec = deployment.get('spec', {})
        template = spec.get('template', {})
        containers = template.get('spec', {}).get('containers', [])
        
        tasks = []
        
        for container in containers:
            task = {
                'type': 'container',
                'name': container.get('name', 'unknown'),
                'image': container.get('image', ''),
                'command': container.get('command', []),
                'args': container.get('args', []),
                'env': container.get('env', []),
                'ports': container.get('ports', []),
                'resources': container.get('resources', {}),
                'volumeMounts': container.get('volumeMounts', [])
            }
            tasks.append(task)
        
        return {
            'type': 'deployment',
            'name': deployment['metadata']['name'],
            'namespace': deployment['metadata'].get('namespace', 'default'),
            'replicas': spec.get('replicas', 1),
            'tasks': tasks
        }
    
    def _convert_service(self, service):
        """转换 Service 为 Ray 服务"""
        spec = service.get('spec', {})
        
        return {
            'type': 'service',
            'name': service['metadata']['name'],
            'namespace': service['metadata'].get('namespace', 'default'),
            'selector': spec.get('selector', {}),
            'ports': spec.get('ports', []),
            'type': spec.get('type', 'ClusterIP')
        }
    
    def _convert_pod(self, pod):
        """转换 Pod 为 Ray 任务"""
        spec = pod.get('spec', {})
        containers = spec.get('containers', [])
        
        tasks = []
        
        for container in containers:
            task = {
                'type': 'container',
                'name': container.get('name', 'unknown'),
                'image': container.get('image', ''),
                'command': container.get('command', []),
                'args': container.get('args', []),
                'env': container.get('env', []),
                'ports': container.get('ports', []),
                'resources': container.get('resources', {}),
                'volumeMounts': container.get('volumeMounts', [])
            }
            tasks.append(task)
        
        return {
            'type': 'pod',
            'name': pod['metadata']['name'],
            'namespace': pod['metadata'].get('namespace', 'default'),
            'tasks': tasks
        }
    
    def _convert_configmap(self, configmap):
        """转换 ConfigMap 为 Ray 配置"""
        data = configmap.get('data', {})
        
        return {
            'type': 'configmap',
            'name': configmap['metadata']['name'],
            'namespace': configmap['metadata'].get('namespace', 'default'),
            'data': data
        }
    
    def _convert_secret(self, secret):
        """转换 Secret 为 Ray 配置"""
        data = secret.get('data', {})
        
        return {
            'type': 'secret',
            'name': secret['metadata']['name'],
            'namespace': secret['metadata'].get('namespace', 'default'),
            'data': data
        }
    
    def generate_ray_script(self, yaml_file, output_file=None):
        """生成 Ray 执行脚本"""
        ray_tasks = self.convert_to_ray(yaml_file)
        
        if not ray_tasks:
            return None
        
        if not output_file:
            output_file = Path(yaml_file).stem + '_ray.py'
        
        script_content = self._generate_script_content(ray_tasks)
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(script_content)
            
            console.print(f"✅ Ray 脚本已生成: {output_file}", style="green")
            return output_file
            
        except Exception as e:
            console.print(f"❌ 生成脚本时发生错误: {e}", style="red")
            return None
    
    def _generate_script_content(self, ray_tasks):
        """生成 Ray 脚本内容"""
        script = '''#!/usr/bin/env python3
"""
自动生成的 Ray 执行脚本
"""

import ray
import docker
import time
from rich.console import Console

console = Console()

@ray.remote
def run_container_task(task_config):
    """运行容器任务"""
    import docker
    
    client = docker.from_env()
    
    # 准备容器配置
    container_config = {
        'image': task_config['image'],
        'detach': True,
        'environment': {env['name']: env['value'] for env in task_config.get('env', [])},
        'ports': {f"{port['containerPort']}/tcp": port['containerPort'] for port in task_config.get('ports', [])}
    }
    
    if task_config.get('command'):
        container_config['command'] = task_config['command']
    
    # 运行容器
    container = client.containers.run(**container_config)
    
    return {
        'container_id': container.id,
        'status': container.status,
        'name': task_config['name']
    }

def main():
    """主函数"""
    console.print("🚀 启动 Ray 集群...", style="blue")
    
    # 初始化 Ray
    ray.init(ignore_reinit_error=True)
    
    console.print("✅ Ray 集群已启动", style="green")
    
    # 部署任务
    tasks = []
'''
        
        # 添加任务部署代码
        for task in ray_tasks:
            if task['type'] == 'deployment':
                script += f'''
    # 部署 {task['name']}
    console.print("📦 部署 {task['name']}...", style="blue")
    for i in range({task['replicas']}):
        for container_task in {json.dumps(task['tasks'], indent=8)}:
            task_ref = run_container_task.remote(container_task)
            tasks.append(task_ref)
'''
            elif task['type'] == 'pod':
                script += f'''
    # 部署 Pod {task['name']}
    console.print("📦 部署 Pod {task['name']}...", style="blue")
    for container_task in {json.dumps(task['tasks'], indent=8)}:
        task_ref = run_container_task.remote(container_task)
        tasks.append(task_ref)
'''
        
        script += '''
    # 等待所有任务完成
    console.print("⏳ 等待任务完成...", style="blue")
    results = ray.get(tasks)
    
    # 显示结果
    console.print("📊 部署结果:", style="green")
    for result in results:
        console.print(f"  ✅ {result['name']}: {result['container_id']}", style="green")
    
    console.print("✅ 所有任务部署完成", style="green")

if __name__ == "__main__":
    main()
'''
        
        return script 