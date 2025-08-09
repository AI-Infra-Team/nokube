# Git 仓库监控示例

本示例演示如何构建一个 Git 仓库监控系统，支持：

- 扫描目标 Git 仓库列表
- 监控仓库内的 YAML 配置文件
- 发现变更时自动更新部署
- 支持指定目标集群进行部署

## 功能特性

- **多仓库监控**: 支持同时监控多个 Git 仓库
- **自动部署**: 检测到变更时自动触发部署
- **多集群支持**: 支持部署到不同的目标集群
- **配置验证**: 自动验证 YAML 配置的有效性
- **通知机制**: 支持 Slack、邮件等通知方式

## 快速开始

### 1. 配置仓库列表

编辑 `repos.yaml` 文件，配置要监控的仓库：

```yaml
repositories:
  - name: "my-app"
    url: "https://github.com/username/my-app.git"
    branch: "main"
    path: "k8s/"
    target_cluster: "ray-prod"
    deployment_target: "ray"
    auto_deploy: true
```

### 2. 设置集群

```bash
# 创建示例集群
python examples/git-watcher/manage_clusters.py

# 查看集群列表
nokube cluster list

# 测试集群连接
python examples/git-watcher/manage_clusters.py test ray-prod
```

### 3. 启动监控

```bash
# 启动 Git 仓库监控
python examples/git-watcher/git_watcher.py

# 或使用 nokube 命令
nokube cluster new-or-update ray-prod examples/cluster-status/ray-config.yaml
```

## 配置说明

### 仓库配置 (repos.yaml)

```yaml
repositories:
  - name: "应用名称"
    url: "Git 仓库 URL"
    branch: "分支名称"
    path: "配置文件路径"
    target_cluster: "ray-prod"  # 指定目标集群
    deployment_target: "ray"     # 部署目标类型
    auto_deploy: true
    webhook_enabled: true
    notification:
      slack_webhook: "Slack Webhook URL"
      email: "通知邮箱"
```

### 集群配置

```yaml
# ray-config.yaml
ray_spec:
  dashboard_port: 8265
  max_workers: 10

nodes:
  - ssh_url: "localhost:10001"
    name: "ray-head-node"
    role: "head"
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/head"
    users:
      - userid: "ray-admin"
        password: "ray123"
```

## 支持的集群类型

- **Ray**: 分布式计算集群

## 部署目标

- **ray**: 部署到 Ray 集群

## 使用示例

### 基本使用

```python
from examples.git_watcher.git_watcher import GitWatcher

# 创建监控器
watcher = GitWatcher('repos.yaml')

# 启动监控
watcher.start_monitoring()
```

### 自定义部署器

```python
from examples.git_watcher.auto_deployer import AutoDeployer

# 创建部署器
deployer = AutoDeployer()

# 部署配置
config_data = {
    'kind': 'Deployment',
    'metadata': {'name': 'my-app'},
    'spec': {...}
}

# 部署到 Ray 集群
success = deployer.deploy_to_target('my-app', config_data, 'ray')
```

### 集群管理

```python
from src.etcd_manager import EtcdManager

# 创建集群管理器
manager = EtcdManager()

# 添加集群
manager.add_cluster("my-cluster", "ray", {
    "head_port": 10001,
    "dashboard_port": 8265,
    "max_workers": 10
})

# 启动集群
manager.start_cluster("my-cluster")
```

## 高级功能

### 条件部署

```python
# 根据目标集群选择部署方式
if target_cluster == "ray-prod":
    self.deploy_to_ray(config_data)
elif target_cluster == "ray-staging":
    self.deploy_to_ray(config_data)
```

### 配置验证

```yaml
# 示例 YAML 配置
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: my-app
        image: my-app:latest
        ports:
        - containerPort: 8080
```

## 故障排除

### 常见问题

1. **集群连接失败**
   - 检查集群是否正在运行
   - 验证网络连接
   - 确认认证信息正确

2. **部署失败**
   - 检查 YAML 配置语法
   - 验证资源配额
   - 查看详细错误日志

3. **监控不工作**
   - 检查 Git 仓库权限
   - 验证 Webhook 配置
   - 确认文件路径正确

## 许可证

本示例遵循 MIT 许可证。 