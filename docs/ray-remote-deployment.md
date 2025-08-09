# Ray 远程部署指南

本文档介绍如何使用 NoKube 进行 Ray 集群的远程部署。

## 概述

NoKube 支持通过 SSH 在远程节点上部署 Ray 集群，包括：

- 自动上传远程执行库
- 启动 head 节点和 worker 节点
- 监控集群状态
- 统一配置管理

## 架构设计

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   NoKube CLI    │    │   远程执行库     │    │   远程节点       │
│                 │───▶│   remote_lib    │───▶│   Ray 进程      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   配置文件       │    │   SSH/SCP       │    │   Ray 集群      │
│   ray-config.yaml│    │   连接          │    │   Head + Workers│
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 配置格式

### 基本配置结构

```yaml
# ray-config.yaml
ray_spec:
  dashboard_port: 8265
  max_workers: 10

nodes:
  - ssh_url: "host:port"
    name: "节点名称"
    role: "head|worker"
    ray_config:
      port: 10001                    # Ray 端口
      dashboard_port: 8265           # Dashboard 端口
      num_cpus: 4                   # CPU 数量
      object_store_memory: 1000000000 # 对象存储内存
    storage:
      type: "local"
      path: "/path/to/storage"
    users:
      - userid: "用户名"
        password: "密码"
      - userid: "用户名"
        sshkey: "SSH 公钥"
```

### 配置说明

#### 节点配置

- **ssh_url**: SSH 连接地址，格式为 `host:port`
- **name**: 节点名称，用于标识
- **role**: 节点角色，`head` 或 `worker`
- **ray_config**: Ray 进程配置
- **storage**: 存储配置
- **users**: 用户配置

#### Ray 配置参数

- **port**: Ray 服务端口（head 节点必需）
- **dashboard_port**: Dashboard 端口（head 节点必需）
- **num_cpus**: 分配的 CPU 数量
- **object_store_memory**: 对象存储内存（字节）

## 使用步骤

### 1. 准备远程节点

在远程节点上安装 Ray：

```bash
# 安装 Ray
pip install ray[serve]

# 验证安装
python -c "import ray; print(ray.__version__)"
```

### 2. 配置 SSH 连接

确保可以通过 SSH 连接到远程节点：

```bash
# 生成 SSH 密钥（如果没有）
ssh-keygen -t rsa -b 4096

# 复制公钥到远程节点
ssh-copy-id user@remote-server

# 测试连接
ssh user@remote-server "echo 'SSH 连接成功'"
```

### 3. 创建集群配置

创建 `ray-config.yaml` 文件：

```yaml
ray_spec:
  dashboard_port: 8265
  max_workers: 10

nodes:
  # Head 节点
  - ssh_url: "prod-server-1:22"
    name: "ray-head-prod"
    role: "head"
    ray_config:
      port: 10001
      dashboard_port: 8265
      num_cpus: 8
      object_store_memory: 2000000000
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/head"
    users:
      - userid: "prod-admin"
        password: "prod123"
  
  # Worker 节点
  - ssh_url: "prod-server-2:22"
    name: "ray-worker-1"
    role: "worker"
    ray_config:
      num_cpus: 16
      object_store_memory: 4000000000
    storage:
      type: "local"
      path: "/opt/nokube/data/ray/worker-1"
    users:
      - userid: "worker-user"
        password: "worker123"
```

### 4. 启动集群

```bash
# 启动 Ray 集群
nokube ray start --config ray-config.yaml
```

### 5. 查看状态

```bash
# 查看集群状态
nokube ray status --config ray-config.yaml
```

### 6. 停止集群

```bash
# 停止 Ray 集群
nokube ray stop --config ray-config.yaml
```

## 高级配置

### 多节点集群

```yaml
nodes:
  # Head 节点
  - ssh_url: "head-server:22"
    name: "ray-head"
    role: "head"
    ray_config:
      port: 10001
      dashboard_port: 8265
      num_cpus: 8
      object_store_memory: 2000000000
  
  # Worker 节点 1
  - ssh_url: "worker-1:22"
    name: "ray-worker-1"
    role: "worker"
    ray_config:
      num_cpus: 16
      object_store_memory: 4000000000
  
  # Worker 节点 2
  - ssh_url: "worker-2:22"
    name: "ray-worker-2"
    role: "worker"
    ray_config:
      num_cpus: 16
      object_store_memory: 4000000000
```

### 本地开发环境

```yaml
nodes:
  - ssh_url: "localhost:10001"
    name: "ray-head-local"
    role: "head"
    ray_config:
      port: 10001
      dashboard_port: 8265
      num_cpus: 4
      object_store_memory: 1000000000
    storage:
      type: "local"
      path: "/tmp/nokube/data/ray/head"
    users:
      - userid: "local-user"
        password: "local123"
```

## 故障排除

### 常见问题

1. **SSH 连接失败**
   ```bash
   # 检查 SSH 连接
   ssh -v user@remote-server
   
   # 检查 SSH 密钥
   ssh-add -l
   ```

2. **Ray 安装失败**
   ```bash
   # 在远程节点上重新安装 Ray
   pip uninstall ray
   pip install ray[serve]
   ```

3. **端口冲突**
   ```bash
   # 检查端口占用
   netstat -tlnp | grep :10001
   
   # 修改配置文件中的端口
   ray_config:
     port: 10002  # 使用不同端口
   ```

4. **权限问题**
   ```bash
   # 确保远程用户有足够权限
   sudo chown -R user:user /opt/nokube
   ```

### 调试模式

启用详细日志：

```bash
# 设置环境变量
export NOKUBE_DEBUG=1

# 启动集群
nokube ray start --config ray-config.yaml
```

## 最佳实践

1. **使用 SSH 密钥认证**：避免密码认证的安全风险
2. **配置防火墙**：确保 Ray 端口（10001）和 Dashboard 端口（8265）可访问
3. **监控资源使用**：定期检查 CPU 和内存使用情况
4. **备份配置**：定期备份集群配置文件
5. **版本管理**：使用版本控制管理配置文件

## 示例配置

完整示例配置请参考：
- `examples/remote-ray-config.yaml` - 远程部署示例
- `ray-template.yaml` - 本地开发模板 