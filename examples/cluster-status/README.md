# 集群状态监控示例

本示例演示如何使用 NoKube 管理集群状态。

## 快速开始

### 1. 创建集群

```bash
# 创建 Ray 集群
nokube cluster new-or-update my-ray examples/cluster-status/ray-config.yaml

# 查看集群列表
nokube cluster list
```

### 2. 管理集群

```bash
# 删除集群
nokube cluster delete my-ray

# 获取集群信息
nokube cluster get my-ray
```

### 3. 集群状态管理

```bash
# 设置集群状态为活跃
nokube cluster status my-ray active

# 获取集群信息
nokube cluster get my-ray
```

## 配置文件说明

### Ray 集群

- `ray-config.yaml` - Ray 集群配置

### 配置结构

```yaml
# Ray 集群配置
ray_spec:
  dashboard_port: 8265
  max_workers: 10

# 节点列表
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