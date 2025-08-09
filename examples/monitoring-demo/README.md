# NoKube 监控演示

这个演示展示了 NoKube 的完整监控和部署功能，包括集群管理、应用部署、监控仪表板和告警系统。

## 功能特性

### 🚀 集群管理
- **多集群支持**: 支持 Ray 和 Kubernetes 集群
- **集群扩展**: 动态扩展集群节点
- **健康检查**: 实时监控集群状态
- **配置管理**: 统一的集群配置管理

### 📊 监控系统
- **实时仪表板**: 可视化的监控界面
- **系统指标**: CPU、内存、磁盘使用率监控
- **集群指标**: Ray 集群状态监控
- **部署指标**: 应用部署状态监控
- **告警系统**: 自动告警和通知

### 🔧 部署管理
- **多种部署类型**: Ray Job、Service、Workflow、Dataset
- **自动扩缩容**: 基于负载的自动扩展
- **滚动更新**: 零停机时间更新
- **回滚功能**: 快速回滚到之前版本
- **健康检查**: 部署健康状态监控

### 🔒 安全功能
- **认证授权**: JWT 认证和 RBAC 授权
- **网络策略**: 细粒度的网络访问控制
- **密钥管理**: 安全的密钥存储和管理
- **审计日志**: 完整的操作审计

## 快速开始

### 1. 安装 NoKube

```bash
# 使用 uv 安装
uv pip install -e . --system

# 或使用 pip 安装
pip install -e .
```

### 2. 创建集群

```bash
# 创建 Ray 集群
nokube cluster new-or-update ray-demo templates/ray-cluster.yaml

# 创建 Kubernetes 集群
nokube cluster new-or-update k8s-demo templates/k8s-cluster.yaml
```

### 3. 启动监控仪表板

```bash
# 启动实时监控仪表板
nokube monitor dashboard

# 或使用简单监控
nokube monitor watch --interval 10
```

### 4. 部署应用

```bash
# 部署演示应用
nokube deploy apply app.yaml --target ray

# 查看部署状态
nokube deploy status --target ray
```

### 5. 管理部署

```bash
# 扩展部署
nokube deploy scale ml-inference-service 5 --target ray

# 检查健康状态
nokube deploy health ml-inference-service --target ray

# 查看日志
nokube deploy logs ml-inference-service --target ray --lines 50

# 回滚部署
nokube deploy rollback ml-inference-service v1.0.0 --target ray
```

## 集群管理命令

### 集群操作

```bash
# 列出所有集群
nokube cluster list

# 查看集群详情
nokube cluster get ray-demo

# 检查集群健康状态
nokube cluster health ray-demo

# 扩展集群
nokube cluster scale ray-demo --workers 5

# 重启集群
nokube cluster restart ray-demo

# 删除集群
nokube cluster delete ray-demo
```

### 集群配置

```bash
# 查看集群配置
nokube cluster config ray-demo --format yaml

# 导入集群配置
nokube cluster import-config ray-demo config.yaml

# 导出集群配置
nokube cluster export clusters.yaml
```

## 监控命令

### 监控仪表板

```bash
# 启动完整监控仪表板
nokube monitor dashboard

# 实时监控（简单模式）
nokube monitor watch --interval 5

# 系统健康检查
nokube monitor health

# 导出指标数据
nokube monitor export-metrics metrics.json
```

### 监控配置

监控系统支持以下配置：

- **指标收集**: CPU、内存、磁盘、网络使用率
- **告警规则**: 自定义告警条件和阈值
- **通知方式**: 邮件、Slack、PagerDuty
- **日志管理**: 结构化日志和日志轮转

## 部署类型

### Ray Job
用于批处理任务，支持定时执行和资源限制。

```yaml
- name: "data-processing-job"
  type: "ray_job"
  entrypoint: "python data_processor.py"
  runtime_env:
    pip: ["pandas", "numpy"]
  schedule: "0 2 * * *"  # 每天凌晨2点执行
```

### Ray Service
用于在线服务，支持负载均衡和健康检查。

```yaml
- name: "ml-inference-service"
  type: "ray_service"
  import_path: "ml_service:MLInferenceService"
  replicas: 3
  health_check:
    enabled: true
    endpoint: "/health"
```

### Ray Workflow
用于复杂的工作流，支持状态管理和容错。

```yaml
- name: "ml-training-workflow"
  type: "ray_workflow"
  workflow_id: "ml-training-pipeline"
  entrypoint: "python ml_training_workflow.py"
```

### Ray Dataset
用于数据处理管道，支持大规模数据转换。

```yaml
- name: "data-pipeline"
  type: "ray_dataset"
  data_source: "s3://nokube-demo/data/"
  format: "parquet"
```

## 配置示例

### 集群配置

```yaml
# Ray 集群配置
ray_spec:
  version: "2.8.0"
  dashboard_port: 8265
  max_workers: 10

nodes:
  - ssh_url: "localhost:22"
    name: "ray-head-node"
    role: "head"
    ray_config:
      port: 10001
      num_cpus: 4
      object_store_memory: 2000000000
```

### 监控配置

```yaml
monitoring:
  metrics:
    enabled: true
    interval: "30s"
    exporters:
      - type: "prometheus"
        endpoint: "/metrics"
  
  alerts:
    - name: "high_cpu_usage"
      condition: "cpu_usage > 80"
      duration: "5m"
      severity: "warning"
```

### 安全配置

```yaml
security:
  authentication:
    type: "jwt"
    secret: "your-jwt-secret"
  
  authorization:
    enabled: true
    roles:
      - name: "admin"
        permissions: ["read", "write", "delete"]
```

## 最佳实践

### 1. 集群设计
- 根据工作负载选择合适的集群类型
- 合理配置资源限制和请求
- 使用节点亲和性和反亲和性优化调度

### 2. 监控策略
- 设置合理的告警阈值
- 配置多种通知方式
- 定期检查和优化监控规则

### 3. 部署管理
- 使用滚动更新避免服务中断
- 配置健康检查和自动恢复
- 实施蓝绿部署或金丝雀发布

### 4. 安全考虑
- 启用 RBAC 和网络策略
- 使用密钥管理存储敏感信息
- 定期审计和更新安全配置

## 故障排除

### 常见问题

1. **集群启动失败**
   ```bash
   # 检查集群状态
   nokube cluster health ray-demo
   
   # 查看集群日志
   nokube cluster logs ray-demo
   ```

2. **部署失败**
   ```bash
   # 检查部署状态
   nokube deploy status --target ray
   
   # 查看部署日志
   nokube deploy logs deployment-name --target ray
   ```

3. **监控数据异常**
   ```bash
   # 检查监控系统
   nokube monitor health
   
   # 导出指标进行分析
   nokube monitor export-metrics debug.json
   ```

### 调试技巧

- 使用 `--verbose` 参数获取详细日志
- 检查系统资源使用情况
- 验证网络连接和防火墙设置
- 查看集群和部署的配置是否正确

## 扩展开发

### 添加新的部署类型

1. 在 `src/deployer.py` 中添加新的部署方法
2. 在 CLI 中添加相应的命令
3. 更新文档和示例

### 自定义监控指标

1. 在 `src/monitor.py` 中添加新的指标收集器
2. 配置告警规则和通知方式
3. 更新监控仪表板

### 集成外部系统

- **Prometheus**: 指标收集和存储
- **Grafana**: 可视化仪表板
- **AlertManager**: 告警管理
- **Elasticsearch**: 日志聚合和分析

## 贡献指南

欢迎贡献代码和文档！

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 创建 Pull Request

## 许可证

MIT License - 详见 [LICENSE](../LICENSE) 文件。 