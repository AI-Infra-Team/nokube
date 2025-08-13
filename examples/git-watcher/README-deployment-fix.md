# Git Watcher 容器持续运行修复

## 问题分析

原先的 watcher 容器在安装完依赖后就退出了，主要原因：

1. **启动命令问题**：原来的启动脚本缺少前台运行机制
2. **错误处理不当**：没有足够的错误处理和调试信息
3. **健康检查缺失**：没有合适的健康检查机制
4. **配置文件创建逻辑**：从环境变量创建配置文件的逻辑不够健壮

## 修复内容

### 1. 改进启动脚本

- 使用 `exec` 命令确保Python进程在前台运行
- 添加详细的启动日志输出
- 改进错误处理和依赖安装流程
- 添加配置文件检查和创建逻辑

### 2. 添加健康检查

```yaml
# 存活检查：确保Python进程在运行
livenessProbe:
  exec:
    command:
    - /bin/sh
    - -c
    - "ps aux | grep -v grep | grep 'start_watcher.py' || exit 1"
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 10
  failureThreshold: 3

# 就绪检查：确保配置文件存在
readinessProbe:
  exec:
    command:
    - /bin/sh  
    - -c
    - "test -f /app/data/watcher-state.json || test -f $CONFIG_PATH"
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### 3. 改进配置管理

- 添加 GitHub token 支持
- 简化仓库配置
- 增加资源限制以防止OOM

### 4. 重启策略

```yaml
restartPolicy: Always
```

## 部署步骤

1. **设置GitHub Token**：
   ```bash
   # 编辑Secret，替换为真实的GitHub token
   kubectl edit secret git-watcher-config -n nokube-system
   ```

2. **应用配置**：
   ```bash
   kubectl apply -f watcher-deployment-paserver.yaml
   ```

3. **检查状态**：
   ```bash
   # 查看Pod状态
   kubectl get pods -n nokube-system -l app=git-watcher
   
   # 查看日志
   kubectl logs -n nokube-system -l app=git-watcher -f
   ```

## 监控检查

容器启动后会显示以下日志：

```
🚀 启动 Git Watcher 容器...
📥 克隆 nokube 仓库...
📦 安装Python依赖...
🔍 检查配置文件: /app/config/repos.yaml
📋 配置文件内容预览:
🎯 启动 Git 仓库监控器...
🚀 启动 Git 仓库监控器
👀 开始监控仓库: pa-server-sing-box (间隔: 300s)
✅ 监控 1 个仓库
```

## 主要改进点

1. **持续运行**：使用 `exec` 确保主进程持续运行
2. **健壮性**：添加健康检查和自动重启
3. **可观测性**：详细的启动日志和状态输出
4. **配置灵活性**：支持环境变量和Secret配置
5. **资源管理**：合理的资源限制和请求

## 注意事项

- 部署前请替换 `github-token` 为真实的GitHub访问令牌
- 确保 `nokube-system` 命名空间存在
- 监控容器日志确保正常运行
