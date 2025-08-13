# Git Watcher 代理配置指南

## 🌐 代理环境变量设置

如果你的环境需要通过代理访问GitHub，请确保在Ray环境中设置以下环境变量：

### 常见代理变量
```bash
export http_proxy=http://proxy.company.com:8080
export https_proxy=http://proxy.company.com:8080
export HTTP_PROXY=http://proxy.company.com:8080
export HTTPS_PROXY=http://proxy.company.com:8080
export no_proxy=localhost,127.0.0.1,.local
export NO_PROXY=localhost,127.0.0.1,.local
```

### 在Ray/Docker环境中设置

1. **在启动Ray之前设置环境变量**：
   ```bash
   # 在启动nokube服务的机器上
   export https_proxy=http://your-proxy:port
   python src/deployer.py deploy examples/git-watcher/watcher-deployment-paserver.yaml
   ```

2. **通过systemd服务设置**（如果使用systemd）：
   ```ini
   [Service]
   Environment=https_proxy=http://your-proxy:port
   Environment=http_proxy=http://your-proxy:port
   ```

## 🔧 自动代理传递

现在watcher部署会自动：

1. ✅ **检测宿主机代理设置**
2. ✅ **传递到Docker容器内**
3. ✅ **显示当前代理配置**（用于调试）
4. ✅ **在git clone失败时显示代理信息**

## 📊 故障排除

### Git克隆超时
如果看到 `SSL connection timeout` 错误：

1. **检查代理设置**：
   ```bash
   env | grep -i proxy
   ```

2. **测试网络连通性**：
   ```bash
   curl -I https://github.com
   ```

3. **查看容器日志**：
   ```bash
   kubectl logs -n nokube-system -l app=git-watcher
   ```

### 代理认证
如果代理需要认证：
```bash
export https_proxy=http://username:password@proxy.company.com:8080
```

### 内网Git仓库
如果使用内网Git仓库，更新Secret中的仓库URL：
```yaml
repositories:
  - name: "internal-app"
    url: "https://git.internal.company.com/team/repo.git"
    # ...
```

## 🚀 验证代理配置

部署后检查容器日志应显示：
```
[时间] CTR xxx | 🌐 检查网络连接...
[时间] CTR xxx | 🔗 使用代理: https_proxy=http://proxy:8080
[时间] CTR xxx | 📥 克隆 nokube 代码仓库...
[时间] CTR xxx | ✅ 仓库克隆完成
```

如果显示 `🔗 未设置代理` 但你的环境需要代理，请检查环境变量设置。
