sing-box 示例（基于 ConfigMap 配置）

本示例通过 ConfigMap 提供 `config.json`，以 Deployment 方式运行 `sing-box`，并通过 Service 暴露 1080 (SOCKS5) 与 7890 (HTTP) 端口。

目录结构
- `configmap.yaml`: 提供 `config.json` 内容
- `deployment.yaml`: 运行 `sing-box` 的 Deployment
- `service.yaml`: 暴露端口的 Service
- `kustomization.yaml`: 支持 `kubectl apply -k` 一键部署

快速开始

```bash
kubectl apply -k examples/sing-box
kubectl get pods -l app=sing-box

# 本地测试端口转发
kubectl port-forward svc/sing-box 1080:1080 &
kubectl port-forward svc/sing-box 7890:7890 &
```

自定义配置

编辑 `configmap.yaml` 中 `data.config.json`，例如添加出站代理与路由规则。默认配置仅做直连：

```json
{
  "inbounds": [
    { "type": "socks", "listen": "0.0.0.0", "listen_port": 1080, "sniff": true, "udp": true },
    { "type": "http",  "listen": "0.0.0.0", "listen_port": 7890 }
  ],
  "outbounds": [ { "type": "direct", "tag": "direct" } ],
  "route": { "rules": [ { "outbound": "direct" } ] }
}
```

提示
- 需外部访问时，可将 `service.yaml` 的 `type` 改为 `NodePort` 或配置 Ingress/LoadBalancer。
- 如需固定镜像版本，修改 `deployment.yaml` 中的镜像 tag（默认 `ghcr.io/sagernet/sing-box:latest`）。





