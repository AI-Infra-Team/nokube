Registry 示例（Docker Distribution）

本示例在 Kubernetes 中部署一个私有镜像仓库（registry:2），使用 PVC 持久化存储，支持可选的 Basic Auth。

目录结构
- pvc.yaml：数据卷声明（10Gi，可按需调整）
- secret.yaml：Basic Auth 凭据（可选，需填入 htpasswd 的 base64）
- deployment.yaml：Deployment 配置（挂载数据卷与认证文件）
- service.yaml：ClusterIP Service 暴露 5000 端口
- kustomization.yaml：一键部署入口

部署

```bash
kubectl apply -k examples/registry
kubectl get pods -l app=registry
kubectl get svc registry
```

如需 NodePort 暴露：编辑 service.yaml 将 `type: ClusterIP` 改为 `NodePort` 并指定 `nodePort`。

配置 Basic Auth
- 生成 htpasswd（示例用户 user，密码 yourpassword）：
```bash
htpasswd -nbB user 'yourpassword' | base64 -w0
```
- 将生成的 base64 填入 `secret.yaml` 的 `data.htpasswd`。
- 重新应用：
```bash
kubectl apply -f examples/registry/secret.yaml
kubectl rollout restart deploy/registry
```

验证

```bash
kubectl port-forward deploy/registry 5000:5000 &
# 登录（如配置了 basic auth）
docker login localhost:5000
# 推送测试镜像
docker pull busybox:latest
docker tag busybox:latest localhost:5000/busybox:latest
docker push localhost:5000/busybox:latest
```

注意
- 如节点 Docker 默认不信任私有仓库，请为 Docker 配置 insecure registries 或 CA 证书。




