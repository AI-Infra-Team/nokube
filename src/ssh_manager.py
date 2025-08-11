#!/usr/bin/env python3
"""
SSH 连接管理器
使用 paramiko 库进行 SSH 连接和文件传输
"""

import paramiko
import os
import tempfile
import shutil
import shlex
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from rich.console import Console

console = Console()


class SSHManager:
    """SSH 连接管理器"""
    
    def __init__(self):
        self.client = None
        self.sftp = None
        self.password: Optional[str] = None
        # 记录已成功完成 sudo 免密 ensure 的目标，键为 "username@host:port"
        self._ensured_targets: set[str] = set()
    
    def connect(self, host: str, port: int = 22, username: str = None, password: str = None, key_filename: str = None) -> bool:
        """建立 SSH 连接"""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.password = password
            
            # 尝试使用密钥文件
            if key_filename and os.path.exists(key_filename):
                self.client.connect(host, port=port, username=username, key_filename=key_filename)
            elif password:
                self.client.connect(host, port=port, username=username, password=password)
            else:
                # 尝试使用默认密钥
                self.client.connect(host, port=port, username=username)
            
            # 创建 SFTP 客户端
            self.sftp = self.client.open_sftp()
            
            console.print(f"✅ SSH 连接到 {host}:{port} 成功", style="green")
            # 首次访问该目标用户时，尝试确保 sudo 免密（lazy ensure）
            self._lazy_ensure_passwordless_sudo(host=host, port=port, username=username)
            return True
            
        except Exception as e:
            console.print(f"❌ SSH 连接失败: {e}", style="red")
            return False

    def _lazy_ensure_passwordless_sudo(self, host: str, port: int, username: Optional[str] = None) -> bool:
        """在首次连接某个 user@host:port 时，尝试确保 sudo 免密。

        不抛出异常：失败仅记录日志并返回 False，不影响后续流程。
        """
        try:
            if not self.client:
                return False

            target_username = username
            if not target_username:
                stdin, stdout, stderr = self.client.exec_command("whoami", get_pty=False)
                who = (stdout.read().decode("utf-8") or "").strip() or "unknown"
                _ = stderr.read()
                _ = stdout.channel.recv_exit_status()
                target_username = who

            target_key = f"{target_username}@{host}:{port}"
            if target_key in self._ensured_targets:
                return True

            ensured_ok = self.ensure_passwordless_sudo(target_username=target_username)
            if ensured_ok:
                self._ensured_targets.add(target_key)
                return True

            console.print(
                f"⚠️  sudo 免密 ensure 未完成（将于后续有密码时再试）: {target_key}",
                style="yellow",
            )
            return False
        except Exception as _e:
            logging.debug(f"lazy ensure_passwordless_sudo failed: {_e}")
            return False
    
    def disconnect(self):
        """断开 SSH 连接"""
        if self.sftp:
            self.sftp.close()
        if self.client:
            self.client.close()
    
    def _is_root(self):
        """检查远程用户是否为 root"""
        try:
            # 直接执行命令，不通过 execute_command 避免循环调用
            if not self.client:
                return False
            
            stdin, stdout, stderr = self.client.exec_command("id -u", get_pty=False)
            stdout_str = stdout.read().decode('utf-8')
            stderr_str = stderr.read().decode('utf-8')
            exit_status = stdout.channel.recv_exit_status()
            # 如果是后台启动命令，获取返回的 PID 便于日志提示
            if 'nohup sh -c' in locals().get('wrapped_command', '') and exit_status == 0:
                try:
                    pid_text = stdout.read().decode('utf-8').strip()
                    if pid_text:
                        console.print(f"📌 后台进程 PID: {pid_text}", style="yellow")
                except Exception:
                    pass
            
            if exit_status == 0 and stdout_str.strip() == "0":
                return True
            return False
        except Exception:
            return False
    
    def _get_sudo_prefix(self):
        """获取 sudo 前缀（如果不是 root 用户）"""
        return "sudo " if not self._is_root() else ""
    
    def _sudo_requires_password(self) -> bool:
        """检测远程 sudo 是否需要密码 (非交互)。返回 True 表示需要密码。"""
        try:
            if not self.client:
                return True
            # -n: non-interactive 模式，若需要密码将立即返回非 0
            stdin, stdout, stderr = self.client.exec_command("sudo -n true", get_pty=False)
            _ = stdout.read()
            _ = stderr.read()
            exit_status = stdout.channel.recv_exit_status()
            return exit_status != 0
        except Exception:
            return True

    def _run_sudo_with_password(self, command: str, get_pty: bool = False) -> Tuple[int, str, str]:
        """使用 sudo -S 并通过 stdin 提供密码执行命令。返回 (exit, stdout, stderr)。"""
        if not self.client:
            return 1, "", "SSH 未连接"
        if not self.password:
            return 1, "", "缺少 sudo 密码，无法执行提权命令"
        sudo_cmd = f"sudo -S -p '' bash -lc {shlex.quote(command)}"
        stdin, stdout, stderr = self.client.exec_command(sudo_cmd, get_pty=get_pty)
        try:
            stdin.write((self.password or "") + "\n")
            stdin.flush()
        except Exception:
            pass
        out = stdout.read().decode('utf-8', errors='ignore')
        err = stderr.read().decode('utf-8', errors='ignore')
        exit_status = stdout.channel.recv_exit_status()
        return exit_status, out, err

    def ensure_passwordless_sudo(self, target_username: Optional[str] = None) -> bool:
        """确保远程用户 sudo 免密。如果当前需要密码，则在 /etc/sudoers.d 添加 NOPASSWD 规则。

        返回 True 表示无需密码或配置成功；False 表示配置失败（缺少密码或写入失败）。"""
        # logging.info(f"ensure_passwordless_sudo: {target_username}")
        print(f"ensure_passwordless_sudo: {target_username}")
        
        try:
            if not self.client:
                console.print("❌ SSH 未连接，无法检查 sudo", style="red")
                return False

            # 无需密码则直接通过
            if not self._sudo_requires_password():
                console.print("✅ 远程 sudo 已免密", style="green")
                return True

            # 获取远程用户名
            if not target_username:
                stdin, stdout, stderr = self.client.exec_command("whoami", get_pty=False)
                username_out = stdout.read().decode('utf-8').strip()
                _ = stderr.read()
                _ = stdout.channel.recv_exit_status()
                target_username = username_out or ""

            if not target_username:
                console.print("❌ 无法确定远程用户名", style="red")
                return False

            if not self.password:
                console.print("❌ 需要 sudo 密码来配置免密，但未提供 SSH 密码", style="red")
                return False

            # 准备 sudoers.d 条目
            filename = f"/etc/sudoers.d/90-nokube-{target_username}"
            tmpfile = f"/tmp/90-nokube-{target_username}"
            content = f"{target_username} ALL=(ALL) NOPASSWD: ALL\n"

            # 上传临时文件到 /tmp
            try:
                if not self.sftp:
                    self.sftp = self.client.open_sftp()
                with self.sftp.file(tmpfile, 'w') as f:
                    f.write(content)
                # 给出保守权限，最终目标文件会设为 440
                self.sftp.chmod(tmpfile, 0o644)
            except Exception as e:
                console.print(f"❌ 上传 sudoers 临时文件失败: {e}", style="red")
                return False

            # 使用 visudo 校验并安装到 /etc/sudoers.d
            validate_cmd = f"visudo -cf {shlex.quote(tmpfile)}"
            exit_code, out, err = self._run_sudo_with_password(validate_cmd)
            if exit_code != 0:
                console.print(f"❌ visudo 校验失败: {err or out}", style="red")
                return False

            install_cmd = (
                f"install -o root -g root -m 440 {shlex.quote(tmpfile)} {shlex.quote(filename)}"
            )
            exit_code, out, err = self._run_sudo_with_password(install_cmd)
            if exit_code != 0:
                console.print(f"❌ 安装 sudoers 条目失败: {err or out}", style="red")
                return False

            # 再次验证免密
            if self._sudo_requires_password():
                console.print("❌ 配置后仍需要 sudo 密码", style="red")
                return False

            console.print("✅ 已配置 sudo 免密", style="green")
            return True
        except Exception as e:
            console.print(f"❌ 配置 sudo 免密时异常: {e}", style="red")
            return False

    def _check_path_permissions(self, remote_path: str) -> Tuple[bool, str]:
        """检查远程路径权限"""
        try:
            # 直接执行命令，避免循环调用
            if not self.client:
                return False, "SSH 连接未建立"
            
            # 获取路径信息
            stdin, stdout, stderr = self.client.exec_command(f"ls -ld '{remote_path}' 2>/dev/null || echo 'PATH_NOT_EXISTS'", get_pty=False)
            stdout_str = stdout.read().decode('utf-8')
            stderr_str = stderr.read().decode('utf-8')
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status != 0:
                return False, f"无法检查路径权限: {stderr_str}"
            
            if stdout_str.strip() == "PATH_NOT_EXISTS":
                # 路径不存在，检查父目录权限
                parent_dir = os.path.dirname(remote_path)
                if parent_dir == remote_path:  # 根目录
                    return False, f"路径不存在且无法创建: {remote_path}"
                
                return self._check_path_permissions(parent_dir)
            
            # 解析权限信息
            parts = stdout_str.strip().split()
            if len(parts) < 3:
                return False, f"无法解析权限信息: {stdout_str}"
            
            permissions = parts[0]
            owner = parts[2]
            
            # 检查当前用户
            stdin, stdout, stderr = self.client.exec_command("whoami", get_pty=False)
            user_stdout = stdout.read().decode('utf-8')
            user_stderr = stderr.read().decode('utf-8')
            user_exit_status = stdout.channel.recv_exit_status()
            
            if user_exit_status != 0:
                return False, f"无法获取当前用户信息: {user_stderr}"
            
            current_user = user_stdout.strip()
            
            # 检查是否为目录所有者或有写权限
            if owner == current_user:
                # 检查写权限
                if permissions[2] == 'w':
                    return True, f"用户 {current_user} 对 {remote_path} 有写权限"
                else:
                    return False, f"用户 {current_user} 对 {remote_path} 无写权限 (权限: {permissions})"
            else:
                # 检查其他用户写权限
                if permissions[8] == 'w':
                    return True, f"用户 {current_user} 对 {remote_path} 有写权限 (其他用户权限)"
                else:
                    return False, f"用户 {current_user} 对 {remote_path} 无写权限 (权限: {permissions}, 所有者: {owner})"
                    
        except Exception as e:
            return False, f"检查权限时发生错误: {e}"
    
    def execute_command(
        self,
        command: str,
        realtime_output: bool = True,
        get_pty: bool = True,
        extra_env: Optional[Dict[str, str]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Tuple[bool, str, str]:
        """执行远程命令"""
        try:
            if not self.client:
                return False, "", "SSH 连接未建立"
            
            # 检查是否需要 sudo
            original_command = command
            if not self._is_root() and not command.startswith("sudo"):
                # 对于某些需要权限的命令，自动添加 sudo
                sudo_commands = ["mkdir", "rm", "chmod", "chown", "systemctl", "service"]
                if any(cmd in command for cmd in sudo_commands):
                    command = f"sudo {command}"
            
            # 实时打印正在执行的命令
            if realtime_output:
                console.print(f"🔧 执行远程命令: {command}", style="cyan")
                if original_command != command:
                    console.print(f"🔍 原始命令: {original_command}", style="yellow")
            
            # 不使用 nohup 后台，所有命令前台执行，便于统一调度与日志收集
            # 注入环境变量（用于网络访问的代理等）。
            # 对于 sudo 命令不强行注入（sudo 默认清理环境），代理主要用于非提权命令如 pip/git/ray。
            env_prefix = ""
            if extra_env:
                # 同时支持小写/大写代理名由调用方传入
                kv_pairs = []
                for k, v in extra_env.items():
                    if v is None:
                        continue
                    try:
                        kv_pairs.append(f"{k}={shlex.quote(str(v))}")
                    except Exception:
                        kv_pairs.append(f"{k}={str(v)}")
                if kv_pairs:
                    env_prefix = "env " + " ".join(kv_pairs) + " "

            wrapped_command = command
            # 统一前缀 env
            if env_prefix and not command.startswith("sudo "):
                wrapped_command = env_prefix + command
            stdin, stdout, stderr = self.client.exec_command(wrapped_command, get_pty=get_pty)
            
            stdout_str = ""
            stderr_str = ""
            
            if realtime_output:
                # 实时读取输出
                import threading
                import queue
                
                stdout_queue = queue.Queue()
                stderr_queue = queue.Queue()
                
                def read_stdout():
                    for line in stdout:
                        # 当使用 text=True 时，line 已经是字符串
                        line_str = line if isinstance(line, str) else line.decode('utf-8')
                        stdout_queue.put(line_str)
                        console.print(f"  {line_str.rstrip()}", style="green")
                
                def read_stderr():
                    for line in stderr:
                        # 当使用 text=True 时，line 已经是字符串
                        line_str = line if isinstance(line, str) else line.decode('utf-8')
                        stderr_queue.put(line_str)
                        console.print(f"  {line_str.rstrip()}", style="red")
                
                # 启动读取线程
                stdout_thread = threading.Thread(target=read_stdout)
                stderr_thread = threading.Thread(target=read_stderr)
                stdout_thread.daemon = True
                stderr_thread.daemon = True
                stdout_thread.start()
                stderr_thread.start()
                
                # 等待命令完成（带超时）
                chan = stdout.channel
                exit_status = None
                if timeout_seconds is None or timeout_seconds <= 0:
                    exit_status = chan.recv_exit_status()
                else:
                    import time as _time
                    deadline = _time.time() + timeout_seconds
                    while not chan.exit_status_ready():
                        if _time.time() >= deadline:
                            try:
                                chan.close()
                            except Exception:
                                pass
                            return False, stdout_str, (stderr_str + "\ncommand timeout")
                        _time.sleep(0.1)
                    exit_status = chan.recv_exit_status()
                
                # 等待线程完成
                stdout_thread.join()
                stderr_thread.join()
                
                # 收集输出
                while not stdout_queue.empty():
                    stdout_str += stdout_queue.get()
                while not stderr_queue.empty():
                    stderr_str += stderr_queue.get()
                
            else:
                # 获取输出（非实时模式）
                stdout_str = stdout.read().decode('utf-8')
                stderr_str = stderr.read().decode('utf-8')
                
                # 等待命令完成（带超时）
                chan = stdout.channel
                if timeout_seconds is None or timeout_seconds <= 0:
                    exit_status = chan.recv_exit_status()
                else:
                    import time as _time
                    deadline = _time.time() + timeout_seconds
                    while not chan.exit_status_ready():
                        if _time.time() >= deadline:
                            try:
                                chan.close()
                            except Exception:
                                pass
                            return False, stdout_str, (stderr_str + "\ncommand timeout")
                        _time.sleep(0.1)
                    exit_status = chan.recv_exit_status()
            
            success = exit_status == 0
            
            if realtime_output:
                if success:
                    console.print(f"✅ 命令执行成功 (退出码: {exit_status})", style="green")
                else:
                    console.print(f"❌ 命令执行失败 (退出码: {exit_status})", style="red")
            
            return success, stdout_str, stderr_str
            
        except Exception as e:
            error_msg = str(e)
            if realtime_output:
                console.print(f"❌ 命令执行异常: {error_msg}", style="red")
            return False, "", error_msg
    
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """上传文件"""
        try:
            if not self.sftp:
                console.print("❌ SFTP 连接未建立", style="red")
                return False
            
            # 检查本地文件是否存在
            if not os.path.exists(local_path):
                console.print(f"❌ 本地文件不存在: {local_path}", style="red")
                return False
            
            # 检查远程路径权限
            has_permission, permission_info = self._check_path_permissions(remote_path)
            if not has_permission:
                console.print(f"❌ 权限不足: {permission_info}", style="red")
                console.print(f"   目标路径: {remote_path}", style="yellow")
                console.print(f"   建议: 使用 sudo 或更改目标路径权限", style="yellow")
                return False
            
            # 确保远程目录存在
            remote_dir = os.path.dirname(remote_path)
            if remote_dir:
                self._ensure_remote_dir(remote_dir)
            
            # 上传文件
            self.sftp.put(local_path, remote_path)
            console.print(f"✅ 文件上传成功: {local_path} -> {remote_path}", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ 文件上传失败: {e}", style="red")
            return False
    
    def upload_directory(self, local_dir: str, remote_dir: str) -> bool:
        """上传目录"""
        try:
            if not self.sftp:
                console.print("❌ SFTP 连接未建立", style="red")
                return False
            
            # 检查本地目录是否存在
            if not os.path.exists(local_dir):
                console.print(f"❌ 本地目录不存在: {local_dir}", style="red")
                return False
            
            # 检查远程目录权限
            has_permission, permission_info = self._check_path_permissions(remote_dir)
            if not has_permission:
                console.print(f"❌ 权限不足: {permission_info}", style="red")
                console.print(f"   目标路径: {remote_dir}", style="yellow")
                console.print(f"   建议: 使用 sudo 或更改目标路径权限", style="yellow")
                return False
            
            # 确保远程目录存在
            self._ensure_remote_dir(remote_dir)
            
            # 递归上传目录
            for root, dirs, files in os.walk(local_dir):
                # 计算相对路径
                rel_path = os.path.relpath(root, local_dir)
                if rel_path == '.':
                    remote_root = remote_dir
                else:
                    remote_root = os.path.join(remote_dir, rel_path)
                
                # 创建远程目录
                self._ensure_remote_dir(remote_root)
                
                # 上传文件
                for file in files:
                    local_file = os.path.join(root, file)
                    remote_file = os.path.join(remote_root, file)
                    
                    # 检查每个文件的远程路径权限
                    has_file_permission, file_permission_info = self._check_path_permissions(remote_file)
                    if not has_file_permission:
                        console.print(f"⚠️  跳过文件 {file}: {file_permission_info}", style="yellow")
                        continue
                    
                    self.sftp.put(local_file, remote_file)
            
            console.print(f"✅ 目录上传成功: {local_dir} -> {remote_dir}", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ 目录上传失败: {e}", style="red")
            return False
    
    def _ensure_remote_dir(self, remote_dir: str):
        """确保远程目录存在"""
        try:
            self.sftp.stat(remote_dir)
        except FileNotFoundError:
            # 目录不存在，创建它
            try:
                self.sftp.mkdir(remote_dir)
            except Exception as e:
                # 如果创建失败，尝试使用 sudo
                console.print(f"⚠️  创建目录失败，尝试使用 sudo: {remote_dir}", style="yellow")
                success, stdout, stderr = self.execute_command(f"sudo mkdir -p '{remote_dir}'")
                if not success:
                    console.print(f"❌ 创建目录失败: {stderr}", style="red")
                    raise e
    
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """下载文件"""
        try:
            if not self.sftp:
                console.print("❌ SFTP 连接未建立", style="red")
                return False
            
            # 确保本地目录存在
            local_dir = os.path.dirname(local_path)
            if local_dir and not os.path.exists(local_dir):
                os.makedirs(local_dir, exist_ok=True)
            
            self.sftp.get(remote_path, local_path)
            console.print(f"✅ 文件下载成功: {remote_path} -> {local_path}", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ 文件下载失败: {e}", style="red")
            return False
    
    def file_exists(self, remote_path: str) -> bool:
        """检查远程文件是否存在"""
        try:
            if not self.sftp:
                return False
            
            self.sftp.stat(remote_path)
            return True
            
        except FileNotFoundError:
            return False
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self.client is not None and self.client.get_transport() is not None


class RemoteExecutor:
    """远程执行器"""
    
    def __init__(self):
        self.ssh_manager = SSHManager()
    
    def execute_ray_command(self, host: str, port: int, username: str, password: str, 
                           command: str, ray_args: list = None, realtime_output: bool = True, 
                           enable_logging: bool = False, logtag: str = None, env: Optional[Dict[str, str]] = None) -> bool:
        """Execute Ray command with optional auto-generated logging"""
        try:
            # Establish connection
            if not self.ssh_manager.connect(host, port, username, password):
                return False

            # 运行 ray 命令不需要 sudo；跳过免密检查
            
            # Build complete command
            full_command = f"python3 /tmp/remote_lib/ray_remote.py {command}"
            if ray_args:
                full_command += " " + " ".join(ray_args)
            
            log_file = None
            # Add logging redirection if enable_logging is True
            if enable_logging:
                # Auto-generate log file path with timestamp and random suffix
                import time
                import random
                import string
                
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
                
                if logtag:
                    log_file = f"/tmp/nokube_{logtag}_{timestamp}_{random_suffix}.log"
                else:
                    log_file = f"/tmp/nokube_{command}_{timestamp}_{random_suffix}.log"
                
                # Ensure log directory exists on remote
                log_dir = os.path.dirname(log_file) if os.path.dirname(log_file) else "/tmp"
                self.ssh_manager.execute_command(f"mkdir -p {log_dir}", realtime_output=False)
                
                # Add tee command to output both to console and log file
                full_command = f"({full_command}) 2>&1 | tee -a {log_file}"
            
            if realtime_output:
                console.print(f"🚀 Executing Ray command: {full_command}", style="blue")
                if enable_logging and log_file:
                    console.print(f"📝 Auto-generated log file: {log_file}", style="yellow")
                # 打印代理（脱敏）
                if env:
                    masked = []
                    from urllib.parse import urlparse
                    for key in ("http_proxy","https_proxy","no_proxy","HTTP_PROXY","HTTPS_PROXY","NO_PROXY"):
                        val = env.get(key)
                        if not val:
                            continue
                        try:
                            if key.lower() == 'no_proxy':
                                items = [s.strip() for s in str(val).split(',') if s.strip()]
                                preview = ','.join(items[:5])
                                suffix = '' if len(items) <= 5 else f" (+{len(items)-5})"
                                masked.append(f"{key}={preview}{suffix}")
                            else:
                                p = urlparse(str(val))
                                if p.scheme and (p.hostname or p.netloc):
                                    host = p.hostname or ''
                                    port = f":{p.port}" if p.port else ''
                                    masked.append(f"{key}={p.scheme}://{host}{port}")
                                else:
                                    sval = str(val)
                                    masked.append(f"{key}={sval[:120]}{'' if len(sval)<=120 else '…'}")
                        except Exception:
                            masked.append(f"{key}={val}")
                    if masked:
                        console.print(f"🌐 代理环境: {', '.join(masked)}", style="cyan")
            
            # Execute command
            # 不分配 pty，避免远程程序收到 SIGHUP/终端关闭导致退出
            success, stdout, stderr = self.ssh_manager.execute_command(full_command, realtime_output=realtime_output, get_pty=False, extra_env=env)
            
            if success:
                if not realtime_output:
                    console.print("✅ Command executed successfully", style="green")
                    if stdout:
                        console.print(f"Output: {stdout}", style="cyan")
                
                # Show absolute log file path if logging was used
                if enable_logging and log_file:
                    abs_path_cmd = f"readlink -f {log_file}"
                    path_success, abs_path_stdout, abs_path_stderr = self.ssh_manager.execute_command(abs_path_cmd, realtime_output=False)
                    
                    if path_success and abs_path_stdout.strip():
                        absolute_log_path = abs_path_stdout.strip()
                        console.print(f"📋 Command output logged to: {host}:{absolute_log_path}", style="cyan")
                    else:
                        # Fallback to the original path if readlink fails
                        console.print(f"📋 Command output logged to: {host}:{log_file}", style="cyan")
                
                return True
            else:
                if not realtime_output:
                    console.print(f"❌ Command execution failed: {stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ Command execution failed: {e}", style="red")
            return False
        finally:
            self.ssh_manager.disconnect()
    
    def execute_ray_command_with_logging(self, host: str, port: int, username: str, password: str, 
                                        command: str, ray_args: list = None, realtime_output: bool = True, 
                                        logtag: str = None, env: Optional[Dict[str, str]] = None) -> bool:
        """Execute Ray command with enhanced logging support - auto-generates log file"""
        try:
            # Establish connection
            if not self.ssh_manager.connect(host, port, username, password):
                return False
            
            # 运行 ray 命令不需要 sudo；跳过免密检查
            
            # Auto-generate log file path with timestamp and random suffix
            import time
            import random
            import string
            
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
            
            if logtag:
                log_file = f"/tmp/nokube_{logtag}_{timestamp}_{random_suffix}.log"
            else:
                log_file = f"/tmp/nokube_{command}_{timestamp}_{random_suffix}.log"
            
            # Build complete command with logging arguments
            full_command = f"python3 /tmp/remote_lib/ray_remote.py"
            
            # Add logging arguments
            full_command += f" --log-file {log_file} --enable-file-logging"
            
            # Add the main command
            full_command += f" {command}"
            
            # Add ray arguments
            if ray_args:
                full_command += " " + " ".join(ray_args)
            
            if realtime_output:
                console.print(f"🚀 Executing Ray command with logging: {command}", style="blue")
                console.print(f"📝 Auto-generated log file: {log_file}", style="yellow")
                # 打印代理（脱敏）
                if env:
                    masked = []
                    from urllib.parse import urlparse
                    for key in ("http_proxy","https_proxy","no_proxy","HTTP_PROXY","HTTPS_PROXY","NO_PROXY"):
                        val = env.get(key)
                        if not val:
                            continue
                        try:
                            if key.lower() == 'no_proxy':
                                items = [s.strip() for s in str(val).split(',') if s.strip()]
                                preview = ','.join(items[:5])
                                suffix = '' if len(items) <= 5 else f" (+{len(items)-5})"
                                masked.append(f"{key}={preview}{suffix}")
                            else:
                                p = urlparse(str(val))
                                if p.scheme and (p.hostname or p.netloc):
                                    host = p.hostname or ''
                                    port = f":{p.port}" if p.port else ''
                                    masked.append(f"{key}={p.scheme}://{host}{port}")
                                else:
                                    sval = str(val)
                                    masked.append(f"{key}={sval[:120]}{'' if len(sval)<=120 else '…'}")
                        except Exception:
                            masked.append(f"{key}={val}")
                    if masked:
                        console.print(f"🌐 代理环境: {', '.join(masked)}", style="cyan")
            
            # Execute command
            # 不分配 pty，避免远程程序收到 SIGHUP/终端关闭导致退出
            success, stdout, stderr = self.ssh_manager.execute_command(full_command, realtime_output=realtime_output, get_pty=False, extra_env=env)
            
            if success:
                if not realtime_output:
                    console.print("✅ Command executed successfully", style="green")
                    if stdout:
                        console.print(f"Output: {stdout}", style="cyan")
                
                # Get absolute path of the log file on remote server
                abs_path_cmd = f"readlink -f {log_file}"
                path_success, abs_path_stdout, abs_path_stderr = self.ssh_manager.execute_command(abs_path_cmd, realtime_output=False)
                
                if path_success and abs_path_stdout.strip():
                    absolute_log_path = abs_path_stdout.strip()
                    console.print(f"📋 Command output logged to: {host}:{absolute_log_path}", style="cyan")
                else:
                    # Fallback to the original path if readlink fails
                    console.print(f"📋 Command output logged to: {host}:{log_file}", style="cyan")
                
                return True
            else:
                if not realtime_output:
                    console.print(f"❌ Command execution failed: {stderr}", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ Command execution failed: {e}", style="red")
            return False
        finally:
            self.ssh_manager.disconnect()

    def get_remote_log(self, host: str, port: int, username: str, password: str, 
                      remote_log_file: str, local_log_file: str = None, env: Optional[Dict[str, str]] = None) -> bool:
        """Download remote log file to local system"""
        try:
            # Establish connection
            if not self.ssh_manager.connect(host, port, username, password):
                return False
            
            # Set default local log file if not provided
            if not local_log_file:
                local_log_file = f"./logs/{host}_{os.path.basename(remote_log_file)}"
            
            # Ensure local log directory exists
            local_log_dir = os.path.dirname(local_log_file)
            if local_log_dir and not os.path.exists(local_log_dir):
                os.makedirs(local_log_dir, exist_ok=True)
            
            # Download log file
            # 下载日志不需要代理注入
            success = self.ssh_manager.download_file(remote_log_file, local_log_file)
            
            if success:
                console.print(f"📋 Log downloaded: {remote_log_file} -> {local_log_file}", style="green")
                return True
            else:
                console.print(f"❌ Failed to download log file", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ Log download failed: {e}", style="red")
            return False
        finally:
            self.ssh_manager.disconnect()
    
    def configure_docker_daemon_proxy(self, host: str, port: int, username: str, password: str,
                                      proxy_env: Optional[Dict[str, str]] = None) -> bool:
        """在远程节点通过 /etc/docker/daemon.json 配置 Docker 守护进程代理并重启。

        仅当传入 proxy_env 含 http_proxy/https_proxy/no_proxy 之一时生效。
        """
        try:
            if not proxy_env:
                return True
            # 规范化键
            def norm(key: str) -> Optional[str]:
                if not proxy_env:
                    return None
                return str(proxy_env.get(key) or proxy_env.get(key.upper()) or proxy_env.get(key.lower()) or "") or None

            http_p = norm('http_proxy')
            https_p = norm('https_proxy')
            no_p = norm('no_proxy')
            if not any([http_p, https_p, no_p]):
                return True

            if not self.ssh_manager.connect(host, port, username, password):
                return False

            # 读取现有 daemon.json
            import json as _json
            daemon_dir = "/etc/docker"
            daemon_file = f"{daemon_dir}/daemon.json"
            ok, out, err = self.ssh_manager.execute_command(f"cat {daemon_file}", realtime_output=False)
            try:
                current = _json.loads(out) if ok and out.strip() else {}
            except Exception:
                current = {}

            if 'proxies' not in current or not isinstance(current['proxies'], dict):
                current['proxies'] = {}
            proxies = current['proxies']
            # Docker 期望的键名采用连字符
            if http_p:
                proxies['http-proxy'] = http_p
            else:
                proxies.pop('http-proxy', None)
            if https_p:
                proxies['https-proxy'] = https_p
            else:
                proxies.pop('https-proxy', None)
            if no_p:
                proxies['no-proxy'] = no_p
            else:
                proxies.pop('no-proxy', None)

            # 写入临时文件并安装
            tmp_json = "/tmp/daemon.json"
            data = _json.dumps(current, indent=2)
            if not self.ssh_manager.sftp:
                self.ssh_manager.sftp = self.ssh_manager.client.open_sftp()
            with self.ssh_manager.sftp.file(tmp_json, 'w') as f:
                f.write(data)
            self.ssh_manager.sftp.chmod(tmp_json, 0o644)

            cmds = [
                f"mkdir -p {daemon_dir}",
                f"install -o root -g root -m 644 {tmp_json} {daemon_file}",
                "systemctl restart docker || service docker restart || true",
            ]
            all_ok = True
            for c in cmds:
                ok, _o, _e = self.ssh_manager.execute_command(c, realtime_output=False)
                if not ok:
                    all_ok = False
            if all_ok:
                console.print("✅ Docker 守护进程代理已配置/更新", style="green")
                return True
            console.print("⚠️  Docker 代理配置命令部分失败（已尝试继续）", style="yellow")
            return False
        except Exception as e:
            console.print(f"❌ 配置 Docker 守护进程代理失败: {e}", style="red")
            return False
        finally:
            self.ssh_manager.disconnect()

    def upload_remote_lib(self, host: str, port: int, username: str, password: str, 
                         local_lib_path: str, env: Optional[Dict[str, str]] = None) -> bool:
        """上传远程执行库"""
        try:
            # 建立连接
            if not self.ssh_manager.connect(host, port, username, password):
                return False
            
            # 检查本地库路径是否存在
            if not os.path.exists(local_lib_path):
                console.print(f"❌ 本地库路径不存在: {local_lib_path}", style="red")
                return False
            
            # 上传目录
            success = self.ssh_manager.upload_directory(local_lib_path, "/tmp/remote_lib")
            if not success:
                return False
            
            # 检查并安装 Ray
            console.print("🔍 检查 Ray 安装状态...", style="cyan")
            success, stdout, stderr = self.ssh_manager.execute_command("python3 /tmp/remote_lib/ray_remote.py _check_ray", realtime_output=False, extra_env=env)
            
            if not success:
                console.print("📦 Ray 未安装，开始自动安装...", style="yellow")
                success, stdout, stderr = self.ssh_manager.execute_command("python3 /tmp/remote_lib/ray_remote.py _install_ray", extra_env=env)
                
                if success:
                    console.print("✅ Ray 安装成功", style="green")
                else:
                    console.print(f"❌ Ray 安装失败: {stderr}", style="red")
                    return False
            else:
                version_info = stdout.strip() if stdout else ""
                if version_info:
                    console.print(f"✅ Ray 已安装: {version_info}", style="green")
                else:
                    console.print("✅ Ray 已安装", style="green")
            
            return True
            
        except Exception as e:
            console.print(f"❌ 上传远程执行库失败: {e}", style="red")
            return False
        finally:
            self.ssh_manager.disconnect()
    
    def check_ray_status(self, host: str, port: int, username: str, password: str, env: Optional[Dict[str, str]] = None) -> str:
        """检查 Ray 状态"""
        try:
            # 建立连接
            if not self.ssh_manager.connect(host, port, username, password):
                return "❌ 连接失败"
            
            # 执行状态检查命令
            success, stdout, stderr = self.ssh_manager.execute_command("python3 /tmp/remote_lib/ray_remote.py status", extra_env=env)
            
            if success:
                return "✅ 运行中"
            else:
                return "❌ 未运行"
                
        except Exception as e:
            return f"❓ 未知: {e}"
        finally:
            self.ssh_manager.disconnect() 