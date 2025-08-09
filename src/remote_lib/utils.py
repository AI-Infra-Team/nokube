#!/usr/bin/env python3
"""
远程执行工具函数
提供权限检查和命令执行的通用功能
"""

import os
import shutil
import subprocess
import stat
from typing import Tuple, Optional
from pathlib import Path


def is_root() -> bool:
    """检查当前用户是否为 root"""
    return os.geteuid() == 0


def get_sudo_prefix() -> str:
    """获取 sudo 前缀（如果不是 root 用户）"""
    return "sudo " if not is_root() else ""


def check_command_exists(command: str) -> bool:
    """检查命令是否存在"""
    return shutil.which(command) is not None


def execute_command(command: str, capture_output: bool = True, timeout: Optional[int] = None) -> Tuple[bool, str, str]:
    """执行命令，自动处理 sudo 权限"""
    # 检查是否需要 sudo
    if not is_root() and not command.startswith("sudo"):
        # 对于某些需要权限的命令，自动添加 sudo
        sudo_commands = ["mkdir", "rm", "chmod", "chown", "systemctl", "service", "apt", "yum", "dnf"]
        if any(cmd in command for cmd in sudo_commands):
            command = f"sudo {command}"
    
    try:
        # 使用 shutil.which 检查命令是否存在
        cmd_parts = command.split()
        if not check_command_exists(cmd_parts[0]):
            return False, "", f"命令不存在: {cmd_parts[0]}"
        
        # 使用 subprocess 执行命令
        if capture_output:
            result = subprocess.run(
                command.split(),
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result.returncode == 0, result.stdout, result.stderr
        else:
            exit_code = os.system(command)
            return exit_code == 0, "", ""
            
    except subprocess.TimeoutExpired:
        return False, "", "命令执行超时"
    except Exception as e:
        return False, "", str(e)


def check_path_permissions(path: str) -> Tuple[bool, str]:
    """检查路径权限"""
    try:
        # 检查路径是否存在
        if not os.path.exists(path):
            # 检查父目录权限
            parent_dir = os.path.dirname(path)
            if parent_dir == path:  # 根目录
                return False, f"路径不存在且无法创建: {path}"
            
            return check_path_permissions(parent_dir)
        
        # 获取路径信息
        stat_info = os.stat(path)
        mode = stat_info.st_mode
        uid = stat_info.st_uid
        current_uid = os.getuid()
        
        # 检查是否为目录所有者
        if uid == current_uid:
            # 检查写权限
            if mode & stat.S_IWUSR:
                return True, f"用户对 {path} 有写权限"
            else:
                return False, f"用户对 {path} 无写权限 (权限: {oct(mode)[-3:]})"
        else:
            # 检查其他用户写权限
            if mode & stat.S_IWOTH:
                return True, f"用户对 {path} 有写权限 (其他用户权限)"
            else:
                return False, f"用户对 {path} 无写权限 (权限: {oct(mode)[-3:]}, 所有者UID: {uid})"
                
    except Exception as e:
        return False, f"检查权限时发生错误: {e}"


def ensure_directory_exists(path: str) -> bool:
    """确保目录存在，如果不存在则创建"""
    try:
        if os.path.exists(path):
            return True
        
        # 尝试创建目录
        os.makedirs(path, exist_ok=True)
        return True
        
    except PermissionError:
        # 权限不足，尝试使用 sudo
        success, stdout, stderr = execute_command(f"mkdir -p '{path}'")
        return success
    except Exception as e:
        print(f"创建目录失败: {e}")
        return False


def copy_file_with_permissions(src: str, dst: str) -> bool:
    """复制文件并保持权限"""
    try:
        # 检查源文件是否存在
        if not os.path.exists(src):
            print(f"源文件不存在: {src}")
            return False
        
        # 检查目标路径权限
        has_permission, permission_info = check_path_permissions(dst)
        if not has_permission:
            print(f"权限不足: {permission_info}")
            return False
        
        # 确保目标目录存在
        dst_dir = os.path.dirname(dst)
        if dst_dir and not ensure_directory_exists(dst_dir):
            return False
        
        # 复制文件
        shutil.copy2(src, dst)
        return True
        
    except Exception as e:
        print(f"复制文件失败: {e}")
        return False


def safe_remove(path: str) -> bool:
    """安全删除文件或目录"""
    try:
        if not os.path.exists(path):
            return True
        
        # 检查权限
        has_permission, permission_info = check_path_permissions(path)
        if not has_permission:
            # 尝试使用 sudo
            success, stdout, stderr = execute_command(f"rm -rf '{path}'")
            return success
        
        # 直接删除
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        
        return True
        
    except Exception as e:
        print(f"删除失败: {e}")
        return False


def get_file_info(path: str) -> Optional[dict]:
    """获取文件信息"""
    try:
        if not os.path.exists(path):
            return None
        
        stat_info = os.stat(path)
        return {
            'size': stat_info.st_size,
            'mode': oct(stat_info.st_mode)[-3:],
            'uid': stat_info.st_uid,
            'gid': stat_info.st_gid,
            'mtime': stat_info.st_mtime,
            'is_dir': os.path.isdir(path),
            'is_file': os.path.isfile(path),
            'is_link': os.path.islink(path)
        }
        
    except Exception as e:
        print(f"获取文件信息失败: {e}")
        return None


def format_file_size(size_bytes: int) -> str:
    """格式化文件大小"""
    if size_bytes == 0:
        return "0B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f}{size_names[i]}"


def check_disk_space(path: str) -> Tuple[bool, str]:
    """检查磁盘空间"""
    try:
        statvfs = os.statvfs(path)
        free_bytes = statvfs.f_frsize * statvfs.f_bavail
        total_bytes = statvfs.f_frsize * statvfs.f_blocks
        
        free_gb = free_bytes / (1024**3)
        total_gb = total_bytes / (1024**3)
        
        if free_gb < 1.0:  # 小于1GB
            return False, f"磁盘空间不足: 可用 {free_gb:.1f}GB / 总计 {total_gb:.1f}GB"
        else:
            return True, f"磁盘空间充足: 可用 {free_gb:.1f}GB / 总计 {total_gb:.1f}GB"
            
    except Exception as e:
        return False, f"检查磁盘空间失败: {e}"


def escape_shell_command(command: str) -> str:
    """转义 shell 命令中的特殊字符"""
    # 如果命令包含引号，引号内的内容不需要转义
    # 对于 pip install "ray[default]" 这样的命令，引号已经保护了方括号
    if '"' in command or "'" in command:
        return command
    
    # 对于没有引号的命令，转义特殊字符
    special_chars = ['[', ']', '*', '?', '{', '}', '(', ')', '|', '&', ';', '<', '>', '`', '$', '\\']
    escaped_command = command
    for char in special_chars:
        escaped_command = escaped_command.replace(char, f'\\{char}')
    return escaped_command 