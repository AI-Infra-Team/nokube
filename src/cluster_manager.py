#!/usr/bin/env python3
"""
集群管理器抽象基类
定义所有集群管理器必须实现的接口
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from rich.console import Console

console = Console()


class ClusterManager(ABC):
    """集群管理器抽象基类"""
    
    def __init__(self):
        self.cluster_name = "default"
    
    @abstractmethod
    def start_cluster(self, **kwargs) -> bool:
        """启动集群"""
        pass
    
    @abstractmethod
    def stop_cluster(self, **kwargs) -> bool:
        """停止集群"""
        pass
    
    @abstractmethod
    def show_status(self, **kwargs) -> None:
        """显示集群状态"""
        pass
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """获取集群信息"""
        return {
            "name": self.cluster_name,
            "type": self.__class__.__name__.replace("ClusterManager", "").lower(),
            "status": "unknown"
        }
    
    def is_running(self) -> bool:
        """检查集群是否正在运行"""
        # 默认实现，子类可以重写
        return False
    
    def get_config(self) -> Dict[str, Any]:
        """获取集群配置"""
        return {}
    
    def apply_config(self, config: Dict[str, Any]) -> bool:
        """应用配置到集群"""
        # 默认实现，子类可以重写
        return False 