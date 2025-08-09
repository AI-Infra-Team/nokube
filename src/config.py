#!/usr/bin/env python3
"""
NoKube Global Configuration Manager
Uses pyscript_util.config_manager for configuration management
Focuses on etcd as the sole storage backend
"""

import os
from typing import Dict, Any, Optional, List

# Try to use local config_manager first
try:
    from config_manager import ConfigManager, ConfigTemplate, ConfigFormat, ConfigNode, ConfigType
except ImportError:
    # Fallback to pyscript_util
    from pyscript_util.config_manager import ConfigManager, ConfigTemplate, ConfigFormat, ConfigNode, ConfigType

from rich.console import Console

console = Console()


class NoKubeConfig:
    """NoKube global configuration management"""
    
    def __init__(self):
        # Define configuration template
        self.config_template = ConfigTemplate(
            name="nokube",
            description="NoKube global configuration file",
            version="1.0"
        )
        
        # Add etcd configuration node
        etcd_node = ConfigNode(
            name="etcd",
            config_type=ConfigType.DICT,
            description="etcd cluster configuration (required)",
            required=True,
            order=1
        )
        
        etcd_node.add_child(ConfigNode(
            name="hosts",
            config_type=ConfigType.STRING,
            description="etcd server addresses, supports multiple addresses separated by commas",
            required=True,
            default="localhost:2379",
            example="localhost:2379,localhost:2380,localhost:2381"
        ))
        
        etcd_node.add_child(ConfigNode(
            name="username",
            config_type=ConfigType.STRING,
            description="etcd username (optional)",
            required=False,
            default=None,
            example="nokube_user"
        ))
        
        etcd_node.add_child(ConfigNode(
            name="password",
            config_type=ConfigType.STRING,
            description="etcd password (optional)",
            required=False,
            default=None,
            example="nokube_password"
        ))
        
        self.config_template.add_node(etcd_node)
        
        # Add Ray configuration node
        ray_node = ConfigNode(
            name="ray",
            config_type=ConfigType.DICT,
            description="Ray cluster default configuration",
            required=False,
            order=2
        )
        
        ray_node.add_child(ConfigNode(
            name="default_dashboard_port",
            config_type=ConfigType.INTEGER,
            description="Ray dashboard default port",
            default=8265,
            example=8265
        ))
        
        ray_node.add_child(ConfigNode(
            name="default_max_workers",
            config_type=ConfigType.INTEGER,
            description="Ray cluster default maximum worker nodes",
            default=10,
            example=10
        ))
        
        self.config_template.add_node(ray_node)
        
        # Add Kubernetes configuration node
        k8s_node = ConfigNode(
            name="kubernetes",
            config_type=ConfigType.DICT,
            description="Kubernetes cluster configuration",
            required=False,
            order=3
        )
        
        k8s_node.add_child(ConfigNode(
            name="config_file",
            config_type=ConfigType.STRING,
            description="Kubernetes configuration file path",
            default="~/.kube/config",
            example="~/.kube/config"
        ))
        
        k8s_node.add_child(ConfigNode(
            name="default_namespace",
            config_type=ConfigType.STRING,
            description="Kubernetes default namespace",
            default="default",
            example="default"
        ))
        
        self.config_template.add_node(k8s_node)
        
        # Add logging configuration node
        logging_node = ConfigNode(
            name="logging",
            config_type=ConfigType.DICT,
            description="Logging configuration",
            required=False,
            order=4
        )
        
        logging_node.add_child(ConfigNode(
            name="level",
            config_type=ConfigType.STRING,
            description="Log level (DEBUG, INFO, WARNING, ERROR)",
            default="INFO",
            example="INFO"
        ))
        
        logging_node.add_child(ConfigNode(
            name="format",
            config_type=ConfigType.STRING,
            description="Log format",
            default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            example="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ))
        
        self.config_template.add_node(logging_node)
        
        # Initialize configuration manager
        self.config_manager = ConfigManager(
            app_name="nokube",
            template=self.config_template,
            config_filename="config",
            supported_formats=[ConfigFormat.YAML, ConfigFormat.JSON]
        )
        
        # Load configuration
        self._config = None
        self.reload()
    
    def reload(self):
        """Reload configuration"""
        try:
            self._config = self.config_manager.load_config()
        except Exception as e:
            console.print(f"⚠️  Failed to load configuration, using default configuration: {e}", style="yellow")
            # Use default configuration
            self._config = {
                "etcd": {
                    "hosts": "localhost:2379",
                    "username": None,
                    "password": None
                },
                "ray": {
                    "default_dashboard_port": 8265,
                    "default_max_workers": 10
                },
                "kubernetes": {
                    "config_file": "~/.kube/config",
                    "default_namespace": "default"
                },
                "logging": {
                    "level": "INFO",
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                }
            }
    
    def get(self, path: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self.config_manager.get_config_value(path, default)
    
    def get_etcd_hosts(self) -> str:
        """Get etcd host addresses"""
        return self.get("etcd.hosts", "localhost:2379")
    
    def get_etcd_timeout(self) -> int:
        """Get etcd timeout"""
        return 10  # Fixed default value
    
    def get_etcd_prefix(self) -> str:
        """Get etcd key prefix"""
        return "/nokube"  # Fixed default value
    
    def get_etcd_username(self) -> Optional[str]:
        """Get etcd username"""
        return self.get("etcd.username", None)
    
    def get_etcd_password(self) -> Optional[str]:
        """Get etcd password"""
        return self.get("etcd.password", None)
    
    def validate(self) -> bool:
        """Validate configuration"""
        errors = self.config_manager.validate_config()
        if errors:
            console.print("❌ Configuration validation failed:", style="red")
            for error in errors:
                console.print(f"   • {error}", style="red")
            
            # Print detailed error information
            console.print("\n" + "="*60, style="red")
            console.print("Configuration Error Detailed Report", style="bold red")
            console.print("="*60, style="red")
            
            # Use custom configuration help information
            self.print_config_help()
            
            return False
        return True
    
    def generate_example_config(self, format_type: ConfigFormat = ConfigFormat.YAML) -> str:
        """Generate example configuration"""
        return self.config_manager.generate_example_config(format_type)
    
    def generate_config_suggestions(self) -> List[str]:
        """Generate NoKube-specific configuration suggestions"""
        suggestions = []
        
        # Explain configuration file search order and priority
        suggestions.append("NoKube configuration file search order (by priority from high to low):")
        suggestions.append("")
        
        # Sort by priority: user > installation path
        try:
            from config_manager import ConfigPathLevel
        except ImportError:
            from pyscript_util.config_manager import ConfigPathLevel
            
        priority_levels = [
            (ConfigPathLevel.USER, "User configuration (recommended)", "applies to current user only", "~/.nokube/config.yaml"),
            (ConfigPathLevel.INSTALL, "Installation location configuration", "shared among users of the same installation package", "./.nokube/config.yaml")
        ]
        
        for level, level_name, description, standard_path in priority_levels:
            suggestions.append(f"📁 {level_name} - {description}:")
            
            # Find paths for this level
            level_path = None
            for config_path in self.config_manager._all_search_paths:
                if config_path.level == level:
                    level_path = config_path
                    break
            
            if level_path:
                config_file = level_path.path / "config.yaml"
                if level_path.path.parent.exists() and os.access(level_path.path.parent, os.W_OK):
                    if level == ConfigPathLevel.USER:
                        suggestions.append(f"   ✓ {config_file} (recommended)")
                    else:
                        suggestions.append(f"   ✓ {config_file} (available)")
                elif level_path.path.parent.exists():
                    suggestions.append(f"   ✗ {config_file} (no write permission)")
                else:
                    suggestions.append(f"   ✗ {config_file} (directory does not exist)")
            else:
                suggestions.append(f"   ✗ {standard_path} (path not found)")
            
            suggestions.append("")
        
        # Recommended configuration file path
        recommended_path = os.path.expanduser("~/.nokube/config.yaml")
        suggestions.append("💡 Recommended user-level configuration file (good isolation, no sudo required):")
        suggestions.append(f"   {recommended_path}")
        suggestions.append("")
        
        # Configuration file example
        suggestions.append("📋 Configuration file example:")
        example_config = self.generate_example_config()
        for line in example_config.split('\n')[:15]:  # Show only first 15 lines
            suggestions.append(f"   {line}")
        if len(example_config.split('\n')) > 15:
            suggestions.append("   ...")
        suggestions.append("")
        
        return suggestions
    
    def print_config_help(self):
        """Print configuration help information"""
        console.print("\n📋 NoKube Configuration Guide:", style="bold blue")
        
        suggestions = self.generate_config_suggestions()
        for suggestion in suggestions:
            if suggestion.startswith("📁"):
                console.print(suggestion, style="bold cyan")
            elif suggestion.startswith("   ✓"):
                console.print(suggestion, style="green")
            elif suggestion.startswith("   ✗"):
                console.print(suggestion, style="red")
            elif suggestion.startswith("💡") or suggestion.startswith("📋"):
                console.print(suggestion, style="bold yellow")
            else:
                console.print(suggestion, style="white")
    
    def check_etcd_requirements(self) -> bool:
        """Check etcd configuration requirements"""
        etcd_hosts = self.get_etcd_hosts()
        if not etcd_hosts:
            console.print("❌ etcd host address not configured", style="red")
            console.print("💡 Create configuration file manually based on error guidance", style="blue")
            return False
            
        if etcd_hosts == "localhost:2379":
            console.print("⚠️  Using default etcd address localhost:2379", style="yellow")
            console.print("   Please ensure etcd service is running", style="yellow")
            console.print("   Or manually create configuration file with correct address", style="yellow")
        
        return True


# Global configuration instance
_global_config: Optional[NoKubeConfig] = None


def get_config() -> NoKubeConfig:
    """Get global configuration instance"""
    global _global_config
    if _global_config is None:
        _global_config = NoKubeConfig()
    return _global_config


def reload_config():
    """Reload global configuration"""
    global _global_config
    if _global_config is not None:
        _global_config.reload()
    else:
        _global_config = NoKubeConfig() 