#!/usr/bin/env python3
"""
NoKube Remote Logging Demo
Demonstrates auto-generated logging functionality with logtag parameter
"""

import sys
import os
from pathlib import Path

# Add src to path to import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ssh_manager import RemoteExecutor

def demo_enhanced_logging():
    """Demonstrate enhanced remote logging with auto-generated log files"""
    print("🚀 NoKube Auto-Generated Logging Demo")
    print("=" * 50)
    
    # Configuration - replace with your actual values
    HOST = "your-remote-server.com"
    PORT = 22
    USERNAME = "your-username"
    PASSWORD = "your-password"  # Or use SSH keys
    
    executor = RemoteExecutor()
    
    print("📤 Uploading remote execution library...")
    success = executor.upload_remote_lib(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        local_lib_path="./src/remote_lib"
    )
    
    if not success:
        print("❌ Failed to upload remote library")
        return
    
    print("\n🔧 Executing Ray command with auto-generated logging...")
    success = executor.execute_ray_command_with_logging(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        command="start-head",
        ray_args=["--port", "10001", "--dashboard-port", "8265"],
        realtime_output=True,
        logtag="demo_head"  # Will generate: /tmp/nokube_demo_head_20231215_143022_a4b2.log
    )
    
    if success:
        print("✅ Command executed successfully with auto-generated logging")
        print("📝 Note: Log file path with timestamp and random suffix is displayed above")
    else:
        print("❌ Command execution failed")

def demo_tee_logging():
    """Demonstrate tee-based auto-generated logging"""
    print("\n🔄 NoKube Tee-based Auto-Generated Logging Demo")
    print("=" * 50)
    
    # Configuration
    HOST = "your-remote-server.com"
    PORT = 22
    USERNAME = "your-username"
    PASSWORD = "your-password"
    
    executor = RemoteExecutor()
    
    print("🔧 Executing Ray command with tee-based auto-generated logging...")
    success = executor.execute_ray_command(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        command="status",
        realtime_output=True,
        enable_logging=True,  # Enable auto-generated logging
        logtag="demo_status"   # Will generate: /tmp/nokube_demo_status_20231215_143025_x9z1.log
    )
    
    if success:
        print("✅ Command executed successfully with tee auto-generated logging")
        print("📝 Note: Log file path with timestamp and random suffix is displayed above")
    else:
        print("❌ Command execution failed")

def demo_without_logtag():
    """Demonstrate auto-generated logging without custom logtag"""
    print("\n🎲 Auto-Generated Logging Without Custom Logtag")
    print("=" * 45)
    
    HOST = "your-remote-server.com"
    PORT = 22
    USERNAME = "your-username"
    PASSWORD = "your-password"
    
    executor = RemoteExecutor()
    
    print("🔧 Executing command without custom logtag (uses command name)...")
    success = executor.execute_ray_command_with_logging(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        command="status",
        realtime_output=True
        # No logtag - will generate: /tmp/nokube_status_20231215_143030_k5m8.log
    )
    
    if success:
        print("✅ Command executed successfully")
        print("📝 Note: Log file uses command name when no logtag is provided")
    else:
        print("❌ Command execution failed")

if __name__ == "__main__":
    print("🎯 NoKube Auto-Generated Logging Demo")
    print("=" * 60)
    print("Choose demo mode:")
    print("1. Enhanced logging with custom logtag")
    print("2. Tee-based logging with custom logtag") 
    print("3. Auto-generated logging without custom logtag")
    print("4. All demos")
    
    choice = input("Enter choice (1-4): ").strip()
    
    if choice == "1":
        demo_enhanced_logging()
    elif choice == "2":
        demo_tee_logging()
    elif choice == "3":
        demo_without_logtag()
    elif choice == "4":
        demo_enhanced_logging()
        demo_tee_logging()
        demo_without_logtag()
    else:
        print("Invalid choice")
        
    print("\n" + "=" * 60)
    print("🎉 Demo completed! Auto-generated log files with timestamps and random suffixes:")
    print("💡 Format: /tmp/nokube_{logtag}_{YYYYMMDD_HHMMSS}_{random4chars}.log")
    print("💡 Example: /tmp/nokube_demo_head_20231215_143022_a4b2.log")
    print("📋 Look for messages like '📋 Command output logged to: host:/absolute/path/to/log'") 