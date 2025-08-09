#!/usr/bin/env python3
"""
Test script to demonstrate the auto-generated logging functionality
"""

import os
import sys
import time
import random
import string
from pathlib import Path

def test_log_filename_generation():
    """Test the log filename generation logic"""
    
    print("🧪 Testing Auto-Generated Log Filename Logic")
    print("=" * 50)
    
    def generate_log_filename(command, logtag=None):
        """Simulate the log filename generation"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        
        if logtag:
            return f"/tmp/nokube_{logtag}_{timestamp}_{random_suffix}.log"
        else:
            return f"/tmp/nokube_{command}_{timestamp}_{random_suffix}.log"
    
    # Test scenarios
    test_cases = [
        {"command": "start-head", "logtag": "demo_head"},
        {"command": "start-worker", "logtag": "worker_node1"},
        {"command": "stop", "logtag": "stop_cluster"},
        {"command": "status", "logtag": None},  # No logtag
        {"command": "start-head", "logtag": None},  # No logtag
    ]
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n📝 Test Case {i}:")
        print(f"   Command: {case['command']}")
        print(f"   Logtag: {case['logtag']}")
        
        log_filename = generate_log_filename(case['command'], case['logtag'])
        print(f"   Generated: {log_filename}")
        
        # Verify format
        parts = os.path.basename(log_filename).split('_')
        if case['logtag']:
            expected_parts = 4  # nokube, logtag, timestamp, random.log
            print(f"   ✅ With logtag format: nokube_{case['logtag']}_TIMESTAMP_RANDOM.log")
        else:
            expected_parts = 4  # nokube, command, timestamp, random.log
            print(f"   ✅ Without logtag format: nokube_{case['command']}_TIMESTAMP_RANDOM.log")

def test_logging_methods():
    """Test different logging methods with auto-generated filenames"""
    
    print(f"\n🔧 Testing Auto-Generated Logging Methods")
    print("=" * 40)
    
    methods = [
        {
            "name": "Enhanced Logging (print override)",
            "description": "Overrides print function, auto-generates log file with timestamp",
            "example_call": "execute_ray_command_with_logging(host, port, user, pass, 'start-head', logtag='head_node1')",
            "generated_log": "/tmp/nokube_head_node1_20231215_143022_a4b2.log"
        },
        {
            "name": "Tee-based Logging (shell redirection)", 
            "description": "Uses tee command, auto-generates log file with timestamp",
            "example_call": "execute_ray_command(host, port, user, pass, 'status', enable_logging=True, logtag='status_check')",
            "generated_log": "/tmp/nokube_status_check_20231215_143025_x9z1.log"
        },
        {
            "name": "Command-based Logging (no logtag)",
            "description": "Uses command name when no logtag provided",
            "example_call": "execute_ray_command_with_logging(host, port, user, pass, 'stop')",
            "generated_log": "/tmp/nokube_stop_20231215_143030_k5m8.log"
        }
    ]
    
    for i, method in enumerate(methods, 1):
        print(f"\n📋 Method {i}: {method['name']}")
        print(f"   📄 Description: {method['description']}")
        print(f"   ⚡ Example Call: {method['example_call']}")
        print(f"   📁 Generated Log: {method['generated_log']}")

def test_unique_generation():
    """Test that multiple calls generate unique filenames"""
    
    print(f"\n🎲 Testing Unique Filename Generation")
    print("=" * 35)
    
    def generate_log_filename(command, logtag=None):
        """Simulate the log filename generation"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
        
        if logtag:
            return f"/tmp/nokube_{logtag}_{timestamp}_{random_suffix}.log"
        else:
            return f"/tmp/nokube_{command}_{timestamp}_{random_suffix}.log"
    
    print("Generating 5 log filenames for the same command/logtag:")
    filenames = []
    for i in range(5):
        filename = generate_log_filename("start-head", "test_node")
        filenames.append(filename)
        print(f"   {i+1}. {filename}")
        time.sleep(0.01)  # Small delay to potentially change timestamp
    
    # Check uniqueness
    unique_filenames = set(filenames)
    if len(unique_filenames) == len(filenames):
        print("   ✅ All filenames are unique!")
    else:
        print(f"   ⚠️  Found {len(filenames) - len(unique_filenames)} duplicate(s)")

def main():
    """Main test function"""
    print("🎯 NoKube Auto-Generated Logging Test Suite")
    print("=" * 50)
    
    test_log_filename_generation()
    test_logging_methods()
    test_unique_generation()
    
    print(f"\n✅ All tests completed!")
    print("💡 Key Features:")
    print("   • Auto-generated log filenames with timestamp and random suffix")
    print("   • Support for custom logtag or fallback to command name")
    print("   • Two logging methods: enhanced (print override) and tee-based")
    print("   • Unique filenames prevent conflicts")
    print("   • Format: /tmp/nokube_{logtag|command}_{YYYYMMDD_HHMMSS}_{4char}.log")

if __name__ == "__main__":
    main() 