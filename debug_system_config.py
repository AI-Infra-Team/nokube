#!/usr/bin/env python3
"""
Debug script to manually test system config DaemonSet creation
"""

import ray
import sys
import os

def main():
    try:
        # Connect to Ray cluster
        ray.init(ignore_reinit_error=True, namespace="nokube")
        print("✅ Connected to Ray cluster")
        
        # List all actors to see current state
        actors = list(ray.util.list_named_actors(namespace="nokube"))
        print(f"📋 Current actors: {[actor['name'] for actor in actors]}")
        
        # Import controller classes
        from src.ray_kube_controller import KubeControllerActor, DaemonSetActor
        from src.actor_utils import ensure_actor
        
        # Get or create KubeControllerActor
        controller = ensure_actor(
            KubeControllerActor,
            "kube-controller", 
            namespace="nokube",
            detached=True,
            replace_existing=False,
            ctor_args=(),
            stop_method="stop",
            stop_timeout=10,
        )
        print("✅ Got KubeControllerActor")
        
        # Call apply with empty resources to trigger system config DaemonSet ensure
        result = ray.get(controller.apply.remote([]))
        print(f"✅ Applied empty resources: {result}")
        
        # List actors again to see if DaemonSet was created
        actors_after = list(ray.util.list_named_actors(namespace="nokube"))
        print(f"📋 Actors after apply: {[actor['name'] for actor in actors_after]}")
        
        # Look for DaemonSet specifically
        ds_actors = [actor for actor in actors_after if 'daemonset' in actor['name'].lower()]
        if ds_actors:
            print(f"✅ Found DaemonSet actors: {[actor['name'] for actor in ds_actors]}")
        else:
            print("❌ No DaemonSet actors found")
            
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
