#!/usr/bin/env python3
"""
Remote utility to ensure system config DaemonSet
"""

def ensure_system_config_daemonset():
    """Ensure system config DaemonSet in remote Ray cluster"""
    try:
        import sys
        sys.path.insert(0, '/tmp/remote_lib')
        
        import ray
        # Connect to local Ray cluster
        ray.init(ignore_reinit_error=True, namespace="nokube")
        
        # Import controller classes (they're copied to remote_lib)
        from src.ray_kube_controller import KubeControllerActor
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
        
        # Call apply with empty resources to trigger system config DaemonSet ensure
        ray.get(controller.apply.remote([]))
        print("✅ System config DaemonSet ensured successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to ensure system config DaemonSet: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    ensure_system_config_daemonset()
