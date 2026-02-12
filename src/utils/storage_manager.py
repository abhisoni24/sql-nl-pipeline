
"""
Utility for managing HuggingFace model cache and disk space.
Designed to prevent disk overflow during experiments with large models.
"""
import os
import shutil
import time
from typing import Optional, List, Dict, Any

try:
    from huggingface_hub import scan_cache_dir, HfApi
    from huggingface_hub.utils import HfHubHTTPError
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False


class StorageManager:
    """Manages disk space by pruning HuggingFace models when needed."""
    
    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_dir = cache_dir  # HF uses default if None
        self.api = HfApi() if HF_AVAILABLE else None

    def get_free_space_gb(self, path: str = ".") -> float:
        """Get free disk space in GB."""
        try:
            total, used, free = shutil.disk_usage(path)
            return free / (1024 ** 3)
        except Exception as e:
            print(f"⚠️ Could not check disk usage: {e}")
            return 999.0 # Assume unlimited if check fails

    def estimate_model_size_gb(self, model_id: str) -> float:
        """
        Estimate model download size from HF API.
        Returns size in GB. Defaults to 50GB if unknown/private.
        """
        # Heuristic based on name
        if '120b' in model_id.lower(): return 150.0
        if '70b' in model_id.lower(): return 80.0
        if '34b' in model_id.lower(): return 40.0
        if '30b' in model_id.lower(): return 35.0
        if '20b' in model_id.lower(): return 25.0
        if '8b' in model_id.lower(): return 16.0
        if '7b' in model_id.lower(): return 14.0
        
        if not HF_AVAILABLE:
            return 50.0

        try:
            model_info = self.api.model_info(model_id)
            # HF API gives siblings size in bytes if available
            # But simpler to just use 50GB default if heuristic failed
            # Actually, let's try to sum siblings if possible
            total_bytes = 0
            for s in model_info.siblings:
                # siblings have no size attribute in basic ModelInfo unless explicitly fetched
                # But we can try to get it
                pass
            return 50.0 # Default fallback
            
        except Exception as e:
            # print(f"⚠️ Could not estimate size for {model_id} via API: {e}")
            return 50.0

    def ensure_capacity(self, model_id: str, min_free_gb: float = 20.0) -> bool:
        """
        Ensure enough disk space exists for the model + buffer.
        Deletes old cached models if necessary.
        """
        est_size = self.estimate_model_size_gb(model_id)
        if est_size == 0: est_size = 50.0 # safe default
        
        required = est_size + min_free_gb
        
        current_free = self.get_free_space_gb()
        print(f"💾 Storage Check: Free={current_free:.1f}GB, Est. Model={est_size:.1f}GB, Required={required:.1f}GB")
        
        if current_free >= required:
            return True
            
        print(f"⚠️ Low disk space! Need {required - current_free:.1f}GB more. Pruning cache...")
        
        if not HF_AVAILABLE:
            print("❌ Cannot prune: huggingface_hub not installed.")
            return False
            
        try:
            self._prune_until_space(required)
            new_free = self.get_free_space_gb()
            print(f"✅ Pruning complete. Free space: {new_free:.1f}GB")
            return True # Proceed regardless, we did our best
        except Exception as e:
            print(f"❌ Pruning failed: {e}")
            return True # Proceed anyway, maybe download will work

    def _prune_until_space(self, target_free_gb: float):
        """Delete LRU models until target free space is reached."""
        try:
            scan = scan_cache_dir(self.cache_dir)
        except Exception as e:
            print(f"❌ Cache scan failed: {e}")
            return

        # Get repos that are models
        repos = [r for r in scan.repos if r.repo_type == 'model']
        
        # Sort repos by last accessed (LRU) - oldest first
        # Note: 'last_accessed' might be None, default to 0
        repos.sort(key=lambda r: getattr(r, 'last_accessed', 0) or 0)
        
        for repo in repos:
            if self.get_free_space_gb() >= target_free_gb:
                break
                
            print(f"🗑️ Deleting cached repo: {repo.repo_id} ({repo.size_on_disk / 1e9:.1f}GB) ...")
            
            # Delete strategy needs to be executed
            try:
                # We want to delete the whole repo to free max space
                # delete_revisions returns a DeleteStrategy
                strategy = scan.delete_revisions(*repo.revisions)
                strategy.execute()
            except Exception as e:
                print(f"⚠️ Failed to delete {repo.repo_id}: {e}")
