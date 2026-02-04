import json
import os
from typing import Set, Tuple, List, Dict
from datetime import datetime

class ExperimentRunner:
    """Manages experiment execution with checkpointing and progress tracking."""
    
    def __init__(self, output_path: str):
        self.output_path = output_path
        self.processed_keys: Set[Tuple[str, str]] = set()
        self._load_checkpoint()
    
    def _load_checkpoint(self):
        """Load already-processed (job_id, model_name) pairs."""
        if os.path.exists(self.output_path):
            with open(self.output_path, 'r') as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line)
                            key = (record['job_id'], record['model_name'])
                            self.processed_keys.add(key)
                        except json.JSONDecodeError:
                            pass
            print(f"📋 Loaded checkpoint: {len(self.processed_keys):,} processed pairs")
    
    def get_pending_tasks(self, tasks: List[Dict], model_name: str) -> List[Dict]:
        """Filter tasks to only those not yet processed for this model."""
        pending = []
        for task in tasks:
            key = (task['job_id'], model_name)
            if key not in self.processed_keys:
                pending.append(task)
        return pending
    
    def run(
        self,
        tasks: List[Dict],
        worker,
        batch_size: int = 500,
        limit: int = None
    ):
        """
        Run experiment for a specific model.
        
        Args:
            tasks: List of task dictionaries
            worker: LLMWorker instance
            batch_size: Batch size for generation
            limit: Optional limit on number of tasks to run (for testing)
        """
        model_name = worker.model_name
        pending = self.get_pending_tasks(tasks, model_name)
        
        if limit:
            pending = pending[:limit]
        
        if not pending:
            print(f"✅ All tasks already processed for {model_name}")
            return
        
        print(f"\n🚀 Running {len(pending):,} tasks for {model_name}")
        
        # Extract prompts
        prompts = [t['input_prompt'] for t in pending]
        
        # Generate responses
        start_time = datetime.now()
        responses = worker.generate_batch(prompts, batch_size=batch_size)
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if elapsed > 0:
            print(f"⏱️ Generation completed in {elapsed:.1f}s ({len(responses)/elapsed:.1f} queries/sec)")
        
        # Save results
        with open(self.output_path, 'a') as f:
            for task, response in zip(pending, responses):
                result = {
                    **task,
                    'model_name': model_name,
                    'generated_response': response,
                    'timestamp': datetime.now().isoformat()
                }
                f.write(json.dumps(result) + '\n')
                self.processed_keys.add((task['job_id'], model_name))
        
        print(f"💾 Results saved to {self.output_path}")
