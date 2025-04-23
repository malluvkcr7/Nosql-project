import json
from datetime import datetime

class OperationLog:
    def __init__(self, system_name):
        self.system_name = system_name
        self.log_file = f"oplog_{system_name.lower()}.json"
        self.merge_history_file = f"merge_history_{system_name.lower()}.json"
        self.operations = []
        self.merge_history = {}  # Format: {'postgresql': timestamp, 'mongodb': timestamp, 'pig': timestamp}
        self._load_data()
    
    def _load_data(self):
        try:
            with open(self.log_file, 'r') as f:
                self.operations = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.operations = []
        
        try:
            with open(self.merge_history_file, 'r') as f:
                self.merge_history = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.merge_history = {
                'postgresql': None,
                'mongodb': None,
                'pig': None
            }
    
    def add_operation(self, op_type, key, value=None):
        timestamp = datetime.now().isoformat()
        operation = {
            'timestamp': timestamp,
            'type': op_type,
            'key': key,
            'value': value
        }
        self.operations.append(operation)
        self._save_log()
    
    def record_merge(self, other_system, merge_time=None):
        """Record a merge operation with another system"""
        if merge_time is None:
            merge_time = datetime.now().isoformat()
        self.merge_history[other_system.lower()] = merge_time
        self._save_merge_history()
    
    def get_operations_since_merge(self, other_system):
        """Get operations since last merge with specified system"""
        last_merge = self.merge_history.get(other_system.lower())
        if last_merge is None:
            return self.operations.copy()
        return [op for op in self.operations if op['timestamp'] > last_merge]
    
    def _save_log(self):
        with open(self.log_file, 'w') as f:
            json.dump(self.operations, f, indent=2)
    
    def _save_merge_history(self):
        with open(self.merge_history_file, 'w') as f:
            json.dump(self.merge_history, f, indent=2)