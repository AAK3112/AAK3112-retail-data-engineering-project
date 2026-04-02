
from dataclasses import dataclass

@dataclass
class EnvConfig:
    env: str = "dev"
    mode: str = "batch"
    
    base_path = "D:/Pyspark Dataset/data_lake_local"
    
    @property
    def raw_path(self):
        return f"{self.base_path}/raw/retail1/*.csv"
    
    @property
    def bronze_path(self):
        return f"{self.base_path}/bronze/retail1"
    
    @property
    def silver_path(self):
        return f"{self.base_path}/silver/retail1"
    
    @property
    def gold_path(self):
        return f"{self.base_path}/gold/retail1"
    
    @property
    def quarantine_path(self):
        return f"{self.base_path}/quarantine/retail1"
    
    @property
    def checkpoint_path(self):
        return f"{self.base_path}/checkpoint/retail1"
    
    @property
    def tracker_path(self):
        return f"{self.base_path}/tracker/retail1"
    
    @property
    def audit_path(self):
        return f"{self.base_path}/audit/retail1"