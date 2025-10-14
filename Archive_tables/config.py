# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    def __init__(self):
        # Database configuration
        self.db_config = {
            'server': os.getenv('DB_SERVER', ''),
            'database': os.getenv('DB_NAME', ''),
            'username': os.getenv('DB_USERNAME', ''),
            'password': os.getenv('DB_PASSWORD', ''),
            'driver': '{ODBC Driver 17 for SQL Server}'
        }
        
        # Azure Blob Storage configuration
        self.storage_config = {
            'account_name': os.getenv('STORAGE_ACCOUNT_NAME', ''),
            'sas_token': os.getenv('STORAGE_SAS_TOKEN', ''),
            'container_name': os.getenv('STORAGE_CONTAINER', '')
        }
        
        # Validate required configurations
        self._validate_config()
    
    def _validate_config(self):
        """Validate that all required configuration values are present."""
        required_db = ['server', 'database', 'username', 'password']
        required_storage = ['account_name', 'sas_token', 'container_name']
        
        missing_db = [field for field in required_db if not self.db_config.get(field)]
        missing_storage = [field for field in required_storage if not self.storage_config.get(field)]
        
        if missing_db:
            raise ValueError(f"Missing required database configuration: {', '.join(missing_db)}")
        if missing_storage:
            raise ValueError(f"Missing required storage configuration: {', '.join(missing_storage)}")
        
        # Validate SAS token format
        sas_token = self.storage_config['sas_token']
        if not sas_token.startswith('?'):
            self.storage_config['sas_token'] = '?' + sas_token
    
    def get_connection_string(self):
        """Get the database connection string."""
        return (
            f"DRIVER={self.db_config['driver']};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        
    def get_blob_service_client(self):
        """Get a BlobServiceClient instance using SAS token."""
        from azure.storage.blob import BlobServiceClient
        # Construct the full SAS URL
        sas_url = f"https://{self.storage_config['account_name']}.blob.core.windows.net{self.storage_config['sas_token']}"
        # Initialize BlobServiceClient with the full URL
        return BlobServiceClient(account_url=sas_url)