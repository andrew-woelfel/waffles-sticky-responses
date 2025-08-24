import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration class for the Help Scout analytics application."""
    
    def __init__(self):
        # OpenAI Configuration
        self.openai_api_key: str = os.getenv('OPENAI_API_KEY', '')
        self.openai_model: str = os.getenv('OPENAI_MODEL', 'gpt-4')
        
        # Database Configuration
        self.database_url: str = os.getenv('DATABASE_URL', 'sqlite:///helpscout_data.db')
        self.duckdb_path: str = os.getenv('DUCKDB_PATH', 'helpscout_data.duckdb')
        
        # Application Settings
        self.debug_mode: bool = os.getenv('DEBUG_MODE', 'True').lower() == 'true'
        self.max_query_results: int = int(os.getenv('MAX_QUERY_RESULTS', '100'))
        self.cache_ttl: int = int(os.getenv('CACHE_TTL', '300'))
        
        # Streamlit Configuration
        self.streamlit_port: int = int(os.getenv('STREAMLIT_PORT', '8501'))
        
        # Data paths
        self.data_dir: str = 'data'
        self.output_dir: str = 'output'
        
        # Validate critical settings
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate critical configuration settings."""
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        # Create directories if they don't exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.debug_mode
    
    def get_database_config(self) -> dict:
        """Get database configuration as dictionary."""
        return {
            'url': self.database_url,
            'duckdb_path': self.duckdb_path,
            'max_results': self.max_query_results
        }
    
    def get_ai_config(self) -> dict:
        """Get AI configuration as dictionary."""
        return {
            'api_key': self.openai_api_key,
            'model': self.openai_model,
            'max_results': self.max_query_results
        }