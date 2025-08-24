import sqlite3
import pandas as pd
import duckdb
from sqlalchemy import create_engine, text
from typing import Dict, List, Optional, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manage database operations for Help Scout customer data."""
    
    def __init__(self, database_url: str = "sqlite:///helpscout_data.db"):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.duckdb_conn = None
        
        # Initialize DuckDB for analytical queries
        try:
            self.duckdb_conn = duckdb.connect(':memory:')
            logger.info("DuckDB connection established")
        except Exception as e:
            logger.warning(f"Could not initialize DuckDB: {e}")
    
    def create_tables(self, tables: Dict[str, pd.DataFrame]) -> None:
        """Create database tables from processed DataFrames."""
        logger.info("Creating database tables")
        
        for table_name, df in tables.items():
            try:
                # Create in SQLite
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                
                # Also load into DuckDB for faster analytics
                if self.duckdb_conn:
                    self.duckdb_conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                
                logger.info(f"Created table '{table_name}' with {len(df)} records")
            except Exception as e:
                logger.error(f"Error creating table '{table_name}': {e}")
                raise
    
    def execute_query(self, query: str, use_duckdb: bool = True) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            if use_duckdb and self.duckdb_conn:
                result = self.duckdb_conn.execute(query).fetchdf()
                logger.info(f"Executed DuckDB query, returned {len(result)} rows")
                return result
            else:
                result = pd.read_sql(query, self.engine)
                logger.info(f"Executed SQLite query, returned {len(result)} rows")
                return result
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query was: {query}")
            raise
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a table."""
        try:
            if self.duckdb_conn:
                schema_query = f"DESCRIBE {table_name}"
                schema_df = self.duckdb_conn.execute(schema_query).fetchdf()
                return {
                    'columns': schema_df['column_name'].tolist(),
                    'types': schema_df['column_type'].tolist(),
                    'schema_details': schema_df.to_dict('records')
                }
            else:
                # Fallback to SQLite
                query = f"PRAGMA table_info({table_name})"
                schema_df = pd.read_sql(query, self.engine)
                return {
                    'columns': schema_df['name'].tolist(),
                    'types': schema_df['type'].tolist(),
                    'schema_details': schema_df.to_dict('records')
                }
        except Exception as e:
            logger.error(f"Error getting schema for table '{table_name}': {e}")
            return {'columns': [], 'types': [], 'schema_details': []}
    
    def get_all_tables(self) -> List[str]:
        """Get list of all available tables."""
        try:
            if self.duckdb_conn:
                result = self.duckdb_conn.execute("SHOW TABLES").fetchall()
                return [row[0] for row in result]
            else:
                query = "SELECT name FROM sqlite_master WHERE type='table'"
                result = pd.read_sql(query, self.engine)
                return result['name'].tolist()
        except Exception as e:
            logger.error(f"Error getting table list: {e}")
            return []
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample data from a table."""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Error getting sample data from '{table_name}': {e}")
            return pd.DataFrame()
    
    def validate_query(self, query: str) -> Dict[str, Any]:
        """Validate SQL query without executing it."""
        try:
            # Simple validation - check if it's a SELECT statement
            query_upper = query.strip().upper()
            
            validation_result = {
                'is_valid': True,
                'is_safe': True,
                'query_type': 'UNKNOWN',
                'warnings': []
            }
            
            # Determine query type
            if query_upper.startswith('SELECT'):
                validation_result['query_type'] = 'SELECT'
            elif any(query_upper.startswith(cmd) for cmd in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE']):
                validation_result['is_safe'] = False
                validation_result['warnings'].append("Potentially unsafe query type detected")
            
            # Check for dangerous keywords
            dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER']
            for keyword in dangerous_keywords:
                if keyword in query_upper:
                    validation_result['is_safe'] = False
                    validation_result['warnings'].append(f"Dangerous keyword '{keyword}' detected")
            
            return validation_result
            
        except Exception as e:
            return {
                'is_valid': False,
                'is_safe': False,
                'query_type': 'ERROR',
                'warnings': [f"Validation error: {str(e)}"]
            }
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get summary information about the database."""
        summary = {
            'tables': {},
            'total_records': 0,
            'database_type': 'SQLite' if 'sqlite' in self.database_url.lower() else 'Other'
        }
        
        tables = self.get_all_tables()
        
        for table in tables:
            try:
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                count_result = self.execute_query(count_query)
                record_count = count_result.iloc[0]['count'] if not count_result.empty else 0
                
                summary['tables'][table] = {
                    'record_count': record_count,
                    'schema': self.get_table_schema(table)
                }
                summary['total_records'] += record_count
                
            except Exception as e:
                logger.warning(f"Could not get summary for table '{table}': {e}")
                summary['tables'][table] = {
                    'record_count': 0,
                    'schema': {'columns': [], 'types': []}
                }
        
        return summary
    
    def close(self):
        """Close database connections."""
        try:
            if self.duckdb_conn:
                self.duckdb_conn.close()
                logger.info("DuckDB connection closed")
            
            if self.engine:
                self.engine.dispose()
                logger.info("SQLite connection closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")

class QueryBuilder:
    """Helper class to build SQL queries for common analytics operations."""
    
    @staticmethod
    def top_customers_by_mrr(limit: int = 10) -> str:
        """Get top customers by MRR."""
        return f"""
        SELECT 
            customer_id,
            company_name,
            plan_type,
            mrr
        FROM customers 
        ORDER BY mrr DESC 
        LIMIT {limit}
        """
    
    @staticmethod
    def usage_correlation_analysis() -> str:
        """Analyze correlation between contacts and workflows."""
        return """
        SELECT 
            c.plan_type,
            AVG(u.contacts_count) as avg_contacts,
            AVG(u.workflows_count) as avg_workflows,
            AVG(u.contacts_per_workflow) as avg_contacts_per_workflow,
            COUNT(*) as customer_count
        FROM customers c
        JOIN usage_metrics u ON c.customer_id = u.customer_id
        GROUP BY c.plan_type
        ORDER BY avg_contacts DESC
        """
    
    @staticmethod
    def top_addons_by_plan(plan_type: str = 'Standard') -> str:
        """Get top add-ons for a specific plan type."""
        return f"""
        SELECT 
            a.add_on_type,
            COUNT(*) as customer_count,
            AVG(a.add_on_cost) as avg_cost,
            SUM(a.add_on_cost) as total_revenue
        FROM add_ons a
        JOIN customers c ON a.customer_id = c.customer_id
        WHERE c.plan_type = '{plan_type}'
        GROUP BY a.add_on_type
        ORDER BY customer_count DESC
        """
    
    @staticmethod
    def customer_segments_analysis() -> str:
        """Analyze customer segments."""
        return """
        SELECT 
            mrr_segment,
            usage_segment,
            COUNT(*) as customer_count,
            AVG(mrr) as avg_mrr,
            AVG(total_usage) as avg_usage,
            plan_type
        FROM customer_segments
        GROUP BY mrr_segment, usage_segment, plan_type
        ORDER BY customer_count DESC
        """
    
    @staticmethod
    def build_custom_query(table: str, columns: List[str], filters: Dict[str, Any] = None, 
                          order_by: str = None, limit: int = None) -> str:
        """Build a custom SELECT query."""
        query = f"SELECT {', '.join(columns)} FROM {table}"
        
        if filters:
            where_clauses = []
            for column, value in filters.items():
                if isinstance(value, str):
                    where_clauses.append(f"{column} = '{value}'")
                else:
                    where_clauses.append(f"{column} = {value}")
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return query