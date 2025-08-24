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
    """Helper class to build SQL queries for the Help Scout data schema."""
    
    @staticmethod
    def top_customers_by_revenue(limit: int = 10) -> str:
        """Get top customers by average monthly revenue."""
        return f"""
        SELECT 
            c.customer_id,
            c.customer_name,
            p.plan_name,
            p.average_monthly_revenue,
            p.billings
        FROM customers c
        JOIN plans p ON c.customer_id = p.customer_id
        ORDER BY CAST(p.average_monthly_revenue AS FLOAT) DESC 
        LIMIT {limit}
        """
    
    @staticmethod
    def usage_patterns_by_plan() -> str:
        """Analyze usage patterns by plan type."""
        return """
        SELECT 
            p.plan_name,
            COUNT(c.customer_id) as customer_count,
            AVG(CAST(a.contacts AS FLOAT)) as avg_contacts,
            AVG(a.workflows) as avg_workflows,
            AVG(a.regular_users) as avg_regular_users,
            AVG(a.monthly_active_users) as avg_monthly_active_users,
            AVG(a.integrations) as avg_integrations,
            AVG(CAST(p.average_monthly_revenue AS FLOAT)) as avg_revenue
        FROM customers c
        JOIN plans p ON c.customer_id = p.customer_id
        JOIN activity a ON c.customer_id = a.customer_id
        GROUP BY p.plan_name
        ORDER BY avg_revenue DESC
        """
    
    @staticmethod
    def customer_engagement_analysis() -> str:
        """Analyze customer engagement metrics."""
        return """
        SELECT 
            c.customer_id,
            c.customer_name,
            p.plan_name,
            a.regular_users,
            a.monthly_active_users,
            CASE 
                WHEN a.regular_users > 0 
                THEN ROUND(CAST(a.monthly_active_users AS FLOAT) / a.regular_users * 100, 2)
                ELSE 0 
            END as user_activation_rate,
            a.contacts,
            a.workflows,
            CASE 
                WHEN a.workflows > 0 
                THEN ROUND(CAST(a.contacts AS FLOAT) / a.workflows, 2)
                ELSE 0 
            END as contacts_per_workflow,
            CAST(p.average_monthly_revenue AS FLOAT) as revenue
        FROM customers c
        JOIN plans p ON c.customer_id = p.customer_id
        JOIN activity a ON c.customer_id = a.customer_id
        ORDER BY revenue DESC
        """
    
    @staticmethod
    def plan_performance_summary() -> str:
        """Get plan performance summary."""
        return """
        SELECT 
            p.plan_name,
            COUNT(*) as total_customers,
            SUM(CASE WHEN p.billings = 'Active' THEN 1 ELSE 0 END) as active_customers,
            ROUND(AVG(CAST(p.average_monthly_revenue AS FLOAT)), 2) as avg_monthly_revenue,
            ROUND(SUM(CAST(p.average_monthly_revenue AS FLOAT)), 2) as total_monthly_revenue,
            ROUND(AVG(a.regular_users), 2) as avg_users,
            ROUND(AVG(CAST(a.contacts AS FLOAT)), 0) as avg_contacts,
            ROUND(AVG(a.workflows), 1) as avg_workflows
        FROM plans p
        JOIN activity a ON p.customer_id = a.customer_id
        GROUP BY p.plan_name
        ORDER BY total_monthly_revenue DESC
        """
    
    @staticmethod
    def high_value_customers_with_low_engagement() -> str:
        """Find high-value customers with low engagement."""
        return """
        SELECT 
            c.customer_name,
            p.plan_name,
            CAST(p.average_monthly_revenue AS FLOAT) as revenue,
            a.regular_users,
            a.monthly_active_users,
            CASE 
                WHEN a.regular_users > 0 
                THEN ROUND(CAST(a.monthly_active_users AS FLOAT) / a.regular_users * 100, 2)
                ELSE 0 
            END as activation_rate,
            a.workflows,
            a.integrations
        FROM customers c
        JOIN plans p ON c.customer_id = p.customer_id
        JOIN activity a ON c.customer_id = a.customer_id
        WHERE CAST(p.average_monthly_revenue AS FLOAT) > 500
        AND (
            CASE 
                WHEN a.regular_users > 0 
                THEN CAST(a.monthly_active_users AS FLOAT) / a.regular_users
                ELSE 0 
            END
        ) < 0.5
        ORDER BY revenue DESC
        """
    
    @staticmethod
    def feature_adoption_by_plan() -> str:
        """Analyze feature adoption by plan type."""
        return """
        SELECT 
            p.plan_name,
            COUNT(*) as customer_count,
            AVG(CASE WHEN a.integrations > 0 THEN 1.0 ELSE 0.0 END) * 100 as integration_adoption_rate,
            AVG(CASE WHEN a.workflows > 5 THEN 1.0 ELSE 0.0 END) * 100 as workflow_power_user_rate,
            AVG(CASE WHEN a.beacons > 0 THEN 1.0 ELSE 0.0 END) * 100 as beacon_adoption_rate,
            AVG(CASE WHEN p.advanced_api_access = 1 THEN 1.0 ELSE 0.0 END) * 100 as api_adoption_rate,
            AVG(CASE WHEN p.advanced_security = 1 THEN 1.0 ELSE 0.0 END) * 100 as security_adoption_rate
        FROM plans p
        JOIN activity a ON p.customer_id = a.customer_id
        GROUP BY p.plan_name
        ORDER BY customer_count DESC
        """
    
    @staticmethod
    def customer_lifecycle_analysis() -> str:
        """Analyze customer lifecycle stages."""
        return """
        SELECT 
            c.customer_name,
            p.plan_name,
            p.months_since_active,
            CASE 
                WHEN p.months_since_active <= 3 THEN 'New'
                WHEN p.months_since_active <= 12 THEN 'Growing' 
                WHEN p.months_since_active <= 24 THEN 'Mature'
                ELSE 'Veteran'
            END as lifecycle_stage,
            CAST(p.average_monthly_revenue AS FLOAT) as revenue,
            p.billings,
            a.monthly_active_users,
            a.workflows
        FROM customers c
        JOIN plans p ON c.customer_id = p.customer_id  
        JOIN activity a ON c.customer_id = a.customer_id
        ORDER BY p.months_since_active DESC
        """
    
    @staticmethod
    def revenue_at_risk_analysis() -> str:
        """Identify revenue at risk from billing issues or low engagement."""
        return """
        SELECT 
            c.customer_name,
            p.plan_name,
            CAST(p.average_monthly_revenue AS FLOAT) as revenue,
            p.billings as billing_status,
            p.months_since_active,
            a.monthly_active_users,
            a.regular_users,
            CASE 
                WHEN p.billings != 'Active' THEN 'Billing Issue'
                WHEN a.monthly_active_users = 0 THEN 'No Activity'
                WHEN a.regular_users > 0 AND CAST(a.monthly_active_users AS FLOAT) / a.regular_users < 0.3 THEN 'Low Engagement'
                ELSE 'Healthy'
            END as risk_category
        FROM customers c
        JOIN plans p ON c.customer_id = p.customer_id
        JOIN activity a ON c.customer_id = a.customer_id
        WHERE p.billings != 'Active' 
           OR a.monthly_active_users = 0
           OR (a.regular_users > 0 AND CAST(a.monthly_active_users AS FLOAT) / a.regular_users < 0.3)
        ORDER BY revenue DESC
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
    
    @staticmethod
    def get_table_relationships() -> Dict[str, str]:
        """Get information about table relationships."""
        return {
            'customers': 'Base table with customer information',
            'activity': 'Customer activity metrics (joined on customer_id)',
            'plans': 'Plan and billing information (joined on customer_id)',
            'unified': 'All tables joined together',
            'customer_summary': 'Summary view with key customer metrics',
            'activity_metrics': 'Processed activity data with engagement scores',
            'plan_performance': 'Aggregated plan performance metrics',
            'revenue_analysis': 'Revenue-focused customer analysis',
            'usage_patterns': 'Usage patterns and ratios'
        }
    
    @staticmethod
    def get_common_queries() -> Dict[str, str]:
        """Get a dictionary of common query patterns."""
        return {
            'top_revenue_customers': QueryBuilder.top_customers_by_revenue(),
            'usage_by_plan': QueryBuilder.usage_patterns_by_plan(),
            'engagement_analysis': QueryBuilder.customer_engagement_analysis(),
            'plan_performance': QueryBuilder.plan_performance_summary(),
            'at_risk_customers': QueryBuilder.revenue_at_risk_analysis(),
            'feature_adoption': QueryBuilder.feature_adoption_by_plan(),
            'lifecycle_analysis': QueryBuilder.customer_lifecycle_analysis(),
            'low_engagement_high_value': QueryBuilder.high_value_customers_with_low_engagement()
        }