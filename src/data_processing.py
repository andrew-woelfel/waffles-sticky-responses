import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Handle data loading, cleaning, and transformation for Help Scout customer data."""
    
    def __init__(self, data_dir: str = "data/"):
        self.data_dir = data_dir
        self.raw_data: Dict[str, pd.DataFrame] = {}
        self.processed_data: Dict[str, pd.DataFrame] = {}
        
        # Expected file names
        self.file_mapping = {
            'customers': 'customer.csv',
            'activity': 'customer_activity.csv', 
            'plans': 'plan.csv'
        }
        
    def load_raw_data(self) -> Dict[str, pd.DataFrame]:
        """Load raw customer data from CSV files."""
        loaded_data = {}
        
        for table_name, filename in self.file_mapping.items():
            filepath = f"{self.data_dir}{filename}"
            try:
                df = pd.read_csv(filepath)
                loaded_data[table_name] = df
                logger.info(f"Loaded {len(df)} records from {filename}")
            except FileNotFoundError:
                logger.warning(f"File not found: {filename}. Generating sample data.")
                loaded_data[table_name] = self._generate_sample_data(table_name)
            except Exception as e:
                logger.error(f"Error loading {filename}: {str(e)}")
                loaded_data[table_name] = self._generate_sample_data(table_name)
        
        self.raw_data = loaded_data
        return loaded_data
    
    def _generate_sample_data(self, table_name: str) -> pd.DataFrame:
        """Generate sample data matching the real schema."""
        np.random.seed(42)
        n_customers = 100
        customer_ids = [f"CUST_{i:04d}" for i in range(1, n_customers + 1)]
        
        if table_name == 'customers':
            return pd.DataFrame({
                'customer_id': customer_ids,
                'customer_name': [f"Customer Company {i}" for i in range(1, n_customers + 1)]
            })
            
        elif table_name == 'activity':
            return pd.DataFrame({
                'customer_id': customer_ids,
                'docs_sites': np.random.randint(1, 10, n_customers),
                'mailboxes': np.random.randint(1, 20, n_customers),
                'regular_users': np.random.randint(5, 100, n_customers),
                'monthly_active_users': np.random.randint(3, 80, n_customers),
                'paid_users': np.random.randint(1, 50, n_customers),
                'contacts': np.random.randint(100, 10000, n_customers).astype(str),
                'workflows': np.random.randint(5, 50, n_customers),
                'integrations': np.random.randint(0, 15, n_customers),
                'beacons': np.random.randint(0, 10, n_customers),
                'tags': np.random.randint(10, 200, n_customers).astype(str),
                'saved_replies': np.random.randint(5, 100, n_customers).astype(str),
                'light_users': np.random.randint(0, 20, n_customers),
                'all_answers_contacts': np.random.randint(50, 5000, n_customers),
                'all_resolutions': np.random.randint(20, 2000, n_customers)
            })
            
        elif table_name == 'plans':
            return pd.DataFrame({
                'customer_id': customer_ids,
                'payment_frequency': np.random.choice(['Monthly', 'Yearly'], n_customers, p=[0.7, 0.3]),
                'close_date': pd.date_range(start='2024-01-01', periods=n_customers, freq='D').strftime('%Y-%m-%d'),
                'start_date': pd.date_range(start='2020-01-01', periods=n_customers, freq='D').strftime('%Y-%m-%d'),
                'end_date': pd.date_range(start='2025-01-01', periods=n_customers, freq='D').strftime('%Y-%m-%d'),
                'months_since_active': np.random.randint(1, 48, n_customers),
                'last_reply_date': pd.date_range(start='2024-08-01', periods=n_customers, freq='D').strftime('%Y-%m-%d'),
                'plan_name': np.random.choice(['Basic', 'Standard', 'Pro', 'Enterprise'], n_customers, p=[0.3, 0.4, 0.2, 0.1]),
                'billings': np.random.choice(['Active', 'Past Due', 'Cancelled'], n_customers, p=[0.8, 0.1, 0.1]),
                'average_monthly_revenue': (np.random.lognormal(6, 0.8, n_customers) * 10).round(2).astype(str),
                'advanced_api_access': np.random.choice([0, 1], n_customers, p=[0.7, 0.3]),
                'api_rate_limit_increase': np.random.choice([0, 1], n_customers, p=[0.8, 0.2]),
                'advanced_security': np.random.choice([0, 1], n_customers, p=[0.6, 0.4])
            })
        
        return pd.DataFrame()
    
    def clean_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Clean and validate the raw data."""
        logger.info("Starting data cleaning process")
        cleaned_data = {}
        
        for table_name, df in raw_data.items():
            logger.info(f"Cleaning {table_name} table")
            cleaned_df = df.copy()
            
            if table_name == 'customers':
                cleaned_df = self._clean_customers_table(cleaned_df)
            elif table_name == 'activity':
                cleaned_df = self._clean_activity_table(cleaned_df)
            elif table_name == 'plans':
                cleaned_df = self._clean_plans_table(cleaned_df)
            
            cleaned_data[table_name] = cleaned_df
            logger.info(f"Cleaned {table_name}: {len(cleaned_df)} records")
        
        return cleaned_data
    
    def _clean_customers_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the customers table."""
        # Handle missing values
        df['customer_name'] = df['customer_name'].fillna('Unknown Customer')
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['customer_id'])
        if len(df) < initial_count:
            logger.info(f"Removed {initial_count - len(df)} duplicate customer records")
        
        return df
    
    def _clean_activity_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the customer activity table."""
        # Convert string columns to numeric where appropriate
        numeric_string_cols = ['contacts', 'tags', 'saved_replies']
        for col in numeric_string_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # Fill missing values for numeric columns
        numeric_cols = ['docs_sites', 'mailboxes', 'regular_users', 'monthly_active_users', 
                       'paid_users', 'workflows', 'integrations', 'beacons', 'light_users',
                       'all_answers_contacts', 'all_resolutions']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Data validation - ensure logical relationships
        if 'monthly_active_users' in df.columns and 'regular_users' in df.columns:
            # Monthly active users shouldn't exceed regular users
            df['monthly_active_users'] = np.minimum(df['monthly_active_users'], df['regular_users'])
        
        if 'paid_users' in df.columns and 'regular_users' in df.columns:
            # Paid users shouldn't exceed regular users
            df['paid_users'] = np.minimum(df['paid_users'], df['regular_users'])
        
        return df
    
    def _clean_plans_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the plans table."""
        # Convert date columns
        date_columns = ['close_date', 'start_date', 'end_date', 'last_reply_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Clean and convert average_monthly_revenue
        if 'average_monthly_revenue' in df.columns:
            # Remove currency symbols and convert to float
            df['average_monthly_revenue'] = df['average_monthly_revenue'].astype(str).str.replace('$', '').str.replace(',', '')
            df['average_monthly_revenue'] = pd.to_numeric(df['average_monthly_revenue'], errors='coerce').fillna(0)
        
        # Standardize plan names
        if 'plan_name' in df.columns:
            plan_mapping = {
                'basic': 'Basic',
                'standard': 'Standard', 
                'pro': 'Pro',
                'enterprise': 'Enterprise'
            }
            df['plan_name'] = df['plan_name'].str.lower().map(plan_mapping).fillna(df['plan_name'])
        
        # Ensure boolean columns are properly typed
        boolean_cols = ['advanced_api_access', 'api_rate_limit_increase', 'advanced_security']
        for col in boolean_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        
        return df
    
    def create_unified_dataset(self, cleaned_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Create a unified dataset by joining all tables on customer_id."""
        logger.info("Creating unified dataset")
        
        # Start with customers table as base
        unified = cleaned_data['customers'].copy()
        
        # Join with activity data
        if 'activity' in cleaned_data:
            unified = unified.merge(
                cleaned_data['activity'], 
                on='customer_id', 
                how='left'
            )
            logger.info("Joined activity data")
        
        # Join with plans data
        if 'plans' in cleaned_data:
            unified = unified.merge(
                cleaned_data['plans'], 
                on='customer_id', 
                how='left'
            )
            logger.info("Joined plans data")
        
        logger.info(f"Created unified dataset with {len(unified)} records and {len(unified.columns)} columns")
        return unified
    
    def create_analytical_views(self, unified_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create analytical views for common queries."""
        logger.info("Creating analytical views")
        views = {}
        
        # Customer summary view
        views['customer_summary'] = self._create_customer_summary(unified_data)
        
        # Activity metrics view
        views['activity_metrics'] = self._create_activity_metrics(unified_data)
        
        # Plan performance view
        views['plan_performance'] = self._create_plan_performance(unified_data)
        
        # Revenue analysis view
        views['revenue_analysis'] = self._create_revenue_analysis(unified_data)
        
        # Usage patterns view
        views['usage_patterns'] = self._create_usage_patterns(unified_data)
        
        logger.info(f"Created {len(views)} analytical views")
        return views
    
    def _create_customer_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create customer summary with key metrics."""
        summary_cols = ['customer_id', 'customer_name', 'plan_name', 'average_monthly_revenue',
                       'months_since_active', 'billings', 'regular_users', 'monthly_active_users']
        
        available_cols = [col for col in summary_cols if col in df.columns]
        summary = df[available_cols].copy()
        
        # Add calculated fields
        if 'average_monthly_revenue' in df.columns:
            summary['revenue_tier'] = pd.cut(
                df['average_monthly_revenue'], 
                bins=[-np.inf, 100, 500, 2000, np.inf],
                labels=['Low', 'Medium', 'High', 'Enterprise']
            )
        
        return summary
    
    def _create_activity_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create activity metrics summary."""
        activity_cols = ['customer_id', 'customer_name', 'plan_name', 'contacts', 'workflows',
                        'integrations', 'beacons', 'all_answers_contacts', 'all_resolutions']
        
        available_cols = [col for col in activity_cols if col in df.columns]
        activity = df[available_cols].copy()
        
        # Calculate engagement scores
        if 'contacts' in df.columns and 'workflows' in df.columns:
            activity['engagement_score'] = (
                df['contacts'].fillna(0) * 0.3 + 
                df['workflows'].fillna(0) * 0.4 + 
                df['integrations'].fillna(0) * 0.2 + 
                df['beacons'].fillna(0) * 0.1
            )
        
        return activity
    
    def _create_plan_performance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create plan performance analysis."""
        if 'plan_name' not in df.columns:
            return pd.DataFrame()
        
        plan_metrics = df.groupby('plan_name').agg({
            'customer_id': 'count',
            'average_monthly_revenue': ['mean', 'sum'],
            'regular_users': 'mean',
            'monthly_active_users': 'mean',
            'contacts': 'mean',
            'workflows': 'mean'
        }).round(2)
        
        # Flatten column names
        plan_metrics.columns = ['customer_count', 'avg_revenue', 'total_revenue', 
                               'avg_regular_users', 'avg_monthly_active', 'avg_contacts', 'avg_workflows']
        
        return plan_metrics.reset_index()
    
    def _create_revenue_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create revenue analysis view."""
        if 'average_monthly_revenue' not in df.columns:
            return pd.DataFrame()
        
        revenue_cols = ['customer_id', 'customer_name', 'plan_name', 'average_monthly_revenue',
                       'payment_frequency', 'billings', 'months_since_active']
        
        available_cols = [col for col in revenue_cols if col in df.columns]
        revenue = df[available_cols].copy()
        
        # Calculate annual revenue
        if 'payment_frequency' in df.columns:
            revenue['estimated_annual_revenue'] = np.where(
                df['payment_frequency'] == 'Yearly',
                df['average_monthly_revenue'],
                df['average_monthly_revenue'] * 12
            )
        
        return revenue
    
    def _create_usage_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create usage patterns analysis."""
        usage_cols = ['customer_id', 'plan_name', 'regular_users', 'monthly_active_users',
                     'contacts', 'workflows', 'integrations', 'saved_replies']
        
        available_cols = [col for col in usage_cols if col in df.columns]
        usage = df[available_cols].copy()
        
        # Calculate usage ratios
        if 'monthly_active_users' in df.columns and 'regular_users' in df.columns:
            usage['user_activation_rate'] = np.where(
                df['regular_users'] > 0,
                df['monthly_active_users'] / df['regular_users'],
                0
            )
        
        if 'contacts' in df.columns and 'workflows' in df.columns:
            usage['contacts_per_workflow'] = np.where(
                df['workflows'] > 0,
                df['contacts'] / df['workflows'],
                0
            )
        
        return usage
    
    def get_data_summary(self) -> Dict:
        """Generate summary statistics for all data."""
        if not self.processed_data:
            return {"error": "No processed data available"}
        
        summary = {
            'tables': {},
            'total_customers': 0,
            'overview': {}
        }
        
        for table_name, df in self.processed_data.items():
            summary['tables'][table_name] = {
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': df.columns.tolist()
            }
        
        # Overall statistics from unified data if available
        if 'unified' in self.processed_data:
            unified_df = self.processed_data['unified']
            summary['total_customers'] = len(unified_df)
            
            if 'average_monthly_revenue' in unified_df.columns:
                summary['overview']['total_mrr'] = unified_df['average_monthly_revenue'].sum()
                summary['overview']['avg_mrr'] = unified_df['average_monthly_revenue'].mean()
            
            if 'plan_name' in unified_df.columns:
                summary['overview']['plan_distribution'] = unified_df['plan_name'].value_counts().to_dict()
        
        return summary
    
    def process_all(self) -> Dict[str, pd.DataFrame]:
        """Execute the complete data processing pipeline."""
        logger.info("Starting complete data processing pipeline")
        
        # Load raw data
        raw_data = self.load_raw_data()
        
        # Clean the data
        cleaned_data = self.clean_data(raw_data)
        
        # Create unified dataset
        unified_data = self.create_unified_dataset(cleaned_data)
        
        # Create analytical views
        analytical_views = self.create_analytical_views(unified_data)
        
        # Store all processed data
        self.processed_data = {
            **cleaned_data,
            'unified': unified_data,
            **analytical_views
        }
        
        logger.info("Data processing pipeline completed successfully")
        return self.processed_data