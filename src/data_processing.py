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
    
    def __init__(self, data_path: str = "data/saas_customer_data.csv"):
        self.data_path = data_path
        self.raw_data: Optional[pd.DataFrame] = None
        self.processed_data: Dict[str, pd.DataFrame] = {}
        
    def load_raw_data(self) -> pd.DataFrame:
        """Load raw customer data from CSV."""
        try:
            self.raw_data = pd.read_csv(self.data_path)
            logger.info(f"Loaded {len(self.raw_data)} records from {self.data_path}")
            return self.raw_data
        except FileNotFoundError:
            logger.error(f"Data file not found: {self.data_path}")
            # Return sample data for development
            return self._generate_sample_data()
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _generate_sample_data(self) -> pd.DataFrame:
        """Generate sample data for development/testing purposes."""
        np.random.seed(42)
        n_customers = 1000
        
        # Generate sample customer data
        data = {
            'customer_id': [f"CUST_{i:04d}" for i in range(1, n_customers + 1)],
            'company_name': [f"Company {i}" for i in range(1, n_customers + 1)],
            'plan_type': np.random.choice(['Basic', 'Standard', 'Pro', 'Enterprise'], n_customers, p=[0.3, 0.4, 0.2, 0.1]),
            'mrr': np.random.lognormal(5.5, 1, n_customers).astype(int),
            'contacts_count': np.random.poisson(100, n_customers),
            'workflows_count': np.random.poisson(15, n_customers),
            'created_date': pd.date_range(start='2020-01-01', periods=n_customers, freq='D'),
            'last_activity_date': pd.date_range(start='2024-01-01', periods=n_customers, freq='H'),
            'add_on_type': np.random.choice(['Advanced Reports', 'Extra Storage', 'Priority Support', None], n_customers, p=[0.2, 0.3, 0.25, 0.25]),
            'add_on_cost': np.where(np.random.choice([True, False], n_customers, p=[0.75, 0.25]), 
                                  np.random.choice([29, 49, 99, 199], n_customers), 0)
        }
        
        df = pd.DataFrame(data)
        logger.info(f"Generated {len(df)} sample records for development")
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate the raw data."""
        logger.info("Starting data cleaning process")
        
        # Make a copy to avoid modifying original
        cleaned_df = df.copy()
        
        # Handle missing values
        cleaned_df['company_name'] = cleaned_df['company_name'].fillna('Unknown Company')
        cleaned_df['plan_type'] = cleaned_df['plan_type'].fillna('Basic')
        cleaned_df['mrr'] = pd.to_numeric(cleaned_df['mrr'], errors='coerce').fillna(0)
        cleaned_df['contacts_count'] = pd.to_numeric(cleaned_df['contacts_count'], errors='coerce').fillna(0)
        cleaned_df['workflows_count'] = pd.to_numeric(cleaned_df['workflows_count'], errors='coerce').fillna(0)
        
        # Standardize plan types
        plan_mapping = {
            'basic': 'Basic',
            'standard': 'Standard', 
            'pro': 'Pro',
            'enterprise': 'Enterprise'
        }
        cleaned_df['plan_type'] = cleaned_df['plan_type'].str.lower().map(plan_mapping).fillna(cleaned_df['plan_type'])
        
        # Convert date columns
        date_columns = ['created_date', 'last_activity_date']
        for col in date_columns:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
        
        # Remove duplicates
        initial_count = len(cleaned_df)
        cleaned_df = cleaned_df.drop_duplicates(subset=['customer_id'])
        if len(cleaned_df) < initial_count:
            logger.info(f"Removed {initial_count - len(cleaned_df)} duplicate records")
        
        # Data validation
        self._validate_data(cleaned_df)
        
        logger.info(f"Data cleaning completed. Final dataset has {len(cleaned_df)} records")
        return cleaned_df
    
    def _validate_data(self, df: pd.DataFrame) -> None:
        """Validate data quality and log issues."""
        issues = []
        
        # Check for negative values where they shouldn't exist
        numeric_columns = ['mrr', 'contacts_count', 'workflows_count']
        for col in numeric_columns:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    issues.append(f"{negative_count} negative values in {col}")
        
        # Check for outliers (values > 99th percentile)
        for col in numeric_columns:
            if col in df.columns:
                threshold = df[col].quantile(0.99)
                outlier_count = (df[col] > threshold).sum()
                if outlier_count > 0:
                    issues.append(f"{outlier_count} potential outliers in {col} (> {threshold:.0f})")
        
        if issues:
            logger.warning("Data quality issues found:")
            for issue in issues:
                logger.warning(f"  - {issue}")
    
    def create_normalized_tables(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create normalized table structure from flat data."""
        logger.info("Creating normalized table structure")
        
        tables = {}
        
        # Customers table (main entity)
        customers_cols = ['customer_id', 'company_name', 'plan_type', 'mrr', 'created_date', 'last_activity_date']
        tables['customers'] = df[customers_cols].drop_duplicates(subset=['customer_id']).reset_index(drop=True)
        
        # Usage metrics table
        usage_cols = ['customer_id', 'contacts_count', 'workflows_count']
        tables['usage_metrics'] = df[usage_cols].reset_index(drop=True)
        
        # Add calculated usage metrics
        tables['usage_metrics']['total_usage'] = (
            tables['usage_metrics']['contacts_count'] + tables['usage_metrics']['workflows_count']
        )
        tables['usage_metrics']['contacts_per_workflow'] = np.where(
            tables['usage_metrics']['workflows_count'] > 0,
            tables['usage_metrics']['contacts_count'] / tables['usage_metrics']['workflows_count'],
            0
        )
        
        # Add-ons table
        addon_data = df[df['add_on_type'].notna()][['customer_id', 'add_on_type', 'add_on_cost']].reset_index(drop=True)
        if not addon_data.empty:
            tables['add_ons'] = addon_data
        else:
            # Create empty table with proper structure
            tables['add_ons'] = pd.DataFrame(columns=['customer_id', 'add_on_type', 'add_on_cost'])
        
        # Customer segments (derived table)
        tables['customer_segments'] = self._create_customer_segments(tables['customers'], tables['usage_metrics'])
        
        # Log table statistics
        for table_name, table_df in tables.items():
            logger.info(f"Created {table_name} table with {len(table_df)} records")
        
        self.processed_data = tables
        return tables
    
    def _create_customer_segments(self, customers: pd.DataFrame, usage: pd.DataFrame) -> pd.DataFrame:
        """Create customer segmentation based on MRR and usage patterns."""
        # Merge customers with usage data
        merged = customers.merge(usage, on='customer_id', how='left')
        
        # Define segmentation logic
        def categorize_customer(row):
            mrr = row['mrr']
            total_usage = row['total_usage']
            
            # MRR categories
            if mrr >= 1000:
                mrr_segment = 'High Value'
            elif mrr >= 500:
                mrr_segment = 'Medium Value'
            else:
                mrr_segment = 'Low Value'
            
            # Usage categories
            if total_usage >= 200:
                usage_segment = 'High Usage'
            elif total_usage >= 50:
                usage_segment = 'Medium Usage'
            else:
                usage_segment = 'Low Usage'
            
            return mrr_segment, usage_segment
        
        # Apply segmentation
        segments = merged.apply(categorize_customer, axis=1, result_type='expand')
        segments.columns = ['mrr_segment', 'usage_segment']
        
        # Combine with customer data
        result = pd.concat([
            merged[['customer_id', 'company_name', 'plan_type', 'mrr', 'total_usage']],
            segments
        ], axis=1)
        
        return result
    
    def get_data_summary(self) -> Dict:
        """Generate summary statistics for the processed data."""
        if not self.processed_data:
            return {"error": "No processed data available"}
        
        summary = {}
        
        # Overall statistics
        customers_df = self.processed_data['customers']
        usage_df = self.processed_data['usage_metrics']
        
        summary['overview'] = {
            'total_customers': len(customers_df),
            'total_mrr': customers_df['mrr'].sum(),
            'avg_mrr': customers_df['mrr'].mean(),
            'plan_distribution': customers_df['plan_type'].value_counts().to_dict()
        }
        
        # Usage statistics
        summary['usage'] = {
            'avg_contacts': usage_df['contacts_count'].mean(),
            'avg_workflows': usage_df['workflows_count'].mean(),
            'total_contacts': usage_df['contacts_count'].sum(),
            'total_workflows': usage_df['workflows_count'].sum()
        }
        
        # Add-on statistics
        if not self.processed_data['add_ons'].empty:
            addon_df = self.processed_data['add_ons']
            summary['add_ons'] = {
                'customers_with_addons': len(addon_df),
                'addon_types': addon_df['add_on_type'].value_counts().to_dict(),
                'total_addon_revenue': addon_df['add_on_cost'].sum(),
                'avg_addon_cost': addon_df['add_on_cost'].mean()
            }
        else:
            summary['add_ons'] = {
                'customers_with_addons': 0,
                'addon_types': {},
                'total_addon_revenue': 0,
                'avg_addon_cost': 0
            }
        
        return summary
    
    def process_all(self) -> Dict[str, pd.DataFrame]:
        """Execute the complete data processing pipeline."""
        logger.info("Starting complete data processing pipeline")
        
        # Load raw data
        raw_data = self.load_raw_data()
        
        # Clean the data
        cleaned_data = self.clean_data(raw_data)
        
        # Create normalized tables
        tables = self.create_normalized_tables(cleaned_data)
        
        logger.info("Data processing pipeline completed successfully")
        return tables