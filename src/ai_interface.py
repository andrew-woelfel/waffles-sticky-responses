import pandas as pd
from typing import Dict, List, Optional, Any
import logging
import re
import os

# Import the updated query builder
try:
    from src.data_model import QueryBuilder
    QUERY_BUILDER_AVAILABLE = True
except ImportError:
    QUERY_BUILDER_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConversationalInterface:
    """AI-powered conversational interface for querying Help Scout customer data."""
    
    def __init__(self, openai_api_key: str, database_manager: Optional[Any] = None):
        self.openai_api_key = openai_api_key
        self.db_manager = database_manager
        
        # Updated query patterns for the real data schema
        self.query_patterns = {
            'top_customers': r'(top|best|highest|leading)\s+(\d+)?\s*(customers?|clients?)',
            'revenue_customers': r'(revenue|mrr|money|income|highest.*(pay|spend))',
            'usage_correlation': r'(correlation|connection|relationship).*?(contacts?|workflows?|usage)',
            'plan_analysis': r'(plan|tier|subscription).*?(performance|analysis|comparison)',
            'engagement': r'(engagement|active|activity|usage).*?(low|high|patterns?)',
            'at_risk': r'(risk|churn|danger|problem|issue).*?(customers?|revenue)',
            'feature_adoption': r'(feature|adoption|integration|api|workflow).*?(usage|rate)',
            'lifecycle': r'(lifecycle|stage|new|veteran|mature|growing)',
            'customer_insights': r'(tell me|show me|what|insights?).*?(about|interesting)',
        }
    
    def process_query(self, user_question: str) -> Dict[str, Any]:
        """Process a natural language query and return results."""
        logger.info(f"Processing query: {user_question}")
        
        try:
            # Try to match against common patterns
            pattern_result = self._match_query_patterns(user_question)
            if pattern_result:
                return pattern_result
            
            # Fallback to generic response
            return self._generate_fallback_response(user_question)
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                'answer': f"I'm sorry, I encountered an error processing your question: {str(e)}",
                'sql': None,
                'data': None,
                'error': str(e)
            }
    
    def _match_query_patterns(self, question: str) -> Optional[Dict[str, Any]]:
        """Match user question against predefined patterns."""
        question_lower = question.lower()
        
        # Top customers by revenue pattern
        if re.search(self.query_patterns['top_customers'], question_lower) or \
           re.search(self.query_patterns['revenue_customers'], question_lower):
            return self._execute_query('top_revenue_customers', question)
        
        # Usage patterns and correlation
        if re.search(self.query_patterns['usage_correlation'], question_lower):
            return self._execute_query('usage_by_plan', question)
        
        # Plan analysis
        if re.search(self.query_patterns['plan_analysis'], question_lower):
            return self._execute_query('plan_performance', question)
        
        # Engagement analysis
        if re.search(self.query_patterns['engagement'], question_lower):
            return self._execute_query('engagement_analysis', question)
        
        # At-risk customers
        if re.search(self.query_patterns['at_risk'], question_lower):
            return self._execute_query('at_risk_customers', question)
        
        # Feature adoption
        if re.search(self.query_patterns['feature_adoption'], question_lower):
            return self._execute_query('feature_adoption', question)
        
        # Customer lifecycle
        if re.search(self.query_patterns['lifecycle'], question_lower):
            return self._execute_query('lifecycle_analysis', question)
        
        # General customer insights
        if re.search(self.query_patterns['customer_insights'], question_lower):
            # Check for specific plan mentions
            plan_match = re.search(r'(basic|standard|pro|enterprise)', question_lower)
            if plan_match:
                plan = plan_match.group(1).title()
                return self._execute_custom_plan_query(plan, question)
            else:
                return self._execute_query('engagement_analysis', question)
        
        return None
    
    def _execute_query(self, query_type: str, original_question: str) -> Dict[str, Any]:
        """Execute a predefined query type."""
        if not QUERY_BUILDER_AVAILABLE:
            return self._generate_sample_response(query_type, original_question)
        
        try:
            # Get the SQL query
            common_queries = QueryBuilder.get_common_queries()
            if query_type not in common_queries:
                return self._generate_fallback_response(original_question)
            
            sql = common_queries[query_type]
            
            # Execute query if database manager is available
            if self.db_manager:
                data = self.db_manager.execute_query(sql)
                answer = self._generate_contextual_answer(query_type, data, original_question)
                
                return {
                    'answer': answer,
                    'sql': sql,
                    'data': data,
                    'error': None
                }
            else:
                # Return query without execution
                return {
                    'answer': f"Here's the SQL query that would answer your question about {query_type.replace('_', ' ')}:",
                    'sql': sql,
                    'data': None,
                    'error': None
                }
                
        except Exception as e:
            logger.error(f"Error executing query type '{query_type}': {e}")
            return self._generate_sample_response(query_type, original_question)
    
    def _execute_custom_plan_query(self, plan: str, original_question: str) -> Dict[str, Any]:
        """Execute a custom query filtered by plan type."""
        try:
            sql = f"""
            SELECT 
                c.customer_name,
                p.plan_name,
                CAST(p.average_monthly_revenue AS FLOAT) as revenue,
                a.regular_users,
                a.monthly_active_users,
                a.contacts,
                a.workflows,
                a.integrations,
                p.billings
            FROM customers c
            JOIN plans p ON c.customer_id = p.customer_id
            JOIN activity a ON c.customer_id = a.customer_id
            WHERE p.plan_name = '{plan}'
            ORDER BY CAST(p.average_monthly_revenue AS FLOAT) DESC
            LIMIT 20
            """
            
            if self.db_manager:
                data = self.db_manager.execute_query(sql)
                answer = f"Here's information about {plan} plan customers:\n\n"
                
                if not data.empty:
                    total_revenue = data['revenue'].sum()
                    avg_revenue = data['revenue'].mean()
                    total_customers = len(data)
                    
                    answer += f"• Total {plan} customers analyzed: {total_customers}\n"
                    answer += f"• Total monthly revenue: ${total_revenue:,.2f}\n"
                    answer += f"• Average revenue per customer: ${avg_revenue:,.2f}\n"
                    
                    if 'regular_users' in data.columns:
                        avg_users = data['regular_users'].mean()
                        answer += f"• Average users per customer: {avg_users:.1f}\n"
                    
                    if 'workflows' in data.columns:
                        avg_workflows = data['workflows'].mean()
                        answer += f"• Average workflows per customer: {avg_workflows:.1f}"
                
                return {
                    'answer': answer,
                    'sql': sql,
                    'data': data,
                    'error': None
                }
            else:
                return self._generate_sample_response('plan_specific', original_question)
                
        except Exception as e:
            logger.error(f"Error executing custom plan query: {e}")
            return self._generate_sample_response('plan_specific', original_question)
    
    def _generate_contextual_answer(self, query_type: str, data: pd.DataFrame, original_question: str) -> str:
        """Generate a contextual answer based on query type and data."""
        if data.empty:
            return f"No results found for your question about {query_type.replace('_', ' ')}."
        
        base_answer = f"Based on your question '{original_question}', here's what I found:\n\n"
        
        if query_type == 'top_revenue_customers':
            if 'average_monthly_revenue' in data.columns or 'revenue' in data.columns:
                revenue_col = 'revenue' if 'revenue' in data.columns else 'average_monthly_revenue'
                total_revenue = data[revenue_col].sum()
                base_answer += f"• Top {len(data)} customers represent ${total_revenue:,.2f} in monthly revenue\n"
                if len(data) > 0:
                    top_customer = data.iloc[0]
                    customer_name = top_customer.get('customer_name', 'Customer')
                    customer_revenue = top_customer.get(revenue_col, 0)
                    base_answer += f"• Highest revenue customer: {customer_name} (${customer_revenue:,.2f}/month)"
        
        elif query_type == 'usage_by_plan':
            base_answer += f"• Analyzed usage patterns across {len(data)} plan types\n"
            if 'avg_contacts' in data.columns:
                highest_usage_plan = data.loc[data['avg_contacts'].idxmax(), 'plan_name']
                base_answer += f"• Highest usage plan: {highest_usage_plan}"
        
        elif query_type == 'engagement_analysis':
            base_answer += f"• Analyzed engagement for {len(data)} customers\n"
            if 'user_activation_rate' in data.columns:
                avg_activation = data['user_activation_rate'].mean()
                base_answer += f"• Average user activation rate: {avg_activation:.1f}%"
        
        elif query_type == 'at_risk_customers':
            base_answer += f"• Found {len(data)} customers at risk\n"
            if 'revenue' in data.columns:
                at_risk_revenue = data['revenue'].sum()
                base_answer += f"• Total revenue at risk: ${at_risk_revenue:,.2f}/month"
        
        elif query_type == 'plan_performance':
            base_answer += f"• Performance analysis across {len(data)} plan types\n"
            if 'total_monthly_revenue' in data.columns:
                best_plan = data.loc[data['total_monthly_revenue'].idxmax(), 'plan_name']
                base_answer += f"• Highest performing plan: {best_plan}"
        
        else:
            base_answer += f"• Found {len(data)} records matching your query"
        
        return base_answer
    
    def _generate_sample_response(self, query_type: str, original_question: str) -> Dict[str, Any]:
        """Generate a sample response when database is not available."""
        sample_data = self._create_sample_data_for_query_type(query_type)
        
        responses = {
            'top_revenue_customers': "Here are the top revenue customers (sample data):",
            'usage_by_plan': "Here's usage pattern analysis by plan type (sample data):",
            'engagement_analysis': "Here's customer engagement analysis (sample data):",
            'at_risk_customers': "Here are customers at risk (sample data):",
            'plan_performance': "Here's plan performance analysis (sample data):",
            'feature_adoption': "Here's feature adoption analysis (sample data):",
            'lifecycle_analysis': "Here's customer lifecycle analysis (sample data):"
        }
        
        answer = responses.get(query_type, f"Here's analysis for '{original_question}' (sample data):")
        
        return {
            'answer': answer,
            'sql': f"-- Sample SQL query for {query_type}",
            'data': sample_data,
            'error': None
        }
    
    def _create_sample_data_for_query_type(self, query_type: str) -> pd.DataFrame:
        """Create appropriate sample data based on query type."""
        import numpy as np
        np.random.seed(42)
        
        if query_type == 'top_revenue_customers':
            return pd.DataFrame({
                'customer_name': [f'Customer {i}' for i in range(1, 11)],
                'plan_name': np.random.choice(['Pro', 'Enterprise', 'Standard'], 10),
                'revenue': np.random.lognormal(6.5, 0.5, 10).round(2),
                'billings': ['Active'] * 10
            })
        
        elif query_type == 'usage_by_plan':
            return pd.DataFrame({
                'plan_name': ['Basic', 'Standard', 'Pro', 'Enterprise'],
                'customer_count': [30, 40, 20, 10],
                'avg_contacts': [150, 500, 1200, 3000],
                'avg_workflows': [5, 12, 25, 50],
                'avg_revenue': [99, 299, 599, 1299]
            })
        
        elif query_type == 'engagement_analysis':
            return pd.DataFrame({
                'customer_name': [f'Customer {i}' for i in range(1, 11)],
                'plan_name': np.random.choice(['Basic', 'Standard', 'Pro'], 10),
                'user_activation_rate': np.random.uniform(20, 95, 10).round(1),
                'contacts_per_workflow': np.random.uniform(10, 100, 10).round(1),
                'revenue': np.random.lognormal(5.5, 0.8, 10).round(2)
            })
        
        elif query_type == 'at_risk_customers':
            return pd.DataFrame({
                'customer_name': [f'At Risk Customer {i}' for i in range(1, 6)],
                'plan_name': np.random.choice(['Standard', 'Pro'], 5),
                'revenue': np.random.uniform(200, 1000, 5).round(2),
                'risk_category': np.random.choice(['Low Engagement', 'Billing Issue', 'No Activity'], 5),
                'activation_rate': np.random.uniform(10, 40, 5).round(1)
            })
        
        else:
            # Generic sample data
            return pd.DataFrame({
                'customer_name': [f'Sample Customer {i}' for i in range(1, 6)],
                'metric_value': np.random.uniform(100, 1000, 5).round(2),
                'category': np.random.choice(['A', 'B', 'C'], 5)
            })
    
    def _generate_fallback_response(self, question: str) -> Dict[str, Any]:
        """Generate a fallback response for unmatched questions."""
        return {
            'answer': f"I understand you're asking: '{question}'. In the full version with database connectivity, I would analyze this question against the Help Scout customer data including customer information, activity metrics, and plan details. For now, try one of the sample questions in the sidebar!",
            'sql': "-- This would be a generated SQL query based on your question",
            'data': None,
            'error': None
        }
    
    def get_example_questions(self) -> List[str]:
        """Get list of example questions the interface can handle."""
        return [
            "Who are the top 10 customers by revenue?",
            "Show me usage patterns across different plans",
            "Which customers have the highest engagement rates?",
            "Find customers at risk of churning based on activity",
            "What's the average monthly revenue by plan type?",
            "Show me workflow usage correlation with customer revenue", 
            "Which Standard plan customers use the most integrations?",
            "Find high-revenue customers with low user activation",
            "What's the feature adoption rate across plan types?",
            "Show me customer lifecycle analysis by months active",
            "Which customers have advanced API access enabled?",
            "Compare contacts per workflow across different plans",
            "Find Enterprise customers with billing issues",
            "Show me the distribution of regular vs active users",
            "Which plan generates the most total revenue?"
        ]
    
    def get_available_analyses(self) -> Dict[str, str]:
        """Get available analysis types with descriptions."""
        return {
            'Revenue Analysis': 'Top customers by average monthly revenue, revenue distribution by plan, billing status analysis',
            'Usage Patterns': 'Contacts, workflows, integrations usage by plan type, user activation rates',
            'Customer Engagement': 'Monthly active users vs regular users, workflow adoption, contact management patterns',
            'Risk Assessment': 'At-risk customers based on billing status, low engagement, inactive users',
            'Plan Performance': 'Revenue and usage comparison across Basic, Standard, Pro, Enterprise plans',
            'Feature Adoption': 'API access usage, advanced security adoption, integrations by plan',
            'Lifecycle Analysis': 'Customer tenure analysis using months_since_active, maturity stages',
            'Billing & Revenue': 'Payment frequency analysis, revenue forecasting, past due accounts',
            'User Activity': 'Regular users, monthly active users, light users analysis by customer',
            'Custom Segmentation': 'Deep-dive analysis for specific plans, revenue tiers, or customer segments'
        }