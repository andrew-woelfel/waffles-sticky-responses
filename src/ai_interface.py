# Navigate to your project
cd waffles-sticky-responses

# Replace the broken ai_interface.py file
cat > src/ai_interface.py << 'EOF'
import pandas as pd
from typing import Dict, List, Optional, Any
import logging
import re
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConversationalInterface:
    """AI-powered conversational interface for querying Help Scout customer data."""
    
    def __init__(self, openai_api_key: str, database_manager: Optional[Any] = None):
        self.openai_api_key = openai_api_key
        self.db_manager = database_manager
        
        # Query patterns for common questions - FIXED SYNTAX
        self.query_patterns = {
            'top_customers': r'(top|best|highest|leading)\s+(\d+)?\s*(customers?|clients?)',
            'usage_correlation': r'(correlation|connection|relationship).*?(contacts?|workflows?)',
            'plan_addons': r'(addons?|add-ons?|extensions?).*?(plan|tier)',
            'customer_insights': r'(tell me|show me|what|insights?).*?(about|pro|standard|basic)',
            'revenue_analysis': r'(revenue|mrr|income|earnings)',
            'churn_analysis': r'(churn|retention|lost|leaving)'
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
        
        # Create sample data for demo responses
        sample_data = self._create_sample_data()
        
        # Top customers pattern
        if re.search(self.query_patterns['top_customers'], question_lower):
            limit_match = re.search(r'(\d+)', question)
            limit = int(limit_match.group(1)) if limit_match else 10
            
            top_customers = sample_data.nlargest(limit, 'mrr')[['company_name', 'plan_type', 'mrr']]
            
            return {
                'answer': f"Here are the top {limit} customers by MRR (sample data):",
                'sql': f"SELECT company_name, plan_type, mrr FROM customers ORDER BY mrr DESC LIMIT {limit}",
                'data': top_customers,
                'error': None
            }
        
        # Usage correlation pattern
        if re.search(self.query_patterns['usage_correlation'], question_lower):
            correlation_data = sample_data.groupby('plan_type').agg({
                'contacts_count': 'mean',
                'workflows_count': 'mean'
            }).round(2)
            
            return {
                'answer': "Here's the analysis of usage patterns by plan type (sample data):",
                'sql': "SELECT plan_type, AVG(contacts_count) as avg_contacts, AVG(workflows_count) as avg_workflows FROM usage_metrics u JOIN customers c ON u.customer_id = c.customer_id GROUP BY plan_type",
                'data': correlation_data.reset_index(),
                'error': None
            }
        
        # Plan add-ons pattern
        if re.search(self.query_patterns['plan_addons'], question_lower):
            plan_match = re.search(r'(basic|standard|pro|enterprise)', question_lower)
            plan = plan_match.group(1).title() if plan_match else 'Standard'
            
            plan_data = sample_data[sample_data['plan_type'] == plan].head(10)
            
            return {
                'answer': f"Here's information about {plan} plan customers (sample data):",
                'sql': f"SELECT * FROM customers WHERE plan_type = '{plan}' LIMIT 10",
                'data': plan_data,
                'error': None
            }
        
        # Customer insights pattern
        if re.search(self.query_patterns['customer_insights'], question_lower):
            insights_data = sample_data.groupby('plan_type').agg({
                'mrr': ['count', 'mean', 'sum'],
                'contacts_count': 'mean',
                'workflows_count': 'mean'
            }).round(2)
            
            insights_data.columns = ['Customer Count', 'Avg MRR', 'Total MRR', 'Avg Contacts', 'Avg Workflows']
            
            return {
                'answer': "Here's an analysis of customer segments (sample data):",
                'sql': "SELECT plan_type, COUNT(*) as customer_count, AVG(mrr) as avg_mrr, SUM(mrr) as total_mrr FROM customers GROUP BY plan_type",
                'data': insights_data.reset_index(),
                'error': None
            }
        
        return None
    
    def _create_sample_data(self):
        """Create sample data for demonstration."""
        import numpy as np
        
        np.random.seed(42)
        n_customers = 100
        
        data = {
            'customer_id': [f"CUST_{i:04d}" for i in range(1, n_customers + 1)],
            'company_name': [f"Company {i}" for i in range(1, n_customers + 1)],
            'plan_type': np.random.choice(['Basic', 'Standard', 'Pro', 'Enterprise'], n_customers, p=[0.3, 0.4, 0.2, 0.1]),
            'mrr': np.random.lognormal(5.5, 1, n_customers).astype(int),
            'contacts_count': np.random.poisson(100, n_customers),
            'workflows_count': np.random.poisson(15, n_customers),
        }
        
        return pd.DataFrame(data)
    
    def _generate_fallback_response(self, question: str) -> Dict[str, Any]:
        """Generate a fallback response for unmatched questions."""
        return {
            'answer': f"I understand you're asking: '{question}'. In the full version, I would analyze this question and provide insights from the customer data. For now, try one of the sample questions in the sidebar!",
            'sql': "-- This would be a generated SQL query based on your question",
            'data': None,
            'error': None
        }
    
    def get_example_questions(self) -> List[str]:
        """Get list of example questions the interface can handle."""
        return [
            "Who are the top 10 customers by MRR?",
            "Do contacts and workflows have a usage connection?",
            "What are the top add-ons for Standard plans?",
            "Tell me something interesting about Pro customers",
            "Show me customer segments analysis",
            "What's the average MRR by plan type?",
            "Which customers have the most add-ons?",
            "How do usage patterns differ across plans?"
        ]
EOF