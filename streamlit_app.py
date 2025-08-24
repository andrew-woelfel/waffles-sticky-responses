import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv
import numpy as np

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Help Scout Analytics - Conversational Interface",
    page_icon="assets/helpscout-icon.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'initialized' not in st.session_state:
    st.session_state.initialized = False
    st.session_state.components_loaded = False
    st.session_state.data_loaded = False

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
    }
    .success-card {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
        margin-bottom: 1rem;
    }
    .warning-card {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def initialize_components():
    """Initialize and cache the application components."""
    try:
        from src.data_processing import DataProcessor
        from src.data_model import DatabaseManager, QueryBuilder
        from src.ai_interface import ConversationalInterface
        from src.config import Config
        
        config = Config()
        data_processor = DataProcessor()
        db_manager = DatabaseManager(config.database_url)
        ai_interface = ConversationalInterface(config.openai_api_key, db_manager)
        
        return True, (config, data_processor, db_manager, ai_interface)
    except ImportError as e:
        st.error(f"Import Error: {e}")
        return False, None
    except Exception as e:
        st.error(f"Initialization Error: {e}")
        return False, None

@st.cache_data
def load_processed_data():
    """Load and cache only the processed data, not database objects."""
    try:
        from src.data_processing import DataProcessor
        
        data_processor = DataProcessor()
        processed_data = data_processor.process_all()
        data_summary = data_processor.get_data_summary()
        
        return True, (processed_data, data_summary), None
        
    except Exception as e:
        return False, None, str(e)

def get_database_components():
    """Get database components without caching."""
    try:
        from src.data_model import DatabaseManager, QueryBuilder
        from src.ai_interface import ConversationalInterface
        from src.config import Config
        
        config = Config()
        db_manager = DatabaseManager(config.database_url)
        ai_interface = ConversationalInterface(config.openai_api_key, db_manager)
        
        return db_manager, ai_interface
        
    except Exception as e:
        st.error(f"Error creating database components: {e}")
        return None, None

def display_data_overview(data_summary: dict, processed_data: dict):
    """Display data overview in the sidebar."""
    st.sidebar.markdown("### 📊 Data Overview")
    
    if 'overview' in data_summary:
        overview = data_summary['overview']
        
        if 'total_mrr' in overview:
            st.sidebar.markdown(f"""
            <div class="success-card">
                <h4 style="margin: 0; color: #28a745;">Total MRR</h4>
                <h3 style="margin: 0.5rem 0 0 0;">${overview['total_mrr']:,.2f}</h3>
            </div>
            """, unsafe_allow_html=True)
        
        if 'plan_distribution' in overview:
            st.sidebar.markdown("**Plan Distribution:**")
            for plan, count in overview['plan_distribution'].items():
                st.sidebar.write(f"• {plan}: {count} customers")
    
    # Table information
    if 'tables' in data_summary:
        st.sidebar.markdown("**Available Data:**")
        key_tables = ['customers', 'activity', 'plans', 'unified']
        for table in key_tables:
            if table in data_summary['tables']:
                count = data_summary['tables'][table]['row_count']
                st.sidebar.write(f"• {table.title()}: {count:,} records")

def create_visualization(data: pd.DataFrame, query_type: str, question: str):
    """Create appropriate visualization based on data and query type."""
    if data.empty:
        st.info("No data to visualize")
        return
    
    try:
        # Revenue-based visualizations
        if any(col in data.columns for col in ['revenue', 'average_monthly_revenue', 'total_monthly_revenue']):
            revenue_col = None
            for col in ['revenue', 'average_monthly_revenue', 'total_monthly_revenue']:
                if col in data.columns:
                    revenue_col = col
                    break
            
            if revenue_col and len(data) > 1:
                if 'customer_name' in data.columns:
                    # Top customers bar chart
                    fig = px.bar(data.head(10), 
                               x='customer_name', y=revenue_col,
                               color='plan_name' if 'plan_name' in data.columns else None,
                               title=f"Revenue Analysis: {question}")
                    fig.update_xaxis(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                    
                elif 'plan_name' in data.columns:
                    # Plan performance chart
                    fig = px.bar(data, x='plan_name', y=revenue_col,
                               title=f"Revenue by Plan: {question}")
                    st.plotly_chart(fig, use_container_width=True)
        
        # Usage pattern visualizations
        elif any(col in data.columns for col in ['avg_contacts', 'avg_workflows', 'contacts', 'workflows']):
            if 'plan_name' in data.columns and 'avg_contacts' in data.columns:
                # Usage by plan
                fig = px.scatter(data, x='avg_contacts', y='avg_workflows', 
                               size='customer_count' if 'customer_count' in data.columns else None,
                               color='plan_name',
                               title="Usage Patterns by Plan",
                               labels={'avg_contacts': 'Avg Contacts', 'avg_workflows': 'Avg Workflows'})
                st.plotly_chart(fig, use_container_width=True)
            
            elif 'contacts' in data.columns and 'workflows' in data.columns:
                # Individual customer usage
                fig = px.scatter(data.head(20), x='contacts', y='workflows',
                               color='plan_name' if 'plan_name' in data.columns else None,
                               hover_name='customer_name' if 'customer_name' in data.columns else None,
                               title="Customer Usage Patterns")
                st.plotly_chart(fig, use_container_width=True)
        
        # Engagement visualizations
        elif 'user_activation_rate' in data.columns:
            fig = px.histogram(data, x='user_activation_rate',
                             color='plan_name' if 'plan_name' in data.columns else None,
                             title="User Activation Rate Distribution",
                             labels={'user_activation_rate': 'Activation Rate (%)'})
            st.plotly_chart(fig, use_container_width=True)
        
        # Plan distribution
        elif 'plan_name' in data.columns and len(data) > 1:
            if 'customer_count' in data.columns:
                # Plan performance pie chart
                fig = px.pie(data, values='customer_count', names='plan_name',
                           title="Customer Distribution by Plan")
                st.plotly_chart(fig, use_container_width=True)
            else:
                # Simple plan distribution
                plan_counts = data['plan_name'].value_counts()
                fig = px.pie(values=plan_counts.values, names=plan_counts.index,
                           title="Plan Distribution")
                st.plotly_chart(fig, use_container_width=True)
        
        # Feature adoption rates
        elif any('_rate' in col or 'adoption' in col for col in data.columns):
            rate_columns = [col for col in data.columns if '_rate' in col or 'adoption' in col]
            if rate_columns and 'plan_name' in data.columns:
                fig = px.bar(data, x='plan_name', y=rate_columns[0],
                           title=f"Feature Adoption by Plan")
                st.plotly_chart(fig, use_container_width=True)
        
        else:
            st.info("Data visualization not available for this query type")
            
    except Exception as e:
        st.warning(f"Could not generate visualization: {str(e)}")

def main():
    # Header
    st.markdown('<h1 class="main-header">🔍 Help Scout Analytics</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Conversational Interface for Customer Data Insights</p>', unsafe_allow_html=True)
    
    # Load processed data (cached)
    data_loaded, data_result, error_msg = load_processed_data()
    
    if not data_loaded:
        st.error(f"Failed to load data: {error_msg}")
        st.info("The application will run in demo mode with sample data.")
        processed_data = None
        data_summary = None
        db_manager = None
        ai_interface = None
    else:
        processed_data, data_summary = data_result
        
        # Get database components (not cached)
        db_manager, ai_interface = get_database_components()
        
        if db_manager and processed_data:
            try:
                # Create database tables
                db_manager.create_tables(processed_data)
                st.session_state.data_loaded = True
                st.session_state.db_manager = db_manager
                st.session_state.ai_interface = ai_interface
            except Exception as e:
                st.error(f"Database setup error: {e}")
                db_manager = None
                ai_interface = None
    
    # Sidebar
    with st.sidebar:
        st.header("🎯 Quick Actions")
        
        # Status indicator
        if data_loaded:
            st.markdown("""
            <div class="success-card">
                <h4 style="margin: 0; color: #28a745;">✅ System Status</h4>
                <p style="margin: 0.5rem 0 0 0;">Data loaded successfully</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Display data overview
            display_data_overview(data_summary, processed_data)
            
        else:
            st.markdown("""
            <div class="warning-card">
                <h4 style="margin: 0; color: #ffc107;">⚠️ Demo Mode</h4>
                <p style="margin: 0.5rem 0 0 0;">Using sample data</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.divider()
        
        # Enhanced sample questions for real data
        st.subheader("Try These Questions:")
        sample_questions = [
            "Who are the top 10 customers by revenue?",
            "Show me usage patterns by plan type",
            "Which customers have low engagement rates?",
            "What customers are at risk of churning?",
            "How do Pro plan customers use features?",
            "Show me plan performance analysis", 
            "What's the feature adoption rate by plan?",
            "Find high-value customers with low activity",
            "Analyze customer lifecycle stages",
            "Show workflow usage correlation with revenue"
        ]
        
        for question in sample_questions:
            if st.button(question, key=f"q_{hash(question)}"):
                st.session_state.current_question = question
        
        st.divider()
        
        # Analysis types
        if data_loaded and ai_interface:
            st.subheader("📈 Available Analyses")
            analyses = ai_interface.get_available_analyses()
            for analysis_type, description in analyses.items():
                with st.expander(analysis_type):
                    st.write(description)
    
    # Main content area
    col1, col2 = st.columns([2.5, 1])
    
    with col1:
        st.header("💬 Ask Your Question")
        
        # Question input
        question = st.text_input(
            "What would you like to know about the customer data?",
            value=st.session_state.get('current_question', ''),
            placeholder="e.g., Which Pro customers have the highest revenue but lowest engagement?"
        )
        
        if st.button("🔍 Get Answer", type="primary") and question:
            with st.spinner("Analyzing your question..."):
                try:
                    if data_loaded and st.session_state.get('ai_interface'):
                        # Use real AI interface with data
                        ai_interface = st.session_state.ai_interface
                        response = ai_interface.process_query(question)
                        
                        # Display response
                        st.subheader("📋 Answer")
                        st.write(response.get('answer', 'No answer available'))
                        
                        # Display any generated SQL
                        if response.get('sql'):
                            with st.expander("🔧 Generated SQL Query"):
                                st.code(response['sql'], language='sql')
                        
                        # Display data if available
                        if response.get('data') is not None and not response['data'].empty:
                            st.subheader("📈 Data Results")
                            df = response['data']
                            
                            # Show data table
                            st.dataframe(df, use_container_width=True)
                            
                            # Create visualization
                            st.subheader("📊 Visualization")
                            create_visualization(df, 'auto', question)
                            
                            # Data insights
                            if len(df) > 0:
                                st.subheader("🔍 Key Insights")
                                col_a, col_b, col_c = st.columns(3)
                                
                                with col_a:
                                    st.metric("Records Found", len(df))
                                
                                # Revenue insights
                                revenue_cols = [col for col in df.columns if 'revenue' in col.lower()]
                                if revenue_cols:
                                    with col_b:
                                        total_revenue = df[revenue_cols[0]].sum()
                                        st.metric("Total Revenue", f"${total_revenue:,.2f}")
                                
                                # Plan insights
                                if 'plan_name' in df.columns:
                                    with col_c:
                                        top_plan = df['plan_name'].mode()[0] if len(df) > 0 else 'N/A'
                                        st.metric("Most Common Plan", top_plan)
                        
                        # Error handling
                        if response.get('error'):
                            st.error(f"Query Error: {response['error']}")
                            
                    else:
                        # Fallback demo mode
                        st.info("🔄 Running in demo mode...")
                        st.subheader("📋 Demo Response")
                        st.write(f"In the full version, I would analyze: '{question}' against your Help Scout customer data including revenue, usage patterns, engagement metrics, and plan performance.")
                        
                        # Show sample data structure
                        st.subheader("📊 Sample Data Structure")
                        sample_customers = pd.DataFrame({
                            'customer_name': ['Acme Corp', 'Tech Startup', 'Enterprise Co'],
                            'plan_name': ['Pro', 'Standard', 'Enterprise'],
                            'revenue': [599.99, 299.99, 1299.99],
                            'engagement_score': [85.2, 67.8, 92.1]
                        })
                        st.dataframe(sample_customers)
                        
                except Exception as e:
                    st.error(f"Error processing question: {str(e)}")
                    st.info("Please try rephrasing your question or try one of the sample questions.")
    
    with col2:
        st.header("📊 Quick Metrics")
        
        if data_loaded and 'data_summary' in locals():
            # Real metrics from loaded data
            overview = data_summary.get('overview', {})
            
            # Total customers
            total_customers = data_summary.get('total_customers', 0)
            st.markdown(f"""
            <div class="metric-card">
                <h4 style="margin: 0; color: #1f77b4;">Total Customers</h4>
                <h2 style="margin: 0.5rem 0 0 0;">{total_customers:,}</h2>
            </div>
            """, unsafe_allow_html=True)
            
            # Total MRR
            if 'total_mrr' in overview:
                st.markdown(f"""
                <div class="metric-card">
                    <h4 style="margin: 0; color: #1f77b4;">Total MRR</h4>
                    <h2 style="margin: 0.5rem 0 0 0;">${overview['total_mrr']:,.2f}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            # Average MRR
            if 'avg_mrr' in overview:
                st.markdown(f"""
                <div class="metric-card">
                    <h4 style="margin: 0; color: #1f77b4;">Average MRR</h4>
                    <h2 style="margin: 0.5rem 0 0 0;">${overview['avg_mrr']:,.2f}</h2>
                </div>
                """, unsafe_allow_html=True)
            
            # Plan distribution chart
            if 'plan_distribution' in overview:
                st.subheader("Plan Distribution")
                plan_data = pd.DataFrame(
                    list(overview['plan_distribution'].items()),
                    columns=['Plan', 'Customers']
                )
                fig = px.pie(plan_data, values='Customers', names='Plan', 
                           title="Customer Distribution")
                st.plotly_chart(fig, use_container_width=True)
        
        else:
            # Demo metrics
            st.markdown("""
            <div class="metric-card">
                <h4 style="margin: 0; color: #1f77b4;">Demo Mode</h4>
                <h2 style="margin: 0.5rem 0 0 0;">Sample Data</h2>
            </div>
            """, unsafe_allow_html=True)
            
            demo_metrics = {
                "Total Customers": "100",
                "Average MRR": "$489",
                "Top Plan": "Standard", 
                "Active Plans": "4"
            }
            
            for metric, value in demo_metrics.items():
                st.markdown(f"""
                <div class="metric-card">
                    <h4 style="margin: 0; color: #1f77b4;">{metric}</h4>
                    <h2 style="margin: 0.5rem 0 0 0;">{value}</h2>
                </div>
                """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    if data_loaded:
        st.markdown("✅ **Help Scout Analytics** - Powered by Real Customer Data | OpenAI GPT-4 & Streamlit")
    else:
        st.markdown("🔄 **Help Scout Analytics** - Demo Mode | Add CSV files to data/ directory for full functionality")
    
    # Debug info (only show in development)
    if st.session_state.get('show_debug', False):
        with st.expander("🔧 Debug Information"):
            st.write("Session State:", st.session_state)
            if data_loaded:
                st.write("Data Summary:", data_summary)

if __name__ == "__main__":
    main()