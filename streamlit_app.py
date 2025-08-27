import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv
import numpy as np

def get_openai_api_key():
    """Get OpenAI API key from various sources."""
    # Try Streamlit secrets first (for cloud deployment)
    try:
        return st.secrets["OPENAI_API_KEY"]
    except (KeyError, FileNotFoundError):
        pass
    
    # Try environment variable
    api_key = os.getenv('OPENAI_API_KEY')
    if api_key:
        return api_key
    
    # Try .env file (for local development)
    try:
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv('OPENAI_API_KEY')
    except ImportError:
        pass
    
    return None

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

# Custom CSS with dark/light mode support
st.markdown("""
<style>
    /* Light mode styles (default) */
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--primary-color, #1f77b4);
        text-align: center;
        margin: 0;
        line-height: 1.2;
    }
    
    .logo-container {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 1rem;
        margin-bottom: 2rem;
        padding: 1rem;
        background: linear-gradient(135deg, 
            var(--header-bg-start, #f8f9fa) 0%, 
            var(--header-bg-end, #ffffff) 100%);
        border-radius: 16px;
        box-shadow: 0 4px 12px var(--shadow-color, rgba(0,0,0,0.1));
        border: 1px solid var(--border-color, #e9ecef);
    }
    
    .logo-container img {
        height: 60px;
        width: auto;
        filter: drop-shadow(0 2px 4px var(--shadow-color, rgba(0,0,0,0.1)));
        transition: transform 0.2s ease;
    }
    
    .logo-container:hover img {
        transform: scale(1.05);
    }
    
    .logo-container .main-header {
        background: linear-gradient(135deg, var(--primary-color, #1f77b4), #1565c0);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    .sub-header {
        font-size: 1.2rem;
        color: var(--text-color, #666);
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, 
            var(--card-bg-start, #f8f9fa) 0%, 
            var(--card-bg-end, #e9ecef) 100%);
        padding: 1.2rem;
        border-radius: 12px;
        border: 1px solid var(--border-color, #dee2e6);
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px var(--shadow-color, rgba(0,0,0,0.1));
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px var(--shadow-hover, rgba(0,0,0,0.15));
    }
    
    .success-card {
        background: linear-gradient(135deg, 
            var(--success-bg-start, #d4edda) 0%, 
            var(--success-bg-end, #c3e6cb) 100%);
        padding: 1.2rem;
        border-radius: 12px;
        border: 1px solid var(--success-border, #c3e6cb);
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px var(--shadow-color, rgba(0,0,0,0.1));
    }
    
    .warning-card {
        background: linear-gradient(135deg, 
            var(--warning-bg-start, #fff3cd) 0%, 
            var(--warning-bg-end, #ffeeba) 100%);
        padding: 1.2rem;
        border-radius: 12px;
        border: 1px solid var(--warning-border, #ffeeba);
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px var(--shadow-color, rgba(0,0,0,0.1));
    }
    
    /* Dark mode detection and styles */
    @media (prefers-color-scheme: dark) {
        :root {
            --primary-color: #4dabf7;
            --text-color: #c1c2c5;
            --card-bg-start: #2c2c2c;
            --card-bg-end: #3c3c3c;
            --header-bg-start: #2c2c2c;
            --header-bg-end: #1a1a1a;
            --border-color: #495057;
            --shadow-color: rgba(0,0,0,0.3);
            --shadow-hover: rgba(0,0,0,0.4);
            --success-bg-start: #2b5a3e;
            --success-bg-end: #1e4532;
            --success-border: #28a745;
            --warning-bg-start: #664d03;
            --warning-bg-end: #554d1f;
            --warning-border: #ffc107;
        }
    }
    
    /* Light mode variables */
    @media (prefers-color-scheme: light) {
        :root {
            --primary-color: #1f77b4;
            --text-color: #666;
            --card-bg-start: #f8f9fa;
            --card-bg-end: #e9ecef;
            --header-bg-start: #f8f9fa;
            --header-bg-end: #ffffff;
            --border-color: #dee2e6;
            --shadow-color: rgba(0,0,0,0.1);
            --shadow-hover: rgba(0,0,0,0.15);
            --success-bg-start: #d4edda;
            --success-bg-end: #c3e6cb;
            --success-border: #c3e6cb;
            --warning-bg-start: #fff3cd;
            --warning-bg-end: #ffeeba;
            --warning-border: #ffeeba;
        }
    }
    
    /* Force light mode for specific Streamlit elements */
    .stApp {
        color-scheme: light dark;
    }
    
    /* Enhanced metric card headers */
    .metric-card h4 {
        margin: 0;
        color: var(--primary-color, #1f77b4);
        font-weight: 600;
        font-size: 0.95rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .metric-card h2, .metric-card h3 {
        margin: 0.5rem 0 0 0;
        color: var(--text-color, #333);
        font-weight: 700;
    }
    
    .success-card h4 {
        margin: 0;
        color: var(--success-text, #155724);
        font-weight: 600;
    }
    
    .warning-card h4 {
        margin: 0;
        color: var(--warning-text, #856404);
        font-weight: 600;
    }
    
    /* Dark mode text colors */
    @media (prefers-color-scheme: dark) {
        .success-card h4 {
            color: #4caf50;
        }
        .warning-card h4 {
            color: #ff9800;
        }
        .metric-card h2, .metric-card h3 {
            color: #e1e1e1;
        }
    }
    
    /* Button styling improvements */
    .stButton > button {
        background: linear-gradient(135deg, var(--primary-color, #1f77b4) 0%, #1565c0 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        transition: all 0.2s ease;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* Sidebar improvements */
    .css-1d391kg {
        background: var(--card-bg-start, #f8f9fa);
    }
    
    /* Chart container improvements */
    .js-plotly-plot {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 2px 8px var(--shadow-color, rgba(0,0,0,0.1));
    }
    
    /* Custom scrollbar for dark mode */
    @media (prefers-color-scheme: dark) {
        ::-webkit-scrollbar {
            width: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #2c2c2c;
        }
        ::-webkit-scrollbar-thumb {
            background: #666;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #888;
        }
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

def get_chart_colors():
    """Get chart color scheme based on system theme preference."""
    # You can detect the theme via JavaScript, but for simplicity we'll use a good universal palette
    return {
        'primary': '#4dabf7',
        'secondary': '#69db7c', 
        'tertiary': '#ffd43b',
        'quaternary': '#ff8cc8',
        'background': 'rgba(0,0,0,0)',
        'text': '#495057',
        'grid': '#e9ecef'
    }

def create_visualization(data: pd.DataFrame, query_type: str, question: str):
    """Create appropriate visualization based on data and query type."""
    if data.empty:
        st.info("No data to visualize")
        return
    
    # Get adaptive color scheme
    colors = get_chart_colors()
    
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
                    # Top customers bar chart with custom colors
                    fig = px.bar(data.head(10), 
                               x='customer_name', y=revenue_col,
                               color='plan_name' if 'plan_name' in data.columns else None,
                               title=f"Revenue Analysis: {question}",
                               color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                      colors['tertiary'], colors['quaternary']])
                    fig.update_xaxis(tickangle=45)
                    fig.update_layout(
                        plot_bgcolor=colors['background'],
                        paper_bgcolor=colors['background'],
                        font_color=colors['text'],
                        showlegend=True,
                        legend=dict(
                            bgcolor="rgba(0,0,0,0)",
                            bordercolor="rgba(0,0,0,0)"
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                elif 'plan_name' in data.columns:
                    # Plan performance chart
                    fig = px.bar(data, x='plan_name', y=revenue_col,
                               title=f"Revenue by Plan: {question}",
                               color='plan_name',
                               color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                      colors['tertiary'], colors['quaternary']])
                    fig.update_layout(
                        plot_bgcolor=colors['background'],
                        paper_bgcolor=colors['background'],
                        font_color=colors['text'],
                        showlegend=False
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        # Usage pattern visualizations
        elif any(col in data.columns for col in ['avg_contacts', 'avg_workflows', 'contacts', 'workflows']):
            if 'plan_name' in data.columns and 'avg_contacts' in data.columns:
                # Usage by plan scatter plot
                fig = px.scatter(data, x='avg_contacts', y='avg_workflows', 
                               size='customer_count' if 'customer_count' in data.columns else None,
                               color='plan_name',
                               title="Usage Patterns by Plan",
                               labels={'avg_contacts': 'Avg Contacts', 'avg_workflows': 'Avg Workflows'},
                               color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                      colors['tertiary'], colors['quaternary']])
                fig.update_layout(
                    plot_bgcolor=colors['background'],
                    paper_bgcolor=colors['background'],
                    font_color=colors['text']
                )
                st.plotly_chart(fig, use_container_width=True)
            
            elif 'contacts' in data.columns and 'workflows' in data.columns:
                # Individual customer usage
                fig = px.scatter(data.head(20), x='contacts', y='workflows',
                               color='plan_name' if 'plan_name' in data.columns else None,
                               hover_name='customer_name' if 'customer_name' in data.columns else None,
                               title="Customer Usage Patterns",
                               color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                      colors['tertiary'], colors['quaternary']])
                fig.update_layout(
                    plot_bgcolor=colors['background'],
                    paper_bgcolor=colors['background'],
                    font_color=colors['text']
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Engagement visualizations
        elif 'user_activation_rate' in data.columns:
            fig = px.histogram(data, x='user_activation_rate',
                             color='plan_name' if 'plan_name' in data.columns else None,
                             title="User Activation Rate Distribution",
                             labels={'user_activation_rate': 'Activation Rate (%)'},
                             color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                    colors['tertiary'], colors['quaternary']])
            fig.update_layout(
                plot_bgcolor=colors['background'],
                paper_bgcolor=colors['background'],
                font_color=colors['text']
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Plan distribution
        elif 'plan_name' in data.columns and len(data) > 1:
            if 'customer_count' in data.columns:
                # Plan performance column chart with custom colors
                fig = px.bar(data, x='plan_name', y='customer_count',
                           title="Customer Distribution by Plan",
                           labels={'plan_name': 'Plan Type', 'customer_count': 'Number of Customers'},
                           color='plan_name',
                           color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                  colors['tertiary'], colors['quaternary']])
                fig.update_layout(
                    plot_bgcolor=colors['background'],
                    paper_bgcolor=colors['background'],
                    font_color=colors['text'],
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                # Simple plan distribution column chart
                plan_counts = data['plan_name'].value_counts()
                fig = px.bar(x=plan_counts.index, y=plan_counts.values,
                           title="Plan Distribution",
                           labels={'x': 'Plan Type', 'y': 'Number of Customers'},
                           color=plan_counts.index,
                           color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                  colors['tertiary'], colors['quaternary']])
                fig.update_layout(
                    plot_bgcolor=colors['background'],
                    paper_bgcolor=colors['background'],
                    font_color=colors['text'],
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Feature adoption rates
        elif any('_rate' in col or 'adoption' in col for col in data.columns):
            rate_columns = [col for col in data.columns if '_rate' in col or 'adoption' in col]
            if rate_columns and 'plan_name' in data.columns:
                fig = px.bar(data, x='plan_name', y=rate_columns[0],
                           title=f"Feature Adoption by Plan",
                           color='plan_name',
                           color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                  colors['tertiary'], colors['quaternary']])
                fig.update_layout(
                    plot_bgcolor=colors['background'],
                    paper_bgcolor=colors['background'],
                    font_color=colors['text'],
                    showlegend=False
                )
                st.plotly_chart(fig, use_container_width=True)
        
        else:
            st.info("Data visualization not available for this query type")
            
    except Exception as e:
        st.warning(f"Could not generate visualization: {str(e)}")

def get_logo_html():
    """Get logo HTML with fallback."""
    try:
        import base64
        with open("assets/helpscout-icon.png", "rb") as img_file:
            img_data = base64.b64encode(img_file.read()).decode()
            return f'''
            <div class="logo-container">
                <img src="data:image/png;base64,{img_data}" 
                     alt="Help Scout Logo">
                <h1 class="main-header">Help Scout Analytics</h1>
            </div>
            '''
    except FileNotFoundError:
        # Fallback to emoji if PNG not found
        return '''
        <div class="logo-container">
            <span style="font-size: 3rem;">🔍</span>
            <h1 class="main-header">Help Scout Analytics</h1>
        </div>
        '''

def main():
    # Header with logo
    st.markdown(get_logo_html(), unsafe_allow_html=True)
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
        
        # Clean sample questions that will return responses
        st.subheader("Try These Questions:")
        sample_questions = [
            "Who are the top 10 customers by revenue?",
            "What's the average monthly revenue by plan type?",
            "Show me customers at risk of churning",
            "Which customers have low user engagement rates?",
            "Show me usage patterns across different plans", 
            "What's the feature adoption rate by plan?",
            "Show me tags and saved replies usage by plan"
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
            placeholder="e.g., Which Pro customers have high revenue but low monthly active users?"
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
                
                colors = get_chart_colors()
                fig = px.bar(plan_data, x='Plan', y='Customers', 
                           title="Customer Distribution by Plan",
                           labels={'Plan': 'Plan Type', 'Customers': 'Number of Customers'},
                           color='Plan',
                           color_discrete_sequence=[colors['primary'], colors['secondary'], 
                                                  colors['tertiary'], colors['quaternary']])
                fig.update_layout(
                    plot_bgcolor=colors['background'],
                    paper_bgcolor=colors['background'],
                    font_color=colors['text'],
                    showlegend=False,
                    height=300
                )
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