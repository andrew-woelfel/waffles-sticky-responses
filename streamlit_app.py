import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import our custom modules
from src.data_processing import DataProcessor
from src.data_model import DatabaseManager
from src.ai_interface import ConversationalInterface
from src.config import Config

# Page configuration
st.set_page_config(
    page_title="Help Scout Analytics - Conversational Interface",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Header
    st.markdown('<h1 class="main-header">🔍 Help Scout Analytics</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Conversational Interface for Customer Data Insights</p>', unsafe_allow_html=True)
    
    # Initialize components
    @st.cache_resource
    def initialize_components():
        config = Config()
        data_processor = DataProcessor()
        db_manager = DatabaseManager(config.database_url)
        ai_interface = ConversationalInterface(config.openai_api_key)
        return config, data_processor, db_manager, ai_interface
    
    try:
        config, data_processor, db_manager, ai_interface = initialize_components()
    except Exception as e:
        st.error(f"Failed to initialize components: {str(e)}")
        st.info("Please check your configuration and API keys.")
        return
    
    # Sidebar
    with st.sidebar:
        st.header("🎯 Quick Actions")
        
        # Sample questions
        st.subheader("Try These Questions:")
        sample_questions = [
            "Who are the top 10 customers by MRR?",
            "Do contacts and workflows have a usage connection?",
            "What are the top add-ons for Standard plans?",
            "Tell me something interesting about Pro customers",
            "Show me customer churn patterns",
            "Which customers have the highest usage rates?"
        ]
        
        for question in sample_questions:
            if st.button(question, key=f"q_{hash(question)}"):
                st.session_state.current_question = question
        
        st.divider()
        
        # Data overview
        st.subheader("📊 Data Overview")
        if st.button("Show Data Summary"):
            st.session_state.show_summary = True
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("💬 Ask Your Question")
        
        # Question input
        question = st.text_input(
            "What would you like to know about the customer data?",
            value=st.session_state.get('current_question', ''),
            placeholder="e.g., Who are our highest value customers?"
        )
        
        if st.button("🔍 Get Answer", type="primary") and question:
            with st.spinner("Analyzing your question..."):
                try:
                    # Process the question through AI interface
                    response = ai_interface.process_query(question)
                    
                    # Display response
                    st.subheader("📋 Answer")
                    st.write(response.get('answer', 'No answer available'))
                    
                    # Display any generated SQL
                    if response.get('sql'):
                        with st.expander("🔧 Generated SQL Query"):
                            st.code(response['sql'], language='sql')
                    
                    # Display data if available
                    if response.get('data') is not None:
                        st.subheader("📈 Data")
                        df = response['data']
                        st.dataframe(df)
                        
                        # Auto-generate visualizations
                        if len(df.columns) >= 2 and len(df) > 1:
                            st.subheader("📊 Visualization")
                            try:
                                # Simple heuristic for chart type
                                if df.select_dtypes(include=['number']).shape[1] >= 1:
                                    numeric_col = df.select_dtypes(include=['number']).columns[0]
                                    categorical_col = df.select_dtypes(include=['object']).columns[0] if len(df.select_dtypes(include=['object']).columns) > 0 else df.columns[0]
                                    
                                    fig = px.bar(df.head(10), x=categorical_col, y=numeric_col, 
                                               title=f"{question}")
                                    st.plotly_chart(fig, use_container_width=True)
                            except Exception as viz_error:
                                st.info(f"Could not generate visualization: {str(viz_error)}")
                    
                except Exception as e:
                    st.error(f"Error processing question: {str(e)}")
                    st.info("Please try rephrasing your question or check the sample questions in the sidebar.")
    
    with col2:
        st.header("📊 Quick Metrics")
        
        # Sample metrics (these would come from your database in a real implementation)
        metrics_data = {
            "Total Customers": "1,247",
            "Average MRR": "$489",
            "Top Plan Type": "Standard",
            "Active Add-ons": "856"
        }
        
        for metric, value in metrics_data.items():
            st.markdown(f"""
            <div class="metric-card">
                <h4 style="margin: 0; color: #1f77b4;">{metric}</h4>
                <h2 style="margin: 0.5rem 0 0 0;">{value}</h2>
            </div>
            """, unsafe_allow_html=True)
            st.write("")  # Add spacing
    
    # Data summary section
    if st.session_state.get('show_summary', False):
        st.header("📈 Data Summary")
        
        # This would load actual data in a real implementation
        st.info("Data summary would be displayed here based on the loaded dataset.")
        
        # Reset the flag
        st.session_state.show_summary = False
    
    # Footer
    st.markdown("---")
    st.markdown("Built for Help Scout Analytics Take-Home Project | Powered by OpenAI GPT-4 & Streamlit")

if __name__ == "__main__":
    main()