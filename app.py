from conva_ai.client import ConvaAIResponse
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from conva_ai import ConvaAI
import json

client = ConvaAI(
    assistant_id="76c46f4c6cc6492e8206b1da43610d14", 
    assistant_version="12.0.0",
    api_key="729f1fca592e424bac3561b2d5dc2009"
)

# Initialize session state if not already done
if 'generated_query' not in st.session_state:
    st.session_state.generated_query = None
if 'optimized_query' not in st.session_state:
    st.session_state.optimized_query = None
if 'query_results' not in st.session_state:
    st.session_state.query_results = None
if 'insights' not in st.session_state:
    st.session_state.insights = None

def fetch_bigquery_data(query: str, project_id: str):
    """
    Execute an SQL query on BigQuery and return the results as a list of dictionaries.

    Parameters:
    query (str): The SQL query to execute.
    project_id (str): The Google Cloud project ID where the BigQuery dataset is located.

    Returns:
    List[dict]: A list of dictionaries representing the rows of the query result.
    """
    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    # Execute the query
    query_job = client.query(query)

    # Wait for the query to finish
    results = query_job.result()

    # Convert the results to a dataframe
    df = results.to_dataframe()

    return df

def beautified_df_to_string(df):
    # Create a header with column names
    header = '\t'.join(df.columns) + '\n'
    
    # Format each row with tab-separated values and newline at the end
    rows = '\n'.join(['\t'.join(map(str, row)) for row in df.values])
    
    # Combine the header and the rows
    beautified_string = header + rows
    
    return beautified_string

def generate_query(requirement: str) -> dict:
    response = client.invoke_capability_name(requirement, stream=False, capability_name="sql_query_generation")
    return response.parameters

def optimize_query(args: dict): 
    response = client.invoke_capability_name(f"""Optimize this query: \n ```{json.dumps(args)}```""", stream=False, capability_name="query_optimization")
    return response.parameters

def analyze_results(data: pd.DataFrame):
    response = client.invoke_capability_name(f"""Goal: \n {st.session_state.generated_query['user_requirement']} \n\n Provide insights for this data - \n{beautified_df_to_string(data)}""", stream=False, capability_name="insight_generation")
    return response.message


def main():
    st.title("Data Lake Analysis Tool")
    
    # Sidebar for app navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Analysis Dashboard"])
    
    # Main content area
    st.header("Step-by-Step Data Analysis")
    
    # Step 1: User Input and Query Generation
    st.subheader("1. Generate Query")
    user_input = st.text_area(
        "Describe what analysis you want to perform:",
        height=100,
        placeholder="Example: Analyze sales trends for the last 6 months and identify top performing products"
    )
    
    # Step 1: Generate Query
    if st.button("Generate Query", key="generate_query"):
        if user_input:
            st.session_state.generated_query = generate_query(user_input)
        else:
            st.warning("Please enter your analysis requirements first.")
    
    # Display the generated query if it exists in session state
    if st.session_state.generated_query:
        st.text_area(
            "Querying steps:", 
            value=st.session_state.generated_query['steps'], 
            height=150, 
            disabled=True, 
            key="querying_steps_text_area"
        )
        st.text_area(
            "Generated SQL Query:", 
            value=st.session_state.generated_query['sql_query'], 
            height=150, 
            disabled=True, 
            key="generated_query_text_area"
        )
    
    # Step 2: Query Optimization
    if st.session_state.generated_query:
        if st.button("Optimize Query", key="optimize_query"):
            response = optimize_query(st.session_state.generated_query)
            st.session_state.optimized_query = response
        
        # Display optimization details and optimized query if available
        if st.session_state.optimized_query:
            st.text_area(
                "Optimization Guidelines:", 
                value="\n".join(st.session_state.optimized_query['fix_guidelines']), 
                height=100, 
                disabled=True, 
                key="optimization_details_area"
            )
            st.session_state.optimized_query['updated_query'] = st.text_area(
                "Optimized Query:", 
                value=st.session_state.optimized_query['updated_query'], 
                height=150, 
                key="optimization_query_area"
            )
    
    # Step 3: Query Execution
    if st.session_state.generated_query:
        st.subheader("2. Execute Query")
        if st.button("Run Query"):
            query_to_run = st.session_state.optimized_query['updated_query'] if st.session_state.optimized_query else st.session_state.generated_query
            with st.spinner("Executing query..."):
                results_df = fetch_bigquery_data(query_to_run, "slangserver")
                st.session_state.query_results = results_df
    
    # Always display the query results table if available
    if st.session_state.query_results is not None:
        st.dataframe(st.session_state.query_results, use_container_width=True)
        
        # Add download button for results
        st.download_button(
            label="Download Results as CSV",
            data=st.session_state.query_results.to_csv(index=False),
            file_name="query_results.csv",
            mime="text/csv"
        )
    
    # Step 4: Data Analysis and Insights Generation
    if st.session_state.query_results is not None:
        st.subheader("3. Analyze Results and Generate Insights")
        
        if st.button("Generate Insights"):
            with st.spinner("Analyzing results..."):
                analysis = analyze_results(st.session_state.query_results)
                st.session_state.insights = analysis
                st.text_area("Analysis Insights", value=analysis, height=200, disabled=True)
        elif st.session_state.insights:
            st.text_area("Analysis Insights", value=st.session_state.insights, height=200, disabled=True)

    # Add footer
    st.markdown("---")
    st.markdown("*Data Lake Analysis Tool v1.0*")

if __name__ == "__main__":
    # Set page config
    st.set_page_config(
        page_title="Data Lake Analysis Tool",
        page_icon="📊",
        layout="wide"
    )
    main()
