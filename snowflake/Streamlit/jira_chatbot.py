import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import time




# List of LLM models available in Snowflake Cortex
MODELS = [
    "llama3.2-3b",
    "claude-3-5-sonnet",
    "mistral-large2",
    "llama3.1-8b",
    "llama3.1-405b",
    "llama3.1-70b",
    "mistral-7b",
    "jamba-1.5-large",
    "mixtral-8x7b",
    "reka-flash",
    "gemma-7b"
]

# Set number of RAG context chunks
CHUNK_NUMBER = [2,4,6,8,10,12]

def build_layout():
    """Builds the layout for the app."""
    
    # Streamlit session state variables
    if 'reset_key' not in st.session_state: 
        st.session_state.reset_key = 0
    if 'conversation_state' not in st.session_state:
        st.session_state.conversation_state = []

    # Build UI
    st.set_page_config(layout="wide")
    st.title("🔥 JIRA AI Assistant")
    st.write("""
        I'm an AI-powered JIRA Assistant that helps you query your JIRA issues, backlog, and project updates
        stored in Snowflake. Ask me about **tickets, status updates, or issue history!**
    """)

    # Sidebar controls
    st.sidebar.selectbox("Select a Snowflake Cortex model:", MODELS, key="model_name", index=2)
    st.sidebar.checkbox('Use JIRA dataset for context?', key="dataset_context", 
                        help="Enable AI retrieval of JIRA tickets stored in Snowflake.")
    
    #st.sidebar.image("https://media-hosting.imagekit.io//8674905faf65485a/Jira-Logo-2017.png", use_column_width=True)
    # Custom CSS Styles
    st.markdown(
        """
        <style>
        /* Main Background */
        .main { background-color: #F4F5F7; } 
        
        /* Buttons */
        .stButton>button { 
            background-color: #0747A6 !important;  /* Default JIRA blue */
            color: white !important; 
            border-radius: 10px;
            padding: 8px;
            transition: background-color 0.3s ease;  /* Smooth transition */
        }
    
        /* Hover Effect */
        .stButton>button:hover { 
            background-color: #1565C0 !important; /* Lighter blue on hover */
        }
    
        /* Active (Pressed) Effect */
        .stButton>button:active { 
            background-color: #0747A6 !important; /* Return to original blue when pressed */
        }
        
        /* User Message Styling */
        .user-message {
            background-color: #0052CC;
            color: white;
            padding: 10px;
            border-radius: 10px;
            margin-bottom: 5px;
        }
    
        /* AI Response Styling */
        .ai-message {
            background-color: #DEEBFF;
            color: black;
            padding: 10px;
            border-radius: 10px;
            margin-bottom: 5px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )



    # Reset conversation button
    if st.button('Reset conversation', key='reset_conversation_button'):
        st.session_state.conversation_state = []
        st.session_state.reset_key += 1
        st.rerun()

    # User input field
    question = st.text_input("", placeholder="Ask about JIRA issues...", 
                             key=f"text_input_{st.session_state.reset_key}", label_visibility="collapsed")

    # Sidebar advanced options
    with st.sidebar.expander("Advanced Options"):
        st.selectbox("Select number of context chunks:", CHUNK_NUMBER, key="num_retrieved_chunks", index=0)

    return question

def build_prompt(question):
    """Builds the prompt for Snowflake Cortex."""
    
    chunks_used = []
    if st.session_state.dataset_context:
        # Query Snowflake for relevant JIRA issues
        context_cmd = f"""
 with context_cte as
          (select issue_id, issue_information as issue_chunk, vector_cosine_similarity(jira_embedding,
                snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', ?)) as v_sim
          from LAB_CES.AI_APP_TEST.JIRA_DATA_SINGLE_STRING_VECTORS
          having v_sim > 0
          order by v_sim desc
          limit ?)
          select issue_id, issue_chunk from context_cte
        """
        chunk_limit = st.session_state.num_retrieved_chunks
        context_df = session.sql(context_cmd, params=[question, chunk_limit]).to_pandas()
        context_len = len(context_df) -1
        # Extract JIRA issue details
        chunks_used = context_df['ISSUE_ID'].tolist()
        rag_context = ""
        for i in range (0, context_len):
            rag_context += context_df.loc[i, 'ISSUE_CHUNK']
        rag_context = rag_context.replace("'", "''")
        # Construct the prompt.
        new_prompt = f"""
          Your name is Avalon. Act as a JIRA Issue expert for data engineers and developers working at CES (Circular Economy Systems), JIRA context is only for the company CES (Circular Economy Systems), each issue id belongs to a unique JIRA that is recorded. 
          Do not go outside the context provided.  
          Context: {rag_context}
          Question: {question} 
          Answer: 
          """
    else:
        # Construct the generic version of the prompt without RAG to only go against what the LLM was trained.
        new_prompt = f"""
          Your name is Avalon. Act as a JIRA Issue expert for data engineers and developers working at CES (Circular Economy Systems), provide only the most accurate information against CES (Circular Economy Systems) jira issues. 
          Do not go outside the context provided.  
          Question: {question} 
          Answer: 
          """
    return new_prompt, chunks_used 


def run_prompt(question):
    """Runs the prompt against Snowflake Cortex."""
    
    formatted_prompt, chunks_used = build_prompt(question)
    start_time = time.time()

    # Execute the Cortex query
    cortex_cmd = f"SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS response"
    sql_resp = session.sql(cortex_cmd, params=[st.session_state.model_name, formatted_prompt])
    answer_df = sql_resp.collect()

    end_time = time.time()
    response = answer_df[0][0]
    return response, chunks_used, end_time - start_time

def main():
    """Controls the flow of the app."""
    
    question = build_layout()
    if question:
        with st.spinner("Retrieving data..."):
            try:
                response, chunks_used, total_time = run_prompt(question)

                # Store conversation history
                st.session_state.conversation_state.append(("You:", question))
                st.session_state.conversation_state.append(("JIRA Assistant:", response))

                # Display results
                for label, message in reversed(st.session_state.conversation_state):
                    st.write(f"**{label}** {message}")

                # Display chunks used
                if chunks_used:
                    st.info(f"Retrieved JIRA Issues: {', '.join(map(str, chunks_used))}")
            except Exception as e:
                st.warning(f"An error occurred: {e}")

if __name__ == "__main__":
    session = get_active_session()
    main()
