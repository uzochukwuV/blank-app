import streamlit as st
from helpers.connect_to_snowflake import connect_to_snowflake


st.title("MEDICAL REPORT Summary (powered by Cortex LLMs)")

# Establishing session
session = connect_to_snowflake()
current_database = session.get_current_database()
print(current_database)

st.file_uploader(
    label="csv",
    type="csv",
    
)

st.text_input( 
    label="chat"
)