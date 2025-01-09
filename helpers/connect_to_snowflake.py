from snowflake.snowpark import Session
import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

@st.cache_resource
def connect_to_snowflake():
    st.write(os.environ["ADMIN"])
    try:
        username = os.environ["USERNAME"]
        password = os.environ["PASSWORD"]
        account = os.environ["ACCOUNT"]
        database_name = os.environ["MEDIC"]
        role = str(os.getenv("ADMIN") + "ADMIN")
        warehouse = "compute_wh"
        schema = "TESTSCHEMA"
        
    except KeyError:
        raise Exception("Could not find one or more required environment variables")
    
    return  Session.builder.configs(
            {
                "account": account,
                "user": username,
                "password": password,
                "role": role,
                "warehouse": warehouse,
                "database": database_name,
                "schema": schema,
                "stage":"TESTDATA",
                "session_parameters":{'QUERY_TAG': 'py-copy-into'}
            }
        ).create()