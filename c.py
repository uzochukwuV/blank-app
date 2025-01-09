import streamlit as st
from helpers.connect_to_snowflake import connect_to_snowflake
from helpers.process_csv import  *
from helpers.process_csv import getHeaders
from snowflake.connector.pandas_tools import write_pandas
from helpers.svae_to_snow import save_to_snowflake

st.title("MEDICAL REPORT Summary (powered by Cortex LLMs)")

# Establishing session
session = connect_to_snowflake()


current_database = session.get_current_database()
user =session.get_current_user()

connection =session.connection.cursor()


connection.execute("CREATE DATABASE IF NOT EXISTS TESTDB")
connection.execute("CREATE SCHEMA IF NOT EXISTS TESTSCHEMA")
connection.execute("USE TESTDB.TESTSCHEMA")
connection.execute("CREATE STAGE IF NOT EXISTS TESTDATA")
# connection.execute("USE STAGE TESTDB.TESTSCHEMA.TESTDATA")
connection.execute("USE ROLE ACCOUNTADMIN")
connection.execute("CREATE ROLE IF NOT EXISTS cortex_user_role")
connection.execute("GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_user_role")
connection.execute("GRANT ROLE cortex_user_role TO USER Uzochukuv")
connection.execute("ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION ='ANY_REGION'")




current_stage = session.get_session_stage()
st.write(current_stage, user)

file = st.file_uploader(
    "Choose a CSV file", accept_multiple_files=True
)

files = getHeaders(file)

st.write(files)
answer =""

if len(files) > 0:
    header, datax = files[0]
    data=" VARCHAR(200)".join(header)
    sql= "CREATE TABLE IF NOT EXISTS newtable ({0} VARCHAR(200))".format(data)
    st.write(sql)
    answer =connection.execute(sql)
    query = """
        INSERT INTO newtable
            VALUES
            ('Lysandra','Reeves','1-212-759-3751','New York',10018),
            ('Michael','Arnett','1-650-230-8467','San Francisco',94116);

    """

    
    list_of_tuples = datax.to_records(index=False).tolist() 

    data = [[row.split(',') for row in inner_list] for inner_list in list_of_tuples]
    column_names = ["i"].extend(header)
    

    df = pd.DataFrame([item for sublist in data for item in sublist], columns=column_names)
    st.dataframe(df) 

    save_to_snowflake(session.connection, list_of_tuples, "", column_names)


    # connection.execute("CREATE TABLE mytable IF NOT EXISTS ()")
st.text_input( 
    label="chat"
)