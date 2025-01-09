# import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import pandas as pd
import os
from io import StringIO

def read_csv(path):
    
    df = pd.read_csv(path)
    ef = pd.read_csv(path,delimiter=';',header=0)
    list_of_column_names = list(df.columns)
    return list_of_column_names, ef

def no_whitespace(data):
    s =data.split(" ")
    j = "".join(s)
    return j

def getHeaders(file):
    files=[]
    for uploaded_file in file:
        name = uploaded_file.name
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path =os.path.join(base_dir, name)
        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
        with open(file_path, 'w') as file:
            file.write(stringio.read())
            file.close()
        header, data = read_csv(file_path)
    
        nheader = list(map(no_whitespace, header))
    
        files.append([nheader, data])
    return files
