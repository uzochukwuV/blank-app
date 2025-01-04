#!/usr/bin/env python
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()
# Gets the version
print(os.getenv("USER"), os.getenv("PASSWORD"), os.getenv("ACCOUNT"))
ctx = snowflake.connector.connect(
    user=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    account=os.getenv("ACCOUNT")
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()