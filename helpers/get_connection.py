#!/usr/bin/env python
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()
# Gets the version


def connection():
    ctx = snowflake.connector.connect(
    user=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
    account=os.getenv("ACCOUNT")
    )
    cs = ctx.cursor()
    return ctx, cs
