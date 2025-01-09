import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()
logging.basicConfig(level=logging.WARN)
from helpers.connect_to_snowflake import connect_to_snowflake


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-copy-into'}, 
    )


def save_to_snowflake(snow, batch, temp_dir):
    logging.debug("inserting batch to db")
    pandas_df = pd.DataFrame(
        batch,
        columns=[
            "TXID",
            "RFID",
            "RESORT",
            "PURCHASE_TIME",
            "EXPIRATION_TIME",
            "DAYS",
            "NAME",
            # "ADDRESS",
            # "PHONE",
            # "EMAIL",
            # "EMERGENCY_CONTACT",
        ],
    )
    arrow_table = pa.Table.from_pandas(pandas_df)
    out_path = f"{temp_dir.name}/{str(uuid.uuid1())}.parquet"
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression="SNAPPY")
    snow.cursor().execute(
        "PUT 'file://{0}' @%LIFT_TICKETS_PY_COPY_INTO".format(out_path)
    )
    # os.unlink(out_path)
    snow.cursor().execute(
        "COPY INTO LIFT_TICKETS_PY_COPY_INTO FILE_FORMAT=(TYPE='PARQUET') MATCH_BY_COLUMN_NAME=CASE_SENSITIVE PURGE=TRUE"
    )
    logging.debug(f"inserted {len(batch)} tickets")


if __name__ == "__main__":
    batch_size = 10
    print("re")
    snow = connect_to_snowflake().connection
    snow.cursor().execute("CREATE OR REPLACE TABLE LIFT_TICKETS_PY_COPY_INTO (TXID varchar(255), RFID varchar(255), RESORT varchar(255), PURCHASE_TIME datetime, EXPIRATION_TIME date, DAYS number, NAME varchar(255))")
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    for message in sys.stdin:
        if message != "\n":
            record = json.loads(message)
            batch.append(
                (
                    record["txid"],
                    record["rfid"],
                    record["resort"],
                    record["purchase_time"],
                    record["expiration_time"],
                    record["days"],
                    record["name"],
                    # record["address"],
                    # record["phone"],
                    # record["email"],
                    # record["emergency_contact"],
                )
            )
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir)
                batch = []
        else:
            break
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir)
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")
