import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile


def save_to_snowflake(snow, batch, temp_dir, column):
    logging.debug("inserting batch to db")
    pandas_df = pd.DataFrame(
        batch,
        columns=column,
    )
    arrow_table = pa.Table.from_pandas(pandas_df)
    out_path = f"{temp_dir.name}/{str(uuid.uuid1())}.parquet"
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression="SNAPPY")
    snow.cursor().execute(
        "PUT 'file://{0}' @%LIFT_TICKETS_PY_COPY_INTO".format(out_path)
    )
    os.unlink(out_path)
    snow.cursor().execute(
        "COPY INTO LIFT_TICKETS_PY_COPY_INTO FILE_FORMAT=(TYPE='PARQUET') MATCH_BY_COLUMN_NAME=CASE_SENSITIVE PURGE=TRUE"
    )
    logging.debug(f"inserted {len(batch)} tickets")

