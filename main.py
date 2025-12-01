import oracledb
import pyodbc
import pandas as pd
import logging
import sys
from tqdm import tqdm
from dotenv import load_dotenv
import os
import importlib.util
import pathlib

def load_query_module(base_dir):
    query_file = pathlib.Path(base_dir) / "query.py"
    spec = importlib.util.spec_from_file_location("query", query_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def is_exe():
    return getattr(sys, "frozen", False)

def safe_tqdm(*args, **kwargs):
    if is_exe():
        kwargs["disable"] = True
    return tqdm(*args, **kwargs)


load_dotenv()

LOG_FILE = "oracle_to_mssql.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def connect_oracle(username: str, password: str, host_name : str, port : int, sid : str):
    try:
        # lib_dir = r"C:\oracle\instantclient_19_28"  # <- your folder path
        # oracledb.init_oracle_client(lib_dir=lib_dir)
        # print(f"✅ Oracle Client initialized in thick mode from {lib_dir}")
        dsn = oracledb.makedsn(host_name, port, sid)
        conn = oracledb.connect(user=username, password=password, dsn=dsn)
        logger.info(f"✅ Connected to Oracle DB - {username}")
        return conn
    except Exception as e:
        logger.exception(f"❌ Oracle connection failed: {e}")
        sys.exit(1)

def connect_mssql(server: str, database: str, username: str, password: str, driver: str = "ODBC Driver 17 for SQL Server", encrypt:str = "yes", TrustServerCertificate:str = "yes"):
    try:
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt={encrypt};TrustServerCertificate={TrustServerCertificate};"
        conn = pyodbc.connect(conn_str, autocommit=False)
        logger.info(f"✅ Connected to MS SQL Server - {server}")
        return conn
    except Exception as e:
        logger.exception(f"❌ SQL Server connection failed: {e}")
        sys.exit(1)


# def map_oracle_to_mssql_dtype(dtype: str, precision=None, scale=None, length=None) -> str:
#     """
#     Maps Oracle data types to SQL Server data types.
#     If scale > 38 (SQL Server limit), it will be reduced to 2.
#     """

#     if scale and scale > 38:
#         logger.warning(f"⚠️ Scale {scale} exceeds SQL Server limit (38). Reduced to 2.")
#         scale = 2
#         dtype = dtype.lower()

#     if "char" in dtype:
#         if not length or length > 4000:
#             length = 4000
#         return f"NVARCHAR({length})"

#     elif "number" in dtype or "decimal" in dtype:
#         # 🔧 Fix: Cap scale to 2 if it's too large
#         if scale and scale > 38:
#             scale = 2

#         if scale and scale > 0:
#             precision = precision or 10
#             return f"DECIMAL({precision},{scale})"
#         else:
#             if precision and precision <= 10:
#                 return "INT"
#             elif precision and precision <= 18:
#                 return "BIGINT"
#             else:
#                 return f"DECIMAL({precision or 10},0)"

#     elif "date" in dtype or "timestamp" in dtype:
#         return "DATETIME"

#     elif "clob" in dtype or "blob" in dtype:
#         return "NVARCHAR(MAX)"

#     elif "float" in dtype or "double" in dtype:
#         return "FLOAT"

#     else:
#         return "NVARCHAR(255)"


# def map_oracle_to_mssql_dtype(dtype: str, precision=None, scale=None, length=None) -> str:
#     dtype = dtype.lower()

#     if "char" in dtype:
#         if not length or length > 4000:
#             length = 4000
#         return f"NVARCHAR({length})"

#     elif "number" in dtype or "decimal" in dtype:
#         precision = precision or 38
#         scale = scale or 2
#         precision = min(precision, 38)
#         scale = min(scale, 38)
#         return f"NUMERIC({precision},{scale})"

#     elif "date" in dtype or "timestamp" in dtype:
#         return "DATETIME2"   # <- FIXED

#     elif "clob" in dtype or "blob" in dtype:
#         return "NVARCHAR(MAX)"

#     elif "float" in dtype or "double" in dtype:
#         return "FLOAT"

#     else:
#         return "NVARCHAR(255)"


def map_oracle_to_mssql_dtype(dtype: str, precision=None, scale=None, length=None) -> str:
    """
    Universal Oracle → MSSQL datatype mapper.
    Handles NUMBER, VARCHAR2, DATE, TIMESTAMP, RAW, CLOB, BLOB, FLOAT, INTEGER, XMLTYPE, etc.
    """

    dtype = (dtype or "").strip().lower()

    # -----------------------------
    # CHARACTER TYPES
    # -----------------------------
    if dtype in ("char", "nchar", "varchar", "varchar2", "nvarchar2"):
        length = length or 255
        if length > 4000:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({length})"

    # -----------------------------
    # NUMBER / INTEGER TYPES
    # -----------------------------
    if "number" in dtype or "decimal" in dtype or "numeric" in dtype:

        # NUMBER with no precision → treat as DECIMAL(38,0)
        if precision is None and scale is None:
            return "DECIMAL(38, 0)"

        # NUMBER(10) → DECIMAL(10,0)
        if precision is not None and (scale is None or scale == 0):
            return f"DECIMAL({precision}, 0)"

        # NUMBER(p,s) → DECIMAL(p,s)
        if precision is not None and scale is not None:
            precision = min(int(precision), 38)
            scale = min(int(scale), precision)
            return f"DECIMAL({precision}, {scale})"

        return "DECIMAL(38, 10)"  # safe fallback

    # Oracle INTEGER maps cleanly
    if "int" in dtype:
        return "INT"

    # -----------------------------
    # FLOAT / BINARY_FLOAT / BINARY_DOUBLE
    # -----------------------------
    if "float" in dtype or "binary_float" in dtype or "double" in dtype or "binary_double" in dtype:
        return "FLOAT"

    # -----------------------------
    # DATE / TIME TYPES
    # -----------------------------
    if "date" in dtype:
        return "DATETIME2"

    if "timestamp" in dtype:
        return "DATETIME2"

    if "time" in dtype:
        return "TIME"

    # -----------------------------
    # LOB TYPES
    # -----------------------------
    if "clob" in dtype:
        return "NVARCHAR(MAX)"

    if "blob" in dtype:
        return "VARBINARY(MAX)"

    # -----------------------------
    # RAW / LONG RAW
    # -----------------------------
    if "raw" in dtype:
        return "VARBINARY(MAX)"

    # -----------------------------
    # XMLTYPE
    # -----------------------------
    if "xml" in dtype:
        return "XML"

    # -----------------------------
    # LONG / LONG VARCHAR
    # -----------------------------
    if "long" in dtype:
        return "NVARCHAR(MAX)"

    # -----------------------------
    # BFILE
    # -----------------------------
    if "bfile" in dtype:
        return "VARBINARY(MAX)"

    # -----------------------------
    # DEFAULT FALLBACK
    # -----------------------------
    return "NVARCHAR(255)"

    
def fetch_oracle_data(oracle_conn, query):
    cursor = oracle_conn.cursor()
    cursor.execute(query)

    columns = [col[0] for col in cursor.description]
    types = []
    for col in cursor.description:
        try:
            types.append(col[1].name)
        except AttributeError:
            types.append(getattr(col[1], "__name__", "str"))

    chunk_size = 5000
    all_rows = []
    total_rows = 0

    logger.info("📥 Fetching data from Oracle...")
    with safe_tqdm(unit="rows", desc="Fetching", ncols=90) as pbar:
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            all_rows.extend(rows)
            total_rows += len(rows)
            pbar.update(len(rows))

    df = pd.DataFrame(all_rows, columns=columns)
    cursor.close()
    logger.info(f"✅ Completed fetching {total_rows:,} rows and {len(columns)} columns.")
    return df, dict(zip(columns, types))


def create_table_if_not_exists(mssql_conn, table_name, schema_map):
    cursor = mssql_conn.cursor()

    if "." in table_name:
        schema, pure_table = table_name.split(".", 1)
    else:
        schema, pure_table = "dbo", table_name

    check_schema_query = f"""
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
        EXEC('CREATE SCHEMA [{schema}]')
    """
    cursor.execute(check_schema_query)

    check_table_query = f"""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{pure_table}'
    )
    BEGIN
        EXEC('CREATE TABLE [{schema}].[{pure_table}] (
            {', '.join([f'[{col}] {map_oracle_to_mssql_dtype(dtype)}' for col, dtype in schema_map.items()])}
        )')
    END
    """

    try:
        cursor.execute(check_table_query)
        mssql_conn.commit()
        logger.info(f"✅ Table [{schema}].[{pure_table}] verified or created.")
    except Exception as e:
        mssql_conn.rollback()
        logger.exception(f"❌ Failed to create table [{schema}].[{pure_table}]")
        sys.exit(1)
    finally:
        cursor.close()

def insert_to_mssql(df, mssql_conn, target_table, batch_size=1000):
    df = df.where(pd.notnull(df), None)
    cursor = mssql_conn.cursor()

    columns = list(df.columns)
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join([f"[{col}]" for col in columns])

    if "." in target_table:
        schema, pure_table = target_table.split(".", 1)
        full_table = f"[{schema}].[{pure_table}]"
    else:
        full_table = f"[dbo].[{target_table}]"

    insert_query = f"INSERT INTO {full_table} ({col_names}) VALUES ({placeholders})"
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    try:
        cursor.fast_executemany = True
        logger.info(f"📤 Inserting {len(data):,} rows into {full_table}...")

        with safe_tqdm(total=len(data), unit="rows", desc="Inserting", ncols=90) as pbar:
            for start in range(0, len(data), batch_size):
                batch = data[start:start + batch_size]
                cursor.executemany(insert_query, batch)
                mssql_conn.commit()
                pbar.update(len(batch))

        logger.info(f"✅ Inserted {len(data):,} rows into {full_table}.")
    except Exception as e:
        mssql_conn.rollback()
        logger.exception(f"❌ Insert failed for {full_table}")
    finally:
        cursor.close()

def main(dbo: str, table_name: str, query: str, save_csv: bool = False):
    import tkinter as tk
    from tkinter import messagebox, filedialog

    # Initialize hidden Tkinter root for dialogs (important for .exe)
    root = tk.Tk()
    root.withdraw()

    target_table = f"{dbo}.{table_name}"
    oracle_conn = connect_oracle(
        username=os.getenv("ORACLE_USERNAME"),
        password=os.getenv("ORACLE_PASSWORD"),
        host_name=os.getenv("ORACLE_HOSTNAME"),
        port=os.getenv("ORACLE_PORT"),
        sid=os.getenv("ORACLE_SID")
    )
    mssql_conn = connect_mssql(
        server=os.getenv("SQL_SERVER"),
        database=os.getenv("SQL_DATABASE"),
        username=os.getenv("SQL_USERNAME"),
        password=os.getenv("SQL_PASSWORD")
    )

    df = None
    try:
        # query is passed directly from MigrationWorker
        df, schema_map = fetch_oracle_data(oracle_conn, query)


        # --- 1️⃣ Fetch Data from Oracle ---
        #df, schema_map = fetch_oracle_data(oracle_conn, query.oracle_query)
        # --- 2️⃣ Auto-export if enabled ---
        if save_csv and not df.empty:
            export_path = f"{table_name}_fetched_data.csv"
            try:
                df.to_csv(export_path, index=False, encoding="utf-8-sig")
                logger.info(f"💾 Auto-exported fetched data to {export_path}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to export CSV automatically: {e}")
        # --- 3️⃣ Create Table (if not exists) ---
        create_table_if_not_exists(mssql_conn, target_table, schema_map)
        # --- 4️⃣ Insert Fetched Data into MSSQL ---
        insert_to_mssql(df, mssql_conn, target_table)
        logger.info("🏁 Data transfer complete.")
    except Exception as e:
        logger.exception("🚨 Fatal error during transfer")
        raise Exception("Error Occured: Please refer to the log or report the issue.")

    finally:
        oracle_conn.close()
        mssql_conn.close()
        file_path = "query.py"
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File '{file_path}' removed successfully.")
        else:
            print(f"File '{file_path}' does not exist.")
        logger.info("🔒 Connections closed.")
    return df




    
