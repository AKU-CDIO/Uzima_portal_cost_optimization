# backup_script.py

import os
import re
import sys
import time
import logging
from datetime import datetime
from typing import List, Optional, Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
from tqdm import tqdm

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings


# ------------------------
# Config helpers
# ------------------------

def getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except ValueError:
        return default


# ------------------------
# Environment variables
# ------------------------

DB_SERVER = os.getenv("DB_SERVER", "")
DB_NAME = os.getenv("DB_NAME", "")
DB_USERNAME = os.getenv("DB_USERNAME")  # optional if using MSI/AAD
DB_PASSWORD = os.getenv("DB_PASSWORD")  # optional if using MSI/AAD

# Storage config
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "")
STORAGE_CONTAINER = os.getenv("STORAGE_CONTAINER", "backups")
STORAGE_SAS_TOKEN = os.getenv("STORAGE_SAS_TOKEN")  # optional; if absent we'll use MSI/AAD

# Behavior/config
BATCH_SIZE = getenv_int("BATCH_SIZE", 100_000)
TEMP_DIR = os.getenv("TEMP_DIR", "/tmp/fitbit-backups")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# SQL connectivity options
USE_MSI_FOR_SQL = getenv_bool("USE_MSI_FOR_SQL", False)  # if true and no SQL creds, try MSI
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")  # for user-assigned MI (optional)
SQL_ENCRYPT = os.getenv("DB_ENCRYPT", "yes")  # yes/no
SQL_TRUST_SERVER_CERT = os.getenv("DB_TRUST_SERVER_CERT", "no")  # yes/no

# Parquet options
PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")


# ------------------------
# Logging
# ------------------------

def setup_logging():
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ------------------------
# Azure Blob helpers
# ------------------------

def get_blob_service_client() -> BlobServiceClient:
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    if STORAGE_SAS_TOKEN:
        sas = STORAGE_SAS_TOKEN if STORAGE_SAS_TOKEN.startswith("?") else f"?{STORAGE_SAS_TOKEN}"
        return BlobServiceClient(account_url=account_url + sas)
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    return BlobServiceClient(account_url=account_url, credential=credential)


def upload_file_to_blob(file_path: str, blob_name: str, overwrite: bool = True, max_retries: int = 5):
    bsc = get_blob_service_client()
    container_client = bsc.get_container_client(STORAGE_CONTAINER)
    try:
        container_client.create_container()
    except Exception:
        pass  # container likely exists

    blob: BlobClient = container_client.get_blob_client(blob_name)
    backoff = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            with open(file_path, "rb") as f:
                blob.upload_blob(
                    f,
                    overwrite=overwrite,
                    content_settings=ContentSettings(content_type="application/octet-stream"),
                )
            return
        except Exception as e:
            if attempt == max_retries:
                raise
            logging.warning(f"Blob upload failed (attempt {attempt}/{max_retries}): {e}. Retrying in {backoff:.1f}s…")
            time.sleep(backoff)
            backoff *= 2.0


# ------------------------
# SQL helpers
# ------------------------

def _build_sql_conn_str_sql_auth() -> str:
    base = (
        f"Server={DB_SERVER};"
        f"Database={DB_NAME};"
        f"UID={DB_USERNAME};PWD={DB_PASSWORD};"
        f"Encrypt={SQL_ENCRYPT};TrustServerCertificate={SQL_TRUST_SERVER_CERT};"
        f"Connection Timeout=30;"
    )
    conn18 = f"Driver={{ODBC Driver 18 for SQL Server}};{base}"
    conn17 = f"Driver={{ODBC Driver 17 for SQL Server}};{base}"
    return conn18 + "||" + conn17  # sentinel to attempt both

def _build_sql_conn_str_msi() -> List[str]:
    msi_extras = ""
    if AZURE_CLIENT_ID:
        msi_extras = f"MSI ClientId={AZURE_CLIENT_ID};"
    base = (
        f"Server={DB_SERVER};Database={DB_NAME};"
        f"Authentication=ActiveDirectoryMsi;{msi_extras}"
        f"Encrypt={SQL_ENCRYPT};TrustServerCertificate={SQL_TRUST_SERVER_CERT};"
        f"Connection Timeout=30;"
    )
    return [
        f"Driver={{ODBC Driver 18 for SQL Server}};{base}",
        f"Driver={{ODBC Driver 17 for SQL Server}};{base}",
    ]

def get_db_connection() -> pyodbc.Connection:
    if DB_USERNAME and DB_PASSWORD:
        logging.info("Connecting to SQL with SQL authentication (username/password).")
        candidates = _build_sql_conn_str_sql_auth().split("||")
    elif USE_MSI_FOR_SQL:
        logging.info("Connecting to SQL with Managed Identity (ActiveDirectoryMsi).")
        candidates = _build_sql_conn_str_msi()
    else:
        if DB_USERNAME and DB_PASSWORD:
            candidates = _build_sql_conn_str_sql_auth().split("||")
        else:
            logging.info("No SQL creds provided; attempting Managed Identity (ActiveDirectoryMsi).")
            candidates = _build_sql_conn_str_msi()

    last_err = None
    for cs in candidates:
        try:
            conn = pyodbc.connect(cs)
            return conn
        except pyodbc.Error as e:
            last_err = e
            logging.warning(f"Connection attempt failed for driver in conn string: {e}")
            continue
    raise last_err if last_err else RuntimeError("Failed to establish SQL connection.")

def get_table_row_count(conn: pyodbc.Connection, table_name: str) -> int:
    sql = f"SELECT COUNT_BIG(1) AS cnt FROM {table_name} WITH (NOLOCK);"
    df = pd.read_sql_query(sql, conn)
    return int(df.iloc[0]["cnt"])

def list_tables(conn: pyodbc.Connection) -> List[str]:
    """Return only the specified tables for backup"""
    return [
        "[DW].[FactFitbitIntraDayCombined]",
        "[DW].[FactFitbitIntraDayCombinedmm]",
        "[DW].[FactFitbitIntraMinbyMin]",
        "[DW].[FactFitbitIntraMinutebyMinute]",
    ]


# ------------------------
# Parquet streaming writer
# ------------------------

def safe_filename_from_table(table_name: str) -> str:
    t = table_name.replace("[", "").replace("]", "").replace(".", "_")
    return re.sub(r"[^A-Za-z0-9_]+", "_", t)

def write_table_to_parquet_streaming(
    conn: pyodbc.Connection,
    table_name: str,
    out_path: str,
    batch_size: int,
    progress_cb: Optional[Callable[[int], None]] = None,
):
    """
    Stream SELECT * in chunks and write to a single Parquet file via PyArrow ParquetWriter.
    Keeps the schema of the first chunk and aligns subsequent chunks.
    """
    query = f"SELECT * FROM {table_name} WITH (NOLOCK);"

    writer = None
    schema = None
    expected_columns = None
    processed_rows = 0
    batch_num = 1

    logging.info(f"[Stage 4/5] Exporting data in batches of {batch_size} rows")

    for chunk in pd.read_sql_query(query, conn, chunksize=batch_size):
        if writer is None:
            expected_columns = list(chunk.columns)
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            schema = table.schema
            writer = pq.ParquetWriter(out_path, schema, compression=PARQUET_COMPRESSION)
        else:
            for col in expected_columns:
                if col not in chunk.columns:
                    chunk[col] = None
            chunk = chunk[expected_columns]
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            try:
                table = table.cast(schema)
            except Exception:
                pass

        writer.write_table(table)

        chunk_size = len(chunk)
        processed_rows += chunk_size
        if progress_cb:
            progress_cb(chunk_size)

        if batch_num % 10 == 1 or chunk_size < batch_size:
            logging.info(
                f"Processing batch {batch_num}: rows {processed_rows - chunk_size + 1:,}-{processed_rows:,}"
            )
        batch_num += 1

    if writer is not None:
        writer.close()


# ------------------------
# Orchestrator
# ------------------------

class FitbitBackupRunner:
    def __init__(self, temp_dir: str, batch_size: int):
        self.temp_dir = temp_dir
        self.batch_size = batch_size
        os.makedirs(self.temp_dir, exist_ok=True)

    def _get_db_connection(self):
        return get_db_connection()

    def _get_table_row_count(self, conn, table_name: str) -> int:
        return get_table_row_count(conn, table_name)

    def _upload_to_blob(self, file_path: str, blob_name: str):
        upload_file_to_blob(file_path, blob_name, overwrite=True)

    def backup_table_to_parquet(self, table_name: str) -> str:
        """Backup a single table to parquet format"""
        try:
            logging.info(f"Starting backup for table: {table_name}")

            # Stage 1: DB connection
            logging.info(f"[Stage 1/5] Connecting to database for table: {table_name}")
            conn = self._get_db_connection()

            try:
                # Stage 2: Row count
                logging.info(f"[Stage 2/5] Getting row count for table: {table_name}")
                total_rows = self._get_table_row_count(conn, table_name)

                if total_rows == 0:
                    logging.warning(f"Table {table_name} is empty. Skipping…")
                    return f"Skipped empty table: {table_name}"

                logging.info(f"Table {table_name} contains {total_rows:,} rows")
                pbar = tqdm(total=total_rows, desc=f"Backing up {table_name}")

                # Stage 3: Output file
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_table = safe_filename_from_table(table_name)
                parquet_path = os.path.join(self.temp_dir, f"{safe_table}_{timestamp}.parquet")
                logging.info(f"[Stage 3/5] Will save backup to: {parquet_path}")

                # Stage 4: Export & write parquet (streaming)
                write_table_to_parquet_streaming(
                    conn, table_name, parquet_path, self.batch_size, progress_cb=pbar.update
                )
                pbar.close()

                # Stage 5: Upload to blob
                size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
                logging.info(f"[Stage 5/5] Uploading {size_mb:.2f} MB to blob storage")
                blob_name = f"backups/{os.path.basename(parquet_path)}"
                self._upload_to_blob(parquet_path, blob_name)

                # Clean up
                os.remove(parquet_path)

                logging.info(f"Completed backup for {table_name}: {total_rows:,} rows processed")
                return f"Successfully backed up {table_name} ({total_rows:,} rows)"
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        except Exception as e:
            logging.error(f"Error backing up {table_name}", exc_info=True)
            return f"Error backing up {table_name}: {str(e)}"

    def run(self) -> List[str]:
        setup_logging()
        if not DB_SERVER or not DB_NAME:
            raise RuntimeError("DB_SERVER and DB_NAME must be set as environment variables.")
        if not STORAGE_ACCOUNT_NAME:
            raise RuntimeError("STORAGE_ACCOUNT_NAME must be set as an environment variable.")
        if not STORAGE_CONTAINER:
            raise RuntimeError("STORAGE_CONTAINER must be set as an environment variable.")

        results = []
        # list_tables ignores the connection, but we pass one to keep signature consistent
        conn = self._get_db_connection()
        try:
            tables = list_tables(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass

        logging.info(f"Found {len(tables)} table(s) to back up.")
        for t in tables:
            res = self.backup_table_to_parquet(t)
            logging.info(res)
            results.append(res)
        return results


# ------------------------
# Entrypoint
# ------------------------

if __name__ == "__main__":
    setup_logging()
    logging.info("Starting Fitbit table backups…")
    logging.info(f"Temp dir: {TEMP_DIR} | Batch size: {BATCH_SIZE} | Container: {STORAGE_CONTAINER}")
    try:
        runner = FitbitBackupRunner(temp_dir=TEMP_DIR, batch_size=BATCH_SIZE)
        results = runner.run()
        logging.info("Backup run complete.")
        for line in results:
            print(line)
    except Exception as e:
        logging.error("Fatal error in backup run", exc_info=True)
        print(f"Backup failed: {e}", file=sys.stderr)
        sys.exit(1)
