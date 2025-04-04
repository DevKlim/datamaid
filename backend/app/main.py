# backend/app/main.py
# --- START OF FILE main.py ---

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import polars as pl
import duckdb
import io
import json
import re
import os
import tempfile
import uuid
import shutil
import numpy as np
import traceback
import ast # Import Abstract Syntax Trees for code parsing
from typing import Optional, List, Dict, Any, Union, Tuple
from pandas.errors import DataError, ParserError, EmptyDataError

# Use relative import if services is a package in the same directory as main's parent
# Services might be less used now, code execution is central
from .services import pandas_service, sql_service, relational_algebra_service, polars_service # pandas/polars services less critical now

TEMP_UPLOAD_DIR = tempfile.gettempdir()
print(f"Using temporary directory: {TEMP_UPLOAD_DIR}")
app = FastAPI(title="Data Analysis GUI API - Multi-Dataset")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
        "http://localhost:3001",
        "https://datamaid.netlify.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- In-memory State for Multiple Datasets ---
# Key: dataset_name (string)
# Value: Dict {
#   "content": bytes (CSV format),
#   "type": "dataframe" | "series",
#   "origin": "upload" | "code" | "ra" | "db",
#   "original_filename": Optional[str],
#   "history": List[bytes] (optional, for simple undo)
# }
datasets_state: Dict[str, Dict[str, Any]] = {}

# Stores paths to temporary DB files for import process
temp_db_files: Dict[str, str] = {}

# --- Helper Functions ---
def _sanitize_variable_name(name: str) -> str:
    """Converts a dataset name into a valid Python variable name."""
    if not name: return 'data' # Changed default
    # Replace non-alphanumeric characters (excluding underscore) with underscore
    s_name = re.sub(r'[^\w]', '_', name)
    # Ensure it doesn't start with a digit
    if s_name and s_name[0].isdigit(): s_name = '_' + s_name
    # Ensure it's not a Python keyword
    keywords = {"if", "else", "while", "for", "def", "class", "import", "from", "try", "except", "finally", "return", "yield", "lambda", "global", "nonlocal", "pass", "break", "continue", "with", "as", "assert", "del", "in", "is", "not", "or", "and", "True", "False", "None"}
    if s_name in keywords: s_name += '_'
    # Handle potential empty string after sanitization
    return s_name if s_name else 'data'

def cleanup_temp_file(file_path: str, delay: int = 0):
     # Placeholder: Implement actual delayed cleanup if needed
     if os.path.exists(file_path):
         try:
             os.remove(file_path)
             print(f"Cleaned up temporary file: {file_path}")
         except OSError as e:
             print(f"Error cleaning up temp file {file_path}: {e}")

def _determine_type_and_content(data: Union[pd.DataFrame, pd.Series]) -> Tuple[str, bytes]:
    """Determines if data is DataFrame or Series and returns type string and CSV bytes."""
    content_bytes: bytes
    data_type: str
    if isinstance(data, pd.DataFrame):
        data_type = "dataframe"
        with io.BytesIO() as buffer:
            data.to_csv(buffer, index=False)
            content_bytes = buffer.getvalue()
    elif isinstance(data, pd.Series):
        data_type = "series"
        # Store Series as a single-column DataFrame CSV
        df_temp = data.to_frame()
        with io.BytesIO() as buffer:
            df_temp.to_csv(buffer, index=False)
            content_bytes = buffer.getvalue()
    else:
        raise TypeError(f"Unsupported data type for state storage: {type(data)}")
    return data_type, content_bytes

def _get_preview_from_content(content: bytes, data_type: str = 'csv', limit: int = 100, offset: int = 0) -> Dict:
    """Generates preview dict from content bytes based on data_type."""
    try:
        if not content: return {"data": [], "columns": [], "row_count": 0}

        df = None
        if data_type == 'csv':
            df = pd.read_csv(io.BytesIO(content))
        # Add elif for other types like 'parquet' if needed in the future
        # elif data_type == 'parquet':
        #     df = pd.read_parquet(io.BytesIO(content))
        else:
            # Fallback or error for unsupported types
            print(f"Preview Warning: Unsupported data_type '{data_type}', attempting CSV read.")
            df = pd.read_csv(io.BytesIO(content)) # Try CSV as default

        # Handle potential non-serializable types during preview generation
        preview_df = df.iloc[offset:offset+limit].copy()
        for col in preview_df.columns:
            if pd.api.types.is_datetime64_any_dtype(preview_df[col]):
                 preview_df[col] = preview_df[col].astype(str)
            # Add more type handling here if needed

        # Replace inf/-inf with None, fill NaN with 'NaN' string
        data_list = preview_df.replace([np.inf, -np.inf], None).fillna('NaN').to_dict(orient="records")

        return {
            "data": data_list,
            "columns": list(df.columns),
            "row_count": len(df)
        }
    except (ParserError, EmptyDataError) as pe:
        print(f"Preview Error ({data_type}): {pe}")
        return {"data": [], "columns": [], "row_count": 0, "error": f"Preview failed ({data_type}): {str(pe)}"}
    except Exception as e:
        print(f"Error generating preview ({data_type}): {type(e).__name__}: {e}")
        traceback.print_exc()
        return {"data": [], "columns": [], "row_count": 0, "error": f"Preview failed ({data_type}): {str(e)}"}

# --- Basic Endpoints ---
@app.get("/")
async def read_root():
    return {"message": "DataMaid API (Multi-Dataset) is running"}

@app.get("/test-connection")
async def test_connection():
    return {"status": "success", "message": "Backend connection is working"}

# --- Upload Endpoints (Update state structure) ---
@app.post("/upload")
async def upload_file(file: UploadFile = File(...), dataset_name: str = Form(...)):
    """Uploads a CSV/JSON file and stores it as a named dataset (DataFrame or Series)."""
    if not dataset_name.strip():
        raise HTTPException(status_code=400, detail="Dataset name cannot be empty.")
    if not file.filename:
         raise HTTPException(status_code=400, detail="Invalid file upload.")

    contents = await file.read()
    if not contents: raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    data_type = "dataframe" # Default assumption
    content_bytes = contents
    original_filename = file.filename

    try:
        # Attempt to read as CSV first
        try:
            df = pd.read_csv(io.BytesIO(contents))
            # Check if it's likely a Series (single column)
            if len(df.columns) == 1:
                # Heuristic: If it has one column, treat as Series for type hint
                # but store as DataFrame CSV for consistency
                data_type = "series"
                print(f"Detected single column, treating '{dataset_name}' as Series type.")
            # Re-serialize to ensure consistent CSV format
            data_type, content_bytes = _determine_type_and_content(df)

        except (ParserError, EmptyDataError, UnicodeDecodeError):
            # If CSV fails, try JSON (records orientation)
            try:
                df_json = pd.read_json(io.StringIO(contents.decode('utf-8')), orient="records")
                # Determine type and serialize back to CSV
                data_type, content_bytes = _determine_type_and_content(df_json)
                original_filename += ".csv" # Indicate stored format change
                print(f"Successfully parsed uploaded file '{file.filename}' as JSON records.")
            except Exception as json_err:
                raise HTTPException(status_code=400, detail=f"File '{file.filename}' is not a valid CSV or JSON (records format): {json_err}")
        except Exception as val_err:
            raise HTTPException(status_code=400, detail=f"Could not validate file '{file.filename}': {val_err}")

        if dataset_name in datasets_state: print(f"Warning: Overwriting dataset '{dataset_name}' via file upload.")

        # Store in the main state dictionary
        datasets_state[dataset_name] = {
            "content": content_bytes,
            "type": data_type,
            "origin": "upload",
            "original_filename": original_filename,
            "history": [] # Initialize history
        }

        preview_info = _get_preview_from_content(content_bytes, data_type, limit=100)

        return {
            "message": f"Successfully uploaded {file.filename} as '{dataset_name}' ({data_type})",
            "dataset_name": dataset_name,
            "dataset_type": data_type,
            "preview": preview_info.get("data", []),
            "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "datasets": sorted(list(datasets_state.keys())) # Return all names
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected Upload error: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Could not process file upload: {str(e)}")
    finally:
         if file: await file.close()


@app.post("/upload-text")
async def upload_text_data(
    dataset_name: str = Form(...),
    data_text: str = Form(...),
    data_format: str = Form("csv", enum=["csv", "json"])
):
    """Uploads text data (CSV/JSON) and stores it as a named dataset."""
    if not dataset_name.strip(): raise HTTPException(status_code=400, detail="Dataset name cannot be empty.")
    if not data_text.strip(): raise HTTPException(status_code=400, detail="Pasted text cannot be empty.")
    if dataset_name in datasets_state: print(f"Warning: Overwriting existing dataset '{dataset_name}' from text upload.")

    data_type = "dataframe"
    content_bytes = None
    original_filename = f"{dataset_name}_pasted"

    try:
        df: Union[pd.DataFrame, pd.Series]
        if data_format == "csv":
            df = pd.read_csv(io.StringIO(data_text))
            original_filename += ".csv"
        elif data_format == "json":
            try:
                # Try records first, then maybe other orientations if needed
                df = pd.read_json(io.StringIO(data_text), orient="records")
                original_filename += ".json" # Original format was JSON
            except ValueError as json_err:
                raise HTTPException(status_code=400, detail=f"Could not parse JSON data (expected records format): {json_err}")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported data_format: {data_format}")

        # Determine type and serialize to CSV bytes
        data_type, content_bytes = _determine_type_and_content(df)
        if data_type == "series":
             print(f"Detected single column from text, treating '{dataset_name}' as Series type.")

        if content_bytes is None: raise ValueError("Failed to convert text data to bytes.")

        # Store in the main state dictionary
        datasets_state[dataset_name] = {
            "content": content_bytes,
            "type": data_type,
            "origin": "upload",
            "original_filename": original_filename,
            "history": []
        }

        preview_info = _get_preview_from_content(content_bytes, data_type, limit=100)

        return {
            "message": f"Successfully loaded data as '{dataset_name}' ({data_type})",
            "dataset_name": dataset_name,
            "dataset_type": data_type,
            "preview": preview_info.get("data", []),
            "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "datasets": sorted(list(datasets_state.keys())) # Return all names
        }
    except (ParserError, EmptyDataError, ValueError) as pe: raise HTTPException(status_code=400, detail=f"Could not parse {data_format.upper()} data: {str(pe)}")
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Text Upload error: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Could not process text data: {str(e)}")


@app.post("/upload-db")
async def upload_database_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # Logic remains the same, stores file path in temp_db_files
    allowed_extensions = {".db", ".sqlite", ".sqlite3", ".duckdb"}
    temp_file_path = None
    try:
        file_ext = os.path.splitext(file.filename)[1].lower() if file.filename else ''
        if file_ext not in allowed_extensions: raise HTTPException(status_code=400, detail=f"Unsupported file type '{file_ext}'. Allowed: {allowed_extensions}")

        temp_id = str(uuid.uuid4())
        temp_file_path = os.path.join(TEMP_UPLOAD_DIR, f"dbupload_{temp_id}{file_ext}")

        # Ensure file object exists before accessing file attribute
        if not hasattr(file, 'file'):
             raise HTTPException(status_code=400, detail="Invalid file object received.")

        with open(temp_file_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)

        con = None
        try:
            con = duckdb.connect(temp_file_path, read_only=True)
            con.execute("SELECT 1") # Test connection
        except duckdb.Error as db_err:
             if temp_file_path: cleanup_temp_file(temp_file_path)
             raise HTTPException(status_code=400, detail=f"Uploaded file is not a valid database or is corrupted: {db_err}")
        finally:
            if con: con.close()

        temp_db_files[temp_id] = temp_file_path
        print(f"Stored temporary DB file: {temp_file_path} with ID: {temp_id}")
        # Schedule cleanup after a delay (e.g., 1 hour)
        # background_tasks.add_task(cleanup_temp_file, temp_file_path, delay=3600) # Requires async sleep or separate scheduler
        return {"message": "Database file uploaded successfully.", "temp_db_id": temp_id}

    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Database Upload error: {type(e).__name__}: {e}")
        traceback.print_exc()
        if temp_file_path and os.path.exists(temp_file_path): cleanup_temp_file(temp_file_path)
        raise HTTPException(status_code=500, detail=f"Could not process database file: {str(e)}")
    finally:
        if file: await file.close()


@app.get("/list-db-tables/{temp_db_id}")
async def list_database_tables(temp_db_id: str):
    # Logic remains the same
    if temp_db_id not in temp_db_files: raise HTTPException(status_code=404, detail="Temporary database ID not found or expired.")
    file_path = temp_db_files[temp_db_id]
    if not os.path.exists(file_path):
         if temp_db_id in temp_db_files: del temp_db_files[temp_db_id]
         raise HTTPException(status_code=404, detail="Temporary database file not found (may have been cleaned up).")
    con = None
    try:
        con = duckdb.connect(file_path, read_only=True)
        tables_result = con.execute("SHOW TABLES;").fetchall()
        table_names = [table[0] for table in tables_result]
        return {"tables": table_names}
    except duckdb.Error as e: raise HTTPException(status_code=500, detail=f"Error reading tables from database file: {e}")
    except Exception as e:
        print(f"List DB tables error: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred listing tables: {str(e)}")
    finally:
         if con: con.close()


@app.post("/import-db-table")
async def import_database_table(
    temp_db_id: str = Form(...),
    table_name: str = Form(...),
    new_dataset_name: str = Form(...)
):
    """Imports a table from an uploaded DB file into the datasets_state."""
    if not new_dataset_name.strip(): raise HTTPException(status_code=400, detail="New dataset name cannot be empty.")
    if temp_db_id not in temp_db_files: raise HTTPException(status_code=404, detail="Temporary database ID not found or expired.")
    file_path = temp_db_files[temp_db_id]
    if not os.path.exists(file_path):
         if temp_db_id in temp_db_files: del temp_db_files[temp_db_id]
         raise HTTPException(status_code=404, detail="Temporary database file not found (may have been cleaned up).")

    if new_dataset_name in datasets_state: print(f"Warning: Overwriting existing dataset '{new_dataset_name}' from DB import.")

    con = None
    try:
        con = duckdb.connect(file_path, read_only=True)
        # Sanitize table name for SQL query
        s_table_name = table_name
        try:
            # Check if table exists
            con.execute(f"SELECT 1 FROM {s_table_name} LIMIT 1;")
        except duckdb.Error:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in the database file.")

        # Fetch data as Pandas DataFrame
        imported_df = con.execute(f"SELECT * FROM {s_table_name};").fetchdf()

        # Determine type and serialize to CSV bytes
        data_type, content_bytes = _determine_type_and_content(imported_df)
        if data_type == "series":
             print(f"Imported table '{table_name}' has one column, treating '{new_dataset_name}' as Series type.")

        # Store in the main state dictionary
        datasets_state[new_dataset_name] = {
            "content": content_bytes,
            "type": data_type,
            "origin": "db",
            "original_filename": f"{new_dataset_name}_from_{table_name}.csv",
            "history": []
        }

        preview_info = _get_preview_from_content(content_bytes, data_type, limit=100)

        # Clean up the temp DB file associated with this import ID? Maybe not yet, user might import another table.
        # Consider adding a separate cleanup mechanism or timeout for temp_db_files.

        return {
            "message": f"Successfully imported table '{table_name}' as dataset '{new_dataset_name}' ({data_type})",
            "dataset_name": new_dataset_name,
            "dataset_type": data_type,
            "preview": preview_info.get("data", []),
            "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "datasets": sorted(list(datasets_state.keys())) # Return all names
        }
    except HTTPException as http_err: raise http_err
    except (duckdb.Error, ValueError) as db_err: raise HTTPException(status_code=500, detail=f"Error importing table '{table_name}': {db_err}")
    except Exception as e:
        print(f"DB Table Import error: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Could not import table: {str(e)}")
    finally:
        if con: con.close()


# --- Dataset Listing & Retrieval (Updated) ---
@app.get("/datasets")
async def get_datasets_list():
    """Returns the names of all currently available datasets."""
    try:
        return {"datasets": sorted(list(datasets_state.keys()))}
    except Exception as e:
        print(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve dataset list.")

@app.get("/dataset/{dataset_name}")
async def get_dataset_view(
    dataset_name: str,
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0)
):
    """Gets the preview, type, and info for a specific named dataset."""
    if dataset_name not in datasets_state:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

    try:
        state_entry = datasets_state[dataset_name]
        content = state_entry["content"]
        data_type = state_entry["type"]
        preview_info = _get_preview_from_content(content, data_type, limit, offset)

        can_undo = bool(state_entry.get("history"))
        # Can reset if it has history (simplification: reset clears history)
        can_reset = can_undo

        return {
            "dataset_name": dataset_name,
            "dataset_type": data_type,
            "data": preview_info.get("data", []),
            "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "can_undo": can_undo,
            "can_reset": can_reset,
            # No last_code needed here, frontend manages editor state
        }
    except Exception as e:
        print(f"Error in get_dataset_view for '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to retrieve view for '{dataset_name}'.")

# --- Info/Stats Endpoints (Operate on the current content) ---
@app.get("/dataset-info/{dataset_name}")
async def get_dataset_info(dataset_name: str):
    """Gets general information about a dataset (DataFrame or Series)."""
    if dataset_name not in datasets_state:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")
    try:
        state_entry = datasets_state[dataset_name]
        content = state_entry["content"]
        data_type = state_entry["type"]

        # Use pandas to calculate info from the current CSV content
        df = pd.read_csv(io.BytesIO(content)) # Read as DataFrame regardless of type for now
        total_rows = len(df)
        column_count = len(df.columns)

        if df.empty:
            return {
                "dataset_name": dataset_name, "dataset_type": data_type,
                "row_count": 0, "column_count": column_count, "memory_usage_bytes": 0,
                "column_types": {}, "numeric_columns": [], "categorical_columns": [],
                "datetime_columns": [], "other_columns": [], "missing_values_count": {},
                "missing_values_percentage": {}, "unique_value_summary": {}
            }

        # --- DataFrame Specific Info ---
        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()
        other_cols = df.select_dtypes(exclude=[np.number, 'object', 'category', 'datetime', 'datetimetz']).columns.tolist()
        column_types = {col: str(df[col].dtype) for col in df.columns}
        missing_values = df.isnull().sum().to_dict()
        missing_percent = {k: round((v / total_rows * 100), 2) if total_rows > 0 else 0 for k, v in missing_values.items()}

        # Unique value summary (only for DataFrames or if Series treated as DF)
        unique_counts = {}
        if data_type == "dataframe": # Only compute detailed unique for dataframes for now
            for col in df.columns:
                if total_rows > 0 and total_rows < 50000: # Limit unique check for performance
                    try:
                        nunique = df[col].nunique()
                        unique_counts[col] = {"total_unique": nunique}
                        if nunique < 100: # Show top 10 values if cardinality is low
                            value_counts = df[col].value_counts().head(10).to_dict()
                            # Ensure keys/values are JSON serializable
                            unique_counts[col]["values"] = {str(k): int(v) if isinstance(v, (np.integer, np.int64)) else v for k, v in value_counts.items()}
                    except Exception as unique_err:
                        print(f"Could not calculate unique counts for column '{col}': {unique_err}")
                        unique_counts[col] = {"error": "Could not calculate"}

        # --- Base Info ---
        info = {
            "dataset_name": dataset_name, "dataset_type": data_type,
            "row_count": total_rows, "column_count": column_count,
            "memory_usage_bytes": int(df.memory_usage(deep=True).sum()),
            "column_types": column_types,
            "missing_values_count": missing_values,
            "missing_values_percentage": missing_percent,
        }

        # Add DataFrame specific fields if applicable
        if data_type == "dataframe":
            info.update({
                "numeric_columns": numeric_cols,
                "categorical_columns": categorical_cols,
                "datetime_columns": datetime_cols,
                "other_columns": other_cols,
                "unique_value_summary": unique_counts
            })
        # If it's a Series, some fields might be simplified or omitted
        elif data_type == "series" and column_count == 1:
             series_col_name = df.columns[0]
             info["series_name"] = series_col_name # Add series name if identifiable
             # Simplify some fields for series view
             info["numeric_columns"] = numeric_cols
             info["categorical_columns"] = categorical_cols
             info["datetime_columns"] = datetime_cols
             info["other_columns"] = other_cols
             # Unique summary for the series itself
             if total_rows > 0 and total_rows < 50000:
                 try:
                     series_col = df[series_col_name]
                     nunique = series_col.nunique()
                     unique_counts[series_col_name] = {"total_unique": nunique}
                     if nunique < 100:
                         value_counts = series_col.value_counts().head(10).to_dict()
                         unique_counts[series_col_name]["values"] = {str(k): int(v) if isinstance(v, (np.integer, np.int64)) else v for k, v in value_counts.items()}
                     info["unique_value_summary"] = unique_counts
                 except Exception as unique_err:
                     print(f"Could not calculate unique counts for series '{series_col_name}': {unique_err}")
                     info["unique_value_summary"] = {series_col_name: {"error": "Could not calculate"}}


        return info

    except (ParserError, EmptyDataError) as pe: raise HTTPException(status_code=400, detail=f"Cannot get info: Invalid data format for '{dataset_name}'. {str(pe)}")
    except Exception as e_inner:
        print(f"Error calculating dataset info for '{dataset_name}': {type(e_inner).__name__}: {e_inner}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error calculating info for '{dataset_name}': {str(e_inner)}")


@app.get("/column-stats/{dataset_name}/{column_name}")
async def get_column_stats(dataset_name: str, column_name: str):
    """Gets detailed statistics for a specific column within a dataset."""
    if dataset_name not in datasets_state:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")
    try:
        state_entry = datasets_state[dataset_name]
        content = state_entry["content"]
        data_type = state_entry["type"] # Needed? Column stats are column stats.

        # Use pandas for stats calculation
        df = pd.read_csv(io.BytesIO(content))
        if column_name not in df.columns:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in dataset '{dataset_name}'.")

        column_data = df[column_name].copy()
        total_rows = len(df)
        stats = {
            "column_name": column_name,
            "dataset_name": dataset_name,
            "dtype": str(column_data.dtype),
            "missing_count": int(column_data.isnull().sum()),
            "missing_percentage": round((column_data.isnull().sum() / total_rows * 100), 2) if total_rows > 0 else 0,
            "memory_usage_bytes": int(column_data.memory_usage(deep=True))
        }

        # Calculate type-specific stats
        if pd.api.types.is_numeric_dtype(column_data.dtype):
            desc = column_data.describe()
            stats.update({
                "mean": float(desc['mean']) if pd.notna(desc.get('mean')) else None,
                "std": float(desc['std']) if pd.notna(desc.get('std')) else None,
                "min": float(desc['min']) if pd.notna(desc.get('min')) else None,
                "max": float(desc['max']) if pd.notna(desc.get('max')) else None,
                "quantiles": {
                    "25%": float(desc['25%']) if pd.notna(desc.get('25%')) else None,
                    "50%": float(desc['50%']) if pd.notna(desc.get('50%')) else None, # Median
                    "75%": float(desc['75%']) if pd.notna(desc.get('75%')) else None,
                }
            })
        elif pd.api.types.is_datetime64_any_dtype(column_data.dtype):
            stats.update({
                "min_date": str(column_data.min()) if not column_data.isnull().all() else None,
                "max_date": str(column_data.max()) if not column_data.isnull().all() else None,
            })
        else: # Assume categorical/object/other
            nunique = column_data.nunique()
            stats["unique_count"] = int(nunique)
            if nunique < 1000 and total_rows > 0: # Only show top values if cardinality is reasonable
                top_values = column_data.value_counts().head(10).to_dict()
                stats["top_values"] = {str(k): int(v) for k, v in top_values.items()} # Ensure keys are strings, values are ints

        # Convert numpy types before returning for JSON serialization
        for key, value in stats.items():
            if isinstance(value, (np.integer, np.int64)): stats[key] = int(value)
            elif isinstance(value, (np.floating, np.float64)): stats[key] = float(value) if pd.notna(value) else None
            elif isinstance(value, np.bool_): stats[key] = bool(value)
            elif isinstance(value, dict): # Handle quantiles and top_values
                stats[key] = {str(k): (int(v) if isinstance(v, (np.integer, np.int64)) else float(v) if isinstance(v, (np.floating, np.float64)) and pd.notna(v) else None if isinstance(v, (np.floating, np.float64)) else v) for k, v in value.items()}

        return stats
    except (ParserError, EmptyDataError) as pe: raise HTTPException(status_code=400, detail=f"Cannot get stats: Invalid data format for '{dataset_name}'. {str(pe)}")
    except KeyError: raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in dataset '{dataset_name}'.")
    except Exception as e_inner:
        print(f"Error calculating column stats for '{column_name}' in '{dataset_name}': {type(e_inner).__name__}: {e_inner}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error calculating stats for column '{column_name}': {str(e_inner)}")


# --- Data Transformation Endpoint (Centralized) ---

@app.post("/execute-code")
async def execute_custom_code(
    code: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"]),
    # Optional: hint which dataset the user is currently viewing, for preview return preference
    current_view_name: Optional[str] = Form(None)
):
    """Executes custom code (Pandas, Polars, SQL), potentially creating/modifying multiple datasets."""
    if not code.strip():
        raise HTTPException(status_code=400, detail="Code cannot be empty.")

    # --- Prepare Execution Environment ---
    exec_globals = {}
    local_vars = {}
    modified_or_created_datasets = set() # Track names of datasets affected
    primary_result_name = current_view_name # Default preview target
    primary_result_type = datasets_state.get(current_view_name, {}).get("type") if current_view_name else None
    primary_result_content = None

    # Store original state keys before execution
    initial_dataset_keys = set(datasets_state.keys())

    try:
        # 1. Load all current datasets into the environment
        if engine == "pandas":
            exec_globals = {"pd": pd, "np": np, "io": io}
            for name, state in datasets_state.items():
                var_name = _sanitize_variable_name(name)
                try:
                    # Load based on stored type
                    df_or_series: Union[pd.DataFrame, pd.Series]
                    df_temp = pd.read_csv(io.BytesIO(state["content"]))
                    if state["type"] == "series" and len(df_temp.columns) == 1:
                        df_or_series = df_temp.iloc[:, 0] # Convert back to Series
                        df_or_series.name = df_temp.columns[0] # Preserve name
                    else:
                        df_or_series = df_temp # Keep as DataFrame
                    local_vars[var_name] = df_or_series
                    print(f"Loaded '{name}' ({state['type']}) as pandas var '{var_name}' ({type(df_or_series).__name__})")
                except Exception as load_err:
                    print(f"Warning: Failed to load dataset '{name}' for pandas execution: {load_err}")
                    # Provide empty DataFrame/Series on load error? Or skip? Let's skip for now.
                    # local_vars[var_name] = pd.DataFrame() if state.get("type", "dataframe") == "dataframe" else pd.Series(dtype='object')

        elif engine == "polars":
            exec_globals = {"pl": pl, "io": io}
            for name, state in datasets_state.items():
                var_name = _sanitize_variable_name(name)
                try:
                    # Polars reads bytes directly. Assume DataFrame for now.
                    # TODO: Add Polars Series handling if needed.
                    if state["type"] == "series":
                         print(f"Warning: Polars execution currently loads Series '{name}' as a single-column DataFrame '{var_name}'.")
                    df = pl.read_csv(state["content"]) # Polars reads bytes
                    local_vars[var_name] = df
                    print(f"Loaded '{name}' ({state['type']}) as polars var '{var_name}'")
                except Exception as load_err:
                    print(f"Warning: Failed to load dataset '{name}' for polars execution: {load_err}")
                    # local_vars[var_name] = pl.DataFrame()

        elif engine == "sql":
            # SQL execution uses DuckDB connection
            con = duckdb.connect(":memory:")
            # Load all datasets as tables (use original name)
            loaded_tables = set() # Keep track of successfully loaded tables
            for name, state in datasets_state.items():
                # Use original dataset name directly as table name
                table_name = name
                try:
                    # Load content into DuckDB table using pandas for robustness
                    df_for_sql = pd.read_csv(io.BytesIO(state["content"]))
                    con.register(table_name, df_for_sql)
                    print(f"Loaded '{name}' ({state['type']}) as SQL table '{table_name}'")
                    loaded_tables.add(table_name)
                except Exception as load_err:
                    print(f"Warning: Failed to load dataset '{name}' for SQL execution: {load_err}")
            # Globals not used directly for SQL execution string

            # --- Execute Code ---
            final_result_df = None
            try:
                # Execute the whole block. DuckDB handles multiple statements separated by ;
                # Use sql() which can return results for the last statement
                query_result = con.sql(code) # Changed from con.execute()

                # Check if the last statement returned results (likely a SELECT)
                if query_result:
                    try:
                        final_result_df = query_result.fetchdf()
                        print(f"SQL code execution resulted in a DataFrame with {len(final_result_df)} rows.")
                    except Exception as fetch_err:
                        print(f"Warning: Could not fetch DataFrame from SQL query result: {fetch_err}")
                        final_result_df = None # Reset if fetch fails

                # --- Infer created/modified tables ---
                # Check for CREATE TABLE statements first (explicit modification)
                create_table_matches = re.findall(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([^\s(]+)", code, re.IGNORECASE | re.MULTILINE)
                for table_match in create_table_matches:
                    # Basic sanitization: remove quotes if present
                    created_table_name = table_match.strip().strip('"`')
                    # Check if it might be schema-qualified (basic check)
                    if '.' in created_table_name:
                        created_table_name = created_table_name.split('.')[-1].strip('"`')

                    modified_or_created_datasets.add(created_table_name)
                    primary_result_name = created_table_name # Assume last created table is primary result
                    print(f"SQL detected CREATE TABLE: {created_table_name}")

                # --- Handle SELECT results updating state (if no CREATE TABLE) ---
                if not create_table_matches and final_result_df is not None and not final_result_df.empty:
                    print("Attempting to update state from SELECT result...")
                    # Try to determine which original table was primarily queried
                    # Simple approach: check FROM clause of the *last* statement for known tables
                    # This requires parsing or making assumptions. Let's use current_view_name hint.
                    if current_view_name and current_view_name in loaded_tables:
                        # Basic check: Does the code string contain the current view name (as table)?
                        # This is weak but avoids complex parsing for now.
                        # A better check would involve parsing the FROM clause.
                        if re.search(r"FROM\s+[\"']?" + re.escape(current_view_name) + r"[\"']?(\s|\b|;)", code, re.IGNORECASE):
                            print(f"SELECT result seems related to current view '{current_view_name}'. Updating its state.")
                            modified_or_created_datasets.add(current_view_name)
                            primary_result_name = current_view_name
                            # The content will be updated below using final_result_df
                        else:
                            print(f"SELECT result detected, but current view '{current_view_name}' not found in FROM clause (basic check). Not updating state.")
                            # Keep primary_result_name as current_view_name for preview return, but don't modify state
                    else:
                        print("SELECT result detected, but no current view hint or hint not loaded. Not updating state.")
                        # If only one table was loaded, assume result applies to it? Risky.
                        if len(loaded_tables) == 1:
                            inferred_target = list(loaded_tables)[0]
                            print(f"Only one table loaded ('{inferred_target}'). Assuming SELECT result updates it.")
                            modified_or_created_datasets.add(inferred_target)
                            primary_result_name = inferred_target
                        else:
                            # Don't update state, just return preview
                            primary_result_name = None # Indicate no specific dataset was updated


            except duckdb.Error as sql_err:
                if con: con.close()
                raise sql_err # Re-raise to be caught by outer handler

            # --- Update State from Execution Results ---
            # Update based on CREATE TABLE or inferred SELECT target
            for dataset_key_name in modified_or_created_datasets:
                try:
                    df_to_save = None
                    # If it was the target of the SELECT result
                    if dataset_key_name == primary_result_name and final_result_df is not None:
                        df_to_save = final_result_df
                        print(f"Using SELECT result DataFrame for '{dataset_key_name}'.")
                    else:
                        # Otherwise, fetch content from the table (must exist if created)
                        print(f"Fetching content from table '{dataset_key_name}' (likely from CREATE TABLE).")
                        # Use the raw name for fetching from DuckDB table
                        df_to_save = con.execute(f"SELECT * FROM {dataset_key_name}").fetchdf()

                    if df_to_save is not None:
                        new_type, new_content = _determine_type_and_content(df_to_save) # Determine type

                        history = datasets_state.get(dataset_key_name, {}).get("history", [])
                        if dataset_key_name in datasets_state: # If overwriting
                            history.append(datasets_state[dataset_key_name]["content"])
                            history = history[-5:]

                        datasets_state[dataset_key_name] = {
                            "content": new_content,
                            "type": new_type,
                            "origin": "code", # Mark as code-generated/modified
                            "original_filename": None, # No original file
                            "history": history,
                            "sql_chain": None # *** CRITICAL: Clear SQL chain after custom code modification ***
                        }
                        print(f"Updated state for dataset: '{dataset_key_name}' ({new_type}). Cleared SQL chain.")

                        # Update primary result info if this was the one identified
                        if dataset_key_name == primary_result_name:
                            primary_result_type = new_type
                            primary_result_content = new_content
                    else:
                        print(f"Warning: Could not get DataFrame to save for '{dataset_key_name}'. State not updated.")

                except Exception as sql_update_err:
                    print(f"Error fetching/updating state for SQL dataset '{dataset_key_name}': {sql_update_err}")
            if con: con.close() # Close connection after processing

            # --- Prepare Response ---
            final_datasets_list = sorted(list(datasets_state.keys()))
            response_preview = {"data": [], "columns": [], "row_count": 0}

            # Try to return preview for the primary result dataset (updated or original view)
            target_preview_name = primary_result_name if primary_result_name in modified_or_created_datasets else current_view_name

            if target_preview_name and target_preview_name in datasets_state:
                state_entry = datasets_state[target_preview_name]
                response_preview = _get_preview_from_content(state_entry["content"], state_entry["type"], limit=100)
                # If SELECT result was shown but didn't update state, use its preview
                if target_preview_name == current_view_name and primary_result_name is None and final_result_df is not None:
                    print(f"Returning preview of SELECT result directly (state not updated).")
                    temp_type, temp_content = _determine_type_and_content(final_result_df)
                    response_preview = _get_preview_from_content(temp_content, temp_type, limit=100)
                    # Don't report undo/reset status for this temporary preview
                    response_preview["can_undo"] = False
                    response_preview["can_reset"] = False


            elif final_result_df is not None: # Fallback: preview the SELECT result if nothing else matches
                print("Returning preview of SELECT result as fallback.")
                temp_type, temp_content = _determine_type_and_content(final_result_df)
                response_preview = _get_preview_from_content(temp_content, temp_type, limit=100)
                target_preview_name = "[SELECT Result]" # Indicate it's not a saved dataset


            # Get undo/reset status for the dataset whose preview is being returned
            can_undo = False
            can_reset = False
            if target_preview_name and target_preview_name in datasets_state:
                can_undo = bool(datasets_state[target_preview_name].get("history"))
                can_reset = bool(datasets_state[target_preview_name].get("history")) # Reset clears history

            return {
                "message": f"SQL code executed. Updated/Created: {', '.join(sorted(list(modified_or_created_datasets))) if modified_or_created_datasets else 'None'}.",
                "datasets": final_datasets_list,
                "primary_result_name": target_preview_name, # Hint to frontend which preview is returned
                "primary_result_type": datasets_state.get(target_preview_name, {}).get("type") if target_preview_name in datasets_state else (temp_type if 'temp_type' in locals() else None),
                "preview": response_preview.get("data", []),
                "columns": response_preview.get("columns", []),
                "row_count": response_preview.get("row_count", 0),
                "can_undo": can_undo,
                "can_reset": can_reset,
            }

        # --- Identify Assignment Targets (for Pandas/Polars) ---
        assigned_vars = set()
        if engine in ["pandas", "polars"]:
            try:
                tree = ast.parse(code)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Assign):
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                assigned_vars.add(target.id)
                    # Handle assignments via attribute (e.g., df['new_col'] = ...) - harder to track perfectly
                    # elif isinstance(node, ast.Assign):
                    #     if isinstance(node.targets[0], ast.Subscript) and isinstance(node.targets[0].value, ast.Name):
                    #         # This detects df['col'] = ..., potentially modifying existing df
                    #         assigned_vars.add(node.targets[0].value.id) # Mark the base df as potentially modified
            except SyntaxError:
                pass # Let exec handle the syntax error reporting
            print(f"Identified potential assignment targets: {assigned_vars}")


        # --- Execute Code ---
        if engine in ["pandas", "polars"]:
            exec(code, exec_globals, local_vars)
        elif engine == "sql":
            try:
                # Execute the whole block. DuckDB handles multiple statements separated by ;
                # Use execute_many if needed, but execute should handle it.
                con.execute(code)

                # Infer created/modified tables (simplified check)
                # Check for CREATE TABLE statements
                create_table_matches = re.findall(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([^\s(]+)", code, re.IGNORECASE)
                for table_match in create_table_matches:
                    created_table_name = table_match.strip('"`') # Remove quotes
                    modified_or_created_datasets.add(created_table_name)
                    primary_result_name = created_table_name # Assume last created table is primary result
                    print(f"SQL detected CREATE TABLE: {created_table_name}")
                # We can't easily detect which tables were modified by UPDATE/DELETE/INSERT via `execute`
                # Assume SELECT queries don't modify state directly (user should use CREATE TABLE AS)
                # If no CREATE TABLE, maybe the last SELECT result is the primary? Hard to tell.

            except duckdb.Error as sql_err:
                 if con: con.close()
                 raise sql_err # Re-raise to be caught by outer handler

        # --- Update State from Execution Results ---
        if engine in ["pandas", "polars"]:
            # Check local_vars for new or modified DataFrames/Series
            for var_name, value in local_vars.items():
                is_df = isinstance(value, pd.DataFrame if engine == "pandas" else pl.DataFrame)
                is_series = isinstance(value, pd.Series) # Check for Series (Pandas only for now)

                if is_df or is_series:
                    # Find the original dataset name if it exists (mapping sanitized var_name back)
                    original_name = next((name for name, state in datasets_state.items() if _sanitize_variable_name(name) == var_name), None)
                    # Determine the key name for the state dictionary
                    dataset_key_name = original_name if original_name else var_name # Use original name if exists, else new var name

                    # Was this variable assigned to, or is it a new variable?
                    is_new_var = dataset_key_name not in initial_dataset_keys
                    was_assigned = var_name in assigned_vars

                    # Heuristic: Update state if the variable was assigned to, OR if it's a new variable.
                    # This might miss in-place modifications not caught by assignment parsing (e.g., df.dropna(inplace=True))
                    # A more robust check would compare content, but that's expensive.
                    if was_assigned or is_new_var:
                        print(f"Found modified/new {type(value).__name__}: '{var_name}' (maps to key: '{dataset_key_name}')")

                        # Serialize back to CSV bytes and determine type
                        new_content: Optional[bytes] = None
                        new_type: Optional[str] = None
                        try:
                            if engine == "pandas":
                                new_type, new_content = _determine_type_and_content(value)
                            elif engine == "polars" and is_df: # Polars Series handling TBD
                                new_type = "dataframe" # Assume DF for Polars for now
                                with io.BytesIO() as buffer:
                                    value.write_csv(buffer)
                                    new_content = buffer.getvalue()
                            # Add Polars Series handling here if needed
                        except Exception as serialize_err:
                            print(f"Error serializing result for '{var_name}': {serialize_err}")
                            continue # Skip updating this one

                        if new_content and new_type:
                            # Add previous state to history if updating existing
                            history = datasets_state.get(dataset_key_name, {}).get("history", [])
                            if dataset_key_name in datasets_state:
                                history.append(datasets_state[dataset_key_name]["content"])
                                history = history[-5:] # Limit history size

                            # Update or add to main state
                            datasets_state[dataset_key_name] = {
                                "content": new_content,
                                "type": new_type,
                                "origin": "code", # Mark as code-generated/modified
                                "original_filename": None, # No original file
                                "history": history
                            }
                            modified_or_created_datasets.add(dataset_key_name)

                            # Update primary result if this matches the initial view or is the only result
                            if dataset_key_name == current_view_name:
                                primary_result_name = current_view_name
                                primary_result_type = new_type
                                primary_result_content = new_content
                            elif len(modified_or_created_datasets) == 1 and is_new_var: # If it's the *only* new dataset created
                                 primary_result_name = dataset_key_name
                                 primary_result_type = new_type
                                 primary_result_content = new_content


        elif engine == "sql":
            # Update state for tables identified as created
            for table_name in modified_or_created_datasets:
                 try:
                     # Fetch content from the created table
                     df = con.execute(f"SELECT * FROM {sql_service._sanitize_identifier(table_name)}").fetchdf()
                     new_type, new_content = _determine_type_and_content(df) # Determine type

                     history = datasets_state.get(table_name, {}).get("history", [])
                     if table_name in datasets_state: # If overwriting via CREATE OR REPLACE
                         history.append(datasets_state[table_name]["content"])
                         history = history[-5:]

                     datasets_state[table_name] = {
                         "content": new_content,
                         "type": new_type,
                         "origin": "code", # Or 'sql'? Let's use 'code'
                         "original_filename": None,
                         "history": history
                     }
                     print(f"Updated state for SQL created table: '{table_name}' ({new_type})")
                     # Update primary result info if this was the one identified
                     if table_name == primary_result_name:
                         primary_result_type = new_type
                         primary_result_content = new_content

                 except Exception as sql_update_err:
                     print(f"Error fetching/updating state for SQL table '{table_name}': {sql_update_err}")
            if con: con.close() # Close connection after processing

        # --- Prepare Response ---
        final_datasets_list = sorted(list(datasets_state.keys()))
        response_preview = {"data": [], "columns": [], "row_count": 0}

        # Try to return preview for the primary result dataset
        if primary_result_name and primary_result_content and primary_result_type:
            response_preview = _get_preview_from_content(primary_result_content, primary_result_type, limit=100)
        elif modified_or_created_datasets:
            # Fallback: return preview of the first modified/created dataset alphabetically
            first_result_name = next(iter(sorted(list(modified_or_created_datasets))), None)
            if first_result_name and first_result_name in datasets_state:
                 primary_result_name = first_result_name
                 state_entry = datasets_state[first_result_name]
                 response_preview = _get_preview_from_content(state_entry["content"], state_entry["type"], limit=100)


        return {
            "message": f"Code executed ({engine}). Updated/Created: {', '.join(sorted(list(modified_or_created_datasets))) if modified_or_created_datasets else 'None'}.",
            "datasets": final_datasets_list,
            "primary_result_name": primary_result_name, # Hint to frontend which preview is returned
            "primary_result_type": datasets_state.get(primary_result_name, {}).get("type") if primary_result_name else None,
            "preview": response_preview.get("data", []),
            "columns": response_preview.get("columns", []),
            "row_count": response_preview.get("row_count", 0),
            # Include undo/reset status for the primary result?
            "can_undo": bool(datasets_state.get(primary_result_name, {}).get("history")),
            "can_reset": bool(datasets_state.get(primary_result_name, {}).get("history")),
        }

    except (SyntaxError, NameError, TypeError, ValueError, AttributeError, KeyError, IndexError,
            pd.errors.PandasError, pl.exceptions.PolarsError if pl else Exception, duckdb.Error) as exec_err:
         traceback.print_exc()
         # Close SQL connection on error if it exists
         if engine == "sql" and 'con' in locals() and con: con.close()
         detail = f"Code execution failed ({engine}): {type(exec_err).__name__}: {str(exec_err)}"
         # Improve error message for NameError (suggesting dataset names)
         if isinstance(exec_err, NameError):
             available_vars = list(local_vars.keys())
             detail += f". Available dataset variables in context: {available_vars}"
         raise HTTPException(status_code=400, detail=detail)
    except HTTPException as http_err:
         if engine == "sql" and 'con' in locals() and con: con.close()
         raise http_err
    except Exception as e:
        print(f"Unexpected error in /execute-code: {type(e).__name__}: {e}")
        traceback.print_exc()
        if engine == "sql" and 'con' in locals() and con: con.close()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during code execution.")


# --- Relational Algebra Endpoints (Updated for multi-dataset state) ---
@app.post("/relational-operation-preview")
async def preview_relational_operation(
    operation: str = Form(...),
    params: str = Form(...),
    # Base dataset is now a list potentially, but preview usually starts from one
    base_dataset_names_json: str = Form(...), # JSON list of names, e.g., ["dataset1", "dataset2"]
    current_sql_state: Optional[str] = Form(None),
    step_alias_base: str = Form("step")
):
    """Previews a relational algebra operation using DuckDB."""
    con = None
    try:
        params_dict = json.loads(params)
        base_dataset_names = json.loads(base_dataset_names_json)
        if not isinstance(base_dataset_names, list) or not base_dataset_names:
             raise HTTPException(status_code=400, detail="RA preview requires 'base_dataset_names_json' (list).")

        # For preview, we often start from one dataset or the result of a previous step.
        # The first name in the list is typically the primary input unless current_sql_state exists.
        primary_base_name = base_dataset_names[0]
        if primary_base_name not in datasets_state:
             raise HTTPException(status_code=404, detail=f"Base dataset '{primary_base_name}' not found.")

        con = duckdb.connect(":memory:")

        # Load ALL specified base datasets into the connection using their original names
        for name in base_dataset_names:
             if name not in datasets_state:
                 raise HTTPException(status_code=404, detail=f"Base dataset '{name}' for RA preview not found.")
             state_entry = datasets_state[name]
             print(f"DEBUG (RA Preview): Loading base data '{name}' ({state_entry['type']}) into DuckDB.")
             relational_algebra_service._load_ra_data(con, name, state_entry["content"]) # Use original name

        # --- RA Preview Logic (largely same as before) ---
        source_sql_or_table: str
        columns_before: List[str] = []
        step_number = 0

        if current_sql_state:
            # Parse previous SQL state to get the source for the next step
            state_strip = current_sql_state.strip()
            match = re.match(r"\((.*)\)\s+AS\s+([\w`\"']+)\s*$", state_strip, re.DOTALL | re.IGNORECASE)
            if not match:
                 # Maybe it's just a table name from a previous step? Unlikely with the AS structure.
                 # Or maybe it's a complex CTE chain? For now, require the (...) AS alias format.
                 raise ValueError(f"Could not parse previous SQL state format: {current_sql_state[:200]}...")

            core_previous_sql = match.group(1).strip()
            alias = match.group(2).strip('"`') # Get alias name
            num_match = re.search(r"(\d+)$", alias)
            step_number = int(num_match.group(1)) + 1 if num_match else 1

            # Create a temporary view from the previous step's SQL core
            temp_prev_view = f"__prev_view_{uuid.uuid4().hex[:8]}"
            print(f"DEBUG (RA Preview): Creating view {temp_prev_view} AS: {core_previous_sql}")
            try:
                con.execute(f"CREATE TEMP VIEW {temp_prev_view} AS {core_previous_sql};")
            except duckdb.Error as view_err:
                raise ValueError(f"Failed to create view from previous step SQL: {view_err}. SQL was: {core_previous_sql}")

            cols_result = con.execute(f"DESCRIBE {temp_prev_view};").fetchall()
            columns_before = [col[0] for col in cols_result]
            source_sql_or_table = temp_prev_view # The source for the *new* snippet is the view

        else:
            # No previous state, start from the primary base dataset
            step_number = 0
            s_primary_base_name = primary_base_name
            cols_result = con.execute(f"DESCRIBE {s_primary_base_name};").fetchall()
            columns_before = [col[0] for col in cols_result]
            source_sql_or_table = s_primary_base_name # Source is the base table itself

        # Handle operations needing column context (like rename)
        if operation.lower() == "rename":
            if not columns_before: raise ValueError("Cannot perform rename: Failed to determine columns from previous step.")
            params_dict["all_columns"] = columns_before
        # Handle operations needing multiple inputs (like join, union)
        if operation.lower() in ["join", "union", "intersect", "difference"]:
             # Ensure required parameters (e.g., other_relation_name) are present and loaded
             other_rel_name = params_dict.get("other_relation_name")
             if not other_rel_name: raise ValueError(f"Operation '{operation}' requires 'other_relation_name' in params.")
             if other_rel_name not in base_dataset_names: raise ValueError(f"Other relation '{other_rel_name}' for '{operation}' not found in loaded base datasets.")
             # The service function _generate_sql_snippet needs to handle using the correct table names

        current_step_alias = f"{step_alias_base}{step_number}"
        # Generate SQL for the *current* operation, using the determined source
        sql_snippet = relational_algebra_service._generate_sql_snippet(operation, params_dict, source_sql_or_table)
        print(f"DEBUG (RA Preview): Generated snippet for current step: {sql_snippet}")

        # Execute the snippet to get preview data
        preview_data, result_columns, total_rows = relational_algebra_service._execute_preview_query(con, sql_snippet)

        # Construct the SQL state for the *next* step
        s_current_step_alias = relational_algebra_service._sanitize_identifier(current_step_alias)
        next_sql_state = f"({sql_snippet}) AS {s_current_step_alias}"
        print(f"DEBUG (RA Preview): Generated next_sql_state: {next_sql_state}")

        return {
            "message": "RA preview generated successfully.",
            "data": preview_data, "columns": result_columns, "row_count": total_rows,
            "generated_sql_state": next_sql_state,
            "current_step_sql_snippet": sql_snippet # The SQL for just this step
        }

    except (ValueError, duckdb.Error, NotImplementedError, json.JSONDecodeError) as e:
         err_type = type(e).__name__
         detail = f"Relational Algebra preview for '{operation}' failed: {err_type}: {str(e)}"
         print(f"RA Preview Error (400): {detail}")
         # Add more specific error details if possible
         if isinstance(e, duckdb.BinderException): detail = f"RA preview failed (Binder Error): {str(e)}. Check column/table names/types."
         elif isinstance(e, duckdb.CatalogException): detail = f"RA preview failed (Catalog Error): {str(e)}. Check if table/view exists."
         elif isinstance(e, duckdb.ParserException): detail = f"RA preview failed (Parser Error): {str(e)}. Check syntax."
         elif isinstance(e, json.JSONDecodeError): detail = f"RA preview failed: Invalid parameters JSON. {str(e)}"
         raise HTTPException(status_code=400, detail=detail)
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /relational-operation-preview: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error during RA preview.")
    finally:
        if con: con.close()

@app.post("/save-ra-result")
async def save_relational_algebra_result(
    final_sql_chain: str = Form(...),
    new_dataset_name: str = Form(...),
    base_dataset_names_json: str = Form(...) # Names of datasets used in the chain
):
    """Executes the final RA SQL chain and saves the result as a new dataset."""
    con = None
    try:
        if not new_dataset_name.strip(): raise ValueError("New dataset name cannot be empty.")
        base_dataset_names = json.loads(base_dataset_names_json)
        if not isinstance(base_dataset_names, list) or not base_dataset_names:
            raise ValueError("Invalid or empty list of base dataset names provided.")

        if new_dataset_name in datasets_state:
            print(f"Warning: Overwriting dataset '{new_dataset_name}' with RA result save.")

        con = duckdb.connect(":memory:")
        # Load all necessary base datasets
        for ds_name in base_dataset_names:
             if ds_name not in datasets_state:
                 raise HTTPException(status_code=404, detail=f"Base dataset '{ds_name}' for RA save not found.")
             state_entry = datasets_state[ds_name]
             relational_algebra_service._load_ra_data(con, ds_name, state_entry["content"]) # Use original name

        print(f"Executing final RA SQL chain for saving '{new_dataset_name}':\n{final_sql_chain}")
        # Execute the final SQL chain provided by the frontend
        full_df = con.execute(final_sql_chain).fetchdf()

        # Determine type and serialize result to CSV bytes
        data_type, new_content = _determine_type_and_content(full_df)
        if data_type == "series":
             print(f"RA result '{new_dataset_name}' has one column, saving as Series type.")

        # Save as a new entry in datasets_state
        datasets_state[new_dataset_name] = {
            "content": new_content,
            "type": data_type,
            "origin": "ra",
            "original_filename": f"{new_dataset_name}_ra_result.csv",
            "history": [] # RA results start with no history
        }

        saved_preview_info = _get_preview_from_content(new_content, data_type, limit=100)
        return {
            "message": f"Successfully saved RA result as '{new_dataset_name}' ({data_type}).",
            "dataset_name": new_dataset_name,
            "dataset_type": data_type,
            "preview": saved_preview_info.get("data", []),
            "columns": saved_preview_info.get("columns", []),
            "row_count": saved_preview_info.get("row_count", 0),
            "datasets": sorted(list(datasets_state.keys())) # Return updated list
        }
    except (ValueError, duckdb.Error, json.JSONDecodeError) as e:
         detail = f"Failed to save RA result as '{new_dataset_name}': {str(e)}"
         print(f"RA Save Error (400): {detail}")
         raise HTTPException(status_code=400, detail=detail)
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /save-ra-result: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error during RA save.")
    finally:
        if con: con.close()
    
@app.post("/apply-operation/{dataset_name}")
async def apply_structured_operation(
    dataset_name: str,
    operation: str = Form(...),
    params_json: str = Form(...), # Parameters as JSON string
    engine: str = Form(..., enum=["pandas", "sql", "polars"]) # Add polars to enum
):
    """Applies a structured operation (filter, groupby, sample etc.) using the specified engine."""
    if dataset_name not in datasets_state:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

    state_entry = datasets_state[dataset_name]
    params = {}
    try:
        params = json.loads(params_json)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid parameters JSON.")

    print(f"Applying Operation: dataset='{dataset_name}', engine='{engine}', operation='{operation}', params='{params}'")

    # --- Engine Dispatch ---
    con = None # For SQL
    result_df = None # For Pandas/Polars
    generated_code = "" # For Pandas/Polars code snippet
    new_content = None # Holds resulting CSV bytes

    try:
        # --- Common logic for tracking history ---
        original_content = state_entry["content"]
        history = state_entry.get("history", [])

        # --- PANDAS ---
        if engine == "pandas":
            if state_entry.get("sql_chain"):
                print(f"Switching '{dataset_name}' from SQL to Pandas. Resetting SQL chain.")
                state_entry["sql_chain"] = None

            try: df = pd.read_csv(io.BytesIO(original_content))
            except Exception as load_err: raise HTTPException(status_code=500, detail=f"Pandas: Failed to load current data: {load_err}")

            if operation == 'merge':
                # ... (keep merge logic as before) ...
                right_dataset_name = params.get("right_dataset")
                if not right_dataset_name or right_dataset_name not in datasets_state: raise HTTPException(status_code=404, detail=f"Pandas Merge: Right dataset '{right_dataset_name}' not found.")
                try: right_df = pd.read_csv(io.BytesIO(datasets_state[right_dataset_name]["content"]))
                except Exception as load_err: raise HTTPException(status_code=500, detail=f"Pandas Merge: Failed to load right dataset '{right_dataset_name}': {load_err}")
                result_df, generated_code = pandas_service.apply_pandas_merge(df, right_df, params)
            else:
                # Dispatch to the main pandas operation handler
                result_df, generated_code = pandas_service.apply_pandas_operation(df, operation, params)

            # Serialize result back to content
            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            state_entry["sql_chain"] = None # Clear SQL chain

        # --- POLARS ---
        elif engine == "polars":
            if state_entry.get("sql_chain"):
                print(f"Switching '{dataset_name}' from SQL to Polars. Resetting SQL chain.")
                state_entry["sql_chain"] = None

            try: df = pl.read_csv(original_content) # Polars reads bytes
            except Exception as load_err: raise HTTPException(status_code=500, detail=f"Polars: Failed to load current data: {load_err}")

            if operation == 'merge':
                 # ... (Polars join logic) ...
                right_dataset_name = params.get("right_dataset")
                if not right_dataset_name or right_dataset_name not in datasets_state: raise HTTPException(status_code=404, detail=f"Polars Join: Right dataset '{right_dataset_name}' not found.")
                try: right_df = pl.read_csv(datasets_state[right_dataset_name]["content"])
                except Exception as load_err: raise HTTPException(status_code=500, detail=f"Polars Join: Failed to load right dataset '{right_dataset_name}': {load_err}")
                # Assuming a polars_service.apply_polars_join exists similar to pandas
                # Need to implement apply_polars_join if not already done
                if hasattr(polars_service, 'apply_polars_join'):
                     result_df, generated_code = polars_service.apply_polars_join(df, right_df, params)
                else:
                     raise NotImplementedError("Polars join/merge function not implemented in polars_service.")
            else:
                # Dispatch to the main polars operation handler
                result_df, generated_code = polars_service.apply_polars_operation(df, operation, params)

            # Serialize result back to content
            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                new_content = buffer.getvalue()
            state_entry["sql_chain"] = None # Clear SQL chain

        # --- SQL ---
        elif engine == "sql":
            con = duckdb.connect(":memory:")
            base_table_name = f"__{dataset_name}_base"
            base_table_ref = sql_service._sanitize_identifier(base_table_name)

            try: sql_service._load_data_to_duckdb(con, base_table_name, original_content)
            except Exception as load_err: raise HTTPException(status_code=500, detail=f"SQL: Failed to load current data into DuckDB: {load_err}")

            previous_sql_chain = state_entry.get("sql_chain")
            if not previous_sql_chain:
                previous_sql_chain = f"SELECT * FROM {base_table_ref}"
                print(f"Starting new SQL chain for '{dataset_name}' from base table {base_table_ref}")

            new_full_sql_chain = ""
            sql_snippet = "" # The CTE definition for this step
            preview_data = []
            result_columns = []
            total_rows = 0

            if operation == 'merge':
                # ... (keep SQL join logic as before) ...
                right_dataset_name = params.get("right_dataset")
                if not right_dataset_name or right_dataset_name not in datasets_state: raise HTTPException(status_code=404, detail=f"SQL Join: Right dataset '{right_dataset_name}' not found.")
                right_base_table_name = f"__{right_dataset_name}_base"
                right_base_table_ref = sql_service._sanitize_identifier(right_base_table_name)
                try: sql_service._load_data_to_duckdb(con, right_base_table_name, datasets_state[right_dataset_name]["content"])
                except Exception as load_err: raise HTTPException(status_code=500, detail=f"SQL Join: Failed to load right dataset '{right_dataset_name}': {load_err}")
                preview_data, result_columns, total_rows, new_full_sql_chain, sql_snippet = sql_service.apply_sql_join(
                    con=con, previous_sql_chain_left=previous_sql_chain, right_table_ref=right_base_table_ref, params=params, base_table_ref_left=base_table_ref
                )
            else:
                # Dispatch to the main SQL operation handler
                preview_data, result_columns, total_rows, new_full_sql_chain, sql_snippet = sql_service.apply_sql_operation(
                    con=con, previous_sql_chain=previous_sql_chain, operation=operation, params=params, base_table_ref=base_table_ref
                )

            # Materialize Result back to CSV content
            print(f"Materializing SQL result for '{dataset_name}'...")
            try:
                final_df = con.execute(new_full_sql_chain).fetchdf()
                with io.BytesIO() as buffer:
                    final_df.to_csv(buffer, index=False)
                    new_content = buffer.getvalue()
                print(f"Materialization successful. Size: {len(new_content)} bytes.")
            except Exception as materialize_err: raise HTTPException(status_code=500, detail=f"SQL: Failed to materialize result: {materialize_err}")

            # Update SQL chain state
            state_entry["sql_chain"] = new_full_sql_chain
            generated_code = sql_snippet # Use CTE snippet as the 'code' for SQL step

        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")

        # --- Update State (Common) ---
        if new_content is None:
            raise HTTPException(status_code=500, detail="Operation failed to produce new content.")

        history.append({ # Store previous content for undo
                "engine": engine,
                "operation": operation,
                "params_or_code": params, # Store params used
                "generated_code_or_snippet": generated_code,
                "previous_content": original_content, # Store previous content bytes
                "previous_sql_chain": state_entry.get("sql_chain") if engine != "sql" else previous_sql_chain # Store chain before this step if switching away or continuing sql
            })
        state_entry["content"] = new_content
        state_entry["history"] = history[-10:] # Limit history size

        # --- Prepare Response ---
        # For Pandas/Polars, generate preview from new_content. For SQL, use preview_data directly.
        response_preview = {}
        if engine == "sql":
            response_preview = {"data": preview_data, "columns": result_columns, "row_count": total_rows}
        else:
            response_preview = _get_preview_from_content(new_content, data_type='csv', limit=100)

        can_undo = bool(state_entry.get("history"))
        # Reset currently means clear history, not revert to original upload.
        can_reset = bool(state_entry.get("history"))

        return {
            "message": f"{engine.capitalize()} operation '{operation}' applied successfully to '{dataset_name}'.",
            "dataset_name": dataset_name,
            "data": response_preview.get("data", []),
            "columns": response_preview.get("columns", []),
            "row_count": response_preview.get("row_count", 0),
            "can_undo": can_undo,
            "can_reset": can_reset,
            "generated_code": generated_code # Code snippet (Pandas/Polars) or CTE (SQL)
        }

    except (ValueError, pd.errors.PandasError, pl.exceptions.PolarsError if pl else Exception, duckdb.Error, KeyError, NotImplementedError) as op_err:
        if con: con.close()
        print(f"Operation Error ({engine}, {operation}): {type(op_err).__name__}: {op_err}")
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Operation failed: {str(op_err)}")
    except HTTPException as http_err:
        if con: con.close()
        raise http_err
    except Exception as e:
        if con: con.close()
        print(f"Unexpected error during operation ({engine}, {operation}): {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during the operation.")
    finally:
        if con:
            try: con.close()
            except Exception as close_err: print(f"Error closing DuckDB connection: {close_err}")

# --- Undo/Reset/Save Transformation Endpoints (Updated for specific dataset) ---
@app.post("/undo/{dataset_name}")
async def undo_last_operation(dataset_name: str):
    """Reverts the dataset to the state before the last operation (if history exists)."""
    if dataset_name not in datasets_state: raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")
    state_entry = datasets_state[dataset_name]
    history = state_entry.get("history", [])
    if not history: raise HTTPException(status_code=400, detail=f"No history to undo for dataset '{dataset_name}'.")

    try:
        last_step = history.pop()
        previous_content = last_step.get("previous_content")
        previous_sql_chain = last_step.get("previous_sql_chain") # Get previous chain if stored

        if previous_content is None:
            # If no previous content stored (old history format?), we can't revert content.
            # Put the step back and raise an error or just log? Let's log and return error.
            history.append(last_step) # Put it back
            print(f"Error during undo for {dataset_name}: History entry missing 'previous_content'.")
            raise HTTPException(status_code=500, detail="Cannot undo: History data is incomplete.")

        # Restore content and potentially the SQL chain
        state_entry["content"] = previous_content
        # Restore SQL chain *only if* the undone step was also SQL or if we are reverting to an SQL state
        # If the previous step was SQL, `previous_sql_chain` should hold the chain *before* that step.
        state_entry["sql_chain"] = previous_sql_chain

        print(f"Undo successful for {dataset_name}. Restored content. SQL chain set to: {'Present' if previous_sql_chain else 'None'}")

        data_type = state_entry["type"]
        preview_info = _get_preview_from_content(previous_content, data_type) # Use data_type here

        return {
            "message": f"Undid last change for {dataset_name}",
            "dataset_name": dataset_name, "dataset_type": data_type,
            "data": preview_info.get("data", []), "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "can_undo": bool(history), "can_reset": bool(history)
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error during undo for '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred during undo.")


@app.post("/reset/{dataset_name}")
async def reset_transformations(dataset_name: str):
    """Resets the dataset by clearing its transformation history and SQL chain."""
    if dataset_name not in datasets_state: raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")
    state_entry = datasets_state[dataset_name]
    if not state_entry.get("history"): raise HTTPException(status_code=400, detail=f"Dataset '{dataset_name}' has no history to reset.")

    try:
        # Clear history and SQL chain, keep current content
        state_entry["history"] = []
        state_entry["sql_chain"] = None
        current_content = state_entry["content"]
        data_type = state_entry["type"]
        print(f"Reset history and SQL chain for '{dataset_name}' (current content kept).")
        preview_info = _get_preview_from_content(current_content, data_type) # Use data_type

        return {
            "message": f"Reset history for {dataset_name}",
            "dataset_name": dataset_name, "dataset_type": data_type,
            "data": preview_info.get("data", []), "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "can_undo": False, "can_reset": False
        }
    except Exception as e:
         print(f"Error during reset for '{dataset_name}': {type(e).__name__}: {e}")
         traceback.print_exc()
         raise HTTPException(status_code=500, detail=f"An error occurred during reset.")

@app.post("/reset/{dataset_name}")
async def reset_transformations(dataset_name: str):
    """Resets the dataset by clearing its transformation history."""
    # Note: This currently does NOT revert to the original uploaded file content.
    # It only clears the undo history, keeping the current state.
    if dataset_name not in datasets_state:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

    state_entry = datasets_state[dataset_name]

    if not state_entry.get("history"):
         raise HTTPException(status_code=400, detail=f"Dataset '{dataset_name}' has no history to reset.")

    try:
        # Clear the history
        state_entry["history"] = []
        state_entry["sql_chain"] = None # Clear SQL chain on reset
        current_content = state_entry["content"] # Keep current content after clearing history
        print(f"Reset history and SQL chain for '{dataset_name}' (current content kept).")
        data_type = state_entry["type"]

        print(f"Reset history for '{dataset_name}' (current content kept).")

        preview_info = _get_preview_from_content(current_content, data_type)

        return {
            "message": f"Reset history for {dataset_name}",
            "dataset_name": dataset_name,
            "dataset_type": data_type,
            "data": preview_info.get("data", []),
            "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "can_undo": False, # History cleared
            "can_reset": False # Cannot reset further
        }
    except Exception as e:
         print(f"Error during reset for '{dataset_name}': {type(e).__name__}: {e}")
         traceback.print_exc()
         raise HTTPException(status_code=500, detail=f"An error occurred during reset.")


# --- Export Endpoint (Operates on current content of specific dataset) ---
@app.get("/export/{dataset_name}")
async def export_dataset(
    dataset_name: str,
    format: str = Query("csv", enum=["csv", "json", "excel"])
):
    """Exports the *current state* of the specified dataset."""
    if dataset_name not in datasets_state:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

    try:
        state_entry = datasets_state[dataset_name]
        content = state_entry["content"]
        data_type = state_entry["type"]
        file_content: Union[bytes, str]
        media_type: str
        filename_base = re.sub(r'[^\w\.\-]', '_', dataset_name) # Sanitize name for filename

        if format == "csv":
            media_type="text/csv"
            filename = f"{filename_base}_export.csv"
            file_content = content
        else:
            # Use pandas for consistent non-CSV export
            try:
                 df = pd.read_csv(io.BytesIO(content))
                 if format == "json":
                     media_type="application/json"
                     filename = f"{filename_base}_export.json"
                     # Handle export based on type
                     if data_type == "series" and len(df.columns) == 1:
                         # Export Series as a simple JSON list
                         series = df.iloc[:, 0]
                         # Handle potential non-serializable data
                         series_serializable = series.replace([np.inf, -np.inf], None).copy()
                         if pd.api.types.is_datetime64_any_dtype(series_serializable.dtype):
                             series_serializable = series_serializable.astype(str)
                         file_content = series_serializable.to_json(orient="values", default_handler=str, force_ascii=False)
                     else: # Export DataFrame as records
                         df_serializable = df.replace([np.inf, -np.inf], None).copy()
                         for col in df_serializable.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns:
                             df_serializable[col] = df_serializable[col].astype(str)
                         file_content = df_serializable.to_json(orient="records", default_handler=str, force_ascii=False)

                 elif format == "excel":
                     media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                     filename = f"{filename_base}_export.xlsx"
                     with io.BytesIO() as buffer:
                         # Excel export might fail on certain types (e.g., timezone-aware datetime)
                         # Consider converting types before export if issues arise
                         df.to_excel(buffer, index=False, engine='openpyxl')
                         file_content = buffer.getvalue()
                 else:
                     raise ValueError(f"Unsupported format: {format}")
            except (ParserError, EmptyDataError) as pe: raise HTTPException(status_code=400, detail=f"Cannot export: Invalid data format for '{dataset_name}'. {str(pe)}")
            except Exception as export_load_err:
                 print(f"Error preparing non-CSV export for '{dataset_name}': {export_load_err}")
                 traceback.print_exc()
                 raise HTTPException(status_code=500, detail=f"Failed to prepare data for {format} export.")

        return Response(
            content=file_content,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename=\"{filename}\""}
        )
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Export Error ({format}) for {dataset_name}: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error exporting dataset '{dataset_name}' as {format}.")


# --- Dataset Rename / Delete Endpoints (Operate on datasets_state) ---
@app.post("/rename-dataset/{old_dataset_name}")
async def rename_dataset(
    old_dataset_name: str,
    new_dataset_name: str = Form(...)
):
    """Renames a dataset in the main state."""
    try:
        if not old_dataset_name or not new_dataset_name: raise ValueError("Old and new dataset names must be provided.")
        new_name = new_dataset_name.strip()
        if not new_name: raise ValueError("New dataset name cannot be empty.")
        # Basic validation for name (allow more chars now, sanitize for code exec)
        # if not re.match(r"^[a-zA-Z0-9_\-\.]+$", new_name): raise ValueError("New name contains invalid characters.")
        if old_dataset_name not in datasets_state: raise HTTPException(status_code=404, detail=f"Dataset '{old_dataset_name}' not found.")
        if new_name == old_dataset_name: return {"message": f"Dataset name '{old_dataset_name}' unchanged.", "datasets": sorted(list(datasets_state.keys()))}
        if new_name in datasets_state: raise HTTPException(status_code=409, detail=f"Dataset name '{new_name}' already exists.")

        # Perform rename in the main state dictionary
        datasets_state[new_name] = datasets_state.pop(old_dataset_name)
        # Note: Code referencing the old name (e.g., in saved snippets) won't be updated automatically.

        print(f"Renamed dataset '{old_dataset_name}' to '{new_name}'")
        return {
            "message": f"Successfully renamed dataset '{old_dataset_name}' to '{new_name}'.",
            "old_name": old_dataset_name, "new_name": new_name,
            "datasets": sorted(list(datasets_state.keys())) # Return updated list
        }
    except ValueError as ve: raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error renaming dataset '{old_dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during rename.")

@app.delete("/dataset/{dataset_name}")
async def delete_dataset(dataset_name: str):
    """Deletes a dataset from the main state."""
    try:
        if dataset_name not in datasets_state:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

        # Delete from the main state dictionary
        del datasets_state[dataset_name]

        print(f"Deleted dataset '{dataset_name}'")
        return {
            "message": f"Successfully deleted dataset '{dataset_name}'.",
            "deleted_name": dataset_name,
            "datasets": sorted(list(datasets_state.keys())) # Return updated list
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error deleting dataset '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during deletion.")

# --- END OF FILE main.py ---