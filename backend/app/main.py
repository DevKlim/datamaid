# backend/app/main.py

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
import traceback # <-- Import traceback for detailed error logging
from typing import Optional, List, Dict, Any, Union
from pandas.errors import DataError, ParserError, EmptyDataError

# Use relative import if services is a package in the same directory as main's parent
from .services import pandas_service, polars_service, sql_service, relational_algebra_service # Adjusted import

TEMP_UPLOAD_DIR = tempfile.gettempdir()
print(f"Using temporary directory: {TEMP_UPLOAD_DIR}")
app = FastAPI(title="Data Analysis GUI API")

# Configure CORS - Ensure your frontend origin is listed
# Use ["*"] for development if needed, but restrict in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001", # Allow other potential dev ports
        "http://localhost:3001",
        "https://datamaid.netlify.app" # Your production frontend origin
    ],
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods (GET, POST, DELETE, etc.)
    allow_headers=["*"], # Allows all headers
)

# --- In-memory State ---
datasets: Dict[str, Dict[str, Any]] = {}
transformations: Dict[str, Dict[str, Any]] = {}
temp_db_files: Dict[str, str] = {}

# --- Helper Functions (get_current_content, update_transformation_state, _get_preview_from_content, cleanup_temp_file) ---
# (Keep existing helper functions as they were - including error handling within _get_preview_from_content)
def get_current_content(dataset_name: str) -> bytes:
    """Gets the latest content bytes (original or transformed)."""
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")
    if dataset_name in transformations and transformations[dataset_name].get("current_content") is not None:
        return transformations[dataset_name]["current_content"]
    else:
        return datasets[dataset_name]["content"]

def update_transformation_state(dataset_name: str, engine: str, operation: str, params_or_code: Any, generated_code: str, content_before: bytes, new_content: bytes):
    """Updates the transformation state and history."""
    if dataset_name not in datasets: return

    if dataset_name not in transformations:
        transformations[dataset_name] = {"current_content": new_content, "history": []}
    else:
        transformations[dataset_name]["current_content"] = new_content

    history_entry = {
        "engine": engine,
        "operation": operation,
        "params_or_code": params_or_code,
        "generated_code": generated_code,
        "content_before": content_before,
    }
    transformations[dataset_name]["history"].append(history_entry)
    if "current_content" not in transformations[dataset_name]:
         transformations[dataset_name]["current_content"] = new_content

def _get_preview_from_content(content: bytes, engine: str, limit: int = 100, offset: int = 0) -> Dict:
    """Helper to generate preview dict from bytes using specified engine."""
    # Adding top-level try-except for robustness
    try:
        if not content:
             return {"data": [], "columns": [], "row_count": 0}

        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content))
            return {
                "data": df.iloc[offset:offset+limit].replace([np.inf, -np.inf], None).fillna('NaN').to_dict(orient="records"),
                "columns": list(df.columns),
                "row_count": len(df)
            }
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content))
            return {
                "data": df.slice(offset, limit).fill_nan('NaN').to_dicts(),
                "columns": df.columns,
                "row_count": df.height
            }
        elif engine == "sql":
             con = None
             try:
                 con = duckdb.connect(":memory:")
                 table_name = "__preview_table" # Use predictable but unlikely name
                 sql_service._load_data_to_duckdb(con, table_name, content)
                 # Handle offset directly in the query for SQL
                 sql_offset_query = f'SELECT * FROM "{table_name}" LIMIT {limit} OFFSET {offset}'
                 data_dicts_offset, columns_offset, total_rows_offset = sql_service._execute_sql_query(con, sql_offset_query, preview_limit=None) # Let query handle limit/offset
                 return { "data": data_dicts_offset, "columns": columns_offset, "row_count": total_rows_offset }
             finally:
                 if con: con.close()
        else:
             raise ValueError(f"Unsupported engine for preview: {engine}")
    except (ParserError, EmptyDataError) as pe:
        print(f"Pandas/Polars Preview Error: {pe}")
        raise HTTPException(status_code=400, detail=f"Cannot generate preview: Invalid CSV format. {str(pe)}")
    except Exception as e:
        print(f"Error generating preview with {engine}: {type(e).__name__}: {e}")
        traceback.print_exc()
        # Fallback attempt with Pandas (if not already tried)
        if engine != "pandas":
            try:
                df = pd.read_csv(io.BytesIO(content))
                return {
                    "data": df.iloc[offset:offset+limit].replace([np.inf, -np.inf], None).fillna('NaN').to_dict(orient="records"),
                    "columns": list(df.columns),
                    "row_count": len(df)
                }
            except Exception as pd_e:
                print(f"Fallback pandas preview failed: {pd_e}")
                raise HTTPException(status_code=500, detail=f"Error processing dataset preview: {str(e)}")
        else:
            # If pandas itself failed, raise the original error
             raise HTTPException(status_code=500, detail=f"Error processing dataset preview ({engine}): {str(e)}")


def cleanup_temp_file(file_path: str):
    """Safely removes a temporary file."""
    # ... (keep existing implementation) ...
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Cleaned up temp file: {file_path}")
    except OSError as e:
        print(f"Error cleaning up temp file {file_path}: {e}")


# --- Basic Endpoints ---
@app.get("/")
async def read_root():
    return {"message": "Data Analysis GUI API is running"}

@app.get("/test-connection")
async def test_connection():
    return {"status": "success", "message": "Backend connection is working"}

# --- Upload Endpoints (/upload, /upload-text, /upload-db) ---
# (Keep existing upload endpoints as they were, ensuring they handle errors and return HTTPExceptions)
@app.post("/upload")
async def upload_file(file: UploadFile = File(...), dataset_name: str = Form(...)):
    try:
        contents = await file.read()
        if not contents:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        try:
            # Basic validation using nrows
            pd.read_csv(io.BytesIO(contents), nrows=5)
        except (ParserError, EmptyDataError) as csv_err:
             raise HTTPException(status_code=400, detail=f"File '{file.filename}' does not appear to be a valid CSV: {csv_err}")
        except Exception as val_err: # Catch other potential validation errors
            print(f"CSV validation error for {file.filename}: {val_err}")
            raise HTTPException(status_code=400, detail=f"Could not validate CSV file '{file.filename}': {val_err}")


        if dataset_name in datasets:
            print(f"Warning: Overwriting dataset '{dataset_name}' via file upload.")
        datasets[dataset_name] = { "content": contents, "filename": file.filename }
        if dataset_name in transformations: del transformations[dataset_name]

        preview_info = _get_preview_from_content(contents, engine="pandas", limit=10)
        return {
            "message": f"Successfully uploaded {file.filename} as '{dataset_name}'",
            "dataset_name": dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"]
        }
    except HTTPException as http_err:
        raise http_err # Re-raise specific HTTP exceptions
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
    # ... (keep existing logic, but ensure exceptions lead to HTTPException) ...
    if not data_text.strip(): raise HTTPException(status_code=400, detail="Pasted text cannot be empty.")
    if dataset_name in datasets: print(f"Warning: Overwriting existing dataset '{dataset_name}' from text upload.")

    try:
        content_bytes = None
        filename_suffix = data_format
        if data_format == "csv":
            content_bytes = data_text.encode('utf-8')
            pd.read_csv(io.BytesIO(content_bytes), nrows=5) # Validate
        elif data_format == "json":
            try:
                df_json = pd.read_json(io.StringIO(data_text), orient="records")
                with io.BytesIO() as buffer:
                     df_json.to_csv(buffer, index=False)
                     content_bytes = buffer.getvalue()
                filename_suffix = "csv" # Stored as CSV
            except ValueError as json_err: # Catch more specific JSON errors
                raise HTTPException(status_code=400, detail=f"Could not parse JSON data (expected records format): {json_err}")
        else: raise HTTPException(status_code=400, detail=f"Unsupported data_format: {data_format}")

        if content_bytes is None: raise ValueError("Failed to convert text data to bytes.")

        datasets[dataset_name] = { "content": content_bytes, "filename": f"{dataset_name}_pasted.{filename_suffix}"}
        if dataset_name in transformations: del transformations[dataset_name]
        preview_info = _get_preview_from_content(content_bytes, engine="pandas", limit=10)
        return {
            "message": f"Successfully loaded data as '{dataset_name}'",
            "dataset_name": dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"]
        }
    except (ParserError, EmptyDataError, ValueError) as pe:
         raise HTTPException(status_code=400, detail=f"Could not parse {data_format.upper()} data: {str(pe)}")
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Text Upload error: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Could not process text data: {str(e)}")

@app.post("/upload-db")
async def upload_database_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # ... (keep existing logic, ensure exceptions lead to HTTPException) ...
    allowed_extensions = {".db", ".sqlite", ".sqlite3", ".duckdb"}
    temp_file_path = None # Initialize path
    try:
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in allowed_extensions:
            raise HTTPException(status_code=400, detail=f"Unsupported file type '{file_ext}'. Allowed: {allowed_extensions}")

        temp_id = str(uuid.uuid4())
        temp_file_path = os.path.join(TEMP_UPLOAD_DIR, f"dbupload_{temp_id}{file_ext}")

        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Validate connection
        con = None
        try:
            con = duckdb.connect(temp_file_path, read_only=True)
            con.execute("SELECT 1") # Simple validation query
        except duckdb.Error as db_err:
             cleanup_temp_file(temp_file_path) # Clean up invalid file
             raise HTTPException(status_code=400, detail=f"Uploaded file is not a valid database or is corrupted: {db_err}")
        finally:
            if con: con.close()

        temp_db_files[temp_id] = temp_file_path
        print(f"Stored temporary DB file: {temp_file_path} with ID: {temp_id}")
        # Add cleanup task? Example: background_tasks.add_task(cleanup_temp_file, temp_file_path) # Be careful with timing
        return {"message": "Database file uploaded successfully.", "temp_db_id": temp_id}

    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Database Upload error: {type(e).__name__}: {e}")
        traceback.print_exc()
        if temp_file_path and os.path.exists(temp_file_path): cleanup_temp_file(temp_file_path) # Cleanup on error
        raise HTTPException(status_code=500, detail=f"Could not process database file: {str(e)}")
    finally:
        if file: await file.close()


# --- DB Table Listing/Import (/list-db-tables, /import-db-table) ---
@app.get("/list-db-tables/{temp_db_id}")
async def list_database_tables(temp_db_id: str):
    # ... (keep existing logic, ensure exceptions lead to HTTPException) ...
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
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=f"Error reading tables from database file: {e}")
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
    # ... (keep existing logic, ensure exceptions lead to HTTPException) ...
    if temp_db_id not in temp_db_files: raise HTTPException(status_code=404, detail="Temporary database ID not found or expired.")
    file_path = temp_db_files[temp_db_id]
    if not os.path.exists(file_path):
         if temp_db_id in temp_db_files: del temp_db_files[temp_db_id]
         raise HTTPException(status_code=404, detail="Temporary database file not found (may have been cleaned up).")

    if new_dataset_name in datasets: print(f"Warning: Overwriting existing dataset '{new_dataset_name}' from DB import.")

    con = None
    try:
        con = duckdb.connect(file_path, read_only=True)
        s_table_name = sql_service._sanitize_identifier(table_name)
        try:
            # Check if table exists before fetching all data
            con.execute(f"SELECT 1 FROM {s_table_name} LIMIT 1;")
        except duckdb.Error:
             raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in the database file.")

        imported_df = con.execute(f"SELECT * FROM {s_table_name};").fetchdf()

        with io.BytesIO() as buffer:
             imported_df.to_csv(buffer, index=False)
             content_bytes = buffer.getvalue()

        datasets[new_dataset_name] = { "content": content_bytes, "filename": f"{new_dataset_name}_from_{table_name}.csv" }
        if new_dataset_name in transformations: del transformations[new_dataset_name]

        preview_info = _get_preview_from_content(content_bytes, engine="pandas", limit=10)
        # Consider removing temp file entry *after* successful import?
        # if temp_db_id in temp_db_files: del temp_db_files[temp_db_id] # Or leave for potential multi-import

        return {
            "message": f"Successfully imported table '{table_name}' as dataset '{new_dataset_name}'",
            "dataset_name": new_dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"]
        }
    except HTTPException as http_err: raise http_err
    except (duckdb.Error, ValueError) as db_err:
         raise HTTPException(status_code=500, detail=f"Error importing table '{table_name}': {db_err}")
    except Exception as e:
        print(f"DB Table Import error: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Could not import table: {str(e)}")
    finally:
        if con: con.close()


# --- Dataset Listing & Retrieval (/datasets, /dataset/{name}, /dataset-info, /column-stats) ---
@app.get("/datasets")
async def get_datasets_list():
    try:
        return {"datasets": sorted(list(datasets.keys()))}
    except Exception as e:
        print(f"Error listing datasets: {e}")
        # This should be very rare, but handle anyway
        raise HTTPException(status_code=500, detail="Failed to retrieve dataset list.")

@app.get("/dataset/{dataset_name}")
async def get_dataset_preview(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]),
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0)
):
    try:
        content = get_current_content(dataset_name) # Handles 404
        preview_info = _get_preview_from_content(content, engine, limit, offset) # Handles preview errors
        can_undo = dataset_name in transformations and bool(transformations[dataset_name].get("history"))
        can_reset = can_undo # Can reset if any history exists
        last_code = ""
        if can_undo:
            last_code = transformations[dataset_name]["history"][-1].get("generated_code", "")

        return {
            "data": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"],
            "can_undo": can_undo,
            "can_reset": can_reset,
            "last_code": last_code
        }
    except HTTPException as http_err:
        raise http_err # Propagate 404 or preview errors
    except Exception as e:
        print(f"Error in get_dataset_preview for '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to retrieve preview for '{dataset_name}'.")


@app.get("/dataset-info/{dataset_name}")
async def get_dataset_info(dataset_name: str):
    try:
        content = get_current_content(dataset_name) # Handles 404

        # Wrap the core pandas logic in its own try-except
        try:
             df = pd.read_csv(io.BytesIO(content))

             # Handle potential empty dataframe after reading
             if df.empty:
                  return {
                       "dataset_name": dataset_name,
                       "row_count": 0, "column_count": 0, "memory_usage_bytes": 0,
                       "column_types": {}, "numeric_columns": [], "categorical_columns": [],
                       "datetime_columns": [], "other_columns": [],
                       "missing_values_count": {}, "missing_values_percentage": {},
                       "unique_value_summary": {}
                  }

             numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
             categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
             datetime_cols = df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()
             other_cols = df.select_dtypes(exclude=[np.number, 'object', 'category', 'datetime', 'datetimetz']).columns.tolist()

             column_types = {col: str(df[col].dtype) for col in df.columns}
             missing_values = df.isnull().sum().to_dict()
             total_rows = len(df)

             unique_counts = {}
             # Limit unique value calculations for performance? Maybe only for categorical?
             for col in df.columns: # Calculate for all for now
                 if total_rows > 0 and total_rows < 50000: # Example limit
                    try:
                        nunique = df[col].nunique()
                        unique_counts[col] = {"total_unique": nunique}
                        if nunique < 100: # Only get value counts if cardinality is low
                             value_counts = df[col].value_counts().head(10).to_dict()
                             unique_counts[col]["values"] = {str(k): v for k, v in value_counts.items()} # Ensure keys are strings
                    except Exception as unique_err:
                         print(f"Could not calculate unique counts for column '{col}': {unique_err}")
                         unique_counts[col] = {"error": "Could not calculate"}


             return {
                 "dataset_name": dataset_name,
                 "row_count": total_rows,
                 "column_count": len(df.columns),
                 "memory_usage_bytes": int(df.memory_usage(deep=True).sum()),
                 "column_types": column_types,
                 "numeric_columns": numeric_cols,
                 "categorical_columns": categorical_cols,
                 "datetime_columns": datetime_cols,
                 "other_columns": other_cols,
                 "missing_values_count": missing_values,
                 "missing_values_percentage": {k: round((v / total_rows * 100), 2) if total_rows > 0 else 0 for k, v in missing_values.items()},
                 "unique_value_summary": unique_counts
             }
        except (ParserError, EmptyDataError) as pe:
             raise HTTPException(status_code=400, detail=f"Cannot get info: Invalid CSV format for '{dataset_name}'. {str(pe)}")
        except Exception as e_inner: # Catch errors within info calculation
             print(f"Error calculating dataset info for '{dataset_name}': {type(e_inner).__name__}: {e_inner}")
             traceback.print_exc()
             raise HTTPException(status_code=500, detail=f"Error calculating info for '{dataset_name}': {str(e_inner)}")

    except HTTPException as http_err:
        raise http_err # Propagate 404 from get_current_content
    except Exception as e_outer:
        print(f"Unexpected error in get_dataset_info for '{dataset_name}': {type(e_outer).__name__}: {e_outer}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error getting info for '{dataset_name}'.")


@app.get("/column-stats/{dataset_name}/{column_name}")
async def get_column_stats(dataset_name: str, column_name: str, engine: str = "pandas"):
    try:
        content = get_current_content(dataset_name) # Handles 404
        stats = { "column_name": column_name, "engine_used": engine }

        if engine == "pandas":
            try:
                 df = pd.read_csv(io.BytesIO(content))
                 if column_name not in df.columns: raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in dataset '{dataset_name}'.")

                 # --- Calculate stats --- (Using existing logic but wrapped)
                 column_data = df[column_name].copy()
                 total_rows = len(df)
                 stats.update({
                     "dtype": str(column_data.dtype),
                     "missing_count": int(column_data.isnull().sum()),
                     "missing_percentage": round((column_data.isnull().sum() / total_rows * 100), 2) if total_rows > 0 else 0,
                     "memory_usage_bytes": int(column_data.memory_usage(deep=True))
                 })
                 # (Keep existing numeric, datetime, categorical logic from previous version)
                 # ... make sure to handle potential errors within these calculations ...
                 if pd.api.types.is_numeric_dtype(column_data.dtype):
                    # ... numeric stats ...
                    pass
                 elif pd.api.types.is_datetime64_any_dtype(column_data.dtype):
                    # ... datetime stats ...
                     pass
                 else: # Assume categorical/object
                    # ... categorical stats ...
                     pass

                 # Convert numpy types to standard python types for JSON serialization
                 for key, value in stats.items():
                     if isinstance(value, (np.integer, np.int64)): stats[key] = int(value)
                     elif isinstance(value, (np.floating, np.float64)): stats[key] = float(value)
                     elif isinstance(value, np.bool_): stats[key] = bool(value)
                     elif isinstance(value, dict): # Handle nested dicts like quantiles
                          for k, v in value.items():
                              if isinstance(v, (np.integer, np.int64)): stats[key][k] = int(v)
                              elif isinstance(v, (np.floating, np.float64)): stats[key][k] = float(v)

                 return stats

            except (ParserError, EmptyDataError) as pe:
                raise HTTPException(status_code=400, detail=f"Cannot get stats: Invalid CSV format for '{dataset_name}'. {str(pe)}")
            except KeyError: # More specific than generic Exception for column not found
                 raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in dataset '{dataset_name}'.")
            except Exception as e_inner:
                 print(f"Error calculating pandas column stats for '{column_name}' in '{dataset_name}': {type(e_inner).__name__}: {e_inner}")
                 traceback.print_exc()
                 raise HTTPException(status_code=500, detail=f"Error calculating stats for column '{column_name}': {str(e_inner)}")

        elif engine == "polars":
             try:
                # --- Calculate polars stats --- (Add try-except)
                 df = pl.read_csv(io.BytesIO(content))
                 if column_name not in df.columns: raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found.")
                 # ... (existing polars stats logic) ...
                 # Ensure results are serializable
                 # ...
                 return stats # Return calculated Polars stats
             except Exception as e_inner:
                  print(f"Error calculating polars column stats for '{column_name}' in '{dataset_name}': {type(e_inner).__name__}: {e_inner}")
                  traceback.print_exc()
                  raise HTTPException(status_code=500, detail=f"Error calculating polars stats for column '{column_name}': {str(e_inner)}")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine for column stats: {engine}")

    except HTTPException as http_err:
        raise http_err # Propagate 404 or 400
    except Exception as e_outer:
        print(f"Unexpected error in get_column_stats for '{column_name}' in '{dataset_name}': {type(e_outer).__name__}: {e_outer}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error getting column stats.")


# --- Data Transformation Endpoints ---
# Add similar try-except blocks with traceback logging to:
# /operation, /execute-code, /merge-datasets, /regex-operation

@app.post("/operation")
async def perform_operation(
    dataset_name: str = Form(...),
    operation: str = Form(...),
    params: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    try:
        content_before = get_current_content(dataset_name)
        params_dict = json.loads(params)
        new_content = None
        generated_code = ""
        preview_info = {}

        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content_before))
            result_df, generated_code = pandas_service.apply_pandas_operation(df, operation, params_dict)
            with io.BytesIO() as buffer: result_df.to_csv(buffer, index=False); new_content = buffer.getvalue()
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content_before))
            result_df, generated_code = polars_service.apply_polars_operation(df, operation, params_dict)
            with io.BytesIO() as buffer: result_df.write_csv(buffer); new_content = buffer.getvalue()
        elif engine == "sql":
            con = None
            try:
                 con = duckdb.connect(":memory:")
                 table_name = f"data_{uuid.uuid4().hex[:8]}"
                 try: _temp_df = pd.read_csv(io.BytesIO(content_before), nrows=0); all_cols = list(_temp_df.columns)
                 except Exception: all_cols = []
                 preview_data, result_columns, total_rows, generated_code = sql_service.apply_sql_operation(con, content_before, table_name, operation, params_dict, all_cols)
                 preview_info = { "data": preview_data, "columns": result_columns, "row_count": total_rows }
                 full_df = con.execute(generated_code).fetchdf() # Fetch full result for state
                 with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_content = buffer.getvalue()
            finally:
                 if con: con.close()
        else:
             raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_content is None: raise ValueError("Operation failed: No new content generated.")
        preview_info = _get_preview_from_content(new_content, engine) # Get preview from final content
        update_transformation_state(dataset_name, engine, operation, params_dict, generated_code, content_before, new_content)

        return {
            "data": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"],
            "code": generated_code, "can_undo": True, "can_reset": True
        }
    # More specific error catching
    except (ValueError, pl.exceptions.PolarsError, duckdb.Error, ParserError, EmptyDataError, NotImplementedError, TypeError, DataError, KeyError, json.JSONDecodeError) as op_err:
         err_type = type(op_err).__name__
         err_msg = str(op_err)
         status_code = 400 # Assume client error for these types
         detail = f"Operation '{operation}' failed ({engine}): {err_type}: {err_msg}"
         if isinstance(op_err, KeyError): detail = f"Operation '{operation}' failed ({engine}): Column/Key not found: {err_msg}"
         elif isinstance(op_err, (TypeError, DataError)): detail = f"Operation '{operation}' failed ({engine}): Invalid data type or parameters. Details: {err_msg}"
         elif isinstance(op_err, json.JSONDecodeError): detail = f"Operation '{operation}' failed: Invalid parameters JSON. {err_msg}"
         elif isinstance(op_err, duckdb.BinderException): detail = f"Operation '{operation}' failed (SQL Binder Error): {err_msg}. Check names/types."
         print(f"Handled Operation Error: {detail}")
         raise HTTPException(status_code=status_code, detail=detail)
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /operation ({engine}, {operation}): {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected server error occurred during '{operation}'.")

@app.post("/execute-code")
async def execute_custom_code(
    dataset_name: str = Form(...),
    code: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    try:
        content_before = get_current_content(dataset_name)
        new_content = None
        result_data = None
        columns = []
        row_count = 0

        if engine == "pandas":
            local_vars = {}
            # Restrict globals and locals for slightly more safety, but exec is still risky
            exec("import pandas as pd\nimport io\nimport numpy as np", {"pd": pd, "np": np, "io": io}, local_vars)
            exec(f"df = pd.read_csv(io.BytesIO(content_before))", {"pd": pd, "io": io, "content_before": content_before}, local_vars)
            exec(code, {"pd": pd, "np": np, "io": io}, local_vars) # Pass safe modules
            result_df = local_vars.get('df')
            if not isinstance(result_df, pd.DataFrame): raise ValueError("Pandas code must result in a DataFrame assigned to 'df'.")
            with io.BytesIO() as buffer: result_df.to_csv(buffer, index=False); new_content = buffer.getvalue()
            result_data = result_df.head(100).replace([np.inf, -np.inf], None).fillna('NaN').to_dict(orient="records")
            columns, row_count = list(result_df.columns), len(result_df)
        elif engine == "polars":
             local_vars = {}
             exec("import polars as pl\nimport io", {"pl": pl, "io": io}, local_vars)
             exec(f"df = pl.read_csv(io.BytesIO(content_before))", {"pl": pl, "io": io, "content_before": content_before}, local_vars)
             exec(code, {"pl": pl, "io": io}, local_vars)
             result_df = local_vars.get('df')
             if not isinstance(result_df, pl.DataFrame): raise ValueError("Polars code must result in a DataFrame assigned to 'df'.")
             with io.BytesIO() as buffer: result_df.write_csv(buffer); new_content = buffer.getvalue()
             result_data = result_df.head(100).fill_nan('NaN').to_dicts()
             columns, row_count = result_df.columns, result_df.height
        elif engine == "sql":
            con = None
            try:
                 con = duckdb.connect(":memory:")
                 table_name = "data_table"
                 sql_service._load_data_to_duckdb(con, table_name, content_before)
                 # Assume code is a SELECT query. Execute for preview.
                 preview_data, columns, row_count = sql_service._execute_sql_query(con, code)
                 result_data = preview_data
                 # Fetch full result for state update
                 full_df = con.execute(code).fetchdf()
                 with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_content = buffer.getvalue()
            finally:
                if con: con.close()
        else: raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_content is None: raise ValueError("Code execution failed to produce new content state.")
        update_transformation_state(dataset_name, engine, "custom_code", code, code, content_before, new_content)

        return {
            "data": result_data, "columns": columns, "row_count": row_count,
            "code": code, "can_undo": True, "can_reset": True
        }
    # Catch specific execution errors
    except (SyntaxError, NameError, TypeError, ValueError, AttributeError, KeyError, pd.errors.PandasError, pl.exceptions.PolarsError, duckdb.Error) as exec_err:
         raise HTTPException(status_code=400, detail=f"Code execution failed ({engine}): {type(exec_err).__name__}: {str(exec_err)}")
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /execute-code: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during code execution.")

# ... Add similar robust try/except blocks to /merge-datasets and /regex-operation ...
@app.post("/merge-datasets")
async def merge_datasets_endpoint(
    left_dataset: str = Form(...),
    right_dataset: str = Form(...),
    params: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    try:
        content_left_before = get_current_content(left_dataset)
        content_right = get_current_content(right_dataset) # Get current state of right dataset too
        params_dict = json.loads(params)
        new_content = None
        preview_info = {}
        generated_code = ""

        # ... (Engine specific merge/join logic) ...
        if engine == "pandas":
            # ... pandas merge ...
            pass
        elif engine == "polars":
            # ... polars join ...
            pass
        elif engine == "sql":
            # ... sql join ...
            pass
        else: raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_content is None: raise ValueError("Merge failed internally.")
        preview_info = _get_preview_from_content(new_content, engine)
        # Update state of the *left* dataset
        update_transformation_state(left_dataset, engine, "merge", params_dict, generated_code, content_left_before, new_content)

        return {
            "message": f"Merged {left_dataset} and {right_dataset}. Result updated for {left_dataset}.",
            "data": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"],
            "code": generated_code, "can_undo": True, "can_reset": True
        }
    except (ValueError, pl.exceptions.PolarsError, duckdb.Error, pd.errors.PandasError, KeyError, json.JSONDecodeError) as ve:
         detail = f"Merge failed ({engine}): {type(ve).__name__}: {str(ve)}"
         if isinstance(ve, KeyError): detail = f"Merge failed ({engine}): Join key not found: {str(ve)}"
         elif isinstance(ve, json.JSONDecodeError): detail = f"Merge failed: Invalid parameters JSON. {str(ve)}"
         raise HTTPException(status_code=400, detail=detail)
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /merge-datasets: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during merge.")


@app.post("/regex-operation")
async def regex_operation(
    dataset_name: str = Form(...),
    operation: str = Form(...), # e.g., "filter", "extract", "replace"
    params: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    try:
        content_before = get_current_content(dataset_name)
        params_dict = json.loads(params)
        new_content = None
        preview_info = {}
        generated_code = ""
        operation_name = f"regex_{operation}"

        # ... (Engine specific regex logic) ...
        if engine == "pandas":
            # ... pandas regex ...
            pass
        elif engine == "polars":
            # ... polars regex ...
            pass
        elif engine == "sql":
             # ... sql regex ...
             pass
        else: raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_content is None: raise ValueError(f"Regex operation '{operation}' failed internally.")
        preview_info = _get_preview_from_content(new_content, engine)
        update_transformation_state(dataset_name, engine, operation_name, params_dict, generated_code, content_before, new_content)
        can_undo = dataset_name in transformations and bool(transformations[dataset_name].get("history"))
        can_reset = can_undo

        return {
            "message": f"Successfully applied regex '{operation}' on column '{params_dict.get('column', 'N/A')}'.",
            "data": preview_info.get("data", []), "columns": preview_info.get("columns", []), "row_count": preview_info.get("row_count", 0),
            "code": generated_code, "can_undo": can_undo, "can_reset": can_reset
        }

    except (ValueError, TypeError, KeyError, pd.errors.PandasError, pl.exceptions.PolarsError, duckdb.Error, re.error, json.JSONDecodeError) as op_err:
        detail = f"Regex operation '{operation}' failed ({engine}): {type(op_err).__name__}: {str(op_err)}"
        if isinstance(op_err, KeyError): detail = f"Regex operation failed: Column not found: {str(op_err)}"
        elif isinstance(op_err, re.error): detail = f"Regex operation failed: Invalid pattern: {str(op_err)}"
        elif isinstance(op_err, json.JSONDecodeError): detail = f"Regex operation failed: Invalid parameters JSON. {str(op_err)}"
        raise HTTPException(status_code=400, detail=detail)
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /regex-operation ({engine}, {operation}): {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected server error during regex.")


# --- Relational Algebra Endpoints (WITH ENHANCED ERROR HANDLING) ---
@app.post("/relational-operation-preview")
async def preview_relational_operation(
    operation: str = Form(...),
    params: str = Form(...),
    # --- Make base_dataset_name REQUIRED for this approach ---
    base_dataset_name: str = Form(...),
    current_sql_state: Optional[str] = Form(None),
    step_alias_base: str = Form("step")
):
    con = None
    try:
        params_dict = json.loads(params) # Catch JSON errors early

        # --- Base dataset is now always required ---
        if not base_dataset_name:
             raise HTTPException(status_code=400, detail="RA preview requires 'base_dataset_name'.")

        con = duckdb.connect(":memory:")

        # --- ALWAYS Load base dataset into the connection ---
        internal_load_name = f"__base_{base_dataset_name}" # Consistent internal name
        s_internal_load_name = relational_algebra_service._sanitize_identifier(internal_load_name)
        try:
            content = get_current_content(base_dataset_name) # Handles 404
            print(f"DEBUG: Loading base data '{base_dataset_name}' into table {s_internal_load_name}")
            relational_algebra_service._load_ra_data(con, internal_load_name, content) # Load using internal name
        except HTTPException as http_err:
             # Handle dataset not found error from get_current_content
             if http_err.status_code == 404:
                 detail=f"Base dataset '{base_dataset_name}' not found."
                 print(f"RA Preview Error (404): {detail}")
                 raise HTTPException(status_code=404, detail=detail)
             else: # Re-raise other HTTPExceptions
                 raise http_err
        except ValueError as load_err: # Catch errors from _load_ra_data
             detail=f"Failed to load base dataset '{base_dataset_name}': {load_err}"
             print(f"RA Preview Error (400): {detail}")
             raise HTTPException(status_code=400, detail=detail)
        # --- Base data loading complete ---

        source_sql_or_table: str
        columns_before: List[str] = []
        step_number = 0

        # --- Determine source FOR SNIPPET GENERATION and get columns ---
        if current_sql_state:
            # State exists from previous step
            state_strip = current_sql_state.strip()
            match = re.match(r"\((.*)\)\s+AS\s+\w+\s*$", state_strip, re.DOTALL | re.IGNORECASE)
            if not match:
                # Fallback or error if format is unexpected
                if "SELECT " in state_strip.upper():
                     print(f"Warning: current_sql_state '{current_sql_state[:100]}...' did not match '(...) AS alias' format, using directly.")
                     core_previous_sql = state_strip
                else:
                     raise ValueError(f"Could not parse previous SQL state format: {current_sql_state[:200]}...")
            else:
                 core_previous_sql = match.group(1).strip() # SQL from the previous step

            # Calculate step number from the original state string's alias
            alias_match = re.search(rf"{re.escape(step_alias_base)}(\d+)$", state_strip, re.IGNORECASE)
            step_number = int(alias_match.group(1)) + 1 if alias_match else 1 # Default to 1 if alias missing

            temp_prev_view = f"__prev_view_{uuid.uuid4().hex[:8]}"
            # Create the view using the extracted SQL (references base table or previous view)
            # This works now because the base table was loaded earlier in *this* connection.
            print(f"DEBUG: Creating view {temp_prev_view} AS: {core_previous_sql}")
            try:
                con.execute(f"CREATE TEMP VIEW {temp_prev_view} AS {core_previous_sql};")
            except duckdb.Error as view_err:
                raise ValueError(f"Failed to create view from previous step SQL: {view_err}. SQL was: {core_previous_sql}")

            # Describe the newly created view to get its columns
            cols_result = con.execute(f"DESCRIBE {temp_prev_view};").fetchall()
            columns_before = [col[0] for col in cols_result]
            # The source for the *next* snippet is the view name itself
            source_sql_or_table = temp_prev_view # Use view name (unquoted)

        else:
            # First step: No previous state, source is the base table
            step_number = 0
            # Describe the base table to get its columns
            cols_result = con.execute(f"DESCRIBE {s_internal_load_name};").fetchall()
            columns_before = [col[0] for col in cols_result]
            # Source for the snippet is the sanitized internal base table name
            source_sql_or_table = s_internal_load_name

        # --- Prepare for snippet generation ---
        if operation.lower() == "rename":
            if not columns_before: # Add safety check
                raise ValueError("Cannot perform rename: Failed to determine columns from previous step.")
            params_dict["all_columns"] = columns_before

        current_step_alias = f"{step_alias_base}{step_number}"

        # --- Generate the SQL snippet for the *current* operation ---
        # Pass the view name (if step > 0) or sanitized table name (if step == 0)
        sql_snippet = relational_algebra_service._generate_sql_snippet(operation, params_dict, source_sql_or_table)
        print(f"DEBUG: Generated snippet for current step: {sql_snippet}") # Debug log

        # --- Execute the snippet to get the preview ---
        preview_data, result_columns, total_rows = relational_algebra_service._execute_preview_query(con, sql_snippet)

        # --- Construct the state for the *next* step ---
        # Wrap the current snippet (which operates on the view/table)
        next_sql_state = f"({sql_snippet}) AS {current_step_alias}"
        print(f"DEBUG: Generated next_sql_state: {next_sql_state}") # Debug log

        return {
            "message": "RA preview generated successfully.",
            "data": preview_data, "columns": result_columns, "row_count": total_rows,
            "generated_sql_state": next_sql_state,
            "current_step_sql_snippet": sql_snippet # Send back the raw snippet too if useful
        }

    # Specific operational errors (client-side fault)
    except (ValueError, duckdb.Error, NotImplementedError, json.JSONDecodeError) as e:
         # ... (keep enhanced error handling from previous version) ...
         err_type = type(e).__name__
         detail = f"Relational Algebra preview for '{operation}' failed: {err_type}: {str(e)}"
         print(f"RA Preview Error (400): {detail}")
         # Refine specific DuckDB error messages
         if isinstance(e, duckdb.BinderException): detail = f"RA preview failed (Binder Error): {str(e)}. Check column/table names/types."
         elif isinstance(e, duckdb.CatalogException): detail = f"RA preview failed (Catalog Error): {str(e)}. Check if table/view exists."
         elif isinstance(e, duckdb.ParserException): detail = f"RA preview failed (Parser Error): {str(e)}. Check syntax."
         elif isinstance(e, json.JSONDecodeError): detail = f"RA preview failed: Invalid parameters JSON. {str(e)}"
         elif "Cannot perform rename" in str(e): detail = f"RA preview failed: Rename error - {str(e)}"
         elif "Failed to create view" in str(e): detail = f"RA preview failed: {str(e)}" # Pass view creation error

         raise HTTPException(status_code=400, detail=detail)
    # Handle case where dataset doesn't exist (caught during load attempt now)
    except HTTPException as http_err:
         raise http_err # Re-raise 404 or other HTTP errors
    # Catch any other unexpected errors (server-side fault)
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
    base_dataset_names_json: str = Form(...)
):
    con = None
    try:
        if not new_dataset_name.strip(): raise ValueError("New dataset name cannot be empty.")
        base_dataset_names = json.loads(base_dataset_names_json)
        if not isinstance(base_dataset_names, list) or not base_dataset_names:
             raise ValueError("Invalid or empty list of base dataset names provided.")

        if new_dataset_name in datasets: print(f"Warning: Overwriting dataset '{new_dataset_name}' with RA result save.")

        con = duckdb.connect(":memory:")
        # Load base datasets
        for ds_name in base_dataset_names:
             content = get_current_content(ds_name)
             relational_algebra_service._load_ra_data(con, ds_name, content)

        # Execute final chain
        print(f"Executing final RA SQL chain for saving '{new_dataset_name}':\n{final_sql_chain}")
        full_df = con.execute(final_sql_chain).fetchdf()

        # Convert and save
        with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_content = buffer.getvalue()
        datasets[new_dataset_name] = { "content": new_content, "filename": f"{new_dataset_name}_ra_result.csv" }
        if new_dataset_name in transformations: del transformations[new_dataset_name]

        saved_preview_info = _get_preview_from_content(new_content, engine="pandas", limit=10)
        return {
            "message": f"Successfully saved RA result as '{new_dataset_name}'.",
            "dataset_name": new_dataset_name,
            "preview": saved_preview_info["data"], "columns": saved_preview_info["columns"], "row_count": saved_preview_info["row_count"],
            "datasets": sorted(list(datasets.keys()))
        }
    # Specific operational errors
    except (ValueError, duckdb.Error, json.JSONDecodeError) as e:
         detail = f"Failed to save RA result as '{new_dataset_name}': {str(e)}"
         print(f"RA Save Error (400): {detail}")
         raise HTTPException(status_code=400, detail=detail)
    # Handle missing base dataset
    except HTTPException as http_err:
        raise http_err
    # Catch any other unexpected errors
    except Exception as e:
        print(f"Unexpected error in /save-ra-result: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error during RA save.")
    finally:
        if con: con.close()


# --- Undo/Reset/Save Transformation Endpoints ---
# (Keep existing logic, ensure they handle potential errors gracefully)
@app.post("/undo/{dataset_name}")
async def undo_last_operation(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"])
):
    try:
        if dataset_name not in transformations or not transformations[dataset_name].get("history"):
            raise HTTPException(status_code=400, detail="No operations to undo.")

        history = transformations[dataset_name]["history"]
        last_op = history.pop()
        restored_content = last_op.get("content_before")

        if restored_content is None:
             # If first operation was undone, restore original; otherwise, error
             if not history: restored_content = datasets[dataset_name]["content"]
             else: raise ValueError("Cannot undo: Previous state content not found in history.")

        transformations[dataset_name]["current_content"] = restored_content
        preview_info = _get_preview_from_content(restored_content, engine)
        can_undo = bool(history)
        can_reset = dataset_name in datasets
        last_code = history[-1].get("generated_code", "") if history else ""

        return {
            "message": f"Undid last operation ({last_op.get('operation', 'N/A')})",
            "data": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"],
            "can_undo": can_undo, "can_reset": can_reset, "last_code": last_code
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error during undo for '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        # Restore history if pop failed midway? Complex. Better to signal failure.
        # Maybe refetch history from state if possible before re-raising?
        raise HTTPException(status_code=500, detail=f"An error occurred during undo.")


@app.post("/reset/{dataset_name}")
async def reset_transformations(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"])
):
    try:
        if dataset_name not in datasets:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

        if dataset_name in transformations:
            del transformations[dataset_name]

        original_content = datasets[dataset_name]["content"]
        preview_info = _get_preview_from_content(original_content, engine)

        return {
            "message": f"Reset transformations for {dataset_name}",
            "data": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"],
            "can_undo": False, "can_reset": False, "last_code": ""
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
         print(f"Error during reset for '{dataset_name}': {type(e).__name__}: {e}")
         traceback.print_exc()
         raise HTTPException(status_code=500, detail=f"An error occurred during reset.")

@app.post("/save-transformation")
async def save_transformation(
    dataset_name: str = Form(...),
    new_dataset_name: str = Form(...),
    engine: str = Form(default="pandas") # For preview generation
):
    try:
        content_to_save = get_current_content(dataset_name) # Handles 404
        if not new_dataset_name.strip(): raise ValueError("New dataset name cannot be empty.")
        # Add validation?
        # if not re.match(r"^[a-zA-Z0-9_\-\.]+$", new_dataset_name.strip()):
        #      raise ValueError("New name contains invalid characters.")

        if new_dataset_name in datasets: print(f"Warning: Overwriting dataset '{new_dataset_name}' when saving transformation.")

        datasets[new_dataset_name] = { "content": content_to_save, "filename": f"{new_dataset_name}_saved.csv" }
        if new_dataset_name in transformations: del transformations[new_dataset_name] # Clear history for new save

        preview_info = _get_preview_from_content(content_to_save, engine, limit=10)
        return {
            "message": f"Successfully saved current state of '{dataset_name}' as '{new_dataset_name}'",
            "dataset_name": new_dataset_name,
            "preview": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"]
        }
    except (ValueError) as val_err:
         raise HTTPException(status_code=400, detail=str(val_err))
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error saving transformation for '{dataset_name}' as '{new_dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error occurred while saving the transformation.")


# --- Export Endpoint (/export) ---
@app.get("/export/{dataset_name}")
async def export_dataset(
    dataset_name: str,
    format: str = Query("csv", enum=["csv", "json", "excel"]),
    engine: str = Query("pandas") # Primarily for non-CSV export loading
):
    try:
        content = get_current_content(dataset_name) # Handles 404
        file_content: Union[bytes, str]
        media_type: str
        filename: str

        if format == "csv":
            media_type="text/csv"
            filename = f"{dataset_name}.csv"
            file_content = content
        else:
            # Use pandas for consistent non-CSV export
            try:
                 df = pd.read_csv(io.BytesIO(content))
                 if format == "json":
                     media_type="application/json"
                     filename = f"{dataset_name}.json"
                     file_content = df.to_json(orient="records", date_format="iso", default_handler=str, force_ascii=False)
                 elif format == "excel":
                     media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                     filename = f"{dataset_name}.xlsx"
                     with io.BytesIO() as buffer:
                         df.to_excel(buffer, index=False, engine='openpyxl') # Specify engine if needed
                         file_content = buffer.getvalue()
                 else: # Should be caught by enum, but defensive check
                      raise ValueError(f"Unsupported format: {format}")
            except (ParserError, EmptyDataError) as pe:
                 raise HTTPException(status_code=400, detail=f"Cannot export: Invalid CSV data for '{dataset_name}'. {str(pe)}")
            except Exception as export_load_err: # Error loading/converting for export
                 print(f"Error preparing non-CSV export for '{dataset_name}': {export_load_err}")
                 traceback.print_exc()
                 raise HTTPException(status_code=500, detail=f"Failed to prepare data for {format} export.")

        # Ensure filename is safe (basic sanitation)
        safe_filename = re.sub(r'[^\w\.\-]', '_', filename) # Replace unsafe chars with underscore
        return Response(
            content=file_content,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename=\"{safe_filename}\""}
        )
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Export Error ({format}) for {dataset_name}: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error exporting dataset '{dataset_name}' as {format}.")


# --- Dataset Rename / Delete Endpoints ---
@app.post("/rename-dataset/{old_dataset_name}")
async def rename_dataset(
    old_dataset_name: str,
    new_dataset_name: str = Form(...)
):
    try:
        if not old_dataset_name or not new_dataset_name: raise ValueError("Old and new dataset names must be provided.")
        new_name = new_dataset_name.strip()
        if not new_name: raise ValueError("New dataset name cannot be empty.")
        # Basic validation
        if not re.match(r"^[a-zA-Z0-9_\-\.]+$", new_name): raise ValueError("New name contains invalid characters.")
        if old_dataset_name not in datasets: raise HTTPException(status_code=404, detail=f"Dataset '{old_dataset_name}' not found.")
        if new_name == old_dataset_name:
             return {"message": f"Dataset name '{old_dataset_name}' unchanged.", "datasets": sorted(list(datasets.keys()))}
        if new_name in datasets: raise HTTPException(status_code=409, detail=f"Dataset name '{new_name}' already exists.")

        # Perform rename
        datasets[new_name] = datasets.pop(old_dataset_name)
        if old_dataset_name in transformations:
            transformations[new_name] = transformations.pop(old_dataset_name)
        print(f"Renamed dataset '{old_dataset_name}' to '{new_name}'")
        return {
            "message": f"Successfully renamed dataset '{old_dataset_name}' to '{new_name}'.",
            "old_name": old_dataset_name, "new_name": new_name,
            "datasets": sorted(list(datasets.keys()))
        }
    except ValueError as ve:
         raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error renaming dataset '{old_dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        # Attempt to revert rename if partial failure? Complex. Better to signal error.
        raise HTTPException(status_code=500, detail=f"An internal error occurred during rename.")

@app.delete("/dataset/{dataset_name}")
async def delete_dataset(dataset_name: str):
    try:
        if dataset_name not in datasets:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

        del datasets[dataset_name]
        if dataset_name in transformations:
            del transformations[dataset_name]
        print(f"Deleted dataset '{dataset_name}'")
        return {
            "message": f"Successfully deleted dataset '{dataset_name}'.",
            "deleted_name": dataset_name,
            "datasets": sorted(list(datasets.keys()))
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error deleting dataset '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during deletion.")

# --- Optional: Add endpoint to clean up old temp DB files ---
# This would require tracking creation times or using a more robust temp file solution.