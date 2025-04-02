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
# Stores the original uploaded datasets
datasets: Dict[str, Dict[str, Any]] = {}

# Stores the current transformed state and history for undo
# Key: dataset_name
transformations: Dict[str, Dict[str, Any]] = {}

# Stores paths to temporary DB files for import process
temp_db_files: Dict[str, str] = {}

# --- Helper Functions ---

def _get_load_code(dataset_name: str, engine: str, content: bytes) -> str:
    """Generates the initial code string for loading the data."""
    # For SQL, we need a table name convention. Let's use the dataset name.
    # For Pandas/Polars, it's a standard read_csv.
    safe_name = re.sub(r'\W|^(?=\d)', '_', dataset_name) # Basic sanitization for variable/table name
    if engine == "pandas":
        # Represent loading from an in-memory object for reproducibility
        # In a real script, this would be pd.read_csv('filename.csv')
        # For display, let's show a conceptual load
        return f"# Load original data for '{dataset_name}'\n# df = pd.read_csv(...)\n# (Initial state loaded)"
    elif engine == "polars":
        return f"# Load original data for '{dataset_name}'\n# df = pl.read_csv(...)\n# (Initial state loaded)"
    elif engine == "sql":
        # The initial 'code' is just selecting from the base table loaded into DuckDB
        sanitized_table_name = sql_service._sanitize_identifier(safe_name)
        return f"-- Load original data for '{dataset_name}' into table {sanitized_table_name}\nSELECT * FROM {sanitized_table_name}"
    else:
        return f"# Load original data for '{dataset_name}'\n# (Initial state loaded)"

def _build_full_code(prev_full_code: str, code_snippet: str, engine: str) -> str:
    """Appends a new code snippet to the previous full code chain."""
    if not prev_full_code: # Should have load code if starting
        return code_snippet # Should not happen ideally

    if engine == "pandas" or engine == "polars":
        # Simple append for script-like code
        return f"{prev_full_code}\n{code_snippet}"
    elif engine == "sql":
        # SQL uses CTE chaining, the 'snippet' is often the definition of the next CTE
        # The sql_service now returns the *new full chain* directly.
        # So, this function might just return the snippet if it's the full chain.
        # Let's adjust: sql_service returns the *new full chain*, so we just use that.
        return code_snippet # Assuming code_snippet *is* the new full SQL chain
    else:
        return f"{prev_full_code}\n# Operation: \n{code_snippet}"


def get_current_state(dataset_name: str) -> Optional[Dict[str, Any]]:
    """Gets the latest transformation state or None if only original exists."""
    return transformations.get(dataset_name)

def get_current_content(dataset_name: str) -> bytes:
    """Gets the latest content bytes (original or transformed)."""
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")

    current_state = get_current_state(dataset_name)
    if current_state:
        return current_state["current_content"]
    else:
        # No transformations yet, return original content
        return datasets[dataset_name]["content"]

def get_current_full_code(dataset_name: str, engine: str) -> str:
    """Gets the latest full code chain for the dataset and engine."""
    current_state = get_current_state(dataset_name)
    if current_state:
        # If engine matches, return stored code. If not, return base load code for the new engine.
        if current_state["current_engine"] == engine:
            return current_state["current_full_code"]
        else:
            # Engine switch: Start new code chain from current content
            # Note: content here is the *result* of the previous engine's ops
            content = current_state["current_content"]
            return _get_load_code(dataset_name, engine, content)
    else:
        # No transformations yet, return base load code for the requested engine
        if dataset_name in datasets:
             content = datasets[dataset_name]["content"]
             return _get_load_code(dataset_name, engine, content)
        else:
             return f"# Dataset '{dataset_name}' not found"


def update_transformation_state(dataset_name: str, engine: str, operation: str, params_or_code: Any, new_full_code: str, new_content: bytes):
    """Updates the transformation state, handling history and engine switches."""
    if dataset_name not in datasets:
        print(f"Error: update_transformation_state called for non-existent dataset '{dataset_name}'")
        return

    # Get previous state or initialize based on original dataset
    prev_state = get_current_state(dataset_name)
    original_content = datasets[dataset_name]["content"]

    # Determine state before this operation for history snapshot
    content_before_op = prev_state["current_content"] if prev_state else original_content
    full_code_before_op = prev_state["current_full_code"] if prev_state else _get_load_code(dataset_name, engine, original_content) # Use target engine for initial load code
    engine_before_op = prev_state["current_engine"] if prev_state else engine # Assume starting with the current engine

    history = prev_state["history"] if prev_state else []

    # --- Engine Switch Logic ---
    if prev_state and engine != engine_before_op:
        print(f"Engine switched from {engine_before_op} to {engine}. Resetting transformation history for {dataset_name}.")
        # The new chain starts from the content resulting from the previous engine's operations.
        # The 'full_code_before_op' for the *new* engine chain is just the load statement.
        full_code_before_op = _get_load_code(dataset_name, engine, content_before_op)
        # Clear history as it's not relevant to the new engine chain
        history = []
        # The new_full_code received should be based on this initial load for the new engine
        # (The caller endpoint needs to handle passing the correct initial code to the service on switch)
    elif prev_state:
         # Append the state *before* this operation to history for undo
         history.append({
             "current_content": content_before_op,
             "current_full_code": full_code_before_op,
             "current_engine": engine_before_op,
             # Store op details that led *to* the new state (optional but useful)
             "operation_applied": operation,
             "engine_applied": engine,
         })

    # Update the main transformation state dictionary
    transformations[dataset_name] = {
        "current_content": new_content,
        "current_full_code": new_full_code, # This is the full code *after* the operation
        "current_engine": engine,
        "history": history
    }
    print(f"State updated for {dataset_name}. History length: {len(history)}")
    # print(f"New Full Code:\n{new_full_code}") # Debug


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
             # SQL preview now needs to handle potentially complex CTE chains
             # The content bytes represent the *result* of the last SQL op.
             # We can load *this result* into a temp table for simple preview.
             con = None
             try:
                 con = duckdb.connect(":memory:")
                 # Load the *current result content* into a temporary table for previewing
                 # Use a unique name to avoid clashes if multiple previews happen concurrently
                 preview_table_name = f"__preview_{uuid.uuid4().hex[:8]}"
                 sql_service._load_data_to_duckdb(con, preview_table_name, content)

                 # Query the temporary preview table
                 count_query = f"SELECT COUNT(*) FROM {sql_service._sanitize_identifier(preview_table_name)}"
                 total_rows = con.execute(count_query).fetchone()[0]

                 preview_query = f"SELECT * FROM {sql_service._sanitize_identifier(preview_table_name)} LIMIT {limit} OFFSET {offset}"
                 preview_result = con.execute(preview_query)
                 columns = [desc[0] for desc in preview_result.description]
                 data_dicts = preview_result.fetch_arrow_table().to_pylist()
                 # Add JSON serialization handling
                 for row in data_dicts:
                     for col, val in row.items():
                         if hasattr(val, 'isoformat'): row[col] = val.isoformat()
                         elif isinstance(val, (np.integer, np.int64)): row[col] = int(val)
                         elif isinstance(val, (np.floating, np.float64)): row[col] = float(val) if not np.isnan(val) else 'NaN'
                         elif isinstance(val, np.bool_): row[col] = bool(val)


                 return { "data": data_dicts, "columns": columns, "row_count": total_rows }
             except Exception as sql_preview_err:
                  print(f"Error generating SQL preview from result content: {sql_preview_err}")
                  # Fallback to pandas preview of the result content
                  df = pd.read_csv(io.BytesIO(content))
                  return {
                      "data": df.iloc[offset:offset+limit].replace([np.inf, -np.inf], None).fillna('NaN').to_dict(orient="records"),
                      "columns": list(df.columns),
                      "row_count": len(df)
                  }
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
# These endpoints initialize the entry in `datasets` and clear any existing transformations.
@app.post("/upload")
async def upload_file(file: UploadFile = File(...), dataset_name: str = Form(...)):
    # ... (validation logic remains the same) ...
    try:
        contents = await file.read()
        if not contents: raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        try: pd.read_csv(io.BytesIO(contents), nrows=5)
        except (ParserError, EmptyDataError) as csv_err: raise HTTPException(status_code=400, detail=f"File '{file.filename}' does not appear to be a valid CSV: {csv_err}")
        except Exception as val_err: raise HTTPException(status_code=400, detail=f"Could not validate CSV file '{file.filename}': {val_err}")

        if dataset_name in datasets: print(f"Warning: Overwriting dataset '{dataset_name}' via file upload.")

        # Store in primary dataset registry
        datasets[dataset_name] = { "content": contents, "filename": file.filename }
        # Clear any previous transformations for this dataset name
        if dataset_name in transformations: del transformations[dataset_name]

        # Generate preview using default engine (e.g., pandas)
        preview_info = _get_preview_from_content(contents, engine="pandas", limit=10)
        # Get initial load code for default engine
        initial_code = _get_load_code(dataset_name, "pandas", contents)

        return {
            "message": f"Successfully uploaded {file.filename} as '{dataset_name}'",
            "dataset_name": dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"],
            "can_undo": False, # New upload, no history
            "can_reset": False,
            "last_code": initial_code # Show initial load code
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
    # ... (validation and conversion logic remains the same) ...
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
                with io.BytesIO() as buffer: df_json.to_csv(buffer, index=False); content_bytes = buffer.getvalue()
                filename_suffix = "csv" # Stored as CSV
            except ValueError as json_err: raise HTTPException(status_code=400, detail=f"Could not parse JSON data (expected records format): {json_err}")
        else: raise HTTPException(status_code=400, detail=f"Unsupported data_format: {data_format}")

        if content_bytes is None: raise ValueError("Failed to convert text data to bytes.")

        # Store in primary dataset registry
        datasets[dataset_name] = { "content": content_bytes, "filename": f"{dataset_name}_pasted.{filename_suffix}"}
        # Clear any previous transformations
        if dataset_name in transformations: del transformations[dataset_name]

        preview_info = _get_preview_from_content(content_bytes, engine="pandas", limit=10)
        initial_code = _get_load_code(dataset_name, "pandas", content_bytes)

        return {
            "message": f"Successfully loaded data as '{dataset_name}'",
            "dataset_name": dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"],
            "can_undo": False,
            "can_reset": False,
            "last_code": initial_code
        }
    except (ParserError, EmptyDataError, ValueError) as pe: raise HTTPException(status_code=400, detail=f"Could not parse {data_format.upper()} data: {str(pe)}")
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Text Upload error: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Could not process text data: {str(e)}")

@app.post("/upload-db")
async def upload_database_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # ... (logic remains the same, stores file path in temp_db_files) ...
    allowed_extensions = {".db", ".sqlite", ".sqlite3", ".duckdb"}
    temp_file_path = None
    try:
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in allowed_extensions: raise HTTPException(status_code=400, detail=f"Unsupported file type '{file_ext}'. Allowed: {allowed_extensions}")

        temp_id = str(uuid.uuid4())
        temp_file_path = os.path.join(TEMP_UPLOAD_DIR, f"dbupload_{temp_id}{file_ext}")

        with open(temp_file_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)

        con = None
        try:
            con = duckdb.connect(temp_file_path, read_only=True)
            con.execute("SELECT 1")
        except duckdb.Error as db_err:
             cleanup_temp_file(temp_file_path)
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
        if temp_file_path and os.path.exists(temp_file_path): cleanup_temp_file(temp_file_path)
        raise HTTPException(status_code=500, detail=f"Could not process database file: {str(e)}")
    finally:
        if file: await file.close()


# --- DB Table Listing/Import (/list-db-tables, /import-db-table) ---
@app.get("/list-db-tables/{temp_db_id}")
async def list_database_tables(temp_db_id: str):
    # ... (logic remains the same) ...
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
    # ... (logic remains the same, but updates datasets and clears transformations) ...
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
        try: con.execute(f"SELECT 1 FROM {s_table_name} LIMIT 1;")
        except duckdb.Error: raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in the database file.")

        imported_df = con.execute(f"SELECT * FROM {s_table_name};").fetchdf()

        with io.BytesIO() as buffer: imported_df.to_csv(buffer, index=False); content_bytes = buffer.getvalue()

        # Store in primary dataset registry
        datasets[new_dataset_name] = { "content": content_bytes, "filename": f"{new_dataset_name}_from_{table_name}.csv" }
        # Clear any previous transformations
        if new_dataset_name in transformations: del transformations[new_dataset_name]

        preview_info = _get_preview_from_content(content_bytes, engine="pandas", limit=10)
        initial_code = _get_load_code(new_dataset_name, "pandas", content_bytes)

        return {
            "message": f"Successfully imported table '{table_name}' as dataset '{new_dataset_name}'",
            "dataset_name": new_dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"],
            "can_undo": False,
            "can_reset": False,
            "last_code": initial_code
        }
    except HTTPException as http_err: raise http_err
    except (duckdb.Error, ValueError) as db_err: raise HTTPException(status_code=500, detail=f"Error importing table '{table_name}': {db_err}")
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
        # Return names from the primary dataset registry
        return {"datasets": sorted(list(datasets.keys()))}
    except Exception as e:
        print(f"Error listing datasets: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve dataset list.")

@app.get("/dataset/{dataset_name}")
async def get_dataset_preview(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]),
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0)
):
    """Gets the preview of the current state and the full code chain."""
    try:
        content = get_current_content(dataset_name) # Handles 404 if dataset_name not in datasets
        preview_info = _get_preview_from_content(content, engine, limit, offset) # Handles preview errors

        current_state = get_current_state(dataset_name)
        can_undo = bool(current_state and current_state.get("history"))
        # Can reset if there's *any* transformation state stored
        can_reset = bool(current_state)
        # Get the full code chain for the *requested* engine
        current_full_code = get_current_full_code(dataset_name, engine)

        return {
            "data": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"],
            "can_undo": can_undo,
            "can_reset": can_reset,
            "last_code": current_full_code # Send the full code chain
        }
    except HTTPException as http_err:
        raise http_err # Propagate 404 or preview errors
    except Exception as e:
        print(f"Error in get_dataset_preview for '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to retrieve preview for '{dataset_name}'.")


@app.get("/dataset-info/{dataset_name}")
async def get_dataset_info(dataset_name: str):
    """Gets info about the *current state* of the dataset."""
    try:
        content = get_current_content(dataset_name) # Handles 404

        # Wrap the core pandas logic in its own try-except
        try:
             # Analyze the *current* content
             df = pd.read_csv(io.BytesIO(content))

             # ... (rest of the info calculation logic remains the same) ...
             if df.empty: return { "dataset_name": dataset_name, "row_count": 0, "column_count": 0, "memory_usage_bytes": 0, "column_types": {}, "numeric_columns": [], "categorical_columns": [], "datetime_columns": [], "other_columns": [], "missing_values_count": {}, "missing_values_percentage": {}, "unique_value_summary": {} }
             numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
             categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
             datetime_cols = df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()
             other_cols = df.select_dtypes(exclude=[np.number, 'object', 'category', 'datetime', 'datetimetz']).columns.tolist()
             column_types = {col: str(df[col].dtype) for col in df.columns}
             missing_values = df.isnull().sum().to_dict()
             total_rows = len(df)
             unique_counts = {}
             for col in df.columns:
                 if total_rows > 0 and total_rows < 50000:
                    try:
                        nunique = df[col].nunique()
                        unique_counts[col] = {"total_unique": nunique}
                        if nunique < 100:
                             value_counts = df[col].value_counts().head(10).to_dict()
                             unique_counts[col]["values"] = {str(k): v for k, v in value_counts.items()}
                    except Exception as unique_err:
                         print(f"Could not calculate unique counts for column '{col}': {unique_err}")
                         unique_counts[col] = {"error": "Could not calculate"}

             return {
                 "dataset_name": dataset_name, "row_count": total_rows, "column_count": len(df.columns),
                 "memory_usage_bytes": int(df.memory_usage(deep=True).sum()), "column_types": column_types,
                 "numeric_columns": numeric_cols, "categorical_columns": categorical_cols, "datetime_columns": datetime_cols,
                 "other_columns": other_cols, "missing_values_count": missing_values,
                 "missing_values_percentage": {k: round((v / total_rows * 100), 2) if total_rows > 0 else 0 for k, v in missing_values.items()},
                 "unique_value_summary": unique_counts
             }
        except (ParserError, EmptyDataError) as pe: raise HTTPException(status_code=400, detail=f"Cannot get info: Invalid CSV format for '{dataset_name}'. {str(pe)}")
        except Exception as e_inner:
             print(f"Error calculating dataset info for '{dataset_name}': {type(e_inner).__name__}: {e_inner}")
             traceback.print_exc()
             raise HTTPException(status_code=500, detail=f"Error calculating info for '{dataset_name}': {str(e_inner)}")

    except HTTPException as http_err: raise http_err # Propagate 404 from get_current_content
    except Exception as e_outer:
        print(f"Unexpected error in get_dataset_info for '{dataset_name}': {type(e_outer).__name__}: {e_outer}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error getting info for '{dataset_name}'.")


@app.get("/column-stats/{dataset_name}/{column_name}")
async def get_column_stats(dataset_name: str, column_name: str, engine: str = "pandas"):
    """Gets stats for a column from the *current state* of the dataset."""
    try:
        content = get_current_content(dataset_name) # Handles 404
        stats = { "column_name": column_name, "engine_used": engine }

        # Always use pandas for stats calculation for simplicity, regardless of 'engine' param
        # The 'engine' param might be removed later if not used elsewhere for stats
        try:
             df = pd.read_csv(io.BytesIO(content))
             if column_name not in df.columns: raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in current state of dataset '{dataset_name}'.")

             # --- Calculate stats using pandas ---
             column_data = df[column_name].copy()
             total_rows = len(df)
             stats.update({
                 "dtype": str(column_data.dtype),
                 "missing_count": int(column_data.isnull().sum()),
                 "missing_percentage": round((column_data.isnull().sum() / total_rows * 100), 2) if total_rows > 0 else 0,
                 "memory_usage_bytes": int(column_data.memory_usage(deep=True))
             })
             # ... (numeric, datetime, categorical logic remains the same) ...
             if pd.api.types.is_numeric_dtype(column_data.dtype):
                 desc = column_data.describe()
                 stats.update({
                     "mean": float(desc['mean']) if 'mean' in desc else None,
                     "std": float(desc['std']) if 'std' in desc else None,
                     "min": float(desc['min']) if 'min' in desc else None,
                     "max": float(desc['max']) if 'max' in desc else None,
                     "quantiles": {
                         "25%": float(desc['25%']) if '25%' in desc else None,
                         "50%": float(desc['50%']) if '50%' in desc else None, # Median
                         "75%": float(desc['75%']) if '75%' in desc else None,
                     }
                 })
             elif pd.api.types.is_datetime64_any_dtype(column_data.dtype):
                  stats.update({
                      "min_date": str(column_data.min()) if not column_data.isnull().all() else None,
                      "max_date": str(column_data.max()) if not column_data.isnull().all() else None,
                  })
             else: # Assume categorical/object
                 nunique = column_data.nunique()
                 stats["unique_count"] = int(nunique)
                 if nunique < 1000: # Only show top values if cardinality is reasonable
                     top_values = column_data.value_counts().head(10).to_dict()
                     stats["top_values"] = {str(k): int(v) for k, v in top_values.items()} # Ensure keys are strings

             # Convert numpy types
             for key, value in stats.items():
                 if isinstance(value, (np.integer, np.int64)): stats[key] = int(value)
                 elif isinstance(value, (np.floating, np.float64)): stats[key] = float(value) if pd.notna(value) else None
                 elif isinstance(value, np.bool_): stats[key] = bool(value)
                 elif isinstance(value, dict):
                      for k, v in value.items():
                          if isinstance(v, (np.integer, np.int64)): stats[key][k] = int(v)
                          elif isinstance(v, (np.floating, np.float64)): stats[key][k] = float(v) if pd.notna(v) else None

             return stats

        except (ParserError, EmptyDataError) as pe: raise HTTPException(status_code=400, detail=f"Cannot get stats: Invalid CSV format for '{dataset_name}'. {str(pe)}")
        except KeyError: raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found in dataset '{dataset_name}'.")
        except Exception as e_inner:
             print(f"Error calculating pandas column stats for '{column_name}' in '{dataset_name}': {type(e_inner).__name__}: {e_inner}")
             traceback.print_exc()
             raise HTTPException(status_code=500, detail=f"Error calculating stats for column '{column_name}': {str(e_inner)}")

    except HTTPException as http_err: raise http_err # Propagate 404 or 400
    except Exception as e_outer:
        print(f"Unexpected error in get_column_stats for '{column_name}' in '{dataset_name}': {type(e_outer).__name__}: {e_outer}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected server error getting column stats.")


# --- Data Transformation Endpoints ---

@app.post("/operation")
async def perform_operation(
    dataset_name: str = Form(...),
    operation: str = Form(...),
    params: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    """Applies a single operation to the current state and updates the chain."""
    try:
        content_before_op = get_current_content(dataset_name) # Content *before* this specific operation
        params_dict = json.loads(params)
        new_step_content = None
        new_full_code = ""
        preview_info = {}

        # Get the full code chain *before* this operation for the target engine
        # This handles the engine switch case implicitly
        full_code_before_op = get_current_full_code(dataset_name, engine)

        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content_before_op))
            result_df, code_snippet = pandas_service.apply_pandas_operation(df, operation, params_dict)
            with io.BytesIO() as buffer: result_df.to_csv(buffer, index=False); new_step_content = buffer.getvalue()
            # Build the new full code chain
            new_full_code = _build_full_code(full_code_before_op, code_snippet, engine)

        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content_before_op))
            result_df, code_snippet = polars_service.apply_polars_operation(df, operation, params_dict)
            with io.BytesIO() as buffer: result_df.write_csv(buffer); new_step_content = buffer.getvalue()
            # Build the new full code chain
            new_full_code = _build_full_code(full_code_before_op, code_snippet, engine)

        elif engine == "sql":
            con = None
            try:
                 con = duckdb.connect(":memory:")
                 # Load the *original* data if starting a new SQL chain, or ensure base table exists
                 current_state = get_current_state(dataset_name)
                 is_new_sql_chain = not current_state or current_state["current_engine"] != "sql"

                 # Define a consistent base table name reference
                 safe_base_name = re.sub(r'\W|^(?=\d)', '_', dataset_name)
                 base_table_ref = sql_service._sanitize_identifier(safe_base_name)

                 if is_new_sql_chain:
                     # Load original data into the base table reference
                     original_content = datasets[dataset_name]["content"]
                     sql_service._load_data_to_duckdb(con, safe_base_name, original_content)
                     # The 'previous' chain is just selecting from the base table
                     previous_sql_chain = f"SELECT * FROM {base_table_ref}"
                     # Ensure full_code_before_op reflects this start
                     full_code_before_op = previous_sql_chain
                 else:
                     # Load the base table from the original content for the CTEs to reference
                     original_content = datasets[dataset_name]["content"]
                     sql_service._load_data_to_duckdb(con, safe_base_name, original_content)
                     # The previous chain is the stored full code
                     previous_sql_chain = full_code_before_op

                 # Apply the SQL operation using the previous chain
                 preview_data, result_columns, total_rows, generated_new_full_sql, _ = sql_service.apply_sql_operation(
                     con=con,
                     previous_sql_chain=previous_sql_chain,
                     operation=operation,
                     params=params_dict,
                     base_table_ref=base_table_ref # Pass the reference name
                 )
                 preview_info = { "data": preview_data, "columns": result_columns, "row_count": total_rows }
                 new_full_code = generated_new_full_sql # The service now returns the full chain

                 # Fetch the full result content for the new state
                 full_df = con.execute(new_full_code).fetchdf()
                 with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_step_content = buffer.getvalue()

            finally:
                 if con: con.close()
        else:
             raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_step_content is None: raise ValueError("Operation failed: No new content generated.")

        # Update the central transformation state
        update_transformation_state(dataset_name, engine, operation, params_dict, new_full_code, new_step_content)

        # Get preview from the final content of this step
        # Use the current engine for preview generation consistency
        final_preview_info = _get_preview_from_content(new_step_content, engine)

        # Determine undo/reset status after update
        current_state_after = get_current_state(dataset_name)
        can_undo_after = bool(current_state_after and current_state_after.get("history"))
        can_reset_after = bool(current_state_after)

        return {
            "data": final_preview_info["data"], "columns": final_preview_info["columns"], "row_count": final_preview_info["row_count"],
            "code": new_full_code, # Return the new full code chain
            "can_undo": can_undo_after,
            "can_reset": can_reset_after
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
         traceback.print_exc() # Log full traceback for server debugging
         raise HTTPException(status_code=status_code, detail=detail)
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /operation ({engine}, {operation}): {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected server error occurred during '{operation}'.")

@app.post("/execute-code")
async def execute_custom_code(
    dataset_name: str = Form(...),
    code: str = Form(...), # User provides the full code/query they want to run
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    """Executes custom code, replacing the current transformation chain."""
    try:
        # Custom code execution *replaces* the existing chain.
        # It operates on the *original* dataset content.
        if dataset_name not in datasets:
             raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")
        original_content = datasets[dataset_name]["content"]

        new_content = None
        result_data = None
        columns = []
        row_count = 0
        executed_code = code # The code to store is the code executed

        if engine == "pandas":
            local_vars = {}
            exec("import pandas as pd\nimport io\nimport numpy as np", {"pd": pd, "np": np, "io": io}, local_vars)
            # Execute assuming 'df' is loaded from original_content
            exec(f"df = pd.read_csv(io.BytesIO(original_content))", {"pd": pd, "io": io, "original_content": original_content}, local_vars)
            exec(code, {"pd": pd, "np": np, "io": io}, local_vars) # Pass safe modules
            result_df = local_vars.get('df')
            if not isinstance(result_df, pd.DataFrame): raise ValueError("Pandas code must result in a DataFrame assigned to 'df'.")
            with io.BytesIO() as buffer: result_df.to_csv(buffer, index=False); new_content = buffer.getvalue()
            result_data = result_df.head(100).replace([np.inf, -np.inf], None).fillna('NaN').to_dict(orient="records")
            columns, row_count = list(result_df.columns), len(result_df)
            # The 'full code' is the load statement + the executed code
            load_code = _get_load_code(dataset_name, engine, original_content)
            executed_code = f"{load_code}\n# --- Custom Code Start ---\n{code}\n# --- Custom Code End ---"

        elif engine == "polars":
             local_vars = {}
             exec("import polars as pl\nimport io", {"pl": pl, "io": io}, local_vars)
             exec(f"df = pl.read_csv(io.BytesIO(original_content))", {"pl": pl, "io": io, "original_content": original_content}, local_vars)
             exec(code, {"pl": pl, "io": io}, local_vars)
             result_df = local_vars.get('df')
             if not isinstance(result_df, pl.DataFrame): raise ValueError("Polars code must result in a DataFrame assigned to 'df'.")
             with io.BytesIO() as buffer: result_df.write_csv(buffer); new_content = buffer.getvalue()
             result_data = result_df.head(100).fill_nan('NaN').to_dicts()
             columns, row_count = result_df.columns, result_df.height
             load_code = _get_load_code(dataset_name, engine, original_content)
             executed_code = f"{load_code}\n# --- Custom Code Start ---\n{code}\n# --- Custom Code End ---"

        elif engine == "sql":
            # SQL code is assumed to be a SELECT query operating on the base table
            con = None
            try:
                 con = duckdb.connect(":memory:")
                 safe_base_name = re.sub(r'\W|^(?=\d)', '_', dataset_name)
                 base_table_ref = sql_service._sanitize_identifier(safe_base_name)
                 sql_service._load_data_to_duckdb(con, safe_base_name, original_content)

                 # Execute the user's query
                 preview_data, columns, row_count = sql_service._execute_sql_query(con, code)
                 result_data = preview_data

                 # Fetch full result for state update
                 full_df = con.execute(code).fetchdf()
                 with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_content = buffer.getvalue()
                 # The executed code *is* the full code chain in this context
                 executed_code = f"-- Custom SQL Query on '{dataset_name}'\n{code}"

            finally:
                if con: con.close()
        else: raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_content is None: raise ValueError("Code execution failed to produce new content state.")

        # Update state - this replaces any previous chain
        update_transformation_state(dataset_name, engine, "custom_code", code, executed_code, new_content)

        # Determine undo/reset status after update
        current_state_after = get_current_state(dataset_name)
        can_undo_after = bool(current_state_after and current_state_after.get("history"))
        can_reset_after = bool(current_state_after)

        return {
            "data": result_data, "columns": columns, "row_count": row_count,
            "code": executed_code, # Return the full code including load/markers
            "can_undo": can_undo_after,
            "can_reset": can_reset_after
        }
    # Catch specific execution errors
    except (SyntaxError, NameError, TypeError, ValueError, AttributeError, KeyError, pd.errors.PandasError, pl.exceptions.PolarsError, duckdb.Error) as exec_err:
         traceback.print_exc()
         raise HTTPException(status_code=400, detail=f"Code execution failed ({engine}): {type(exec_err).__name__}: {str(exec_err)}")
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /execute-code: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during code execution.")

# --- Merge/Join Endpoint ---
@app.post("/merge-datasets")
async def merge_datasets_endpoint(
    left_dataset: str = Form(...), # The dataset being modified
    right_dataset: str = Form(...), # The dataset to join with
    params: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    """Performs a merge/join, adding a step to the left dataset's chain."""
    try:
        params_dict = json.loads(params)
        # Get current state of LEFT dataset
        content_left_before_op = get_current_content(left_dataset)
        full_code_left_before_op = get_current_full_code(left_dataset, engine)

        # Get current state of RIGHT dataset (needed for the operation itself)
        content_right_current = get_current_content(right_dataset)
        # We also need the *original* content of the right dataset if the code chain needs it (e.g., SQL)
        original_content_right = datasets[right_dataset]["content"] if right_dataset in datasets else None
        if original_content_right is None:
             raise HTTPException(status_code=404, detail=f"Right dataset '{right_dataset}' original content not found.")


        new_step_content = None
        new_full_code = ""

        if engine == "pandas":
            df_left = pd.read_csv(io.BytesIO(content_left_before_op))
            df_right = pd.read_csv(io.BytesIO(content_right_current)) # Join uses current state of right table
            result_df, code_snippet = pandas_service.apply_pandas_merge(df_left, df_right, params_dict, right_dataset_name=right_dataset)
            with io.BytesIO() as buffer: result_df.to_csv(buffer, index=False); new_step_content = buffer.getvalue()
            new_full_code = _build_full_code(full_code_left_before_op, code_snippet, engine)

        elif engine == "polars":
            df_left = pl.read_csv(io.BytesIO(content_left_before_op))
            df_right = pl.read_csv(io.BytesIO(content_right_current))
            result_df, code_snippet = polars_service.apply_polars_join(df_left, df_right, params_dict, right_dataset_name=right_dataset)
            with io.BytesIO() as buffer: result_df.write_csv(buffer); new_step_content = buffer.getvalue()
            new_full_code = _build_full_code(full_code_left_before_op, code_snippet, engine)

        elif engine == "sql":
            con = None
            try:
                 con = duckdb.connect(":memory:")
                 # Define base table names
                 safe_base_left = re.sub(r'\W|^(?=\d)', '_', left_dataset)
                 base_table_ref_left = sql_service._sanitize_identifier(safe_base_left)
                 safe_base_right = re.sub(r'\W|^(?=\d)', '_', right_dataset)
                 base_table_ref_right = sql_service._sanitize_identifier(safe_base_right)

                 # Load *original* content for both tables
                 original_content_left = datasets[left_dataset]["content"]
                 sql_service._load_data_to_duckdb(con, safe_base_left, original_content_left)
                 sql_service._load_data_to_duckdb(con, safe_base_right, original_content_right)

                 # Get the SQL chain for the left table before this join
                 current_state_left = get_current_state(left_dataset)
                 is_new_sql_chain_left = not current_state_left or current_state_left["current_engine"] != "sql"
                 if is_new_sql_chain_left:
                     previous_sql_chain_left = f"SELECT * FROM {base_table_ref_left}"
                     full_code_left_before_op = previous_sql_chain_left # Reset code chain start
                 else:
                     previous_sql_chain_left = full_code_left_before_op

                 # Get the SQL chain for the right table (to represent its current state for the join)
                 # Note: The join operation itself will use the base table ref for the right side,
                 # but the generated code needs to reflect the conceptual join.
                 # This is tricky. Let's simplify: SQL join operates on the *base* tables,
                 # assuming the user wants to join the original datasets.
                 # If they want to join transformed states, they should save them first.
                 # TODO: Revisit joining transformed SQL states later if needed.

                 # Apply the join operation
                 # The 'previous_sql_chain' here refers to the left table's state before the join
                 preview_data, result_columns, total_rows, generated_new_full_sql, _ = sql_service.apply_sql_join(
                     con=con,
                     previous_sql_chain_left=previous_sql_chain_left, # Chain for left table
                     right_table_ref=base_table_ref_right, # Use base ref for right table
                     params=params_dict,
                     base_table_ref_left=base_table_ref_left,
                     # We might need columns from the right base table for validation inside apply_sql_join
                 )
                 new_full_code = generated_new_full_sql # Service returns the new full chain for the left table

                 # Fetch full result content
                 full_df = con.execute(new_full_code).fetchdf()
                 with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_step_content = buffer.getvalue()

            finally:
                 if con: con.close()
        else: raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_step_content is None: raise ValueError("Merge failed internally.")

        # Update state of the *left* dataset
        update_transformation_state(left_dataset, engine, "merge", params_dict, new_full_code, new_step_content)

        final_preview_info = _get_preview_from_content(new_step_content, engine)
        current_state_after = get_current_state(left_dataset)
        can_undo_after = bool(current_state_after and current_state_after.get("history"))
        can_reset_after = bool(current_state_after)

        return {
            "message": f"Merged {left_dataset} and {right_dataset}. Result updated for {left_dataset}.",
            "data": final_preview_info["data"], "columns": final_preview_info["columns"], "row_count": final_preview_info["row_count"],
            "code": new_full_code,
            "can_undo": can_undo_after,
            "can_reset": can_reset_after
        }
    except (ValueError, pl.exceptions.PolarsError, duckdb.Error, pd.errors.PandasError, KeyError, json.JSONDecodeError) as ve:
         traceback.print_exc()
         detail = f"Merge failed ({engine}): {type(ve).__name__}: {str(ve)}"
         if isinstance(ve, KeyError): detail = f"Merge failed ({engine}): Join key not found: {str(ve)}"
         elif isinstance(ve, json.JSONDecodeError): detail = f"Merge failed: Invalid parameters JSON. {str(ve)}"
         raise HTTPException(status_code=400, detail=detail)
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Unexpected error in /merge-datasets: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during merge.")


# --- Regex Operation Endpoint ---
@app.post("/regex-operation")
async def regex_operation(
    dataset_name: str = Form(...),
    operation: str = Form(...), # e.g., "filter", "extract", "replace"
    params: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    """Applies a regex operation as a step in the chain."""
    try:
        content_before_op = get_current_content(dataset_name)
        full_code_before_op = get_current_full_code(dataset_name, engine)
        params_dict = json.loads(params)
        new_step_content = None
        new_full_code = ""
        operation_name = f"regex_{operation}" # For history tracking

        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content_before_op))
            result_df, code_snippet = pandas_service.apply_regex_operation(df, operation, params_dict)
            with io.BytesIO() as buffer: result_df.to_csv(buffer, index=False); new_step_content = buffer.getvalue()
            new_full_code = _build_full_code(full_code_before_op, code_snippet, engine)

        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content_before_op))
            result_df, code_snippet = polars_service.apply_regex_operation(df, operation, params_dict)
            with io.BytesIO() as buffer: result_df.write_csv(buffer); new_step_content = buffer.getvalue()
            new_full_code = _build_full_code(full_code_before_op, code_snippet, engine)

        elif engine == "sql":
            con = None
            try:
                 con = duckdb.connect(":memory:")
                 current_state = get_current_state(dataset_name)
                 is_new_sql_chain = not current_state or current_state["current_engine"] != "sql"
                 safe_base_name = re.sub(r'\W|^(?=\d)', '_', dataset_name)
                 base_table_ref = sql_service._sanitize_identifier(safe_base_name)

                 if is_new_sql_chain:
                     original_content = datasets[dataset_name]["content"]
                     sql_service._load_data_to_duckdb(con, safe_base_name, original_content)
                     previous_sql_chain = f"SELECT * FROM {base_table_ref}"
                     full_code_before_op = previous_sql_chain
                 else:
                     original_content = datasets[dataset_name]["content"]
                     sql_service._load_data_to_duckdb(con, safe_base_name, original_content)
                     previous_sql_chain = full_code_before_op

                 # Use the generic apply_sql_operation for regex as well
                 preview_data, result_columns, total_rows, generated_new_full_sql, _ = sql_service.apply_sql_operation(
                     con=con,
                     previous_sql_chain=previous_sql_chain,
                     operation=operation_name, # Pass specific regex op type like 'regex_filter'
                     params=params_dict,
                     base_table_ref=base_table_ref
                 )
                 new_full_code = generated_new_full_sql

                 full_df = con.execute(new_full_code).fetchdf()
                 with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_step_content = buffer.getvalue()
            finally:
                 if con: con.close()
        else: raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")


        if new_step_content is None: raise ValueError(f"Regex operation '{operation}' failed internally.")

        update_transformation_state(dataset_name, engine, operation_name, params_dict, new_full_code, new_step_content)

        final_preview_info = _get_preview_from_content(new_step_content, engine)
        current_state_after = get_current_state(dataset_name)
        can_undo_after = bool(current_state_after and current_state_after.get("history"))
        can_reset_after = bool(current_state_after)

        return {
            "message": f"Successfully applied regex '{operation}' on column '{params_dict.get('column', 'N/A')}'.",
            "data": final_preview_info.get("data", []), "columns": final_preview_info.get("columns", []), "row_count": final_preview_info.get("row_count", 0),
            "code": new_full_code,
            "can_undo": can_undo_after,
            "can_reset": can_reset_after
        }

    except (ValueError, TypeError, KeyError, pd.errors.PandasError, pl.exceptions.PolarsError, duckdb.Error, re.error, json.JSONDecodeError) as op_err:
        traceback.print_exc()
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


# --- Relational Algebra Endpoints ---
# RA operations are treated as one-off transformations creating a *new* dataset,
# they don't modify the chain of existing datasets.
# Keep /relational-operation-preview and /save-ra-result as they were.
@app.post("/relational-operation-preview")
async def preview_relational_operation(
    operation: str = Form(...),
    params: str = Form(...),
    base_dataset_name: str = Form(...), # Still required
    current_sql_state: Optional[str] = Form(None),
    step_alias_base: str = Form("step")
):
    # ... (Keep existing RA preview logic - it uses its own temporary state) ...
    con = None
    try:
        params_dict = json.loads(params)
        if not base_dataset_name: raise HTTPException(status_code=400, detail="RA preview requires 'base_dataset_name'.")

        con = duckdb.connect(":memory:")
        internal_load_name = f"__base_{base_dataset_name}"
        s_internal_load_name = relational_algebra_service._sanitize_identifier(internal_load_name)
        try:
            # Use get_current_content to get potentially transformed base data for RA
            content = get_current_content(base_dataset_name)
            print(f"DEBUG: Loading base data '{base_dataset_name}' (current state) into table {s_internal_load_name}")
            relational_algebra_service._load_ra_data(con, internal_load_name, content)
        except HTTPException as http_err:
             if http_err.status_code == 404: raise HTTPException(status_code=404, detail=f"Base dataset '{base_dataset_name}' not found.")
             else: raise http_err
        except ValueError as load_err: raise HTTPException(status_code=400, detail=f"Failed to load base dataset '{base_dataset_name}': {load_err}")

        source_sql_or_table: str
        columns_before: List[str] = []
        step_number = 0

        if current_sql_state:
            state_strip = current_sql_state.strip()
            match = re.match(r"\((.*)\)\s+AS\s+\w+\s*$", state_strip, re.DOTALL | re.IGNORECASE)
            if not match:
                if "SELECT " in state_strip.upper():
                     print(f"Warning: current_sql_state '{current_sql_state[:100]}...' did not match '(...) AS alias' format, using directly.")
                     core_previous_sql = state_strip
                else: raise ValueError(f"Could not parse previous SQL state format: {current_sql_state[:200]}...")
            else: core_previous_sql = match.group(1).strip()

            alias_match = re.search(rf"{re.escape(step_alias_base)}(\d+)$", state_strip, re.IGNORECASE)
            step_number = int(alias_match.group(1)) + 1 if alias_match else 1

            temp_prev_view = f"__prev_view_{uuid.uuid4().hex[:8]}"
            print(f"DEBUG: Creating view {temp_prev_view} AS: {core_previous_sql}")
            try: con.execute(f"CREATE TEMP VIEW {temp_prev_view} AS {core_previous_sql};")
            except duckdb.Error as view_err: raise ValueError(f"Failed to create view from previous step SQL: {view_err}. SQL was: {core_previous_sql}")

            cols_result = con.execute(f"DESCRIBE {temp_prev_view};").fetchall()
            columns_before = [col[0] for col in cols_result]
            source_sql_or_table = temp_prev_view

        else:
            step_number = 0
            cols_result = con.execute(f"DESCRIBE {s_internal_load_name};").fetchall()
            columns_before = [col[0] for col in cols_result]
            source_sql_or_table = s_internal_load_name

        if operation.lower() == "rename":
            if not columns_before: raise ValueError("Cannot perform rename: Failed to determine columns from previous step.")
            params_dict["all_columns"] = columns_before

        current_step_alias = f"{step_alias_base}{step_number}"
        sql_snippet = relational_algebra_service._generate_sql_snippet(operation, params_dict, source_sql_or_table)
        print(f"DEBUG: Generated snippet for current step: {sql_snippet}")

        preview_data, result_columns, total_rows = relational_algebra_service._execute_preview_query(con, sql_snippet)
        next_sql_state = f"({sql_snippet}) AS {current_step_alias}"
        print(f"DEBUG: Generated next_sql_state: {next_sql_state}")

        return {
            "message": "RA preview generated successfully.",
            "data": preview_data, "columns": result_columns, "row_count": total_rows,
            "generated_sql_state": next_sql_state,
            "current_step_sql_snippet": sql_snippet
        }

    except (ValueError, duckdb.Error, NotImplementedError, json.JSONDecodeError) as e:
         err_type = type(e).__name__
         detail = f"Relational Algebra preview for '{operation}' failed: {err_type}: {str(e)}"
         print(f"RA Preview Error (400): {detail}")
         if isinstance(e, duckdb.BinderException): detail = f"RA preview failed (Binder Error): {str(e)}. Check column/table names/types."
         elif isinstance(e, duckdb.CatalogException): detail = f"RA preview failed (Catalog Error): {str(e)}. Check if table/view exists."
         elif isinstance(e, duckdb.ParserException): detail = f"RA preview failed (Parser Error): {str(e)}. Check syntax."
         elif isinstance(e, json.JSONDecodeError): detail = f"RA preview failed: Invalid parameters JSON. {str(e)}"
         elif "Cannot perform rename" in str(e): detail = f"RA preview failed: Rename error - {str(e)}"
         elif "Failed to create view" in str(e): detail = f"RA preview failed: {str(e)}"
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
    base_dataset_names_json: str = Form(...)
):
    # ... (Keep existing RA save logic - creates a new entry in `datasets`) ...
    con = None
    try:
        if not new_dataset_name.strip(): raise ValueError("New dataset name cannot be empty.")
        base_dataset_names = json.loads(base_dataset_names_json)
        if not isinstance(base_dataset_names, list) or not base_dataset_names: raise ValueError("Invalid or empty list of base dataset names provided.")

        if new_dataset_name in datasets: print(f"Warning: Overwriting dataset '{new_dataset_name}' with RA result save.")

        con = duckdb.connect(":memory:")
        for ds_name in base_dataset_names:
             # Use current content of base datasets for the final execution
             content = get_current_content(ds_name)
             relational_algebra_service._load_ra_data(con, ds_name, content)

        print(f"Executing final RA SQL chain for saving '{new_dataset_name}':\n{final_sql_chain}")
        full_df = con.execute(final_sql_chain).fetchdf()

        with io.BytesIO() as buffer: full_df.to_csv(buffer, index=False); new_content = buffer.getvalue()

        # Save as a new base dataset
        datasets[new_dataset_name] = { "content": new_content, "filename": f"{new_dataset_name}_ra_result.csv" }
        # Clear any transformations associated with the *new* name
        if new_dataset_name in transformations: del transformations[new_dataset_name]

        saved_preview_info = _get_preview_from_content(new_content, engine="pandas", limit=10)
        return {
            "message": f"Successfully saved RA result as '{new_dataset_name}'.",
            "dataset_name": new_dataset_name,
            "preview": saved_preview_info["data"], "columns": saved_preview_info["columns"], "row_count": saved_preview_info["row_count"],
            "datasets": sorted(list(datasets.keys())) # Return updated list
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


# --- Undo/Reset/Save Transformation Endpoints ---
@app.post("/undo/{dataset_name}")
async def undo_last_operation(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]) # Engine for preview
):
    """Reverts the dataset to the state before the last operation."""
    try:
        current_state = get_current_state(dataset_name)
        if not current_state or not current_state.get("history"):
            raise HTTPException(status_code=400, detail="No operations to undo.")

        # Pop the last saved state from history
        history = current_state["history"]
        last_saved_state = history.pop() # This is the state *before* the last operation

        # Restore the state from the snapshot
        transformations[dataset_name] = {
            "current_content": last_saved_state["current_content"],
            "current_full_code": last_saved_state["current_full_code"],
            "current_engine": last_saved_state["current_engine"],
            "history": history # Assign the modified history list back
        }
        print(f"Undo successful for {dataset_name}. Rolled back to state after op: {last_saved_state.get('operation_applied', 'N/A')}")

        # Generate preview using the *restored* state and the requested engine
        restored_content = last_saved_state["current_content"]
        restored_engine = last_saved_state["current_engine"]
        preview_info = _get_preview_from_content(restored_content, engine) # Use requested engine for preview
        restored_full_code = get_current_full_code(dataset_name, engine) # Get code for requested engine

        # Determine new undo/reset status
        can_undo_after = bool(history)
        can_reset_after = True # Can always reset if there was a state

        return {
            "message": f"Undid last operation", # Removed ({last_op.get('operation', 'N/A')}) as it's complex to track
            "data": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"],
            "can_undo": can_undo_after,
            "can_reset": can_reset_after,
            "last_code": restored_full_code
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error during undo for '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        # Attempt to restore state? Difficult. Signal failure.
        raise HTTPException(status_code=500, detail=f"An error occurred during undo.")


@app.post("/reset/{dataset_name}")
async def reset_transformations(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]) # Engine for preview
):
    """Resets the dataset to its original uploaded state."""
    try:
        if dataset_name not in datasets:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

        # Remove the transformation state entry entirely
        if dataset_name in transformations:
            del transformations[dataset_name]
            print(f"Reset transformations for '{dataset_name}'")
        else:
             # No transformations to reset, but still return the original state preview
             print(f"No transformations found to reset for '{dataset_name}', showing original.")


        # Get original content and generate preview/code for the requested engine
        original_content = datasets[dataset_name]["content"]
        preview_info = _get_preview_from_content(original_content, engine)
        initial_code = _get_load_code(dataset_name, engine, original_content)

        return {
            "message": f"Reset transformations for {dataset_name}",
            "data": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"],
            "can_undo": False, # Reset state has no history
            "can_reset": False, # Cannot reset further
            "last_code": initial_code
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
         print(f"Error during reset for '{dataset_name}': {type(e).__name__}: {e}")
         traceback.print_exc()
         raise HTTPException(status_code=500, detail=f"An error occurred during reset.")

@app.post("/save-transformation")
async def save_transformation(
    dataset_name: str = Form(...), # The dataset whose current state to save
    new_dataset_name: str = Form(...),
    engine: str = Form(default="pandas") # For preview generation of the saved state
):
    """Saves the current transformed state as a new base dataset."""
    try:
        content_to_save = get_current_content(dataset_name) # Handles 404 for source
        if not new_dataset_name.strip(): raise ValueError("New dataset name cannot be empty.")
        # Add validation?
        # if not re.match(r"^[a-zA-Z0-9_\-\.]+$", new_dataset_name.strip()): raise ValueError("New name contains invalid characters.")

        if new_dataset_name in datasets: print(f"Warning: Overwriting dataset '{new_dataset_name}' when saving transformation.")

        # Add to the main datasets registry
        datasets[new_dataset_name] = { "content": content_to_save, "filename": f"{new_dataset_name}_saved.csv" }
        # Clear any transformations associated with the *new* name
        if new_dataset_name in transformations: del transformations[new_dataset_name]

        preview_info = _get_preview_from_content(content_to_save, engine, limit=10)
        # Return updated list of datasets
        return {
            "message": f"Successfully saved current state of '{dataset_name}' as '{new_dataset_name}'",
            "dataset_name": new_dataset_name, # Return the new name
            "preview": preview_info["data"], "columns": preview_info["columns"], "row_count": preview_info["row_count"],
            "datasets": sorted(list(datasets.keys())) # Send back the updated list
        }
    except (ValueError) as val_err: raise HTTPException(status_code=400, detail=str(val_err))
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
    """Exports the *current state* of the dataset."""
    try:
        content = get_current_content(dataset_name) # Handles 404
        file_content: Union[bytes, str]
        media_type: str
        filename: str

        if format == "csv":
            media_type="text/csv"
            filename = f"{dataset_name}_current.csv" # Indicate it's the current state
            file_content = content
        else:
            # Use pandas for consistent non-CSV export of the current state
            try:
                 df = pd.read_csv(io.BytesIO(content))
                 if format == "json":
                     media_type="application/json"
                     filename = f"{dataset_name}_current.json"
                     file_content = df.to_json(orient="records", date_format="iso", default_handler=str, force_ascii=False)
                 elif format == "excel":
                     media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                     filename = f"{dataset_name}_current.xlsx"
                     with io.BytesIO() as buffer:
                         df.to_excel(buffer, index=False, engine='openpyxl')
                         file_content = buffer.getvalue()
                 else: raise ValueError(f"Unsupported format: {format}")
            except (ParserError, EmptyDataError) as pe: raise HTTPException(status_code=400, detail=f"Cannot export: Invalid CSV data for '{dataset_name}'. {str(pe)}")
            except Exception as export_load_err:
                 print(f"Error preparing non-CSV export for '{dataset_name}': {export_load_err}")
                 traceback.print_exc()
                 raise HTTPException(status_code=500, detail=f"Failed to prepare data for {format} export.")

        safe_filename = re.sub(r'[^\w\.\-]', '_', filename)
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
    """Renames a dataset in both the base registry and transformations."""
    try:
        if not old_dataset_name or not new_dataset_name: raise ValueError("Old and new dataset names must be provided.")
        new_name = new_dataset_name.strip()
        if not new_name: raise ValueError("New dataset name cannot be empty.")
        if not re.match(r"^[a-zA-Z0-9_\-\.]+$", new_name): raise ValueError("New name contains invalid characters.")
        if old_dataset_name not in datasets: raise HTTPException(status_code=404, detail=f"Dataset '{old_dataset_name}' not found.")
        if new_name == old_dataset_name: return {"message": f"Dataset name '{old_dataset_name}' unchanged.", "datasets": sorted(list(datasets.keys()))}
        if new_name in datasets: raise HTTPException(status_code=409, detail=f"Dataset name '{new_name}' already exists.")

        # Perform rename in primary registry
        datasets[new_name] = datasets.pop(old_dataset_name)
        # Perform rename in transformations state if exists
        if old_dataset_name in transformations:
            transformations[new_name] = transformations.pop(old_dataset_name)
            # TODO: Update code chains within the renamed transformation history/state?
            # This is complex. For now, the code might still reference the old name.
            print(f"Warning: Code chains within renamed dataset '{new_name}' might still reference '{old_dataset_name}'.")

        print(f"Renamed dataset '{old_dataset_name}' to '{new_name}'")
        return {
            "message": f"Successfully renamed dataset '{old_dataset_name}' to '{new_name}'.",
            "old_name": old_dataset_name, "new_name": new_name,
            "datasets": sorted(list(datasets.keys())) # Return updated list
        }
    except ValueError as ve: raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error renaming dataset '{old_dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        # Attempt to revert rename if partial failure? Complex. Better to signal error.
        raise HTTPException(status_code=500, detail=f"An internal error occurred during rename.")

@app.delete("/dataset/{dataset_name}")
async def delete_dataset(dataset_name: str):
    """Deletes a dataset from the base registry and transformations."""
    try:
        if dataset_name not in datasets:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

        # Delete from primary registry
        del datasets[dataset_name]
        # Delete from transformations state if exists
        if dataset_name in transformations:
            del transformations[dataset_name]

        print(f"Deleted dataset '{dataset_name}'")
        return {
            "message": f"Successfully deleted dataset '{dataset_name}'.",
            "deleted_name": dataset_name,
            "datasets": sorted(list(datasets.keys())) # Return updated list
        }
    except HTTPException as http_err: raise http_err
    except Exception as e:
        print(f"Error deleting dataset '{dataset_name}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An internal error occurred during deletion.")

# --- Optional: Add endpoint to clean up old temp DB files ---
# This would require tracking creation times or using a more robust temp file solution.
