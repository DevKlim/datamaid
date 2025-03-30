from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import polars as pl
import duckdb
import io
import json
import re
import os
import tempfile
from typing import Optional, List, Dict, Any, Union
from pandas.errors import DataError

# Import the services
from .services import pandas_service, polars_service, sql_service # Adjusted import path

app = FastAPI(title="Data Analysis GUI API")

# Configure CORS to allow requests from frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development only, replace with specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for uploaded datasets and transformations
datasets = {}
transformations = {}

# --- Helper to get current content ---
def get_current_content(dataset_name: str) -> bytes:
    """Gets the latest content bytes (original or transformed)."""
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")
    if dataset_name in transformations and transformations[dataset_name]["history"]:
        # Return the content after the last transformation
        return transformations[dataset_name]["current_content"]
    else:
        # Return original content if no transformations applied yet
        return datasets[dataset_name]["content"]

def update_transformation_state(dataset_name: str, engine: str, operation: str, params_or_code: Any, generated_code: str, previous_content: bytes, new_content: bytes):
    """Updates the transformation state and history."""
    if dataset_name not in datasets: return # Should not happen if called correctly

    if dataset_name not in transformations:
        transformations[dataset_name] = {"current_content": new_content, "history": []}
    else:
        transformations[dataset_name]["current_content"] = new_content

    history_entry = {
        "engine": engine,
        "operation": operation,
        "params_or_code": params_or_code,
        "generated_code": generated_code,
        "previous_content": previous_content,
    }
    transformations[dataset_name]["history"].append(history_entry)

def _get_preview_from_content(content: bytes, engine: str, limit: int = 100, offset: int = 0) -> Dict:
    """Helper to generate preview dict from bytes using specified engine."""
    # Simplified preview generation logic based on get_dataset endpoint
    try:
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content))
            return {
                "data": df.iloc[offset:offset+limit].fillna('NaN').to_dict(orient="records"),
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
             # For preview, we create a temporary connection
            con = duckdb.connect(":memory:")
            table_name = "preview_table"
            sql_service._load_data_to_duckdb(con, table_name, content)
            query = f'SELECT * FROM "{table_name}"'
            data_dicts, columns, total_rows = sql_service._execute_sql_query(con, query, preview_limit=limit)
            con.close()
            return { "data": data_dicts, "columns": columns, "row_count": total_rows }
        else:
             raise ValueError(f"Unsupported engine for preview: {engine}")
    except Exception as e:
         # Log error details for debugging
        print(f"Error generating preview with {engine}: {e}")
        # Fallback to pandas preview if preferred engine fails
        try:
            df = pd.read_csv(io.BytesIO(content))
            return {
                "data": df.iloc[offset:offset+limit].fillna('NaN').to_dict(orient="records"),
                "columns": list(df.columns),
                "row_count": len(df)
            }
        except Exception as pd_e:
            print(f"Fallback pandas preview failed: {pd_e}")
            raise HTTPException(status_code=500, detail=f"Error processing dataset preview: {e}")


@app.get("/")
async def read_root():
    return {"message": "Data Analysis GUI API is running"}

@app.post("/test-connection")
async def test_connection():
    """Test endpoint to verify the connection is working"""
    return {"status": "success", "message": "Backend connection is working"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), dataset_name: str = Form(...)):
    try:
        contents = await file.read()
        datasets[dataset_name] = {
            "content": contents,
            "filename": file.filename
        }
        # Clear any previous transformations if overwriting
        if dataset_name in transformations:
            del transformations[dataset_name]

        # Generate initial preview (using pandas by default)
        preview_info = _get_preview_from_content(contents, engine="pandas", limit=10)

        return {
            "message": f"Successfully uploaded {file.filename}",
            "dataset_name": dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"]
        }
    except Exception as e:
        print(f"Upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Could not process file: {str(e)}")


@app.get("/datasets")
async def get_datasets_list(): # Renamed to avoid conflict
    return {"datasets": list(datasets.keys())}

@app.get("/dataset/{dataset_name}")
async def get_dataset_preview( # Renamed for clarity
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]),
    limit: int = Query(100, ge=1), # Increase default preview size
    offset: int = Query(0, ge=0)
):
    """Gets the preview of the current state of the dataset."""
    content = get_current_content(dataset_name)
    preview_info = _get_preview_from_content(content, engine, limit, offset)

    # Determine if undo/reset is possible
    can_undo = dataset_name in transformations and bool(transformations[dataset_name]["history"])
    can_reset = dataset_name in transformations and bool(transformations[dataset_name]["history"])

    # Determine the code for the *last* operation, if any
    last_code = ""
    if can_undo:
        last_code = transformations[dataset_name]["history"][-1].get("generated_code", "")


    return {
        "data": preview_info["data"],
        "columns": preview_info["columns"],
        "row_count": preview_info["row_count"],
        "can_undo": can_undo,
        "can_reset": can_reset,
        "last_code": last_code # Send code of last operation for display consistency
    }

@app.get("/dataset-info/{dataset_name}")
async def get_dataset_info(dataset_name: str):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    try:
        content = datasets[dataset_name]["content"]
        df = pd.read_csv(io.BytesIO(content))
        
        # Get basic statistics
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime']).columns.tolist()
        
        # Get column types
        column_types = {col: str(df[col].dtype) for col in df.columns}
        
        # Count missing values
        missing_values = df.isnull().sum().to_dict()
        
        # Get unique value counts for categorical columns (limit to avoid huge result)
        unique_counts = {}
        for col in categorical_cols:
            value_counts = df[col].value_counts().head(10).to_dict()
            unique_counts[col] = {
                "values": value_counts,
                "total_unique": df[col].nunique()
            }
        
        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "memory_usage": df.memory_usage(deep=True).sum(),
            "column_types": column_types,
            "numeric_columns": numeric_cols,
            "categorical_columns": categorical_cols,
            "datetime_columns": datetime_cols,
            "missing_values": missing_values,
            "unique_value_counts": unique_counts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting dataset info: {str(e)}")

@app.get("/column-stats/{dataset_name}/{column_name}")
async def get_column_stats(dataset_name: str, column_name: str, engine: str = "pandas"):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    try:
        content = datasets[dataset_name]["content"]
        
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content))
            
            if column_name not in df.columns:
                raise HTTPException(status_code=404, detail=f"Column {column_name} not found")
            
            column_data = df[column_name]
            column_type = str(column_data.dtype)
            
            stats = {
                "column_name": column_name,
                "dtype": column_type,
                "missing_count": column_data.isnull().sum(),
                "missing_percentage": (column_data.isnull().sum() / len(df)) * 100
            }
            
            if pd.api.types.is_numeric_dtype(column_data):
                # Numeric column
                stats.update({
                    "min": float(column_data.min()) if not column_data.isnull().all() else None,
                    "max": float(column_data.max()) if not column_data.isnull().all() else None,
                    "mean": float(column_data.mean()) if not column_data.isnull().all() else None,
                    "median": float(column_data.median()) if not column_data.isnull().all() else None,
                    "std": float(column_data.std()) if not column_data.isnull().all() else None,
                    "quantiles": {
                        "25%": float(column_data.quantile(0.25)) if not column_data.isnull().all() else None,
                        "50%": float(column_data.quantile(0.5)) if not column_data.isnull().all() else None,
                        "75%": float(column_data.quantile(0.75)) if not column_data.isnull().all() else None
                    },
                    "histogram": create_histogram_data(column_data)
                })
            else:
                # Categorical column
                value_counts = column_data.value_counts().head(20).to_dict()
                stats.update({
                    "unique_count": column_data.nunique(),
                    "top_values": value_counts,
                    "top_value": column_data.mode()[0] if not column_data.empty else None
                })
            
            return stats
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content))
            
            if column_name not in df.columns:
                raise HTTPException(status_code=404, detail=f"Column {column_name} not found")
            
            column_data = df.select(column_name)
            column_type = str(column_data[column_name].dtype)
            
            stats = {
                "column_name": column_name,
                "dtype": column_type,
                "missing_count": column_data.null_count().item(),
                "missing_percentage": (column_data.null_count().item() / df.height) * 100
            }
            
            if pl.datatypes.is_numeric(column_data[column_name].dtype):
                # Numeric column
                describe = df.select(pl.col(column_name).describe())
                describe_dict = {row["statistic"]: row[column_name] for row in describe.to_dicts()}
                
                stats.update({
                    "min": float(describe_dict["min"]) if "min" in describe_dict else None,
                    "max": float(describe_dict["max"]) if "max" in describe_dict else None,
                    "mean": float(describe_dict["mean"]) if "mean" in describe_dict else None,
                    "median": float(df.select(pl.col(column_name).median()).item()) if not column_data.is_empty() else None,
                    "std": float(describe_dict["std"]) if "std" in describe_dict else None,
                    "quantiles": {
                        "25%": float(df.select(pl.col(column_name).quantile(0.25)).item()) if not column_data.is_empty() else None,
                        "50%": float(df.select(pl.col(column_name).quantile(0.5)).item()) if not column_data.is_empty() else None,
                        "75%": float(df.select(pl.col(column_name).quantile(0.75)).item()) if not column_data.is_empty() else None
                    }
                })
            else:
                # Categorical column
                value_counts = df.select(pl.col(column_name).value_counts()).limit(20).to_dicts()
                value_counts_dict = {row[column_name]: row["counts"] for row in value_counts}
                
                stats.update({
                    "unique_count": df.select(pl.col(column_name).n_unique()).item(),
                    "top_values": value_counts_dict
                })
            
            return stats
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting column stats: {str(e)}")

def create_histogram_data(series):
    """Generate histogram data for visualization"""
    try:
        # Skip if all values are NaN
        if series.isnull().all():
            return None
            
        hist, bin_edges = pd.np.histogram(series.dropna(), bins=10)
        return {
            "counts": hist.tolist(),
            "bin_edges": [float(x) for x in bin_edges]
        }
    except Exception:
        return None

@app.post("/operation")
async def perform_operation(
    dataset_name: str = Form(...),
    operation: str = Form(...),
    params: str = Form(...), # JSON string
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    previous_content = get_current_content(dataset_name) # Get state *before* this op
    params_dict = json.loads(params)
    new_content = None
    result_data = None
    columns = []
    row_count = 0
    generated_code = ""

    try:
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(previous_content))
            result_df, generated_code = pandas_service.apply_pandas_operation(df, operation, params_dict)
            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            preview_info = _get_preview_from_content(new_content, engine) # Use helper

        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(previous_content))
            result_df, generated_code = polars_service.apply_polars_operation(df, operation, params_dict)
            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                new_content = buffer.getvalue()
            preview_info = _get_preview_from_content(new_content, engine)

        elif engine == "sql":
            con = duckdb.connect(":memory:")
            table_name = f"data_{dataset_name}"
            _temp_df = pd.read_csv(io.BytesIO(previous_content), nrows=0)
            all_cols = list(_temp_df.columns)

            # apply_sql_operation returns preview directly, no need for separate preview call
            preview_data, result_columns, total_rows, generated_code = sql_service.apply_sql_operation(
                con, previous_content, table_name, operation, params_dict, all_cols
            )
            preview_info = { "data": preview_data, "columns": result_columns, "row_count": total_rows }

            # Need full result for state update
            full_df = con.execute(generated_code).fetchdf()
            with io.BytesIO() as buffer:
                full_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            con.close()

        if new_content is None:
             # This check might be redundant now if services raise errors, but keep for safety
             raise HTTPException(status_code=500, detail="Operation failed internally: No new content generated.")

        # Update state *after* successful execution
        update_transformation_state(dataset_name, engine, operation, params_dict, generated_code, previous_content, new_content)

        return {
            "data": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"],
            "code": generated_code,
            "can_undo": True, # An operation just succeeded
            "can_reset": True
        }

    except (ValueError, pl.exceptions.PolarsError, duckdb.Error, pd.errors.ParserError) as ve:
         # Catch specific known operational errors (like bad params, SQL errors, load errors)
         print(f"Operation Handled Error ({engine}, {operation}): {type(ve).__name__}: {ve}")
         raise HTTPException(status_code=400, detail=f"Operation failed: {str(ve)}")
    except (TypeError, DataError) as pde:
        # Catch specific Pandas data/type errors often from aggregations
        print(f"Operation Pandas Data Error ({engine}, {operation}): {type(pde).__name__}: {pde}")
        raise HTTPException(status_code=400, detail=f"Operation failed: Invalid data type for '{params_dict.get('agg_column', 'N/A')}'? Details: {str(pde)}")
    except KeyError as ke:
        # Catch errors from trying to access non-existent columns
        print(f"Operation Key Error ({engine}, {operation}): {ke}")
        raise HTTPException(status_code=400, detail=f"Operation failed: Column not found: {str(ke)}")
    except Exception as e:
        # Catch any other unexpected errors
        print(f"Unexpected error in /operation ({engine}, {operation}): {type(e).__name__}: {e}")
        # Print stack trace for debugging unexpected errors
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected server error occurred: {str(e)}")

@app.post("/execute-code")
async def execute_custom_code(
    dataset_name: str = Form(...),
    code: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    content = get_current_content(dataset_name)
    new_content = None
    result_data = None
    columns = []
    row_count = 0

    try:
        if engine == "pandas":
            local_vars = {}
            # Provide pandas and io in the execution context
            exec("import pandas as pd\nimport io", globals(), local_vars)
            # Load current df state into 'df' variable
            exec(f"df = pd.read_csv(io.BytesIO(content))", {"io": io, "content": content}, local_vars)

            # Execute user's code
            exec(code, globals(), local_vars)

            result_df = local_vars.get('df')
            if not isinstance(result_df, pd.DataFrame):
                raise ValueError("Pandas code did not result in a DataFrame assigned to the 'df' variable.")

            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            result_data = result_df.head(100).fillna('NaN').to_dict(orient="records")
            columns = list(result_df.columns)
            row_count = len(result_df)

        elif engine == "polars":
            local_vars = {}
            exec("import polars as pl\nimport io", globals(), local_vars)
            exec(f"df = pl.read_csv(io.BytesIO(content))", {"io": io, "content": content}, local_vars)
            exec(code, globals(), local_vars)
            result_df = local_vars.get('df')
            if not isinstance(result_df, pl.DataFrame):
                 raise ValueError("Polars code did not result in a DataFrame assigned to the 'df' variable.")

            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                new_content = buffer.getvalue()
            result_data = result_df.head(100).fill_nan('NaN').to_dicts()
            columns = result_df.columns
            row_count = result_df.height

        elif engine == "sql":
            con = duckdb.connect(":memory:")
            table_name = "data_table" # Use a consistent name for SQL execution
            sql_service._load_data_to_duckdb(con, table_name, content)

            # Execute the user's SQL query
            # The query *is* the operation result
            preview_data, columns, row_count = sql_service._execute_sql_query(con, code)
            result_data = preview_data # Already limited preview

            # Update state requires full result -> CSV
            full_df = con.execute(code).fetchdf()
            with io.BytesIO() as buffer:
                full_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            con.close()

        # --- Update state and return ---
        if new_content is not None:
             update_transformation_state(dataset_name, "custom_code", code, engine, new_content, code) # For custom code, generated_code is the code itself
        else:
             raise HTTPException(status_code=500, detail="Code execution failed to produce new content state.")

        return {
            "data": result_data,
            "columns": columns,
            "row_count": row_count,
            "code": code # Return the executed code
        }

    except (SyntaxError, NameError, TypeError, ValueError, AttributeError, KeyError, pd.errors.PandasError, pl.exceptions.PolarsError, duckdb.Error) as exec_err:
         raise HTTPException(status_code=400, detail=f"Code execution failed: {type(exec_err).__name__}: {str(exec_err)}")
    except Exception as e:
        print(f"Unexpected error in /execute-code: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during code execution: {str(e)}")

# --- NEW Undo Endpoint ---
@app.post("/undo/{dataset_name}")
async def undo_last_operation(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]) # Engine for preview
):
    if dataset_name not in transformations or not transformations[dataset_name]["history"]:
        raise HTTPException(status_code=400, detail="No operations to undo.")

    history = transformations[dataset_name]["history"]
    last_op = history.pop() # Remove last operation
    restored_content = last_op["previous_content"]

    # Update current content to the restored state
    transformations[dataset_name]["current_content"] = restored_content

    # Generate preview based on the restored state using the requested engine
    preview_info = _get_preview_from_content(restored_content, engine)

    # Check if further undo/reset is possible
    can_undo = bool(history)
    can_reset = bool(history) # Can reset if any history remains

     # Determine the code for the *new* last operation (if any)
    last_code = ""
    if can_undo:
        last_code = history[-1].get("generated_code", "")


    return {
        "message": f"Undid last operation ({last_op['operation']})",
        "data": preview_info["data"],
        "columns": preview_info["columns"],
        "row_count": preview_info["row_count"],
        "can_undo": can_undo,
        "can_reset": can_reset,
        "last_code": last_code
    }

# --- NEW Reset Endpoint ---
@app.post("/reset/{dataset_name}")
async def reset_transformations(
    dataset_name: str,
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]) # Engine for preview
):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found.")

    # Remove the transformation history and state
    if dataset_name in transformations:
        del transformations[dataset_name]

    # Get the original content
    original_content = datasets[dataset_name]["content"]

    # Generate preview based on the original state
    preview_info = _get_preview_from_content(original_content, engine)

    return {
        "message": f"Reset transformations for {dataset_name}",
        "data": preview_info["data"],
        "columns": preview_info["columns"],
        "row_count": preview_info["row_count"],
        "can_undo": False, # Reset means no history
        "can_reset": False,
        "last_code": "" # No code after reset
    }

@app.post("/save-transformation")
async def save_transformation(
    dataset_name: str = Form(...),
    new_dataset_name: str = Form(...),
    engine: str = Form(default="pandas") # Engine used for preview, not critical here
):
    content_to_save = get_current_content(dataset_name) # Get latest state

    # Save as a new *original* dataset
    datasets[new_dataset_name] = {
        "content": content_to_save,
        "filename": f"{new_dataset_name}.csv"
    }
    # Clear any potential transforms under the new name if overwriting
    if new_dataset_name in transformations:
        del transformations[new_dataset_name]

    try:
        preview_info = _get_preview_from_content(content_to_save, engine, limit=10)
        return {
            "message": f"Successfully saved transformation as {new_dataset_name}",
            "dataset_name": new_dataset_name,
            "preview": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"]
        }
    except Exception as e:
        # Clean up the potentially saved dataset if preview fails? Maybe not critical.
        raise HTTPException(status_code=500, detail=f"Error generating preview for saved transformation: {str(e)}")

@app.get("/export/{dataset_name}")
async def export_dataset(
    dataset_name: str,
    format: str = Query("csv", enum=["csv", "json", "excel"]),
    engine: str = Query("pandas", enum=["pandas", "polars", "sql"]) # Used for JSON/Excel conversion
):
    content = get_current_content(dataset_name) # Get latest state

    try:
        if format == "csv":
            media_type="text/csv"
            filename = f"{dataset_name}.csv"
            file_content = content
        elif format == "json":
            media_type="application/json"
            filename = f"{dataset_name}.json"
            if engine == "polars":
                df = pl.read_csv(io.BytesIO(content))
                # Polars to_json might need buffer or file path depending on version
                # Using pandas as intermediate might be simpler for now
                # file_content = df.to_json(pretty=False) # Or df.write_json(buffer)
                df_pd = pd.read_csv(io.BytesIO(content)) # Use pandas for JSON consistency
                file_content = df_pd.to_json(orient="records", date_format="iso")

            else: # Default to pandas for JSON export
                df = pd.read_csv(io.BytesIO(content))
                file_content = df.to_json(orient="records", date_format="iso")
        elif format == "excel":
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"{dataset_name}.xlsx"
            # Excel export typically needs pandas
            df = pd.read_csv(io.BytesIO(content))
            with io.BytesIO() as buffer:
                df.to_excel(buffer, index=False)
                file_content = buffer.getvalue()
        else: # Should be caught by Query enum, but belt-and-suspenders
             raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")

        return Response(
            content=file_content,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        print(f"Export Error ({format}, {engine}): {e}")
        raise HTTPException(status_code=500, detail=f"Error exporting dataset: {str(e)}")

@app.post("/merge-datasets")
async def merge_datasets_endpoint(
    left_dataset: str = Form(...),
    right_dataset: str = Form(...),
    params: str = Form(...),
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    # Get state of left dataset *before* merge
    previous_content_left = get_current_content(left_dataset)
    # Get current content of right dataset (could also be transformed)
    right_content = get_current_content(right_dataset)
    params_dict = json.loads(params)

    new_content = None
    preview_info = {}
    generated_code = ""

    try:
        # --- Pandas Merge ---
        if engine == "pandas":
            left_df = pd.read_csv(io.BytesIO(previous_content_left))
            right_df = pd.read_csv(io.BytesIO(right_content))
            result_df, generated_code = pandas_service.apply_pandas_merge(left_df, right_df, params_dict)
            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            preview_info = _get_preview_from_content(new_content, engine)

        # --- Polars Join ---
        elif engine == "polars":
            left_df = pl.read_csv(io.BytesIO(previous_content_left))
            right_df = pl.read_csv(io.BytesIO(right_content))
            result_df, generated_code = polars_service.apply_polars_join(left_df, right_df, params_dict)
            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                new_content = buffer.getvalue()
            preview_info = _get_preview_from_content(new_content, engine)

        # --- SQL Join ---
        elif engine == "sql":
            con = duckdb.connect(":memory:")
            l_table, r_table = "left_table", "right_table"
            sql_service._load_data_to_duckdb(con, l_table, previous_content_left)
            sql_service._load_data_to_duckdb(con, r_table, right_content)
            left_cols = [c[0] for c in con.execute(f'DESCRIBE "{l_table}"').fetchall()]
            right_cols = [c[0] for c in con.execute(f'DESCRIBE "{r_table}"').fetchall()]

            preview_data, result_columns, total_rows, generated_code = sql_service.apply_sql_join(
                con, l_table, r_table, params_dict, left_cols, right_cols
            )
            preview_info = { "data": preview_data, "columns": result_columns, "row_count": total_rows }

            full_df = con.execute(generated_code).fetchdf()
            with io.BytesIO() as buffer:
                full_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            con.close()

        if new_content is None:
             raise HTTPException(status_code=500, detail="Merge failed internally.")

        # Update state of the *left* dataset
        update_transformation_state(left_dataset, engine, "merge", params_dict, generated_code, previous_content_left, new_content)

        return {
            "message": f"Merged {left_dataset} and {right_dataset}. Result updated for {left_dataset}.",
            "data": preview_info["data"],
            "columns": preview_info["columns"],
            "row_count": preview_info["row_count"],
            "code": generated_code,
            "can_undo": True,
            "can_reset": True
        }

    except (ValueError, pl.exceptions.PolarsError, duckdb.Error, pd.errors.PandasError) as ve:
        raise HTTPException(status_code=400, detail=f"Merge failed: {str(ve)}")
    except Exception as e:
        print(f"Unexpected error in /merge-datasets: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during merge: {str(e)}")

@app.post("/regex-operation")
async def regex_operation(
    dataset_name: str = Form(...),
    operation: str = Form(...), # e.g., "filter_contains", "extract", "replace"
    params: str = Form(...), # JSON string containing: column, regex, new_column (optional), replacement (optional), case_sensitive (optional), group (optional)
    engine: str = Form(default="pandas", enum=["pandas", "polars", "sql"])
):
    try:
        previous_content = get_current_content(dataset_name)
    except HTTPException as e:
        raise e

    params_dict = json.loads(params)
    new_content = None
    preview_info = {}
    generated_code = ""
    operation_name = f"regex_{operation}" # For history

    try:
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(previous_content), low_memory=False)
            # Delegate actual regex logic to pandas_service
            result_df, generated_code = pandas_service.apply_pandas_regex(df, operation, params_dict)

            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                new_content = buffer.getvalue()
            preview_info = _get_preview_from_content(new_content, engine)

        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(previous_content))
            # Delegate actual regex logic to polars_service
            result_df, generated_code = polars_service.apply_polars_regex(df, operation, params_dict)

            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                new_content = buffer.getvalue()
            preview_info = _get_preview_from_content(new_content, engine)

        elif engine == "sql":
            # Delegate to sql_service which should return full content bytes and code
             # NOTE: Assumes sql_service is updated to handle regex and return full result + code
            con = duckdb.connect(":memory:")
            table_name = f"data_{dataset_name.replace('-', '_')}"

            # This service function needs to be implemented/updated
            new_content, generated_code = sql_service.apply_sql_regex_get_full(
                 con, previous_content, table_name, operation, params_dict
            )
            con.close()

            if new_content is None:
                 raise ValueError("SQL regex operation failed to produce results.")

            preview_info = _get_preview_from_content(new_content, engine)


        if new_content is None:
             raise HTTPException(status_code=500, detail=f"Regex operation '{operation}' failed internally.")

        # Update state after successful execution
        update_transformation_state(dataset_name, engine, operation_name, params_dict, generated_code, previous_content, new_content)

        can_undo = dataset_name in transformations and bool(transformations[dataset_name].get("history"))
        can_reset = can_undo

        return {
            "message": f"Successfully applied regex '{operation}' on column '{params_dict.get('column', 'N/A')}'.",
            "data": preview_info.get("data", []),
            "columns": preview_info.get("columns", []),
            "row_count": preview_info.get("row_count", 0),
            "code": generated_code,
            "can_undo": can_undo,
            "can_reset": can_reset
        }

    # Catch specific errors from services or regex processing
    except (ValueError, TypeError, KeyError, pd.errors.PandasError, pl.exceptions.PolarsError, duckdb.Error, re.error, json.JSONDecodeError) as op_err:
        print(f"Regex Operation Error ({engine}, {operation}): {type(op_err).__name__}: {op_err}")
        # Provide more specific error if possible (e.g., invalid regex)
        detail = f"Regex operation '{operation}' failed: {str(op_err)}"
        if isinstance(op_err, KeyError):
            detail = f"Regex operation failed: Column not found: {str(op_err)}"
        elif isinstance(op_err, re.error):
            detail = f"Regex operation failed: Invalid regular expression pattern: {str(op_err)}"
        raise HTTPException(status_code=400, detail=detail)
    except Exception as e:
        print(f"Unexpected error in /regex-operation ({engine}, {operation}): {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected server error occurred during regex operation: {str(e)}")