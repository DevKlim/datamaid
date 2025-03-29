from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import polars as pl
import duckdb
import io
import json
import re
import tempfile
from typing import Optional, List, Dict, Any, Union

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
        # Print some debug information
        print(f"Received file: {file.filename}, size: {file.size if hasattr(file, 'size') else 'unknown'}")
        print(f"Dataset name: {dataset_name}")
        
        # Read the file content
        contents = await file.read()
        print(f"Successfully read file content, size: {len(contents)} bytes")
        
        # Store the file content in memory
        datasets[dataset_name] = {
            "content": contents,
            "filename": file.filename
        }
        
        # Try to read the CSV to validate it
        try:
            df = pd.read_csv(io.BytesIO(contents))
            preview = df.head(10).to_dict(orient="records")
            columns = list(df.columns)
            row_count = len(df)
            
            print(f"Successfully parsed CSV with {row_count} rows and {len(columns)} columns")
            
        except Exception as e:
            print(f"CSV parsing error: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Invalid CSV file: {str(e)}")
        
        return {
            "message": f"Successfully uploaded {file.filename}",
            "dataset_name": dataset_name,
            "preview": preview,
            "columns": columns,
            "row_count": row_count
        }
    except Exception as e:
        print(f"Upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Could not process file: {str(e)}")


@app.get("/datasets")
async def get_datasets():
    return {"datasets": list(datasets.keys())}

@app.get("/dataset/{dataset_name}")
async def get_dataset(
    dataset_name: str, 
    engine: str = "pandas", 
    limit: int = 10,
    offset: int = 0
):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    try:
        content = datasets[dataset_name]["content"]
        
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content))
            return {
                "data": df.iloc[offset:offset+limit].to_dict(orient="records"),
                "columns": list(df.columns),
                "row_count": len(df)
            }
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content))
            return {
                "data": df.slice(offset, limit).to_dicts(),
                "columns": df.columns,
                "row_count": df.height
            }
        elif engine == "sql":
            # Create an in-memory DuckDB database
            con = duckdb.connect(":memory:")
            
            # Write the CSV to a temporary file for DuckDB to read
            with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
                tmp.write(content)
                tmp.flush()
                
                # Register the CSV as a table
                con.execute(f"CREATE TABLE temp_data AS SELECT * FROM read_csv_auto('{tmp.name}')")
            
            # Read the data with pagination
            result = con.execute(f"SELECT * FROM temp_data LIMIT {limit} OFFSET {offset}").fetchall()
            columns = [desc[0] for desc in con.description]
            
            # Get row count
            row_count = con.execute("SELECT COUNT(*) FROM temp_data").fetchone()[0]
            
            return {
                "data": [dict(zip(columns, row)) for row in result],
                "columns": columns,
                "row_count": row_count
            }
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing dataset: {str(e)}")

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
    params: str = Form(...),
    engine: str = Form(default="pandas")
):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    try:
        content = datasets[dataset_name]["content"]
        params_dict = json.loads(params)
        
        # Store current state for this dataset if not already tracked
        if dataset_name not in transformations:
            transformations[dataset_name] = {
                "original_content": content,
                "current_content": content,
                "history": []
            }
        
        # Load data based on engine
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(transformations[dataset_name]["current_content"]))
            result_df, code = process_pandas_operation(df, operation, params_dict)
            
            # Update the current state
            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                buffer.seek(0)
                transformations[dataset_name]["current_content"] = buffer.getvalue()
                transformations[dataset_name]["history"].append({
                    "operation": operation,
                    "params": params_dict,
                    "code": code
                })
            
            return {
                "data": result_df.head(100).to_dict(orient="records"),
                "columns": list(result_df.columns),
                "row_count": len(result_df),
                "code": code
            }
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(transformations[dataset_name]["current_content"]))
            result_df, code = process_polars_operation(df, operation, params_dict)
            
            # Update the current state
            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                buffer.seek(0)
                transformations[dataset_name]["current_content"] = buffer.getvalue()
                transformations[dataset_name]["history"].append({
                    "operation": operation,
                    "params": params_dict,
                    "code": code
                })
            
            return {
                "data": result_df.head(100).to_dicts(),
                "columns": result_df.columns,
                "row_count": result_df.height,
                "code": code
            }
        elif engine == "sql":
            # Here we would process SQL operations
            con = duckdb.connect(":memory:")
            result_data, columns, row_count, code = process_sql_operation(
                con, 
                transformations[dataset_name]["current_content"], 
                operation, 
                params_dict, 
                dataset_name
            )
            
            # For SQL, we need to convert back to a DataFrame and store the CSV
            with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
                df_result = pd.DataFrame(result_data)
                df_result.to_csv(tmp.name, index=False)
                with open(tmp.name, 'rb') as f:
                    transformations[dataset_name]["current_content"] = f.read()
                    transformations[dataset_name]["history"].append({
                        "operation": operation,
                        "params": params_dict,
                        "code": code
                    })
            
            return {
                "data": result_data,
                "columns": columns,
                "row_count": row_count,
                "code": code
            }
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing operation: {str(e)}")

@app.post("/execute-code")
async def execute_custom_code(
    dataset_name: str = Form(...),
    code: str = Form(...),
    engine: str = Form(default="pandas")
):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    try:
        # Get the current content (which might be transformed from original)
        if dataset_name in transformations:
            content = transformations[dataset_name]["current_content"]
        else:
            content = datasets[dataset_name]["content"]
            
        if engine == "pandas":
            # Set up environment
            local_vars = {}
            
            # Load the DataFrame
            exec("import pandas as pd", globals(), local_vars)
            exec(f"df = pd.read_csv(io.BytesIO(content))", 
                {"io": io, "content": content}, 
                local_vars)
            
            # Execute the user code
            exec(code, globals(), local_vars)
            
            # The result should be stored in the 'df' variable
            result_df = local_vars.get('df')
            
            if result_df is None or not isinstance(result_df, pd.DataFrame):
                raise HTTPException(
                    status_code=400, 
                    detail="Execution did not produce a valid DataFrame. Make sure your code assigns the result to 'df'."
                )
            
            # Update the current state
            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                buffer.seek(0)
                
                if dataset_name not in transformations:
                    transformations[dataset_name] = {
                        "original_content": datasets[dataset_name]["content"],
                        "current_content": buffer.getvalue(),
                        "history": []
                    }
                else:
                    transformations[dataset_name]["current_content"] = buffer.getvalue()
                
                transformations[dataset_name]["history"].append({
                    "operation": "custom_code",
                    "code": code
                })
            
            return {
                "data": result_df.head(100).to_dict(orient="records"),
                "columns": list(result_df.columns),
                "row_count": len(result_df),
                "code": code
            }
        elif engine == "polars":
            # Set up environment
            local_vars = {}
            
            # Load the DataFrame
            exec("import polars as pl", globals(), local_vars)
            exec(f"df = pl.read_csv(io.BytesIO(content))", 
                {"io": io, "content": content}, 
                local_vars)
            
            # Execute the user code
            exec(code, globals(), local_vars)
            
            # The result should be stored in the 'df' variable
            result_df = local_vars.get('df')
            
            if result_df is None or not isinstance(result_df, pl.DataFrame):
                raise HTTPException(
                    status_code=400, 
                    detail="Execution did not produce a valid DataFrame. Make sure your code assigns the result to 'df'."
                )
            
            # Update the current state
            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                buffer.seek(0)
                
                if dataset_name not in transformations:
                    transformations[dataset_name] = {
                        "original_content": datasets[dataset_name]["content"],
                        "current_content": buffer.getvalue(),
                        "history": []
                    }
                else:
                    transformations[dataset_name]["current_content"] = buffer.getvalue()
                
                transformations[dataset_name]["history"].append({
                    "operation": "custom_code",
                    "code": code
                })
            
            return {
                "data": result_df.head(100).to_dicts(),
                "columns": result_df.columns,
                "row_count": result_df.height,
                "code": code
            }
        elif engine == "sql":
            # Set up DuckDB connection and execute SQL
            con = duckdb.connect(":memory:")
            
            # Load data into DuckDB
            with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
                tmp.write(content)
                tmp.flush()
                
                # Create temp table from CSV
                con.execute(f"CREATE TABLE temp_data AS SELECT * FROM read_csv_auto('{tmp.name}')")
            
            # Execute the SQL query
            try:
                result = con.execute(code).fetchall()
                columns = [desc[0] for desc in con.description]
                row_count = len(result)
                
                # Convert to list of dictionaries
                result_data = [dict(zip(columns, row)) for row in result]
                
                # Update transformation state by first converting SQL result to a CSV
                df_result = pd.DataFrame(result_data)
                
                with io.BytesIO() as buffer:
                    df_result.to_csv(buffer, index=False)
                    buffer.seek(0)
                    
                    if dataset_name not in transformations:
                        transformations[dataset_name] = {
                            "original_content": datasets[dataset_name]["content"],
                            "current_content": buffer.getvalue(),
                            "history": []
                        }
                    else:
                        transformations[dataset_name]["current_content"] = buffer.getvalue()
                    
                    transformations[dataset_name]["history"].append({
                        "operation": "custom_code",
                        "code": code
                    })
                
                return {
                    "data": result_data[:100],  # Limit to 100 rows
                    "columns": columns,
                    "row_count": row_count,
                    "code": code
                }
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"SQL error: {str(e)}")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing code: {str(e)}")

@app.post("/save-transformation")
async def save_transformation(
    dataset_name: str = Form(...),
    new_dataset_name: str = Form(...),
    engine: str = Form(default="pandas")
):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    if dataset_name not in transformations:
        raise HTTPException(status_code=400, detail=f"No transformations exist for {dataset_name}")
    
    # Save current state as a new dataset
    datasets[new_dataset_name] = {
        "content": transformations[dataset_name]["current_content"],
        "filename": f"{new_dataset_name}.csv"
    }
    
    # Get a preview of the new dataset
    try:
        df = pd.read_csv(io.BytesIO(datasets[new_dataset_name]["content"]))
        preview = df.head(10).to_dict(orient="records")
        columns = list(df.columns)
        row_count = len(df)
        
        return {
            "message": f"Successfully saved transformation as {new_dataset_name}",
            "dataset_name": new_dataset_name,
            "preview": preview,
            "columns": columns,
            "row_count": row_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving transformation: {str(e)}")

@app.get("/export/{dataset_name}")
async def export_dataset(
    dataset_name: str,
    format: str = "csv", 
    engine: str = "pandas"
):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    # Get current content (original or transformed)
    content = datasets[dataset_name]["content"]
    if dataset_name in transformations:
        content = transformations[dataset_name]["current_content"]
    
    try:
        if format == "csv":
            return Response(
                content=content,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={dataset_name}.csv"}
            )
        elif format == "json":
            if engine == "pandas":
                df = pd.read_csv(io.BytesIO(content))
                json_content = df.to_json(orient="records", date_format="iso")
                return Response(
                    content=json_content,
                    media_type="application/json",
                    headers={"Content-Disposition": f"attachment; filename={dataset_name}.json"}
                )
            elif engine == "polars":
                df = pl.read_csv(io.BytesIO(content))
                json_content = df.to_json()
                return Response(
                    content=json_content,
                    media_type="application/json",
                    headers={"Content-Disposition": f"attachment; filename={dataset_name}.json"}
                )
        elif format == "excel":
            df = pd.read_csv(io.BytesIO(content))
            
            with io.BytesIO() as buffer:
                df.to_excel(buffer, index=False)
                buffer.seek(0)
                excel_content = buffer.getvalue()
            
            return Response(
                content=excel_content,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={dataset_name}.xlsx"}
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported export format: {format}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting dataset: {str(e)}")

@app.post("/merge-datasets")
async def merge_datasets(
    left_dataset: str = Form(...),
    right_dataset: str = Form(...),
    params: str = Form(...),
    engine: str = Form(default="pandas")
):
    if left_dataset not in datasets:
        raise HTTPException(status_code=404, detail=f"Left dataset {left_dataset} not found")
    
    if right_dataset not in datasets:
        raise HTTPException(status_code=404, detail=f"Right dataset {right_dataset} not found")
    
    try:
        # Get current content for both datasets
        left_content = datasets[left_dataset]["content"]
        if left_dataset in transformations:
            left_content = transformations[left_dataset]["current_content"]
            
        right_content = datasets[right_dataset]["content"]
        if right_dataset in transformations:
            right_content = transformations[right_dataset]["current_content"]
        
        params_dict = json.loads(params)
        
        if engine == "pandas":
            left_df = pd.read_csv(io.BytesIO(left_content))
            right_df = pd.read_csv(io.BytesIO(right_content))
            
            # Get merge parameters
            how = params_dict.get("join_type", "inner")
            left_on = params_dict.get("left_on")
            right_on = params_dict.get("right_on")
            
            # Perform merge
            result_df = pd.merge(
                left_df,
                right_df,
                how=how,
                left_on=left_on,
                right_on=right_on
            )
            
            # Generate code
            code = f"# Merge {left_dataset} with {right_dataset}\n"
            code += f"result_df = pd.merge(\n"
            code += f"    left_df,\n"
            code += f"    right_df,\n"
            code += f"    how='{how}',\n"
            code += f"    left_on='{left_on}',\n"
            code += f"    right_on='{right_on}'\n"
            code += f")"
            
            return {
                "data": result_df.head(100).to_dict(orient="records"),
                "columns": list(result_df.columns),
                "row_count": len(result_df),
                "code": code
            }
        elif engine == "polars":
            left_df = pl.read_csv(io.BytesIO(left_content))
            right_df = pl.read_csv(io.BytesIO(right_content))
            
            # Get merge parameters
            how = params_dict.get("join_type", "inner")
            left_on = params_dict.get("left_on")
            right_on = params_dict.get("right_on")
            
            # Perform join
            result_df = left_df.join(
                right_df,
                left_on=left_on,
                right_on=right_on,
                how=how
            )
            
            # Generate code
            code = f"# Join {left_dataset} with {right_dataset}\n"
            code += f"result_df = left_df.join(\n"
            code += f"    right_df,\n"
            code += f"    left_on='{left_on}',\n"
            code += f"    right_on='{right_on}',\n"
            code += f"    how='{how}'\n"
            code += f")"
            
            return {
                "data": result_df.head(100).to_dicts(),
                "columns": result_df.columns,
                "row_count": result_df.height,
                "code": code
            }
        elif engine == "sql":
            # Set up DuckDB connection
            con = duckdb.connect(":memory:")
            
            # Load both datasets into temporary tables
            with tempfile.NamedTemporaryFile(suffix='.csv') as left_tmp, tempfile.NamedTemporaryFile(suffix='.csv') as right_tmp:
                left_tmp.write(left_content)
                left_tmp.flush()
                
                right_tmp.write(right_content)
                right_tmp.flush()
                
                # Create tables
                con.execute(f"CREATE TABLE left_table AS SELECT * FROM read_csv_auto('{left_tmp.name}')")
                con.execute(f"CREATE TABLE right_table AS SELECT * FROM read_csv_auto('{right_tmp.name}')")
            
            # Get join parameters
            how = params_dict.get("join_type", "inner").upper()
            left_on = params_dict.get("left_on")
            right_on = params_dict.get("right_on")
            
            # Construct and execute SQL query
            join_sql = f"""
            SELECT *
            FROM left_table
            {how} JOIN right_table
            ON left_table."{left_on}" = right_table."{right_on}"
            """
            
            result = con.execute(join_sql).fetchall()
            columns = [desc[0] for desc in con.description]
            row_count = len(result)
            
            # Convert to list of dictionaries
            result_data = [dict(zip(columns, row)) for row in result[:100]]
            
            return {
                "data": result_data,
                "columns": columns,
                "row_count": row_count,
                "code": join_sql
            }
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error merging datasets: {str(e)}")

@app.post("/regex-operation")
async def regex_operation(
    dataset_name: str = Form(...),
    operation: str = Form(...),
    regex: str = Form(...),
    options: str = Form(...),
    engine: str = Form(default="pandas")
):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    try:
        # Get current content
        content = datasets[dataset_name]["content"]
        if dataset_name in transformations:
            content = transformations[dataset_name]["current_content"]
        
        options_dict = json.loads(options)
        
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content))
            
            if operation == "filter_contains":
                column = options_dict.get("column")
                case_sensitive = options_dict.get("case_sensitive", False)
                
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column {column} not found")
                
                result_df = df[df[column].astype(str).str.contains(
                    regex, 
                    case=case_sensitive,
                    regex=True,
                    na=False
                )]
                
                code = f"# Filter rows where {column} contains regex pattern\n"
                code += f"df = df[df['{column}'].astype(str).str.contains(\n"
                code += f"    r'{regex}',\n"
                code += f"    case={case_sensitive},\n"
                code += f"    regex=True,\n"
                code += f"    na=False\n"
                code += f")]"
            
            elif operation == "extract":
                column = options_dict.get("column")
                new_column = options_dict.get("new_column", f"{column}_extracted")
                group_idx = options_dict.get("group", 0)
                
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column {column} not found")
                
                df[new_column] = df[column].astype(str).str.extract(f'({regex})', expand=False)
                result_df = df
                
                code = f"# Extract data using regex pattern\n"
                code += f"df['{new_column}'] = df['{column}'].astype(str).str.extract(r'({regex})', expand=False)"
            
            elif operation == "replace":
                column = options_dict.get("column")
                replacement = options_dict.get("replacement", "")
                
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column {column} not found")
                
                df[column] = df[column].astype(str).str.replace(
                    regex,
                    replacement,
                    regex=True
                )
                result_df = df
                
                code = f"# Replace text using regex pattern\n"
                code += f"df['{column}'] = df['{column}'].astype(str).str.replace(\n"
                code += f"    r'{regex}',\n"
                code += f"    '{replacement}',\n"
                code += f"    regex=True\n"
                code += f")"
            
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported regex operation: {operation}")
            
            # Update the current state
            with io.BytesIO() as buffer:
                result_df.to_csv(buffer, index=False)
                buffer.seek(0)
                
                if dataset_name not in transformations:
                    transformations[dataset_name] = {
                        "original_content": datasets[dataset_name]["content"],
                        "current_content": buffer.getvalue(),
                        "history": []
                    }
                else:
                    transformations[dataset_name]["current_content"] = buffer.getvalue()
                
                transformations[dataset_name]["history"].append({
                    "operation": f"regex_{operation}",
                    "regex": regex,
                    "options": options_dict,
                    "code": code
                })
            
            return {
                "data": result_df.head(100).to_dict(orient="records"),
                "columns": list(result_df.columns),
                "row_count": len(result_df),
                "code": code
            }
        
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content))
            
            if operation == "filter_contains":
                column = options_dict.get("column")
                case_sensitive = options_dict.get("case_sensitive", False)
                
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column {column} not found")
                
                result_df = df.filter(
                    pl.col(column).cast(pl.Utf8).str.contains(
                        regex,
                        literal=False,
                        case_sensitive=case_sensitive
                    )
                )
                
                code = f"# Filter rows where {column} contains regex pattern\n"
                code += f"df = df.filter(\n"
                code += f"    pl.col('{column}').cast(pl.Utf8).str.contains(\n"
                code += f"        r'{regex}',\n"
                code += f"        literal=False,\n"
                code += f"        case_sensitive={case_sensitive}\n"
                code += f"    )\n"
                code += f")"
            
            elif operation == "extract":
                column = options_dict.get("column")
                new_column = options_dict.get("new_column", f"{column}_extracted")
                group_idx = options_dict.get("group", 0)
                
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column {column} not found")
                
                result_df = df.with_column(
                    pl.col(column).cast(pl.Utf8).str.extract(regex, group_index=group_idx).alias(new_column)
                )
                
                code = f"# Extract data using regex pattern\n"
                code += f"df = df.with_column(\n"
                code += f"    pl.col('{column}').cast(pl.Utf8).str.extract(\n"
                code += f"        r'{regex}',\n"
                code += f"        group_index={group_idx}\n"
                code += f"    ).alias('{new_column}')\n"
                code += f")"
            
            elif operation == "replace":
                column = options_dict.get("column")
                replacement = options_dict.get("replacement", "")
                
                if column not in df.columns:
                    raise HTTPException(status_code=400, detail=f"Column {column} not found")
                
                result_df = df.with_column(
                    pl.col(column).cast(pl.Utf8).str.replace_all(regex, replacement).alias(column)
                )
                
                code = f"# Replace text using regex pattern\n"
                code += f"df = df.with_column(\n"
                code += f"    pl.col('{column}').cast(pl.Utf8).str.replace_all(\n"
                code += f"        r'{regex}',\n"
                code += f"        '{replacement}'\n"
                code += f"    ).alias('{column}')\n"
                code += f")"
            
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported regex operation: {operation}")
            
            # Update the current state
            with io.BytesIO() as buffer:
                result_df.write_csv(buffer)
                buffer.seek(0)
                
                if dataset_name not in transformations:
                    transformations[dataset_name] = {
                        "original_content": datasets[dataset_name]["content"],
                        "current_content": buffer.getvalue(),
                        "history": []
                    }
                else:
                    transformations[dataset_name]["current_content"] = buffer.getvalue()
                
                transformations[dataset_name]["history"].append({
                    "operation": f"regex_{operation}",
                    "regex": regex,
                    "options": options_dict,
                    "code": code
                })
            
            return {
                "data": result_df.head(100).to_dicts(),
                "columns": result_df.columns,
                "row_count": result_df.height,
                "code": code
            }
        
        elif engine == "sql":
            # Set up DuckDB connection
            con = duckdb.connect(":memory:")
            
            # Load data into DuckDB
            with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
                tmp.write(content)
                tmp.flush()
                
                # Create temp table from CSV
                con.execute(f"CREATE TABLE temp_data AS SELECT * FROM read_csv_auto('{tmp.name}')")
            
            if operation == "filter_contains":
                column = options_dict.get("column")
                case_sensitive = options_dict.get("case_sensitive", False)
                
                # # Construct SQL query with REGEXP_MATCHES
                # sql_query = fr"""
                # SELECT *
                # FROM temp_data
                # WHERE REGEXP_MATCHES("{column}", '{regex}'{', \'i\'' if not case_sensitive else ''})
                # """
                
                result = con.execute(sql_query).fetchall()
                columns = [desc[0] for desc in con.description]
                row_count = len(result)
                
                # Convert to list of dictionaries
                result_data = [dict(zip(columns, row)) for row in result[:100]]
                
                return {
                    "data": result_data,
                    "columns": columns,
                    "row_count": row_count,
                    "code": sql_query
                }
            
            elif operation == "extract":
                column = options_dict.get("column")
                new_column = options_dict.get("new_column", f"{column}_extracted")
                
                # Construct SQL query with REGEXP_EXTRACT
                sql_query = f"""
                SELECT *,
                       REGEXP_EXTRACT("{column}", '{regex}') AS "{new_column}"
                FROM temp_data
                """
                
                result = con.execute(sql_query).fetchall()
                columns = [desc[0] for desc in con.description]
                row_count = len(result)
                
                # Convert to list of dictionaries
                result_data = [dict(zip(columns, row)) for row in result[:100]]
                
                return {
                    "data": result_data,
                    "columns": columns,
                    "row_count": row_count,
                    "code": sql_query
                }
            
            elif operation == "replace":
                column = options_dict.get("column")
                replacement = options_dict.get("replacement", "")
                
                # Construct SQL query with REGEXP_REPLACE
                sql_query = f"""
                SELECT *,
                       REGEXP_REPLACE("{column}", '{regex}', '{replacement}') AS "{column}_replaced"
                FROM temp_data
                """
                
                result = con.execute(sql_query).fetchall()
                columns = [desc[0] for desc in con.description]
                row_count = len(result)
                
                # Convert to list of dictionaries
                result_data = [dict(zip(columns, row)) for row in result[:100]]
                
                return {
                    "data": result_data,
                    "columns": columns,
                    "row_count": row_count,
                    "code": sql_query
                }
            
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported regex operation: {operation}")
        
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported engine: {engine}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error performing regex operation: {str(e)}")
    
# Add these functions to the main.py file if they're not already defined

def process_pandas_operation(df, operation, params):
    """Process pandas operations on DataFrame."""
    code = ""
    
    if operation == "filter":
        column = params.get("column")
        operator = params.get("operator")
        value = params.get("value")
        
        if not all([column, operator]):
            raise ValueError("Column and operator are required for filter operation")
        
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        # Convert value to appropriate type based on column data
        try:
            if pd.api.types.is_numeric_dtype(df[column]):
                value = float(value) if '.' in value else int(value)
        except (ValueError, TypeError):
            # Keep as string if conversion fails
            pass
        
        # Create filter condition based on operator
        if operator == "==":
            code = f"df = df[df['{column}'] == {repr(value)}]"
            result_df = df[df[column] == value]
        elif operator == "!=":
            code = f"df = df[df['{column}'] != {repr(value)}]"
            result_df = df[df[column] != value]
        elif operator == ">":
            code = f"df = df[df['{column}'] > {repr(value)}]"
            result_df = df[df[column] > value]
        elif operator == "<":
            code = f"df = df[df['{column}'] < {repr(value)}]"
            result_df = df[df[column] < value]
        elif operator == ">=":
            code = f"df = df[df['{column}'] >= {repr(value)}]"
            result_df = df[df[column] >= value]
        elif operator == "<=":
            code = f"df = df[df['{column}'] <= {repr(value)}]"
            result_df = df[df[column] <= value]
        elif operator == "contains":
            code = f"df = df[df['{column}'].astype(str).str.contains({repr(value)}, na=False)]"
            result_df = df[df[column].astype(str).str.contains(value, na=False)]
        elif operator == "startswith":
            code = f"df = df[df['{column}'].astype(str).str.startswith({repr(value)}, na=False)]"
            result_df = df[df[column].astype(str).str.startswith(value, na=False)]
        elif operator == "endswith":
            code = f"df = df[df['{column}'].astype(str).str.endswith({repr(value)}, na=False)]"
            result_df = df[df[column].astype(str).str.endswith(value, na=False)]
        elif operator == "regex":
            code = f"df = df[df['{column}'].astype(str).str.contains({repr(value)}, regex=True, na=False)]"
            result_df = df[df[column].astype(str).str.contains(value, regex=True, na=False)]
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    
    elif operation == "select_columns":
        selected_columns = params.get("selected_columns", [])
        
        if not selected_columns:
            raise ValueError("No columns selected for the operation")
        
        # Verify all columns exist
        missing_columns = [col for col in selected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columns not found: {', '.join(missing_columns)}")
        
        code = f"df = df[{repr(selected_columns)}]"
        result_df = df[selected_columns]
    
    elif operation == "sort":
        sort_column = params.get("sort_column")
        sort_order = params.get("sort_order", "ascending")
        
        if not sort_column:
            raise ValueError("Sort column is required")
        
        if sort_column not in df.columns:
            raise ValueError(f"Column '{sort_column}' not found in dataset")
        
        ascending = sort_order == "ascending"
        code = f"df = df.sort_values('{sort_column}', ascending={ascending})"
        result_df = df.sort_values(sort_column, ascending=ascending)
    
    elif operation == "rename":
        renames = params.get("renames", [])
        
        if not renames:
            raise ValueError("No column renames specified")
        
        rename_dict = {item["old_name"]: item["new_name"] for item in renames if "old_name" in item and "new_name" in item}
        
        if not rename_dict:
            raise ValueError("Invalid rename parameters")
        
        # Check if all old_name columns exist
        missing_columns = [col for col in rename_dict.keys() if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columns not found: {', '.join(missing_columns)}")
        
        code = f"df = df.rename(columns={repr(rename_dict)})"
        result_df = df.rename(columns=rename_dict)
    
    elif operation == "drop_columns":
        drop_columns = params.get("drop_columns", [])
        
        if not drop_columns:
            raise ValueError("No columns selected for dropping")
        
        # Check if all columns exist
        missing_columns = [col for col in drop_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columns not found: {', '.join(missing_columns)}")
        
        code = f"df = df.drop(columns={repr(drop_columns)})"
        result_df = df.drop(columns=drop_columns)
    
    elif operation == "groupby":
        group_column = params.get("group_column")
        agg_column = params.get("agg_column")
        agg_function = params.get("agg_function", "mean")
        
        if not all([group_column, agg_column]):
            raise ValueError("Group column and aggregation column are required")
        
        if group_column not in df.columns:
            raise ValueError(f"Group column '{group_column}' not found in dataset")
        
        if agg_column not in df.columns:
            raise ValueError(f"Aggregation column '{agg_column}' not found in dataset")
        
        code = f"df = df.groupby('{group_column}')['{agg_column}'].{agg_function}().reset_index()"
        result_df = getattr(df.groupby(group_column)[agg_column], agg_function)().reset_index()
    
    else:
        raise ValueError(f"Unsupported operation: {operation}")
    
    return result_df, code

def process_polars_operation(df, operation, params):
    """Process polars operations on DataFrame."""
    code = ""
    
    if operation == "filter":
        column = params.get("column")
        operator = params.get("operator")
        value = params.get("value")
        
        if not all([column, operator]):
            raise ValueError("Column and operator are required for filter operation")
        
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        # Try to convert value to appropriate type
        try:
            dtype = df[column].dtype
            if pl.datatypes.is_numeric(dtype):
                value = float(value) if '.' in value else int(value)
        except (ValueError, TypeError):
            # Keep as string if conversion fails
            pass
        
        # Create filter condition based on operator
        if operator == "==":
            code = f"df = df.filter(pl.col('{column}') == {repr(value)})"
            result_df = df.filter(pl.col(column) == value)
        elif operator == "!=":
            code = f"df = df.filter(pl.col('{column}') != {repr(value)})"
            result_df = df.filter(pl.col(column) != value)
        elif operator == ">":
            code = f"df = df.filter(pl.col('{column}') > {repr(value)})"
            result_df = df.filter(pl.col(column) > value)
        elif operator == "<":
            code = f"df = df.filter(pl.col('{column}') < {repr(value)})"
            result_df = df.filter(pl.col(column) < value)
        elif operator == ">=":
            code = f"df = df.filter(pl.col('{column}') >= {repr(value)})"
            result_df = df.filter(pl.col(column) >= value)
        elif operator == "<=":
            code = f"df = df.filter(pl.col('{column}') <= {repr(value)})"
            result_df = df.filter(pl.col(column) <= value)
        elif operator == "contains":
            code = f"df = df.filter(pl.col('{column}').cast(pl.Utf8).str.contains({repr(value)}))"
            result_df = df.filter(pl.col(column).cast(pl.Utf8).str.contains(value))
        elif operator == "startswith":
            code = f"df = df.filter(pl.col('{column}').cast(pl.Utf8).str.starts_with({repr(value)}))"
            result_df = df.filter(pl.col(column).cast(pl.Utf8).str.starts_with(value))
        elif operator == "endswith":
            code = f"df = df.filter(pl.col('{column}').cast(pl.Utf8).str.ends_with({repr(value)}))"
            result_df = df.filter(pl.col(column).cast(pl.Utf8).str.ends_with(value))
        elif operator == "regex":
            code = f"df = df.filter(pl.col('{column}').cast(pl.Utf8).str.contains({repr(value)}, literal=False))"
            result_df = df.filter(pl.col(column).cast(pl.Utf8).str.contains(value, literal=False))
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    
    # Add other polars operations here...
    else:
        raise ValueError(f"Unsupported operation: {operation}")
    
    return result_df, code

def process_sql_operation(con, content, operation, params, dataset_name):
    """Process SQL operations."""
    import tempfile
    
    # Load data into DuckDB
    with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
        tmp.write(content)
        tmp.flush()
        
        # Create temp table from CSV
        con.execute(f"CREATE TABLE temp_data AS SELECT * FROM read_csv_auto('{tmp.name}')")
    
    if operation == "filter":
        column = params.get("column")
        operator = params.get("operator")
        value = params.get("value")
        
        if not all([column, operator]):
            raise ValueError("Column and operator are required for filter operation")
        
        # Map operators to SQL syntax
        sql_operators = {
            "==": "=",
            "!=": "!=",
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<="
        }
        
        if operator in sql_operators:
            # For basic comparison operators
            sql_op = sql_operators[operator]
            
            # Check if value should be quoted (non-numeric)
            try:
                float(value)
                sql_value = value
            except ValueError:
                sql_value = f"'{value}'"
            
            sql_query = f'SELECT * FROM temp_data WHERE "{column}" {sql_op} {sql_value}'
        
        elif operator == "contains":
            sql_query = f'SELECT * FROM temp_data WHERE "{column}" LIKE \'%{value}%\''
        elif operator == "startswith":
            sql_query = f'SELECT * FROM temp_data WHERE "{column}" LIKE \'{value}%\''
        elif operator == "endswith":
            sql_query = f'SELECT * FROM temp_data WHERE "{column}" LIKE \'%{value}\''
        elif operator == "regex":
            sql_query = f'SELECT * FROM temp_data WHERE REGEXP_MATCHES("{column}", \'{value}\')'
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        
        result = con.execute(sql_query).fetchall()
        columns = [desc[0] for desc in con.description]
        row_count = len(result)
        
        # Convert to list of dictionaries
        result_data = [dict(zip(columns, row)) for row in result]
        
        return result_data, columns, row_count, sql_query
    
    # Add other SQL operations here...
    else:
        raise ValueError(f"Unsupported operation: {operation}")