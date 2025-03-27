from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import polars as pl
import duckdb
import io
import json
from typing import Optional, List, Dict, Any

app = FastAPI(title="Data Analysis GUI API")

# Configure CORS to allow requests from frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for uploaded datasets
datasets = {}

@app.get("/")
async def read_root():
    return {"message": "Data Analysis GUI API is running"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), dataset_name: str = Form(...)):
    try:
        contents = await file.read()
        
        # Store the file content in memory
        datasets[dataset_name] = {
            "content": contents,
            "filename": file.filename
        }
        
        # Read first few rows to return as preview
        df = pd.read_csv(io.BytesIO(contents))
        preview = df.head(5).to_dict(orient="records")
        columns = list(df.columns)
        row_count = len(df)
        
        return {
            "message": f"Successfully uploaded {file.filename}",
            "dataset_name": dataset_name,
            "preview": preview,
            "columns": columns,
            "row_count": row_count
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not process file: {str(e)}")

@app.get("/datasets")
async def get_datasets():
    return {"datasets": list(datasets.keys())}

@app.get("/dataset/{dataset_name}")
async def get_dataset(dataset_name: str, engine: str = "pandas", limit: int = 100):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")
    
    try:
        content = datasets[dataset_name]["content"]
        
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content))
            return {
                "data": df.head(limit).to_dict(orient="records"),
                "columns": list(df.columns),
                "row_count": len(df)
            }
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content))
            return {
                "data": df.head(limit).to_dicts(),
                "columns": df.columns,
                "row_count": df.height
            }
        elif engine == "sql":
            # Create an in-memory DuckDB database
            con = duckdb.connect(":memory:")
            
            # Register the CSV as a table
            query = f"CREATE TABLE temp_data AS SELECT * FROM read_csv_auto('{dataset_name}.csv');"
            con.execute(query)
            
            # Read the data
            result = con.execute(f"SELECT * FROM temp_data LIMIT {limit}").fetchall()
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
        
        # Load data based on engine
        if engine == "pandas":
            df = pd.read_csv(io.BytesIO(content))
            result_df, code = process_pandas_operation(df, operation, params_dict)
            return {
                "data": result_df.head(100).to_dict(orient="records"),
                "columns": list(result_df.columns),
                "row_count": len(result_df),
                "code": code
            }
        elif engine == "polars":
            df = pl.read_csv(io.BytesIO(content))
            result_df, code = process_polars_operation(df, operation, params_dict)
            return {
                "data": result_df.head(100).to_dicts(),
                "columns": result_df.columns,
                "row_count": result_df.height,
                "code": code
            }
        elif engine == "sql":
            # Here we would process SQL operations
            con = duckdb.connect(":memory:")
            result_data, columns, row_count, code = process_sql_operation(con, content, operation, params_dict, dataset_name)
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

def process_pandas_operation(df, operation, params):
    code = ""
    
    if operation == "filter":
        column = params.get("column")
        operator = params.get("operator")
        value = params.get("value")
        
        if operator == "==":
            result_df = df[df[column] == value]
            code = f"df[df['{column}'] == {repr(value)}]"
        elif operator == ">":
            result_df = df[df[column] > float(value)]
            code = f"df[df['{column}'] > {value}]"
        elif operator == "<":
            result_df = df[df[column] < float(value)]
            code = f"df[df['{column}'] < {value}]"
        elif operator == ">=":
            result_df = df[df[column] >= float(value)]
            code = f"df[df['{column}'] >= {value}]"
        elif operator == "<=":
            result_df = df[df[column] <= float(value)]
            code = f"df[df['{column}'] <= {value}]"
        elif operator == "!=":
            result_df = df[df[column] != value]
            code = f"df[df['{column}'] != {repr(value)}]"
        elif operator == "contains":
            result_df = df[df[column].astype(str).str.contains(value)]
            code = f"df[df['{column}'].astype(str).str.contains({repr(value)})]"
        else:
            raise ValueError(f"Unsupported operator: {operator}")
            
    elif operation == "groupby":
        group_columns = params.get("columns", [])
        agg_function = params.get("agg_function", "mean")
        agg_column = params.get("agg_column")
        
        if agg_function == "mean":
            result_df = df.groupby(group_columns)[agg_column].mean().reset_index()
            code = f"df.groupby({repr(group_columns)})['{agg_column}'].mean().reset_index()"
        elif agg_function == "sum":
            result_df = df.groupby(group_columns)[agg_column].sum().reset_index()
            code = f"df.groupby({repr(group_columns)})['{agg_column}'].sum().reset_index()"
        elif agg_function == "count":
            result_df = df.groupby(group_columns)[agg_column].count().reset_index()
            code = f"df.groupby({repr(group_columns)})['{agg_column}'].count().reset_index()"
        elif agg_function == "min":
            result_df = df.groupby(group_columns)[agg_column].min().reset_index()
            code = f"df.groupby({repr(group_columns)})['{agg_column}'].min().reset_index()"
        elif agg_function == "max":
            result_df = df.groupby(group_columns)[agg_column].max().reset_index()
            code = f"df.groupby({repr(group_columns)})['{agg_column}'].max().reset_index()"
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_function}")
            
    elif operation == "rename":
        old_name = params.get("old_name")
        new_name = params.get("new_name")
        
        result_df = df.rename(columns={old_name: new_name})
        code = f"df.rename(columns={{'{old_name}': '{new_name}'}})"
        
    else:
        raise ValueError(f"Unsupported operation: {operation}")
        
    return result_df, code

def process_polars_operation(df, operation, params):
    code = ""
    
    if operation == "filter":
        column = params.get("column")
        operator = params.get("operator")
        value = params.get("value")
        
        if operator == "==":
            result_df = df.filter(pl.col(column) == value)
            code = f"df.filter(pl.col('{column}') == {repr(value)})"
        elif operator == ">":
            result_df = df.filter(pl.col(column) > float(value))
            code = f"df.filter(pl.col('{column}') > {value})"
        elif operator == "<":
            result_df = df.filter(pl.col(column) < float(value))
            code = f"df.filter(pl.col('{column}') < {value})"
        elif operator == ">=":
            result_df = df.filter(pl.col(column) >= float(value))
            code = f"df.filter(pl.col('{column}') >= {value})"
        elif operator == "<=":
            result_df = df.filter(pl.col(column) <= float(value))
            code = f"df.filter(pl.col('{column}') <= {value})"
        elif operator == "!=":
            result_df = df.filter(pl.col(column) != value)
            code = f"df.filter(pl.col('{column}') != {repr(value)})"
        elif operator == "contains":
            result_df = df.filter(pl.col(column).cast(pl.Utf8).str.contains(value))
            code = f"df.filter(pl.col('{column}').cast(pl.Utf8).str.contains({repr(value)}))"
        else:
            raise ValueError(f"Unsupported operator: {operator}")
            
    elif operation == "groupby":
        group_columns = params.get("columns", [])
        agg_function = params.get("agg_function", "mean")
        agg_column = params.get("agg_column")
        
        if agg_function == "mean":
            result_df = df.group_by(group_columns).agg(pl.col(agg_column).mean())
            code = f"df.group_by({repr(group_columns)}).agg(pl.col('{agg_column}').mean())"
        elif agg_function == "sum":
            result_df = df.group_by(group_columns).agg(pl.col(agg_column).sum())
            code = f"df.group_by({repr(group_columns)}).agg(pl.col('{agg_column}').sum())"
        elif agg_function == "count":
            result_df = df.group_by(group_columns).agg(pl.col(agg_column).count())
            code = f"df.group_by({repr(group_columns)}).agg(pl.col('{agg_column}').count())"
        elif agg_function == "min":
            result_df = df.group_by(group_columns).agg(pl.col(agg_column).min())
            code = f"df.group_by({repr(group_columns)}).agg(pl.col('{agg_column}').min())"
        elif agg_function == "max":
            result_df = df.group_by(group_columns).agg(pl.col(agg_column).max())
            code = f"df.group_by({repr(group_columns)}).agg(pl.col('{agg_column}').max())"
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_function}")
            
    elif operation == "rename":
        old_name = params.get("old_name")
        new_name = params.get("new_name")
        
        result_df = df.rename({old_name: new_name})
        code = f"df.rename({{'{old_name}': '{new_name}'}})"
        
    else:
        raise ValueError(f"Unsupported operation: {operation}")
        
    return result_df, code

def process_sql_operation(con, content, operation, params, dataset_name):
    # First, set up the table from the CSV
    con.execute(f"CREATE TABLE temp_data AS SELECT * FROM read_csv_auto(?)", [io.BytesIO(content)])
    
    sql_query = ""
    if operation == "filter":
        column = params.get("column")
        operator = params.get("operator")
        value = params.get("value")
        
        if operator in ["==", "=", "equals"]:
            operator = "="
        
        sql_query = f"SELECT * FROM temp_data WHERE \"{column}\" {operator} ?"
        parameter = value
        result = con.execute(sql_query, [parameter]).fetchall()
        
    elif operation == "groupby":
        group_columns = params.get("columns", [])
        agg_function = params.get("agg_function", "AVG").upper()
        agg_column = params.get("agg_column")
        
        # Format column list for GROUP BY
        group_cols_sql = ", ".join([f'"{col}"' for col in group_columns])
        
        sql_query = f'SELECT {group_cols_sql}, {agg_function}("{agg_column}") as result FROM temp_data GROUP BY {group_cols_sql}'
        result = con.execute(sql_query).fetchall()
        
    elif operation == "rename":
        old_name = params.get("old_name")
        new_name = params.get("new_name")
        
        # In SQL, renaming requires creating a new view or table
        sql_query = f'SELECT *, "{old_name}" as "{new_name}" FROM temp_data'
        result = con.execute(sql_query).fetchall()
        
    else:
        raise ValueError(f"Unsupported operation: {operation}")
    
    # Get column information
    columns = [desc[0] for desc in con.description]
    
    # Get row count (for the result)
    row_count = len(result)
    
    # Convert to list of dictionaries
    result_data = [dict(zip(columns, row)) for row in result][:100]  # Limit to 100 rows
    
    return result_data, columns, row_count, sql_query

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)