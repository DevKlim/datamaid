# backend/app/services/sql_service.py
import duckdb
import pandas as pd
import io
import re
from typing import Dict, Any, Tuple, List, Optional

# --- Helper Functions (Keep _sanitize_identifier, _sanitize_literal, _load_data_to_duckdb, _execute_sql_query) ---

def _sanitize_identifier(identifier: Optional[str]) -> Optional[str]:
    """Sanitizes table or column names for safe use in SQL queries."""
    if identifier is None:
        return None
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'

def _sanitize_literal(value: Any) -> str:
    """Sanitizes literal values for safe use in SQL WHERE clauses."""
    if isinstance(value, (int, float)):
        return str(value)
    elif value is None:
        return "NULL"
    else:
        sanitized_value = str(value).replace("'", "''")
        return f"'{sanitized_value}'"

def _load_data_to_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, content: bytes):
    """Loads data from CSV bytes content into a DuckDB table."""
    sanitized_table_name = _sanitize_identifier(table_name)
    try:
        # This implementation is already correct!
        df_temp = pd.read_csv(io.BytesIO(content))
        con.register(f'df_temp_sql_view_{table_name}', df_temp) # Use unique view name
        con.execute(f"CREATE OR REPLACE TABLE {sanitized_table_name} AS SELECT * FROM df_temp_sql_view_{table_name};")
        con.unregister(f'df_temp_sql_view_{table_name}')
    except (pd.errors.ParserError, duckdb.Error, Exception) as e: # Catch pandas errors too
        raise ValueError(f"Failed to load data into DuckDB table {table_name}: {e}")

def _execute_sql_query(con: duckdb.DuckDBPyConnection, query: str, preview_limit: int = 100) -> Tuple[List[Dict], List[str], int]:
    """Executes a SQL query, gets preview data, columns, and total row count."""
    try:
        # Use CTE for count only if the query is complex? Direct count might be faster for simple SELECTs.
        # For simplicity, keep CTE approach for now.
        count_query = f"WITH result_set AS ({query}) SELECT COUNT(*) FROM result_set;"
        total_rows_result = con.execute(count_query).fetchone()
        if total_rows_result is None:
             raise ValueError("Count query returned no result.")
        total_rows = total_rows_result[0]


        preview_query = f"{query} LIMIT {preview_limit};"
        preview_result = con.execute(preview_query)

        columns = [desc[0] for desc in preview_result.description]
        data_dicts = preview_result.fetch_arrow_table().to_pylist()

        for row in data_dicts:
            for col, val in row.items():
                if hasattr(val, 'isoformat'):
                    row[col] = val.isoformat()

        return data_dicts, columns, total_rows
    except (duckdb.Error, AttributeError, TypeError, ValueError) as e: # Catch ValueError from count query
        print(f"SQL Execution/Processing Error: {type(e).__name__} - {e}\nQuery: {query}") # Log error
        raise ValueError(f"Failed to execute or process SQL query: {e}")

# --- Main Operation Dispatcher ---

def apply_sql_operation(
    con: duckdb.DuckDBPyConnection,
    content: bytes,
    base_table_name: str,
    operation: str,
    params: Dict[str, Any],
    all_cols: List[str] # List of column names in the current state
) -> Tuple[List[Dict], List[str], int, str]:
    """Applies a specified SQL operation by generating and executing a query."""
    _load_data_to_duckdb(con, base_table_name, content)
    sanitized_base_table = _sanitize_identifier(base_table_name)

    try:
        generated_sql = ""
        # --- Existing Operations ---
        if operation == "filter":
            generated_sql = _filter_rows_sql(sanitized_base_table, params, all_cols)
        elif operation == "select_columns":
            generated_sql = _select_columns_sql(sanitized_base_table, params, all_cols)
        elif operation == "sort":
            generated_sql = _sort_values_sql(sanitized_base_table, params, all_cols)
        elif operation == "rename":
            generated_sql = _rename_columns_sql(sanitized_base_table, params, all_cols)
        elif operation == "drop_columns":
            generated_sql = _drop_columns_sql(sanitized_base_table, params, all_cols)
        elif operation == "groupby":
            generated_sql = _group_by_sql(sanitized_base_table, params, all_cols)
        elif operation == "groupby_multi":
            generated_sql = _group_by_multi_sql(sanitized_base_table, params, all_cols)
        elif operation == "groupby_multi_agg":
            generated_sql = _group_by_multi_agg_sql(sanitized_base_table, params, all_cols)

        # --- NEW Operations ---
        elif operation == "fillna":
            generated_sql = _fillna_sql(sanitized_base_table, params, all_cols)
        elif operation == "dropna":
             generated_sql = _dropna_sql(sanitized_base_table, params, all_cols)
        elif operation == "astype":
             generated_sql = _astype_sql(sanitized_base_table, params, all_cols)
        elif operation == "string_operation": # Generic endpoint for string ops
             generated_sql = _string_op_sql(sanitized_base_table, params, all_cols)
        elif operation == "date_extract":
             generated_sql = _date_extract_sql(sanitized_base_table, params, all_cols)
        elif operation == "drop_duplicates":
             generated_sql = _drop_duplicates_sql(sanitized_base_table, params, all_cols)
        elif operation == "create_column": # From expression
             generated_sql = _create_column_sql(sanitized_base_table, params, all_cols)
        elif operation == "window_function":
             generated_sql = _window_function_sql(sanitized_base_table, params, all_cols)
        elif operation == "sample":
             generated_sql = _sample_sql(sanitized_base_table, params, all_cols)

        # --- Other Ops ---
        # elif operation == "pivot_table" or operation == "pivot":
        #    raise NotImplementedError("SQL Pivot not yet implemented.")
        # elif operation == "melt":
        #    raise NotImplementedError("SQL Melt not yet implemented.")
        elif operation in ["set_index", "reset_index"]:
           print(f"Warning: '{operation}' is a no-op in SQL context. Returning original data.")
           generated_sql = f"SELECT * FROM {sanitized_base_table}" # No-op for SQL
        else:
            raise ValueError(f"Unsupported SQL operation: {operation}")

        # Execute the generated query
        preview_data, result_columns, total_rows = _execute_sql_query(con, generated_sql)

        return preview_data, result_columns, total_rows, generated_sql

    except Exception as e:
        print(f"Error preparing/executing SQL operation '{operation}': {type(e).__name__}: {e}")
        raise e


# --- Specific Operation Implementations ---

def _filter_rows_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    column = params.get("column")
    operator = params.get("operator")
    value = params.get("value")

    if not all([column, operator]):
        raise ValueError("Column and operator are required for filter")
    # --- Add column validation ---
    if column not in all_cols:
        raise ValueError(f"Filter column '{column}' not found.")
    # ---

    sanitized_col = _sanitize_identifier(column)
    condition = ""
    op_map = { "==": "=", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<=" }

    if operator in op_map:
        # Add explicit cast for comparison safety, though DuckDB might infer
        # Try casting the literal first? No, cast the column.
        # What type to cast to? Hard to know. Let DuckDB try first.
        # condition = f"{sanitized_col} {op_map[operator]} {_sanitize_literal(value)}"
        # Safer: Cast column for string comparisons if needed, numbers usually ok
        # Let's rely on DuckDB's implicit casting for numeric/date and cast for string ops
         condition = f"{sanitized_col} {op_map[operator]} {_sanitize_literal(value)}"
    elif operator in ["contains", "startswith", "endswith"]:
        # Use LIKE, requires casting column to TEXT and sanitizing value
        like_value = str(value).replace("'", "''").replace("%", "\\%").replace("_", "\\_")
        if operator == "contains": pattern = f"'%{like_value}%'"
        elif operator == "startswith": pattern = f"'{like_value}%'"
        else: pattern = f"'%{like_value}'" # endswith
        # Add explicit cast to TEXT for LIKE
        condition = f"{sanitized_col}::TEXT LIKE {pattern}"
    elif operator == "regex":
        # Use REGEXP_MATCHES, requires casting column to TEXT and sanitizing pattern
        # Add explicit cast to TEXT for REGEXP_MATCHES
        condition = f"REGEXP_MATCHES({sanitized_col}::TEXT, {_sanitize_literal(str(value))})"
    else:
        raise ValueError(f"Unsupported filter operator for SQL: {operator}")

    return f"SELECT * FROM {table_name} WHERE {condition}"


def _select_columns_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    # This function already checks against all_cols implicitly via the loop below
    selected_columns = params.get("selected_columns", [])
    if not selected_columns:
        raise ValueError("No columns selected")

    missing = [col for col in selected_columns if col not in all_cols]
    if missing:
        raise ValueError(f"Selected columns not found: {', '.join(missing)}")

    select_list = ", ".join([_sanitize_identifier(col) for col in selected_columns])
    return f"SELECT {select_list} FROM {table_name}"

def _sort_values_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    sort_column = params.get("sort_column")
    sort_order = params.get("sort_order", "ascending").upper()
    if not sort_column:
        raise ValueError("Sort column required")
    # --- Add column validation ---
    if sort_column not in all_cols:
        raise ValueError(f"Sort column '{sort_column}' not found.")
    # ---
    if sort_order not in ["ASC", "DESCENDING", "DESC"]:
        raise ValueError("Sort order must be 'ascending' or 'descending'")
    if sort_order == "DESCENDING": sort_order = "DESC"

    sanitized_col = _sanitize_identifier(sort_column)
    return f"SELECT * FROM {table_name} ORDER BY {sanitized_col} {sort_order}"

def _rename_columns_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    # This function already checks against all_cols implicitly via the loop below
    renames = params.get("renames", [])
    if not renames:
        raise ValueError("No rename mappings provided")
    rename_map = {item['old_name']: item['new_name'] for item in renames if item.get('old_name') and item.get('new_name')}
    if not rename_map:
         raise ValueError("Invalid rename parameters.")

    select_clauses = []
    missing_rename = [old for old in rename_map if old not in all_cols]
    if missing_rename:
        raise ValueError(f"Columns to rename not found: {', '.join(missing_rename)}")

    for col in all_cols:
        sanitized_old = _sanitize_identifier(col)
        if col in rename_map:
            sanitized_new = _sanitize_identifier(rename_map[col])
            select_clauses.append(f"{sanitized_old} AS {sanitized_new}")
        else:
            select_clauses.append(sanitized_old)

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {table_name}"

def _drop_columns_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    # This function already checks against all_cols implicitly via the loop below
    drop_columns = params.get("drop_columns", [])
    if not drop_columns:
        raise ValueError("No columns specified for dropping")

    # Validate columns to drop actually exist
    missing_drop = [col for col in drop_columns if col not in all_cols]
    if missing_drop:
        raise ValueError(f"Columns to drop not found: {', '.join(missing_drop)}")

    keep_cols = [col for col in all_cols if col not in drop_columns]
    if not keep_cols:
        raise ValueError("Cannot drop all columns")

    select_list = ", ".join([_sanitize_identifier(col) for col in keep_cols])
    return f"SELECT {select_list} FROM {table_name}"


def _map_agg_func_sql(func_name: str) -> str:
    """Maps common function names to SQL aggregate functions."""
    func_lower = func_name.lower()
    mapping = {
        'mean': 'AVG', 'average': 'AVG',
        'sum': 'SUM',
        'count': 'COUNT', # Special handling for COUNT(*) vs COUNT(col)
        'min': 'MIN',
        'max': 'MAX',
        'median': 'MEDIAN',
        'std': 'STDDEV_SAMP', 'stddev': 'STDDEV_SAMP',
        'var': 'VAR_SAMP', 'variance': 'VAR_SAMP',
        'first': 'FIRST',
        'last': 'LAST',
        'nunique': 'COUNT(DISTINCT {})'
    }
    if func_lower not in mapping:
        raise ValueError(f"Unsupported SQL aggregation function: {func_name}")
    return mapping[func_lower]

def _build_agg_clause_sql(agg_col: str, func_name: str) -> str:
    """Builds a single SELECT clause for an aggregation, adding casts defensively."""
    sql_func_template = _map_agg_func_sql(func_name)
    sanitized_agg_col = _sanitize_identifier(agg_col)
    alias = _sanitize_identifier(f"{agg_col}_{func_name}")

    numeric_funcs = ['AVG', 'SUM', 'MEDIAN', 'STDDEV_SAMP', 'VAR_SAMP']
    target_col = sanitized_agg_col

    # Add explicit casting for numeric functions as a safeguard
    if sql_func_template in numeric_funcs:
        target_col = f"{sanitized_agg_col}::NUMERIC" # Cast to NUMERIC

    if sql_func_template == 'COUNT(DISTINCT {})':
        # DISTINCT count usually works on most types, no cast needed unless specific error
        return f"{sql_func_template.format(target_col)} AS {alias}"
    elif sql_func_template == 'COUNT':
        # COUNT(col) counts non-nulls, COUNT(*) counts rows. Default to COUNT(col).
        # UI could perhaps specify COUNT(*) explicitly if needed.
        return f"COUNT({target_col}) AS {alias}"
    else:
        return f"{sql_func_template}({target_col}) AS {alias}"

def _group_by_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    group_column = params.get("group_column")
    agg_column = params.get("agg_column")
    agg_function = params.get("agg_function", "count") # Default

    if not all([group_column, agg_column, agg_function]):
        raise ValueError("Group column, aggregation column, and function required")
    # --- Validation ---
    if group_column not in all_cols: raise ValueError(f"Group column '{group_column}' not found.")
    if agg_column not in all_cols: raise ValueError(f"Aggregation column '{agg_column}' not found.")
    # ---

    sanitized_group_col = _sanitize_identifier(group_column)
    # _build_agg_clause_sql handles function mapping and alias
    agg_clause = _build_agg_clause_sql(agg_column, agg_function)

    return f"SELECT {sanitized_group_col}, {agg_clause} FROM {table_name} GROUP BY {sanitized_group_col}"

def _group_by_multi_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    group_columns = params.get("group_columns") # List
    agg_column = params.get("agg_column")
    agg_function = params.get("agg_function", "count") # Default

    if not all([group_columns, agg_column, agg_function]):
        raise ValueError("Group columns (list), aggregation column, and function required")
    if not isinstance(group_columns, list) or not group_columns:
         raise ValueError("group_columns must be a non-empty list")
    # --- Validation ---
    missing_group = [col for col in group_columns if col not in all_cols]
    if missing_group: raise ValueError(f"Group columns not found: {', '.join(missing_group)}")
    if agg_column not in all_cols: raise ValueError(f"Aggregation column '{agg_column}' not found.")
    # ---

    sanitized_group_cols = [_sanitize_identifier(col) for col in group_columns]
    group_by_list = ", ".join(sanitized_group_cols)
    agg_clause = _build_agg_clause_sql(agg_column, agg_function)

    return f"SELECT {group_by_list}, {agg_clause} FROM {table_name} GROUP BY {group_by_list}"


def _group_by_multi_agg_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    group_columns = params.get("group_columns") # String or list
    aggregations = params.get("aggregations") # List of {'column': 'c', 'function': 'f'}

    if not group_columns or not aggregations:
        raise ValueError("Group column(s) and aggregations list are required")
    if isinstance(group_columns, str): group_columns = [group_columns]
    if not isinstance(group_columns, list) or not group_columns:
         raise ValueError("group_columns must be a non-empty list")
    # --- Validation ---
    missing_group = [col for col in group_columns if col not in all_cols]
    if missing_group: raise ValueError(f"Group columns not found: {', '.join(missing_group)}")
    # ---

    sanitized_group_cols = [_sanitize_identifier(col) for col in group_columns]
    group_by_list = ", ".join(sanitized_group_cols)

    agg_clauses = []
    for agg_spec in aggregations:
        col = agg_spec.get("column")
        func = agg_spec.get("function")
        if not col or not func: raise ValueError(f"Invalid aggregation spec: {agg_spec}")
        # --- Validation ---
        if col not in all_cols: raise ValueError(f"Aggregation column '{col}' not found.")
        # ---
        agg_clauses.append(_build_agg_clause_sql(col, func))

    if not agg_clauses: raise ValueError("No valid aggregations provided")
    agg_select_list = ", ".join(agg_clauses)

    return f"SELECT {group_by_list}, {agg_select_list} FROM {table_name} GROUP BY {group_by_list}"


# --- Join Operation (Keep as is, but ensure main.py passes correct args) ---
def apply_sql_join(
    con: duckdb.DuckDBPyConnection,
    left_table: str,
    right_table: str,
    params: Dict[str, Any],
    left_cols: List[str], # Column names from left table
    right_cols: List[str] # Column names from right table
) -> Tuple[List[Dict], List[str], int, str]:
    # ... (Keep existing implementation, ensuring _sanitize_identifier is used) ...
    how = params.get("join_type", "inner").upper()
    left_on = params.get("left_on")
    right_on = params.get("right_on")

    if not left_on or not right_on:
         raise ValueError("Both left_on and right_on key columns are required for join")

    if left_on not in left_cols: raise ValueError(f"Left key '{left_on}' not found in left dataset columns.")
    if right_on not in right_cols: raise ValueError(f"Right key '{right_on}' not found in right dataset columns.")

    valid_joins = ['INNER', 'LEFT', 'RIGHT', 'FULL OUTER', 'OUTER', 'CROSS', 'SEMI', 'ANTI'] # DuckDB supports SEMI/ANTI
    if how not in valid_joins:
        raise ValueError(f"Invalid join type: {how}. Must be one of {valid_joins}")
    if how == 'OUTER': how = 'FULL OUTER'

    s_left_table = _sanitize_identifier(left_table)
    s_right_table = _sanitize_identifier(right_table)
    s_left_on = _sanitize_identifier(left_on)
    s_right_on = _sanitize_identifier(right_on)

    # Build SELECT list, aliasing columns to avoid collisions
    select_clauses = []
    final_columns = []
    added_right_cols = set()

    for col in left_cols:
        s_col = _sanitize_identifier(col)
        alias = _sanitize_identifier(f"{left_table}_{col}") # Prefix with table name
        select_clauses.append(f"{s_left_table}.{s_col} AS {alias}")
        final_columns.append(f"{left_table}_{col}")

    for col in right_cols:
        # Skip right join key if names are same to avoid duplication? Or always include?
        # Let's always include right table columns but alias them.
        s_col = _sanitize_identifier(col)
        alias = _sanitize_identifier(f"{right_table}_{col}") # Prefix with table name
        # Handle potential duplicate alias if tables/cols have weird names (unlikely)
        while alias in final_columns:
            alias = _sanitize_identifier(f"{alias}_") # Append underscore

        select_clauses.append(f"{s_right_table}.{s_col} AS {alias}")
        final_columns.append(alias.strip('"')) # Store alias without quotes
        added_right_cols.add(col)


    select_list = ", ".join(select_clauses) if select_clauses else "*"

    # Build the JOIN query
    join_condition = f"{s_left_table}.{s_left_on} = {s_right_table}.{s_right_on}"
    if how == 'CROSS':
        generated_sql = f"SELECT {select_list} FROM {s_left_table} {how} JOIN {s_right_table}"
    elif how in ['SEMI', 'ANTI']:
         # SEMI/ANTI joins typically only return columns from the left table
         left_select_list = ", ".join(select_clauses[:len(left_cols)])
         final_columns = final_columns[:len(left_cols)]
         generated_sql = f"SELECT {left_select_list} FROM {s_left_table} {how} JOIN {s_right_table} ON {join_condition}"
    else:
        generated_sql = f"SELECT {select_list} FROM {s_left_table} {how} JOIN {s_right_table} ON {join_condition}"

    try:
        # Execute query to get results
        preview_data, _, total_rows = _execute_sql_query(con, generated_sql) # Use derived final_columns

        return preview_data, final_columns, total_rows, generated_sql
    except Exception as e:
        print(f"SQL Join Error: {type(e).__name__} - {e}\nQuery: {generated_sql}")
        raise ValueError(f"Error during SQL JOIN operation: {str(e)}")
    
def _fillna_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Generates SQL using COALESCE to fill missing values."""
    columns_to_fill = params.get("columns") # List of columns, or None for all
    fill_value = params.get("value")

    if fill_value is None: # Can't fill with NULL using COALESCE this way
        raise ValueError("fillna requires a non-null 'value' parameter for SQL COALESCE.")

    sanitized_fill_value = _sanitize_literal(fill_value)
    select_clauses = []

    target_cols = columns_to_fill if columns_to_fill else all_cols
    missing = [col for col in target_cols if col not in all_cols]
    if missing:
        raise ValueError(f"Columns to fill not found: {', '.join(missing)}")

    for col in all_cols:
        s_col = _sanitize_identifier(col)
        if col in target_cols:
            # Apply COALESCE(column, fill_value)
            # Cast fill value to column type? DuckDB is often good at implicit casting.
            # Let's try without explicit cast first. Add if errors occur.
            # Example: COALESCE("age"::INTEGER, 0) - might be needed sometimes.
            select_clauses.append(f"COALESCE({s_col}, {sanitized_fill_value}) AS {s_col}")
        else:
            select_clauses.append(s_col) # Keep other columns as is

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {table_name}"

def _dropna_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Generates SQL using WHERE IS NOT NULL to drop rows with missing values."""
    subset = params.get("subset") # Optional list of columns to check for NULLs
    if subset and not isinstance(subset, list):
        raise ValueError("'subset' parameter for dropna must be a list of column names.")

    where_clauses = []
    target_cols = subset if subset else all_cols
    missing = [col for col in target_cols if col not in all_cols]
    if missing:
        raise ValueError(f"Columns in dropna subset not found: {', '.join(missing)}")

    for col in target_cols:
        s_col = _sanitize_identifier(col)
        where_clauses.append(f"{s_col} IS NOT NULL")

    where_condition = " AND ".join(where_clauses)
    if not where_condition: # Should not happen if target_cols is not empty
         return f"SELECT * FROM {table_name}"

    return f"SELECT * FROM {table_name} WHERE {where_condition}"

def _astype_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Generates SQL using CAST (::) to change column types."""
    column = params.get("column")
    new_type = params.get("new_type") # e.g., "INTEGER", "VARCHAR", "FLOAT", "DATE", "TIMESTAMP"
    # Need validation on new_type against supported DuckDB types
    supported_types = ["INTEGER", "BIGINT", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL",
                       "VARCHAR", "TEXT", "DATE", "TIMESTAMP", "BOOLEAN", "BLOB"]
    if not column or not new_type:
        raise ValueError("astype requires 'column' and 'new_type' parameters.")
    if column not in all_cols:
        raise ValueError(f"Column '{column}' not found for astype.")
    if new_type.upper() not in supported_types:
        raise ValueError(f"Unsupported SQL type '{new_type}'. Supported: {supported_types}")

    select_clauses = []
    for col in all_cols:
        s_col = _sanitize_identifier(col)
        if col == column:
            # Use DuckDB's cast operator ::
            select_clauses.append(f"{s_col}::{new_type.upper()} AS {s_col}")
        else:
            select_clauses.append(s_col)

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {table_name}"

def _string_op_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Handles various string operations like upper, lower, trim, split."""
    column = params.get("column")
    string_func = params.get("string_function") # e.g., 'upper', 'lower', 'trim', 'split'
    new_col_name = params.get("new_column_name", f"{column}_{string_func}") # Optional new name
    # Split specific params
    delimiter = params.get("delimiter") # For split
    part_index = params.get("part_index") # For split (1-based index)

    if not column or not string_func:
        raise ValueError("string_operation requires 'column' and 'string_function'.")
    if column not in all_cols:
        raise ValueError(f"Column '{column}' not found for string operation.")

    s_col = _sanitize_identifier(column)
    s_new_col = _sanitize_identifier(new_col_name)
    op_clause = ""

    func_lower = string_func.lower()
    if func_lower == 'upper':
        op_clause = f"UPPER({s_col}::TEXT)"
    elif func_lower == 'lower':
        op_clause = f"LOWER({s_col}::TEXT)"
    elif func_lower == 'trim':
        op_clause = f"TRIM({s_col}::TEXT)" # Removes leading/trailing whitespace
    elif func_lower == 'split':
        if not delimiter or part_index is None:
            raise ValueError("String split requires 'delimiter' and 'part_index' (1-based).")
        s_delim = _sanitize_literal(delimiter)
        # DuckDB's string_split returns a list. Access element using [index].
        # Need to handle potential errors if index is out of bounds or split fails.
        # Using list_extract (DuckDB specific, 1-based index)
        op_clause = f"list_extract(string_split({s_col}::TEXT, {s_delim}), {int(part_index)})"
    else:
        raise ValueError(f"Unsupported string_function: {string_func}")

    # Generate SELECT list, adding the new/modified column
    select_clauses = []
    found_target = False
    for existing_col in all_cols:
        s_existing = _sanitize_identifier(existing_col)
        if existing_col == column:
            # If not creating a new column, replace the original
            if new_col_name == column:
                 select_clauses.append(f"{op_clause} AS {s_col}")
                 found_target = True
            else: # Keep original column if creating a new one
                 select_clauses.append(s_existing)
        else:
             select_clauses.append(s_existing)

    # Add the new column if it wasn't replacing an existing one
    if not found_target:
         select_clauses.append(f"{op_clause} AS {s_new_col}")


    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {table_name}"


def _date_extract_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Extracts parts from date/timestamp columns using EXTRACT."""
    column = params.get("column")
    part = params.get("part") # e.g., 'year', 'month', 'day', 'hour', 'dow', 'week'
    new_col_name = params.get("new_column_name", f"{column}_{part}")

    if not column or not part:
        raise ValueError("date_extract requires 'column' and 'part' parameters.")
    if column not in all_cols:
         raise ValueError(f"Column '{column}' not found for date_extract.")

    valid_parts = ['year', 'month', 'day', 'hour', 'minute', 'second', 'dow', 'doy', 'week']
    part_lower = part.lower()
    if part_lower not in valid_parts:
         raise ValueError(f"Invalid date part '{part}'. Valid parts: {valid_parts}")

    s_col = _sanitize_identifier(column)
    s_new_col = _sanitize_identifier(new_col_name)
    # EXTRACT(part FROM column)
    # Assume column is DATE or TIMESTAMP, add cast? DuckDB usually handles it.
    extract_clause = f"EXTRACT({part_lower} FROM {s_col})"

    select_list = ", ".join([_sanitize_identifier(c) for c in all_cols])
    select_list += f", {extract_clause} AS {s_new_col}"

    return f"SELECT {select_list} FROM {table_name}"

def _drop_duplicates_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Removes duplicate rows, optionally based on a subset of columns."""
    subset = params.get("subset") # Optional list of columns to check for duplicates

    if subset and not isinstance(subset, list):
        raise ValueError("'subset' parameter for drop_duplicates must be a list.")

    if subset:
        missing = [col for col in subset if col not in all_cols]
        if missing:
            raise ValueError(f"Columns in drop_duplicates subset not found: {', '.join(missing)}")
        # Use ROW_NUMBER() partitioned by subset columns
        s_subset = ", ".join([_sanitize_identifier(c) for c in subset])
        s_all_cols = ", ".join([_sanitize_identifier(c) for c in all_cols])
        # Pick an arbitrary column for ORDER BY within partition if no order specified, needed for deterministic row_number
        order_by_col = _sanitize_identifier(all_cols[0]) # Use first column
        # Subquery assigns row number, outer query selects rows with rn = 1
        sql = f"""
        WITH numbered_rows AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {s_subset} ORDER BY {order_by_col}) as rn
            FROM {table_name}
        )
        SELECT {s_all_cols}
        FROM numbered_rows
        WHERE rn = 1
        """
        return sql.strip()
    else:
        # Drop duplicates across all columns -> DISTINCT
        return f"SELECT DISTINCT * FROM {table_name}"


def _create_column_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Creates a new column based on an SQL expression."""
    new_col_name = params.get("new_column_name")
    expression = params.get("expression") # e.g., "col_a + col_b", "CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END"

    if not new_col_name or not expression:
        raise ValueError("create_column requires 'new_column_name' and 'expression'.")

    # !!! WARNING: EXPRESSION IS NOT SANITIZED !!!
    # Extremely risky if the expression comes directly from user input without validation.
    # Assume the expression is valid, safe SQL for now.
    print(f"Warning: Executing create_column with potentially unsafe expression: {expression}")

    s_new_col = _sanitize_identifier(new_col_name)
    s_all_cols = ", ".join([_sanitize_identifier(c) for c in all_cols])

    return f"SELECT {s_all_cols}, ({expression}) AS {s_new_col} FROM {table_name}"

def _window_function_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Applies a window function like RANK, LEAD, LAG."""
    func = params.get("window_function") # 'rank', 'lead', 'lag'
    target_column = params.get("target_column") # For lead/lag
    order_by_column = params.get("order_by_column")
    partition_by_columns = params.get("partition_by_columns") # Optional list
    new_col_name = params.get("new_column_name", f"{func}_{order_by_column}")
    # Lead/Lag specific
    offset = params.get("offset", 1)
    default_value = params.get("default_value") # Optional default for lead/lag

    if not func or not order_by_column:
        raise ValueError("window_function requires 'window_function' and 'order_by_column'.")
    func_lower = func.lower()
    if func_lower in ['lead', 'lag'] and not target_column:
        raise ValueError("Lead/Lag requires 'target_column'.")

    if order_by_column not in all_cols: raise ValueError(f"Order by column '{order_by_column}' not found.")
    if target_column and target_column not in all_cols: raise ValueError(f"Target column '{target_column}' not found.")
    if partition_by_columns:
         if not isinstance(partition_by_columns, list): raise ValueError("partition_by_columns must be a list.")
         missing_part = [c for c in partition_by_columns if c not in all_cols]
         if missing_part: raise ValueError(f"Partition columns not found: {', '.join(missing_part)}")

    # Build OVER clause
    over_clauses = []
    if partition_by_columns:
        s_part = ", ".join([_sanitize_identifier(c) for c in partition_by_columns])
        over_clauses.append(f"PARTITION BY {s_part}")
    s_order = _sanitize_identifier(order_by_column)
    over_clauses.append(f"ORDER BY {s_order}") # Add ASC/DESC option later? Assume ASC.
    over_clause = f"OVER ({' '.join(over_clauses)})"

    # Build window function call
    window_expr = ""
    s_new_col = _sanitize_identifier(new_col_name)

    if func_lower == 'rank':
        window_expr = f"RANK() {over_clause}"
    elif func_lower == 'dense_rank':
         window_expr = f"DENSE_RANK() {over_clause}"
    elif func_lower == 'row_number':
         window_expr = f"ROW_NUMBER() {over_clause}"
    elif func_lower in ['lead', 'lag']:
        s_target = _sanitize_identifier(target_column)
        lead_lag_args = [s_target, str(int(offset))]
        if default_value is not None:
             lead_lag_args.append(_sanitize_literal(default_value))
        window_expr = f"{func_lower.upper()}({', '.join(lead_lag_args)}) {over_clause}"
    else:
        raise ValueError(f"Unsupported window_function: {func}. Try 'rank', 'dense_rank', 'row_number', 'lead', 'lag'.")

    s_all_cols = ", ".join([_sanitize_identifier(c) for c in all_cols])
    return f"SELECT {s_all_cols}, {window_expr} AS {s_new_col} FROM {table_name}"


def _sample_sql(table_name: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    """Generates SQL for sampling rows."""
    n = params.get("n") # Number of rows
    frac = params.get("frac") # Fraction of rows
    method = params.get("method", "system") # 'system' or 'bernoulli' for TABLESAMPLE, or 'random' for ORDER BY
    seed = params.get("seed") # Optional seed for reproducibility

    if n is None and frac is None:
        raise ValueError("Sample requires either 'n' (number of rows) or 'frac' (fraction).")
    if n is not None and frac is not None:
         raise ValueError("Provide either 'n' or 'frac' for sample, not both.")

    if method in ['system', 'bernoulli']:
         sample_clause = ""
         if n is not None:
             # TABLESAMPLE doesn't guarantee exact 'n', often used with percentage/fraction
             # Using percentage approximation. TABLESAMPLE (10 ROWS) is not standard SQL/DuckDB.
             # Estimate percentage needed (this is crude). Need total rows first.
             # Alternative: Use ORDER BY RANDOM() LIMIT n
             print(f"Warning: TABLESAMPLE with 'n={n}' is approximated. Using ORDER BY RANDOM() instead for exact count.")
             method = "random" # Fallback to ORDER BY for exact n
         else: # frac is not None
             percentage = float(frac) * 100
             sample_clause = f"USING SAMPLE {percentage:.2f} PERCENT ({method.upper()}"
             if seed is not None:
                 sample_clause += f" REPEATABLE ({int(seed)})"
             sample_clause += ")"
         if method != "random": # If still using TABLESAMPLE
              return f"SELECT * FROM {table_name} {sample_clause}"

    # Default or fallback: Use ORDER BY RANDOM() LIMIT n
    if method == "random" or n is not None:
         if seed is not None:
             # Set seed for the connection if possible (DuckDB specific?)
             # DuckDB uses SET seed=...; Not easy to inject here safely.
             # Let's ignore seed for ORDER BY RANDOM for now.
              print(f"Warning: Seed is ignored for ORDER BY RANDOM() sampling method.")

         limit_val = 0
         if n is not None:
             limit_val = int(n)
         elif frac is not None:
              # Need total rows to calculate limit from fraction. Requires extra query.
              # For simplicity, we'll require 'n' if using 'random' method.
              raise ValueError("Sampling with 'frac' using 'random' method is not directly supported without total row count. Use TABLESAMPLE or provide 'n'.")

         if limit_val <= 0:
             raise ValueError("Sample size 'n' must be positive.")

         return f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {limit_val}"
    else: # Should have been handled by TABLESAMPLE block
        raise ValueError(f"Invalid sampling state: method={method}, n={n}, frac={frac}")
    
def apply_sql_regex_get_full( # Assumed endpoint used by main.py for regex
    con: duckdb.DuckDBPyConnection,
    content: bytes,
    table_name: str,
    operation: str, # filter_contains, extract, replace etc.
    params: Dict[str, Any]
) -> Tuple[Optional[bytes], str]:
    """Applies regex operations using DuckDB's functions and returns full result bytes."""
    _load_data_to_duckdb(con, table_name, content)
    sanitized_base_table = _sanitize_identifier(table_name)
    
    # Get columns after load
    all_cols = [c[0] for c in con.execute(f'DESCRIBE {sanitized_base_table}').fetchall()]

    column = params.get("column")
    regex_pattern = params.get("regex")
    new_column = params.get("new_column", f"{column}_regex_{operation}")
    replacement = params.get("replacement", "") # For replace op
    # DuckDB regex flags are usually part of the pattern e.g., (?i) for case-insensitive

    if not column or not regex_pattern:
        raise ValueError("Regex operation requires 'column' and 'regex' pattern.")
    if column not in all_cols:
        raise ValueError(f"Column '{column}' not found for regex operation.")

    s_col = _sanitize_identifier(column)
    s_pattern = _sanitize_literal(regex_pattern)
    s_new_col = _sanitize_identifier(new_column)
    s_replacement = _sanitize_literal(replacement)
    s_all_cols = ", ".join([_sanitize_identifier(c) for c in all_cols])

    generated_sql = ""

    if operation == "filter": # Filter rows matching regex
        # Uses regexp_matches function
        generated_sql = f"SELECT * FROM {sanitized_base_table} WHERE regexp_matches({s_col}::TEXT, {s_pattern})"
    elif operation == "extract": # Extract first match into new column
        # Uses regexp_extract function (group 0 for full match)
        extract_expr = f"regexp_extract({s_col}::TEXT, {s_pattern}, 0) AS {s_new_col}"
        generated_sql = f"SELECT {s_all_cols}, {extract_expr} FROM {sanitized_base_table}"
    elif operation == "extract_group": # Extract specific group
        group_idx = params.get("group", 1) # Default to first capture group
        extract_expr = f"regexp_extract({s_col}::TEXT, {s_pattern}, {int(group_idx)}) AS {s_new_col}"
        generated_sql = f"SELECT {s_all_cols}, {extract_expr} FROM {sanitized_base_table}"
    elif operation == "replace": # Replace matches in the column (or create new)
        replace_expr = f"regexp_replace({s_col}::TEXT, {s_pattern}, {s_replacement}) AS {s_new_col}"
        select_list = []
        replaced = False
        for c in all_cols:
            s_c = _sanitize_identifier(c)
            if c == column and new_column == column: # Replace in place
                 select_list.append(replace_expr)
                 replaced = True
            else:
                 select_list.append(s_c)
        if not replaced: # Add as new column
            select_list.append(replace_expr)
        generated_sql = f"SELECT {', '.join(select_list)} FROM {sanitized_base_table}"
    else:
        raise ValueError(f"Unsupported regex operation type: {operation}")

    try:
        # Fetch the *full* result dataframe
        full_df = con.execute(generated_sql).fetchdf()
        # Convert full result back to CSV bytes
        with io.BytesIO() as buffer:
            full_df.to_csv(buffer, index=False)
            new_content_bytes = buffer.getvalue()
        return new_content_bytes, generated_sql

    except duckdb.Error as e:
        print(f"SQL Regex Error: {type(e).__name__} - {e}\nQuery: {generated_sql}")
        raise ValueError(f"Error during SQL Regex '{operation}': {str(e)}")
