# backend/app/services/sql_service.py
import duckdb
import pandas as pd
import io
import re
from typing import Dict, Any, Tuple, List

# --- Helper Functions (Keep _sanitize_identifier, _sanitize_literal, _load_data_to_duckdb, _execute_sql_query) ---
# ... (Keep existing helpers) ...

def _sanitize_identifier(identifier: str) -> str:
    """Sanitizes table or column names for safe use in SQL queries."""
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
        df_temp = pd.read_csv(io.BytesIO(content))
        con.register('df_temp_view', df_temp)
        con.execute(f"CREATE OR REPLACE TABLE {sanitized_table_name} AS SELECT * FROM df_temp_view;")
        con.unregister('df_temp_view')
    except (pd.errors.ParserError, duckdb.Error) as e:
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
    all_cols: List[str] # Pass this down!
) -> Tuple[List[Dict], List[str], int, str]:
    """Applies a specified SQL operation by generating and executing a query."""
    _load_data_to_duckdb(con, base_table_name, content)
    sanitized_base_table = _sanitize_identifier(base_table_name)

    try:
        generated_sql = ""
        if operation == "filter":
            generated_sql = _filter_rows_sql(sanitized_base_table, params, all_cols) # Pass all_cols
        elif operation == "select_columns":
            generated_sql = _select_columns_sql(sanitized_base_table, params, all_cols) # Already has all_cols
        elif operation == "sort":
            generated_sql = _sort_values_sql(sanitized_base_table, params, all_cols) # Pass all_cols
        elif operation == "rename":
            generated_sql = _rename_columns_sql(sanitized_base_table, params, all_cols) # Already has all_cols
        elif operation == "drop_columns":
            generated_sql = _drop_columns_sql(sanitized_base_table, params, all_cols) # Already has all_cols
        elif operation == "groupby":
            generated_sql = _group_by_sql(sanitized_base_table, params, all_cols) # Pass all_cols
        elif operation == "groupby_multi":
            generated_sql = _group_by_multi_sql(sanitized_base_table, params, all_cols) # Pass all_cols
        elif operation == "groupby_multi_agg":
            generated_sql = _group_by_multi_agg_sql(sanitized_base_table, params, all_cols) # Pass all_cols
        # Add stubs for other ops if needed, raising errors
        # elif operation == "pivot_table" or operation == "pivot":
        #    raise ValueError("SQL Pivot not yet implemented.")
        # elif operation == "melt":
        #    raise ValueError("SQL Melt not yet implemented.")
        # elif operation == "set_index" or operation == "reset_index":
        #    generated_sql = f"SELECT * FROM {sanitized_base_table}" # No-op
        else:
            raise ValueError(f"Unsupported SQL operation: {operation}")

        # Execute the generated query
        preview_data, result_columns, total_rows = _execute_sql_query(con, generated_sql)

        return preview_data, result_columns, total_rows, generated_sql

    except Exception as e:
        # Let main.py handle specific exceptions, just log and re-raise
        print(f"Error preparing SQL operation '{operation}': {type(e).__name__}: {e}")
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
    """Performs a SQL JOIN operation between two tables in DuckDB."""
    how = params.get("join_type", "inner").upper()
    left_on = params.get("left_on")
    right_on = params.get("right_on")

    if not left_on or not right_on:
         raise ValueError("Both left_on and right_on key columns are required for join")

    # --- Validation ---
    if left_on not in left_cols: raise ValueError(f"Left key '{left_on}' not found in left dataset columns.")
    if right_on not in right_cols: raise ValueError(f"Right key '{right_on}' not found in right dataset columns.")
    # ---

    valid_joins = ['INNER', 'LEFT', 'RIGHT', 'FULL OUTER', 'OUTER', 'CROSS']
    if how not in valid_joins:
        raise ValueError(f"Invalid join type: {how}. Must be one of {valid_joins}")
    if how == 'OUTER': how = 'FULL OUTER'

    s_left_table = _sanitize_identifier(left_table)
    s_right_table = _sanitize_identifier(right_table)
    s_left_on = _sanitize_identifier(left_on)
    s_right_on = _sanitize_identifier(right_on)

    # Build SELECT list, aliasing columns
    select_clauses = []
    final_columns = []

    for col in left_cols:
        s_col = _sanitize_identifier(col)
        alias = _sanitize_identifier(f"l_{col}")
        select_clauses.append(f"{s_left_table}.{s_col} AS {alias}")
        final_columns.append(f"l_{col}")

    for col in right_cols:
        s_col = _sanitize_identifier(col)
        # Avoid adding the right join key if it's identical to the left one? Optional.
        # If keys have different names, alias both.
        alias = _sanitize_identifier(f"r_{col}")
        if col == right_on and left_on == right_on:
             # Maybe add option to include/exclude duplicate join key? For now, skip.
             # select_clauses.append(f"{s_right_table}.{s_col} AS {alias}") # Add if needed
             # final_columns.append(f"r_{col}")
             continue
        else:
            select_clauses.append(f"{s_right_table}.{s_col} AS {alias}")
            final_columns.append(f"r_{col}")


    select_list = ", ".join(select_clauses) if select_clauses else "*" # Failsafe

    # Build the JOIN query
    join_condition = f"{s_left_table}.{s_left_on} = {s_right_table}.{s_right_on}"
    # Handle CROSS JOIN separately as it doesn't use ON
    if how == 'CROSS':
        generated_sql = f"SELECT {select_list} FROM {s_left_table} {how} JOIN {s_right_table}"
    else:
        generated_sql = f"SELECT {select_list} FROM {s_left_table} {how} JOIN {s_right_table} ON {join_condition}"


    try:
        # Execute query to get results
        preview_data, _, total_rows = _execute_sql_query(con, generated_sql) # Use constructed final_columns

        return preview_data, final_columns, total_rows, generated_sql
    except Exception as e:
        print(f"SQL Join Error: {type(e).__name__} - {e}\nQuery: {generated_sql}") # Log error
        raise ValueError(f"Error during SQL JOIN operation: {str(e)}")