# backend/app/services/sql_service.py
import duckdb
import pandas as pd
import io
import re
import uuid
import numpy as np
import traceback
from typing import Dict, Any, Tuple, List, Optional

# --- Helper Functions ---

def _sanitize_identifier(identifier: Optional[str]) -> Optional[str]:
    """Sanitizes table or column names for safe use in SQL queries."""
    if identifier is None: return None
    # Allow already quoted identifiers
    if identifier.startswith('"') and identifier.endswith('"'): return identifier
    # Basic check for common SQL keywords - very incomplete, just illustrative
    keywords = {"SELECT", "FROM", "WHERE", "TABLE", "GROUP", "ORDER", "UPDATE", "DELETE", "INSERT", "CREATE"}
    if identifier.upper() in keywords:
        print(f"Warning: Identifier '{identifier}' might be a SQL keyword. Quoting.")
    # Escape double quotes within the identifier and wrap the whole thing
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'

def _sanitize_literal(value: Any) -> str:
    """Sanitizes literal values for safe use in SQL WHERE clauses."""
    if isinstance(value, (int, float)): return str(value)
    elif isinstance(value, bool): return str(value).upper() # TRUE/FALSE
    elif value is None: return "NULL"
    else:
        # Basic SQL injection prevention: replace single quotes
        sanitized_value = str(value).replace("'", "''")
        return f"'{sanitized_value}'"

def _load_data_to_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, content: bytes):
    """Loads data from CSV bytes content into a DuckDB table."""
    sanitized_table_name = _sanitize_identifier(table_name)
    # Use a temporary unique view name for registration to avoid conflicts
    temp_view_name = f'__load_view_{uuid.uuid4().hex[:8]}'
    try:
        # Read into pandas first to handle potential CSV issues robustly
        df_temp = pd.read_csv(io.BytesIO(content))
        # Register the DataFrame as a view
        con.register(temp_view_name, df_temp)
        # Create the actual table from the view
        con.execute(f"CREATE OR REPLACE TABLE {sanitized_table_name} AS SELECT * FROM {temp_view_name};")
        print(f"Loaded data into DuckDB table: {sanitized_table_name}")
    except (pd.errors.ParserError, pd.errors.EmptyDataError, duckdb.Error, Exception) as e:
        raise ValueError(f"Failed to load data into DuckDB table {sanitized_table_name}: {e}")
    finally:
        # Always unregister the temporary view
        try:
            con.unregister(temp_view_name)
        except Exception:
            pass # Ignore errors during cleanup

def _execute_sql_query(con: duckdb.DuckDBPyConnection, query: str, preview_limit: int = 100) -> Tuple[List[Dict], List[str], int]:
    """Executes a SQL query, gets preview data, columns, and total row count."""
    try:
        # Use CTE for count to handle complex queries correctly
        count_query = f"WITH result_set AS ({query}) SELECT COUNT(*) FROM result_set;"
        total_rows_result = con.execute(count_query).fetchone()
        if total_rows_result is None: raise ValueError("Count query returned no result.")
        total_rows = total_rows_result[0]

        # Add limit for preview fetching
        preview_query = f"WITH result_set AS ({query}) SELECT * FROM result_set LIMIT {preview_limit};"
        preview_result = con.execute(preview_query)

        columns = [desc[0] for desc in preview_result.description]
        # Fetch as Arrow table and convert to list of dicts for JSON compatibility
        data_dicts = preview_result.fetch_arrow_table().to_pylist()

        # Convert numpy/pandas types to standard Python types for JSON
        for row in data_dicts:
            for col, val in row.items():
                if hasattr(val, 'isoformat'): row[col] = val.isoformat()
                elif isinstance(val, (pd.Timestamp)): row[col] = val.isoformat()
                elif isinstance(val, (np.integer, np.int64)): row[col] = int(val)
                elif isinstance(val, (np.floating, np.float64)): row[col] = float(val) if pd.notna(val) else None # Convert NaN to None
                elif isinstance(val, np.bool_): row[col] = bool(val)
                elif pd.isna(val): row[col] = None # Ensure other NA types become None

        return data_dicts, columns, total_rows
    except (duckdb.Error, AttributeError, TypeError, ValueError) as e:
        print(f"SQL Execution/Processing Error: {type(e).__name__} - {e}\nQuery: {query}")
        raise ValueError(f"Failed to execute or process SQL query: {e}")

# --- Main Operation Dispatcher ---

def apply_sql_operation(
    con: duckdb.DuckDBPyConnection,
    previous_sql_chain: str, # The full SQL query representing the state *before* this operation
    operation: str,
    params: Dict[str, Any],
    base_table_ref: str # The sanitized identifier of the *original* data table (e.g., '"my_data"')
) -> Tuple[List[Dict], List[str], int, str, str]: # Returns: preview, cols, count, *new_full_sql_chain*, *sql_snippet* (CTE definition)
    """
    Applies a specified SQL operation by adding a CTE to the previous chain.

    Args:
        con: DuckDB connection.
        previous_sql_chain: The full SQL query representing the previous state.
        operation: The name of the operation to apply.
        params: Parameters for the operation.
        base_table_ref: Sanitized identifier for the table holding the original data.

    Returns:
        Tuple containing preview data, columns, total row count, the new full SQL chain,
        and the SQL snippet (CTE definition) for the applied step.
    """
    try:
        # Determine the alias/reference for the *previous* step's result
        # Use a CTE naming convention: step0, step1, ...
        # Count existing CTE steps based on a pattern like 'WITH stepX AS' or the final 'SELECT * FROM stepX'
        step_match = re.findall(r"step(\d+)", previous_sql_chain, re.IGNORECASE)
        num_steps = max([int(m) for m in step_match]) + 1 if step_match else 0

        # Alias for the result of the previous step (or the base table if first step)
        previous_step_alias = f"step{num_steps - 1}" if num_steps > 0 else base_table_ref
        current_step_alias = f"step{num_steps}"

        print(f"Applying SQL Step {num_steps}: Operation='{operation}', PreviousAlias='{previous_step_alias}', CurrentAlias='{current_step_alias}'")

        # Get columns from the previous step for validation
        all_cols = []
        try:
            # Create a temporary view of the *previous* state to get columns
            temp_view_name = f"__describe_prev_{uuid.uuid4().hex[:8]}"
            # Ensure the previous chain is executable (might need the base table loaded)
            con.execute(f"CREATE OR REPLACE TEMP VIEW {temp_view_name} AS ({previous_sql_chain});")
            cols_result = con.execute(f"DESCRIBE {temp_view_name};").fetchall()
            all_cols = [col[0] for col in cols_result]
            con.execute(f"DROP VIEW IF EXISTS {temp_view_name};")
            print(f"Columns from previous step ({previous_step_alias}): {all_cols}")
        except duckdb.Error as desc_err:
            # This might fail if the base table wasn't loaded in this connection context yet
            # Or if the previous_sql_chain is invalid.
            print(f"Warning: Could not describe previous SQL state '{previous_step_alias}' to get columns: {desc_err}")
            # Proceed without column validation if describe fails, rely on execution error
            all_cols = []


        # Generate the SQL logic *for this step*, operating on the previous_step_alias
        # These helper functions now need to return the core SELECT logic based on the input alias
        step_logic_sql = ""
        if operation == "filter": step_logic_sql = _filter_rows_sql(previous_step_alias, params, all_cols)
        elif operation == "select_columns": step_logic_sql = _select_columns_sql(previous_step_alias, params, all_cols)
        elif operation == "sort": step_logic_sql = _sort_values_sql(previous_step_alias, params, all_cols)
        elif operation == "rename": step_logic_sql = _rename_columns_sql(previous_step_alias, params, all_cols)
        elif operation == "drop_columns": step_logic_sql = _drop_columns_sql(previous_step_alias, params, all_cols)
        elif operation == "groupby": step_logic_sql = _group_by_sql(previous_step_alias, params, all_cols)
        # elif operation == "groupby_multi": step_logic_sql = _group_by_multi_sql(previous_step_alias, params, all_cols) # Deprecated?
        elif operation == "groupby_multi_agg": step_logic_sql = _group_by_multi_agg_sql(previous_step_alias, params, all_cols)
        elif operation == "fillna": step_logic_sql = _fillna_sql(previous_step_alias, params, all_cols)
        elif operation == "dropna": step_logic_sql = _dropna_sql(previous_step_alias, params, all_cols)
        elif operation == "astype": step_logic_sql = _astype_sql(previous_step_alias, params, all_cols)
        elif operation == "string_operation": step_logic_sql = _string_op_sql(previous_step_alias, params, all_cols)
        elif operation == "date_extract": step_logic_sql = _date_extract_sql(previous_step_alias, params, all_cols)
        elif operation == "drop_duplicates": step_logic_sql = _drop_duplicates_sql(previous_step_alias, params, all_cols)
        elif operation == "create_column": step_logic_sql = _create_column_sql(previous_step_alias, params, all_cols)
        elif operation == "window_function": step_logic_sql = _window_function_sql(previous_step_alias, params, all_cols)
        elif operation == "sample": step_logic_sql = _sample_sql(previous_step_alias, params, all_cols)
        # Handle Regex operations routed here
        elif operation == "regex_filter": step_logic_sql = _regex_filter_sql(previous_step_alias, params, all_cols)
        elif operation == "regex_extract": step_logic_sql = _regex_extract_sql(previous_step_alias, params, all_cols, group=0)
        elif operation == "regex_extract_group": step_logic_sql = _regex_extract_sql(previous_step_alias, params, all_cols, group=params.get("group", 1))
        elif operation == "regex_replace": step_logic_sql = _regex_replace_sql(previous_step_alias, params, all_cols)
        # Handle No-op operations
        elif operation in ["set_index", "reset_index"]:
            print(f"Warning: '{operation}' is a no-op in SQL context. Passing through previous state.")
            step_logic_sql = f"SELECT * FROM {previous_step_alias}"
        else:
            raise ValueError(f"Unsupported SQL operation: {operation}")

        # Construct the *new full SQL chain* using CTEs
        new_full_sql_chain = ""
        # The snippet to store in history is the definition of the current step's CTE
        current_step_cte_definition = f"{current_step_alias} AS (\n  {step_logic_sql}\n)"

        if num_steps == 0:
            # First step: Define step0 based on the operation applied to the base table
            new_full_sql_chain = f"WITH {current_step_cte_definition}\nSELECT * FROM {current_step_alias}"
        else:
            # Append the new step's CTE definition to the previous chain
            # Find the start of the final SELECT statement in the previous chain
            select_match = re.search(r"SELECT\s+\*\s+FROM\s+step\d+\s*$", previous_sql_chain, re.IGNORECASE | re.DOTALL)
            if select_match:
                # Insert the new CTE definition before the final SELECT
                insertion_point = select_match.start()
                existing_ctes = previous_sql_chain[:insertion_point].strip()
                # Ensure comma separation if needed
                separator = ",\n" if existing_ctes.strip().endswith(")") else "\n" # Check if previous CTE exists
                new_full_sql_chain = f"{existing_ctes}{separator}{current_step_cte_definition}\nSELECT * FROM {current_step_alias}"
            else:
                # Fallback or error - chain structure might be unexpected
                print(f"Warning: Could not find final 'SELECT * FROM stepX' in previous chain. Appending CTE might be incorrect.")
                # Attempt to append naively (might break)
                new_full_sql_chain = f"{previous_sql_chain},\n{current_step_cte_definition}\nSELECT * FROM {current_step_alias}"
                # Or raise error:
                # raise ValueError("Could not parse previous SQL chain structure to insert new CTE.")

        print(f"Generated New Full SQL Chain:\n{new_full_sql_chain}")

        # Execute the new full query to get the preview data for *this* step's result
        preview_data, result_columns, total_rows = _execute_sql_query(con, new_full_sql_chain)

        # Return preview, columns, count, the *new full chain*, and the *snippet* (CTE definition)
        return preview_data, result_columns, total_rows, new_full_sql_chain, current_step_cte_definition

    except Exception as e:
        print(f"Error preparing/executing SQL operation '{operation}' (Step {num_steps}): {type(e).__name__}: {e}")
        traceback.print_exc() # Print full traceback for debugging
        # Re-raise the exception so the endpoint handler can catch it
        raise e


# --- Specific Operation Implementations ---
# These functions now generate the core SELECT logic for a step,
# assuming the input table is the alias of the previous step.

def _validate_columns(required_cols: List[str], available_cols: List[str], operation_name: str):
    """Checks if required columns exist, raises ValueError if not."""
    if not available_cols: # Skip validation if previous columns couldn't be determined
        print(f"Warning: Skipping column validation for {operation_name} due to missing column info from previous step.")
        return
    missing = [col for col in required_cols if col not in available_cols]
    if missing:
        raise ValueError(f"{operation_name} failed: Column(s) not found in previous step: {', '.join(missing)}")

def _filter_rows_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    column = params.get("column")
    operator = params.get("operator")
    value = params.get("value")
    if not all([column, operator]): raise ValueError("Filter requires 'column' and 'operator'")
    _validate_columns([column], all_cols, "Filter")

    sanitized_col = _sanitize_identifier(column)
    condition = ""
    op_map = { "==": "=", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<=" }

    if operator in op_map:
         # Attempt direct comparison, rely on DuckDB casting
         condition = f"{sanitized_col} {op_map[operator]} {_sanitize_literal(value)}"
    elif operator in ["contains", "startswith", "endswith"]:
        like_value = str(value).replace("'", "''").replace("%", "\\%").replace("_", "\\_")
        if operator == "contains": pattern = f"'%{like_value}%'"
        elif operator == "startswith": pattern = f"'{like_value}%'"
        else: pattern = f"'%{like_value}'" # endswith
        # Add explicit cast to TEXT for LIKE robustness
        condition = f"{sanitized_col}::TEXT LIKE {pattern} ESCAPE '\\'"
    elif operator == "regex":
        # Add explicit cast to TEXT for REGEXP_MATCHES
        condition = f"REGEXP_MATCHES({sanitized_col}::TEXT, {_sanitize_literal(str(value))})"
    elif operator == "isnull":
        condition = f"{sanitized_col} IS NULL"
    elif operator == "notnull":
        condition = f"{sanitized_col} IS NOT NULL"
    else:
        raise ValueError(f"Unsupported filter operator for SQL: {operator}")

    return f"SELECT * FROM {input_alias} WHERE {condition}"

def _select_columns_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    selected_columns = params.get("selected_columns", [])
    if not selected_columns: raise ValueError("Select requires 'selected_columns'")
    _validate_columns(selected_columns, all_cols, "Select Columns")

    select_list = ", ".join([_sanitize_identifier(col) for col in selected_columns])
    return f"SELECT {select_list} FROM {input_alias}"

def _sort_values_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    sort_columns = params.get("sort_columns") # Expect list of dicts: [{'column': 'c1', 'ascending': True}, ...]
    if not sort_columns or not isinstance(sort_columns, list):
        raise ValueError("Sort requires 'sort_columns' as a list of {'column': name, 'ascending': bool}")

    order_by_clauses = []
    for sort_spec in sort_columns:
        col = sort_spec.get("column")
        ascending = sort_spec.get("ascending", True)
        if not col: raise ValueError("Each item in 'sort_columns' must have a 'column' key.")
        _validate_columns([col], all_cols, f"Sort by {col}")
        order = "ASC" if ascending else "DESC"
        order_by_clauses.append(f"{_sanitize_identifier(col)} {order}")

    order_by_list = ", ".join(order_by_clauses)
    return f"SELECT * FROM {input_alias} ORDER BY {order_by_list}"


def _rename_columns_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    renames = params.get("renames", []) # Expect list of {'old_name': 'o', 'new_name': 'n'}
    if not renames: raise ValueError("Rename requires 'renames' list")
    rename_map = {item['old_name']: item['new_name'] for item in renames if item.get('old_name') and item.get('new_name')}
    if not rename_map: raise ValueError("Invalid rename parameters.")
    _validate_columns(list(rename_map.keys()), all_cols, "Rename")

    select_clauses = []
    if not all_cols: # If column info unavailable, just rename specified columns and select rest with *
         print("Warning: Renaming without full column list; using '*' for unspecified columns.")
         select_clauses.append("*") # This might fail if columns clash, but best effort
         for old, new in rename_map.items():
             select_clauses.append(f"{_sanitize_identifier(old)} AS {_sanitize_identifier(new)}")
    else:
        for col in all_cols:
            sanitized_old = _sanitize_identifier(col)
            if col in rename_map:
                sanitized_new = _sanitize_identifier(rename_map[col])
                select_clauses.append(f"{sanitized_old} AS {sanitized_new}")
            else:
                select_clauses.append(sanitized_old)

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {input_alias}"

def _drop_columns_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    drop_columns = params.get("drop_columns", [])
    if not drop_columns: raise ValueError("Drop requires 'drop_columns'")
    _validate_columns(drop_columns, all_cols, "Drop Columns")

    if not all_cols:
         raise ValueError("Cannot perform drop operation reliably without knowing all columns from the previous step.")

    keep_cols = [col for col in all_cols if col not in drop_columns]
    if not keep_cols: raise ValueError("Cannot drop all columns")

    select_list = ", ".join([_sanitize_identifier(col) for col in keep_cols])
    return f"SELECT {select_list} FROM {input_alias}"


def _map_agg_func_sql(func_name: str) -> str:
    """Maps common function names to SQL aggregate functions."""
    func_lower = func_name.lower()
    # DuckDB specific functions included
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
        'nunique': 'COUNT(DISTINCT {})',
        'list': 'LIST', # DuckDB specific
        'mode': 'MODE'   # DuckDB specific
    }
    if func_lower not in mapping:
        raise ValueError(f"Unsupported SQL aggregation function: {func_name}")
    return mapping[func_lower]

def _build_agg_clause_sql(agg_col: str, func_name: str, alias_suffix: bool = True) -> str:
    """Builds a single SELECT clause for an aggregation."""
    sql_func_template = _map_agg_func_sql(func_name)
    sanitized_agg_col = _sanitize_identifier(agg_col) if agg_col != '*' else '*'
    alias = _sanitize_identifier(f"{agg_col}_{func_name}") if alias_suffix and agg_col != '*' else _sanitize_identifier(func_name)

    # Handle COUNT(*) separately
    if func_name.lower() == 'count' and agg_col == '*':
        return f"COUNT(*) AS {alias}"

    # Handle functions requiring DISTINCT or specific casting
    numeric_funcs = ['AVG', 'SUM', 'MEDIAN', 'STDDEV_SAMP', 'VAR_SAMP', 'MODE']
    target_col = sanitized_agg_col

    # Add explicit casting for numeric functions as a safeguard? Maybe not needed.
    # if sql_func_template in numeric_funcs:
    #     target_col = f"{sanitized_agg_col}::NUMERIC" # Cast to NUMERIC

    if sql_func_template == 'COUNT(DISTINCT {})':
        return f"{sql_func_template.format(target_col)} AS {alias}"
    else:
        return f"{sql_func_template}({target_col}) AS {alias}"

def _group_by_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    # Simplified version, use groupby_multi_agg for more complex cases
    group_column = params.get("group_column")
    agg_column = params.get("agg_column")
    agg_function = params.get("agg_function", "count")
    if not all([group_column, agg_column, agg_function]): raise ValueError("GroupBy requires 'group_column', 'agg_column', and 'agg_function'")
    _validate_columns([group_column, agg_column], all_cols, "GroupBy")

    sanitized_group_col = _sanitize_identifier(group_column)
    agg_clause = _build_agg_clause_sql(agg_column, agg_function)

    return f"SELECT {sanitized_group_col}, {agg_clause} FROM {input_alias} GROUP BY {sanitized_group_col}"

def _group_by_multi_agg_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    group_columns = params.get("group_columns") # List
    aggregations = params.get("aggregations") # List of {'column': 'c', 'function': 'f'}

    if not group_columns or not aggregations: raise ValueError("GroupBy MultiAgg requires 'group_columns' (list) and 'aggregations' (list)")
    if not isinstance(group_columns, list) or not group_columns: raise ValueError("'group_columns' must be a non-empty list")
    if not isinstance(aggregations, list) or not aggregations: raise ValueError("'aggregations' must be a non-empty list")

    _validate_columns(group_columns, all_cols, "GroupBy MultiAgg (Group Keys)")

    sanitized_group_cols = [_sanitize_identifier(col) for col in group_columns]
    group_by_list = ", ".join(sanitized_group_cols)

    agg_clauses = []
    agg_cols_to_validate = []
    for agg_spec in aggregations:
        col = agg_spec.get("column")
        func = agg_spec.get("function")
        if not col or not func: raise ValueError(f"Invalid aggregation spec: {agg_spec}")
        # Allow COUNT(*)
        if col != '*': agg_cols_to_validate.append(col)
        agg_clauses.append(_build_agg_clause_sql(col, func))

    _validate_columns(agg_cols_to_validate, all_cols, "GroupBy MultiAgg (Agg Columns)")

    if not agg_clauses: raise ValueError("No valid aggregations provided")
    agg_select_list = ", ".join(agg_clauses)

    return f"SELECT {group_by_list}, {agg_select_list} FROM {input_alias} GROUP BY {group_by_list}"


# --- Join Operation ---
# This needs to be handled differently now. It combines the chain of the left table
# with the base state of the right table.
def apply_sql_join(
    con: duckdb.DuckDBPyConnection,
    previous_sql_chain_left: str, # The full SQL query representing the left state *before* this join
    right_table_ref: str, # Sanitized identifier for the *original* right table
    params: Dict[str, Any],
    base_table_ref_left: str, # Sanitized identifier for the *original* left table
    # Add right_cols for validation?
) -> Tuple[List[Dict], List[str], int, str, str]: # Returns: preview, cols, count, *new_full_sql_chain*, *sql_snippet*
    """Applies a JOIN operation, adding a step to the left table's SQL chain."""
    try:
        how = params.get("join_type", "inner").upper()
        left_on = params.get("left_on")
        right_on = params.get("right_on")

        if not left_on or not right_on: raise ValueError("Join requires 'left_on' and 'right_on' keys")
        valid_joins = ['INNER', 'LEFT', 'RIGHT', 'FULL OUTER', 'OUTER', 'CROSS', 'SEMI', 'ANTI']
        if how not in valid_joins: raise ValueError(f"Invalid join type: {how}. Must be one of {valid_joins}")
        if how == 'OUTER': how = 'FULL OUTER'

        # Determine step number and alias for the new join step
        step_match = re.findall(r"step(\d+)", previous_sql_chain_left, re.IGNORECASE)
        num_steps = max([int(m) for m in step_match]) + 1 if step_match else 0
        previous_left_alias = f"step{num_steps - 1}" if num_steps > 0 else base_table_ref_left
        current_join_alias = f"step{num_steps}" # Alias for the result of the join

        # Get columns from the left previous step and the right base table for validation/select list
        left_cols, right_cols = [], []
        try:
            # Describe left previous step
            temp_view_left = f"__desc_j_left_{uuid.uuid4().hex[:8]}"
            con.execute(f"CREATE OR REPLACE TEMP VIEW {temp_view_left} AS ({previous_sql_chain_left});")
            left_cols = [c[0] for c in con.execute(f"DESCRIBE {temp_view_left};").fetchall()]
            con.execute(f"DROP VIEW IF EXISTS {temp_view_left};")
            # Describe right base table
            right_cols = [c[0] for c in con.execute(f"DESCRIBE {right_table_ref};").fetchall()]
        except duckdb.Error as desc_err:
            print(f"Warning: Could not describe tables for JOIN column validation: {desc_err}")
            # Proceed without validation if describe fails

        # Validate join keys exist
        if left_cols and left_on not in left_cols: raise ValueError(f"Join failed: Left key '{left_on}' not found in previous step result.")
        if right_cols and right_on not in right_cols: raise ValueError(f"Join failed: Right key '{right_on}' not found in base table {right_table_ref}.")

        # Build SELECT list for the join step, aliasing to avoid collisions
        select_clauses = []
        final_columns = [] # Track final column names after aliasing

        # Alias columns from the left side (result of previous step)
        for col in left_cols:
            alias = _sanitize_identifier(f"left_{col}") # Prefix to avoid clashes
            select_clauses.append(f"{previous_left_alias}.{_sanitize_identifier(col)} AS {alias}")
            final_columns.append(f"left_{col}")

        # Alias columns from the right side (base table)
        for col in right_cols:
            # Skip right join key if names are same and it's not needed? Maybe keep for clarity.
            alias = _sanitize_identifier(f"right_{col}") # Prefix
            # Handle potential duplicate alias (unlikely with prefixes but safe)
            while alias in final_columns: alias = _sanitize_identifier(f"{alias}_")
            select_clauses.append(f"{right_table_ref}.{_sanitize_identifier(col)} AS {alias}")
            final_columns.append(alias.strip('"'))

        select_list = ", ".join(select_clauses) if select_clauses else "*" # Fallback to * if columns unknown

        # Build the JOIN logic for the current step's CTE
        s_left_on = _sanitize_identifier(left_on)
        s_right_on = _sanitize_identifier(right_on)
        join_condition = f"{previous_left_alias}.{s_left_on} = {right_table_ref}.{s_right_on}"

        # Special handling for SEMI/ANTI joins (only select from left)
        if how in ['SEMI', 'ANTI']:
             select_list = ", ".join(select_clauses[:len(left_cols)]) # Only left columns
             final_columns = final_columns[:len(left_cols)]

        if how == 'CROSS':
            step_logic_sql = f"SELECT {select_list} FROM {previous_left_alias} CROSS JOIN {right_table_ref}"
        else:
            step_logic_sql = f"SELECT {select_list} FROM {previous_left_alias} {how} JOIN {right_table_ref} ON {join_condition}"

        # Construct the new full SQL chain
        current_step_cte_definition = f"{current_join_alias} AS (\n  {step_logic_sql}\n)"
        new_full_sql_chain = ""
        if num_steps == 0:
            new_full_sql_chain = f"WITH {current_step_cte_definition}\nSELECT * FROM {current_join_alias}"
        else:
            select_match = re.search(r"SELECT\s+\*\s+FROM\s+step\d+\s*$", previous_sql_chain_left, re.IGNORECASE | re.DOTALL)
            if select_match:
                insertion_point = select_match.start()
                existing_ctes = previous_sql_chain_left[:insertion_point].strip()
                separator = ",\n" if existing_ctes.strip().endswith(")") else "\n"
                new_full_sql_chain = f"{existing_ctes}{separator}{current_step_cte_definition}\nSELECT * FROM {current_join_alias}"
            else:
                raise ValueError("Could not parse previous SQL chain structure for JOIN.")

        print(f"Generated New Full SQL Chain (Join):\n{new_full_sql_chain}")

        # Execute for preview
        preview_data, _, total_rows = _execute_sql_query(con, new_full_sql_chain) # Use derived final_columns

        # Return preview, final columns, count, new chain, and CTE definition
        return preview_data, final_columns, total_rows, new_full_sql_chain, current_step_cte_definition

    except Exception as e:
        print(f"Error during SQL JOIN operation: {type(e).__name__}: {e}")
        traceback.print_exc()
        raise e

# --- Implementations for other operations (_fillna_sql, _dropna_sql, etc.) ---
# These follow the pattern: take input_alias, params, all_cols; return SELECT logic string.

def _fillna_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    columns_to_fill = params.get("columns") # List or None for all
    fill_value = params.get("value")
    if fill_value is None: raise ValueError("fillna requires a non-null 'value' for SQL COALESCE.")
    sanitized_fill_value = _sanitize_literal(fill_value)

    target_cols = columns_to_fill if columns_to_fill else all_cols
    if target_cols: _validate_columns(target_cols, all_cols, "Fill NA")

    select_clauses = []
    if not all_cols: # Handle missing column info
         print("Warning: FillNA without full column list; applying only to specified columns.")
         if not target_cols: raise ValueError("Cannot apply FillNA to all columns without knowing them.")
         select_clauses.append("*") # Select existing columns
         for col in target_cols: # Add COALESCE expressions separately (might cause duplicates if target_cols exist)
             s_col = _sanitize_identifier(col)
             select_clauses.append(f"COALESCE({s_col}, {sanitized_fill_value}) AS {s_col}") # Overwrite
    else:
        for col in all_cols:
            s_col = _sanitize_identifier(col)
            if col in target_cols:
                select_clauses.append(f"COALESCE({s_col}, {sanitized_fill_value}) AS {s_col}")
            else:
                select_clauses.append(s_col)

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {input_alias}"

def _dropna_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    subset = params.get("subset") # Optional list
    if subset and not isinstance(subset, list): raise ValueError("'subset' for dropna must be a list.")

    target_cols = subset if subset else all_cols
    if not target_cols: raise ValueError("Cannot drop NA without columns to check.")
    if target_cols: _validate_columns(target_cols, all_cols, "Drop NA")

    where_clauses = [f"{_sanitize_identifier(col)} IS NOT NULL" for col in target_cols]
    where_condition = " AND ".join(where_clauses)

    return f"SELECT * FROM {input_alias} WHERE {where_condition}"

def _astype_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    column = params.get("column")
    new_type = params.get("new_type")
    supported_types = ["INTEGER", "BIGINT", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL", "VARCHAR", "TEXT", "DATE", "TIMESTAMP", "BOOLEAN", "BLOB", "UUID", "TIME"]
    if not column or not new_type: raise ValueError("astype requires 'column' and 'new_type'.")
    _validate_columns([column], all_cols, "Cast Type")
    if new_type.upper() not in supported_types: raise ValueError(f"Unsupported SQL type '{new_type}'. Supported: {supported_types}")

    select_clauses = []
    if not all_cols:
        print("Warning: Cast Type without full column list; using '*' and adding cast.")
        select_clauses.append("*")
        select_clauses.append(f"{_sanitize_identifier(column)}::{new_type.upper()} AS {_sanitize_identifier(column)}") # Overwrite
    else:
        for col in all_cols:
            s_col = _sanitize_identifier(col)
            if col == column:
                select_clauses.append(f"{s_col}::{new_type.upper()} AS {s_col}")
            else:
                select_clauses.append(s_col)

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {input_alias}"

def _string_op_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    column = params.get("column")
    string_func = params.get("string_function")
    new_col_name = params.get("new_column_name", f"{column}_{string_func}")
    delimiter = params.get("delimiter")
    part_index = params.get("part_index")

    if not column or not string_func: raise ValueError("string_operation requires 'column' and 'string_function'.")
    _validate_columns([column], all_cols, f"String Operation '{string_func}'")

    s_col = _sanitize_identifier(column)
    s_new_col = _sanitize_identifier(new_col_name)
    op_clause = ""
    func_lower = string_func.lower()

    if func_lower == 'upper': op_clause = f"UPPER({s_col}::TEXT)"
    elif func_lower == 'lower': op_clause = f"LOWER({s_col}::TEXT)"
    elif func_lower == 'trim': op_clause = f"TRIM({s_col}::TEXT)"
    elif func_lower == 'length': op_clause = f"LENGTH({s_col}::TEXT)"
    elif func_lower == 'split':
        if not delimiter or part_index is None: raise ValueError("String split requires 'delimiter' and 'part_index' (1-based).")
        s_delim = _sanitize_literal(delimiter)
        op_clause = f"list_extract(string_split({s_col}::TEXT, {s_delim}), {int(part_index)})"
    else: raise ValueError(f"Unsupported string_function: {string_func}")

    select_clauses = []
    found_target = False
    if not all_cols:
         print("Warning: String op without full column list; using '*' and adding new column.")
         select_clauses.append("*")
         select_clauses.append(f"{op_clause} AS {s_new_col}")
    else:
        for existing_col in all_cols:
            s_existing = _sanitize_identifier(existing_col)
            if existing_col == column and new_col_name == column: # Replace in place
                 select_clauses.append(f"{op_clause} AS {s_col}")
                 found_target = True
            else:
                 select_clauses.append(s_existing)
        if not found_target: # Add as new column
             select_clauses.append(f"{op_clause} AS {s_new_col}")

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {input_alias}"


def _date_extract_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    column = params.get("column")
    part = params.get("part")
    new_col_name = params.get("new_column_name", f"{column}_{part}")

    if not column or not part: raise ValueError("date_extract requires 'column' and 'part'.")
    _validate_columns([column], all_cols, "Date Extract")
    valid_parts = ['year', 'month', 'day', 'hour', 'minute', 'second', 'dow', 'doy', 'week', 'quarter', 'epoch']
    part_lower = part.lower()
    if part_lower not in valid_parts: raise ValueError(f"Invalid date part '{part}'. Valid parts: {valid_parts}")

    s_col = _sanitize_identifier(column)
    s_new_col = _sanitize_identifier(new_col_name)
    extract_clause = f"EXTRACT({part_lower} FROM {s_col}::TIMESTAMP)" # Cast to timestamp for flexibility

    select_list = "*" if not all_cols else ", ".join([_sanitize_identifier(c) for c in all_cols])
    select_list += f", {extract_clause} AS {s_new_col}"

    return f"SELECT {select_list} FROM {input_alias}"

def _drop_duplicates_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    subset = params.get("subset") # Optional list
    if subset and not isinstance(subset, list): raise ValueError("'subset' for drop_duplicates must be a list.")
    if subset: _validate_columns(subset, all_cols, "Drop Duplicates")

    if not all_cols:
        raise ValueError("Cannot perform drop_duplicates reliably without knowing all columns.")

    s_all_cols = ", ".join([_sanitize_identifier(c) for c in all_cols])
    if subset:
        s_subset = ", ".join([_sanitize_identifier(c) for c in subset])
        # Need a deterministic order within partition - use all columns as fallback
        order_by_clause = ", ".join([_sanitize_identifier(c) for c in all_cols])
        # Subquery assigns row number, outer query selects rows with rn = 1
        sql = f"""
        WITH numbered_rows AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY {s_subset} ORDER BY {order_by_clause}) as rn
            FROM {input_alias}
        )
        SELECT {s_all_cols}
        FROM numbered_rows
        WHERE rn = 1
        """
        return sql.strip()
    else:
        # Drop duplicates across all columns -> DISTINCT
        # DISTINCT ON is not standard, use ROW_NUMBER partitioned by all columns
        order_by_clause = ", ".join([_sanitize_identifier(c) for c in all_cols])
        sql = f"""
         WITH numbered_rows AS (
             SELECT
                 *,
                 ROW_NUMBER() OVER (PARTITION BY {s_all_cols} ORDER BY (SELECT NULL)) as rn -- Order by constant
             FROM {input_alias}
         )
         SELECT {s_all_cols}
         FROM numbered_rows
         WHERE rn = 1
         """
        # Alternative: SELECT DISTINCT * (simpler but potentially less performant?)
        # return f"SELECT DISTINCT * FROM {input_alias}"
        return sql.strip()


def _create_column_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    new_col_name = params.get("new_column_name")
    expression = params.get("expression")
    if not new_col_name or not expression: raise ValueError("create_column requires 'new_column_name' and 'expression'.")

    # !!! WARNING: EXPRESSION IS NOT SANITIZED !!!
    print(f"Warning: Executing create_column with potentially unsafe expression: {expression}")

    s_new_col = _sanitize_identifier(new_col_name)
    s_all_cols = "*" if not all_cols else ", ".join([_sanitize_identifier(c) for c in all_cols])

    return f"SELECT {s_all_cols}, ({expression}) AS {s_new_col} FROM {input_alias}"

def _window_function_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    func = params.get("window_function")
    target_column = params.get("target_column")
    order_by_columns = params.get("order_by_columns") # List of dicts: [{'column': 'c', 'ascending': True}]
    partition_by_columns = params.get("partition_by_columns") # Optional list
    new_col_name = params.get("new_column_name", f"{func}_window")
    offset = params.get("offset", 1)
    default_value = params.get("default_value")

    if not func or not order_by_columns: raise ValueError("Window function requires 'window_function' and 'order_by_columns'.")
    func_lower = func.lower()
    if func_lower in ['lead', 'lag'] and not target_column: raise ValueError("Lead/Lag requires 'target_column'.")

    # Validate columns
    order_cols = [spec['column'] for spec in order_by_columns]
    _validate_columns(order_cols, all_cols, "Window Order By")
    if target_column: _validate_columns([target_column], all_cols, "Window Target")
    if partition_by_columns: _validate_columns(partition_by_columns, all_cols, "Window Partition By")

    # Build OVER clause
    over_clauses = []
    if partition_by_columns:
        s_part = ", ".join([_sanitize_identifier(c) for c in partition_by_columns])
        over_clauses.append(f"PARTITION BY {s_part}")
    s_order_list = []
    for spec in order_by_columns:
        order = "ASC" if spec.get("ascending", True) else "DESC"
        s_order_list.append(f"{_sanitize_identifier(spec['column'])} {order}")
    over_clauses.append(f"ORDER BY {', '.join(s_order_list)}")
    over_clause = f"OVER ({' '.join(over_clauses)})"

    # Build window function call
    window_expr = ""
    s_new_col = _sanitize_identifier(new_col_name)
    valid_funcs = ['rank', 'dense_rank', 'row_number', 'lead', 'lag', 'first_value', 'last_value', 'nth_value']
    # Add aggregates like SUM, AVG etc. over window
    agg_funcs = ['sum', 'avg', 'count', 'min', 'max']

    if func_lower in valid_funcs:
        if func_lower == 'rank': window_expr = f"RANK() {over_clause}"
        elif func_lower == 'dense_rank': window_expr = f"DENSE_RANK() {over_clause}"
        elif func_lower == 'row_number': window_expr = f"ROW_NUMBER() {over_clause}"
        elif func_lower in ['lead', 'lag']:
            s_target = _sanitize_identifier(target_column)
            lead_lag_args = [s_target, str(int(offset))]
            if default_value is not None: lead_lag_args.append(_sanitize_literal(default_value))
            window_expr = f"{func_lower.upper()}({', '.join(lead_lag_args)}) {over_clause}"
        elif func_lower in ['first_value', 'last_value']:
             s_target = _sanitize_identifier(target_column)
             window_expr = f"{func_lower.upper()}({s_target}) {over_clause}"
        # Add Nth value if needed
    elif func_lower in agg_funcs:
         # Apply aggregate function over the window
         s_target = _sanitize_identifier(target_column) if target_column else "*" # COUNT(*) case
         agg_sql = _map_agg_func_sql(func_lower)
         if func_lower == 'count' and target_column is None:
             window_expr = f"COUNT(*) {over_clause}"
         elif func_lower == 'count_distinct': # Custom handling
              s_target = _sanitize_identifier(target_column)
              window_expr = f"COUNT(DISTINCT {s_target}) {over_clause}"
         else:
             s_target = _sanitize_identifier(target_column)
             window_expr = f"{agg_sql}({s_target}) {over_clause}"
    else:
        raise ValueError(f"Unsupported window_function: {func}.")

    s_all_cols = "*" if not all_cols else ", ".join([_sanitize_identifier(c) for c in all_cols])
    return f"SELECT {s_all_cols}, {window_expr} AS {s_new_col} FROM {input_alias}"


def _sample_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    n = params.get("n")
    frac = params.get("frac")
    method = params.get("method", "system") # 'system' or 'bernoulli'
    seed = params.get("seed")

    if n is None and frac is None: raise ValueError("Sample requires 'n' or 'frac'.")
    if n is not None and frac is not None: raise ValueError("Provide either 'n' or 'frac', not both.")

    sample_clause = ""
    if n is not None:
        sample_clause = f"USING SAMPLE {int(n)} ROWS"
    else: # frac is not None
        percentage = float(frac) * 100
        if not (0 <= percentage <= 100): raise ValueError("Fraction must be between 0 and 1.")
        sample_clause = f"USING SAMPLE {percentage:.4f} PERCENT" # Use percentage for fraction

    if method.lower() == 'bernoulli':
        sample_clause += " (BERNOULLI)"
    elif method.lower() == 'system':
         sample_clause += " (SYSTEM)"
    else:
        raise ValueError("Unsupported sample method. Use 'system' or 'bernoulli'.")

    if seed is not None:
        sample_clause += f" REPEATABLE ({int(seed)})"

    return f"SELECT * FROM {input_alias} {sample_clause}"

# --- Regex Specific Helpers ---
def _regex_filter_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    column = params.get("column")
    regex_pattern = params.get("regex")
    if not column or not regex_pattern: raise ValueError("Regex filter requires 'column' and 'regex'.")
    _validate_columns([column], all_cols, "Regex Filter")
    s_col = _sanitize_identifier(column)
    s_pattern = _sanitize_literal(regex_pattern)
    return f"SELECT * FROM {input_alias} WHERE regexp_matches({s_col}::TEXT, {s_pattern})"

def _regex_extract_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str], group: int = 0) -> str:
    column = params.get("column")
    regex_pattern = params.get("regex")
    new_column = params.get("new_column", f"{column}_extracted")
    if not column or not regex_pattern: raise ValueError("Regex extract requires 'column' and 'regex'.")
    _validate_columns([column], all_cols, "Regex Extract")
    s_col = _sanitize_identifier(column)
    s_pattern = _sanitize_literal(regex_pattern)
    s_new_col = _sanitize_identifier(new_column)
    s_all_cols = "*" if not all_cols else ", ".join([_sanitize_identifier(c) for c in all_cols])
    extract_expr = f"regexp_extract({s_col}::TEXT, {s_pattern}, {int(group)}) AS {s_new_col}"
    return f"SELECT {s_all_cols}, {extract_expr} FROM {input_alias}"

def _regex_replace_sql(input_alias: str, params: Dict[str, Any], all_cols: List[str]) -> str:
    column = params.get("column")
    regex_pattern = params.get("regex")
    replacement = params.get("replacement", "")
    new_column = params.get("new_column", column) # Default to replace in place
    if not column or not regex_pattern: raise ValueError("Regex replace requires 'column' and 'regex'.")
    _validate_columns([column], all_cols, "Regex Replace")

    s_col = _sanitize_identifier(column)
    s_pattern = _sanitize_literal(regex_pattern)
    s_replacement = _sanitize_literal(replacement)
    s_new_col = _sanitize_identifier(new_column)
    replace_expr = f"regexp_replace({s_col}::TEXT, {s_pattern}, {s_replacement})"

    select_clauses = []
    if not all_cols:
         print("Warning: Regex Replace without full column list; using '*' and adding/replacing column.")
         select_clauses.append("*")
         select_clauses.append(f"{replace_expr} AS {s_new_col}") # Add/overwrite
    else:
        for c in all_cols:
            s_c = _sanitize_identifier(c)
            if c == column and new_column == column: # Replace in place
                 select_clauses.append(f"{replace_expr} AS {s_new_col}")
            elif c == column and new_column != column: # Keep original if creating new
                 select_clauses.append(s_c)
            else: # Keep other columns
                 select_clauses.append(s_c)
        # Add as new column if name is different and original was kept
        if new_column != column:
             select_clauses.append(f"{replace_expr} AS {s_new_col}")

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {input_alias}"
