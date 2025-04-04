# backend/app/services/sql_service.py
import duckdb
import pandas as pd
import io
import re
import traceback
from typing import Dict, Any, Tuple, List, Optional

# --- Helper Functions ---

def _sanitize_identifier(name: str, allow_star=False) -> str:
    """
    Sanitizes a column or table name for safe use in SQL queries by quoting it.
    Handles schema.table format. Allows '*' if explicitly permitted.
    Prevents basic SQL injection risks for identifiers.
    """
    if allow_star and name == '*':
        return '*'
    if not isinstance(name, str) or not name:
        raise ValueError("Identifier name must be a non-empty string.")

    # Allow simple alphanumeric names with underscores directly (unquoted)
    # This helps with DuckDB functions/keywords if needed, but use quoted by default for safety.
    # if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
    #     return name

    # Handle qualified names (schema.table) - quote each part
    parts = name.split('.', 1)
    sanitized_parts = []
    for part in parts:
        # Replace double quotes with two double quotes for escaping inside quoted identifier
        sanitized_part = part.replace('"', '""')
        # Enclose in double quotes
        sanitized_parts.append(f'"{sanitized_part}"')

    return '.'.join(sanitized_parts)

def _load_data_to_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, content: bytes):
    """Loads CSV content bytes into a DuckDB table using pandas for robust parsing."""
    try:
        # Use pandas to read CSV first for better type inference and error handling
        df = pd.read_csv(io.BytesIO(content))
        # Register the DataFrame directly. Use the raw table_name for registration.
        # DuckDB handles the table name internally. No need to sanitize here for registration.
        con.register(table_name, df)
        print(f"Successfully registered DataFrame as table '{table_name}' in DuckDB.")
    except pd.errors.EmptyDataError:
        print(f"Warning: CSV content for table '{table_name}' is empty. Registering empty table.")
        con.register(table_name, pd.DataFrame())
    except Exception as e:
        print(f"Error loading data for table '{table_name}' into DuckDB: {type(e).__name__}: {e}")
        traceback.print_exc()
        # Raise a more specific error or handle as needed
        raise ValueError(f"Failed to load data into DuckDB table '{table_name}': {e}")


def _build_cte_chain(previous_sql_chain: str, current_step_sql: str, step_number: int) -> Tuple[str, str]:
    """Builds a chain of CTEs for SQL operations."""
    step_alias = f"step{step_number}"
    sanitized_alias = _sanitize_identifier(step_alias)

    # The SQL snippet for the *current* step, defining the CTE
    current_cte_snippet = f"{sanitized_alias} AS (\n    {current_step_sql}\n)"

    # The full SQL chain including the new CTE
    if step_number == 0: # First step
        full_chain = f"WITH {current_cte_snippet}\nSELECT * FROM {sanitized_alias}"
    else:
        # Find the position of the last SELECT to insert the new CTE
        select_pos = previous_sql_chain.upper().rfind("SELECT")
        if select_pos == -1:
            raise ValueError("Could not find SELECT statement in previous SQL chain to append CTE.")

        # Insert the new CTE definition before the final SELECT
        # Assumes the previous chain ended with "SELECT * FROM previous_alias"
        # We need to find the end of the last CTE definition (closing parenthesis)
        # This is simpler: just append the new CTE after the WITH or comma
        with_pos = previous_sql_chain.upper().find("WITH ")
        if with_pos != -1:
             # Append after WITH using a comma
             insert_point = previous_sql_chain.find('\n', with_pos) # Find end of first line
             if insert_point == -1: insert_point = len(previous_sql_chain) # Failsafe

             # Find the start of the final SELECT
             final_select_pos = previous_sql_chain.upper().rfind("SELECT")
             # Insert the new CTE definition before the final SELECT
             new_chain_prefix = previous_sql_chain[:final_select_pos].strip()
             # Ensure the previous CTE definition ends with a comma if needed
             if new_chain_prefix.strip().endswith(')'):
                 new_chain_prefix += ','

             full_chain = f"{new_chain_prefix}\n{current_cte_snippet}\nSELECT * FROM {sanitized_alias}"

        else:
             # Should not happen if step_number > 0, but handle defensively
             print("Warning: Previous SQL chain did not start with WITH. Starting new chain.")
             full_chain = f"WITH {current_cte_snippet}\nSELECT * FROM {sanitized_alias}"


    return full_chain, current_cte_snippet


# --- Core Operation Functions ---

def apply_sql_operation(
    con: duckdb.DuckDBPyConnection,
    previous_sql_chain: str,
    operation: str,
    params: Dict[str, Any],
    base_table_ref: str # The original, registered table name (unsanitized)
) -> Tuple[List[Dict], List[str], int, str, str]:
    """
    Applies a structured SQL operation, extending the CTE chain.

    Args:
        con: Active DuckDB connection.
        previous_sql_chain: The SQL query string representing the state before this operation.
        operation: The name of the operation (e.g., 'filter', 'groupby_multi_agg').
        params: Dictionary of parameters for the operation.
        base_table_ref: The name of the base table registered in DuckDB (e.g., '__datasetname_base').

    Returns:
        Tuple containing:
        - preview_data: List of dicts for the first 100 rows of the result.
        - result_columns: List of column names in the result.
        - total_rows: Total number of rows in the result.
        - new_full_sql_chain: The updated SQL query string including the new operation as a CTE.
        - sql_snippet: The SQL snippet (CTE definition) for the current operation.
    """
    step_number = 0
    source_relation = _sanitize_identifier(base_table_ref) # Start with base table if no chain

    if previous_sql_chain:
        # If there's a previous chain, find the alias of the last step
        match = re.search(r"SELECT\s+\*\s+FROM\s+([\w\"`']+)\s*$", previous_sql_chain, re.IGNORECASE)
        if match:
            source_relation = match.group(1) # Use the alias from the previous step
            # Extract step number from alias like "stepN"
            num_match = re.search(r"(\d+)$", source_relation.strip('"`'))
            if num_match:
                step_number = int(num_match.group(1)) + 1
            else:
                 # Could be the initial base table ref if chain was just SELECT * FROM base
                 if source_relation == _sanitize_identifier(base_table_ref):
                     step_number = 0
                 else:
                     print(f"Warning: Could not determine step number from alias '{source_relation}'. Defaulting to 0.")
                     step_number = 0 # Reset step number if alias format is unexpected
        else:
            # If the previous chain doesn't end predictably, we might need to wrap it
            print("Warning: Previous SQL chain format unexpected. Wrapping it as subquery 'prev_step'.")
            source_relation = "( " + previous_sql_chain + " ) AS prev_step" # Less ideal, but works
            step_number = 1 # Assume it's the first step after the complex previous one

    # --- Generate SQL Snippet for the current operation ---
    current_step_sql = ""
    order_by_clause = "" # Store ORDER BY separately as it applies at the end
    try:
        if operation == "filter":
            col = _sanitize_identifier(params['column'])
            op = params['operator']
            val = params.get('value') # May not exist for IS NULL/NOT NULL
            # Basic value quoting (improve for different types if needed)
            sql_val = ""
            if op not in ['isnull', 'notnull']:
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    sql_val = str(val)
                else: # Treat as string, requires escaping single quotes
                    escaped_val = str(val).replace("'", "''")
                    sql_val = f"'{escaped_val}'"
            if op == 'isnull': current_step_sql = f"SELECT * FROM {source_relation} WHERE {col} IS NULL"
            elif op == 'notnull': current_step_sql = f"SELECT * FROM {source_relation} WHERE {col} IS NOT NULL"
            elif op == 'contains': current_step_sql = f"SELECT * FROM {source_relation} WHERE {col} LIKE '%{str(val).replace('%', '%%').replace('_', '__')}%'" # Basic LIKE escape
            elif op == 'startswith': current_step_sql = f"SELECT * FROM {source_relation} WHERE {col} LIKE '{str(val).replace('%', '%%').replace('_', '__')}%'"
            elif op == 'endswith': current_step_sql = f"SELECT * FROM {source_relation} WHERE {col} LIKE '%{str(val).replace('%', '%%').replace('_', '__')}'"
            elif op == 'regex': current_step_sql = f"SELECT * FROM {source_relation} WHERE regexp_matches({col}::VARCHAR, {sql_val})" # DuckDB regex
            elif op in ['==', '!=', '>', '<', '>=', '<=']:
                # Use standard SQL operators, handle == as =
                sql_op = '=' if op == '==' else op
                current_step_sql = f"SELECT * FROM {source_relation} WHERE {col} {sql_op} {sql_val}"
            else: raise ValueError(f"Unsupported filter operator for SQL: {op}")

        elif operation == "select_columns":
            cols = [_sanitize_identifier(c) for c in params['selected_columns']]
            current_step_sql = f"SELECT {', '.join(cols)} FROM {source_relation}"

        elif operation == "sort":
            # SQL sort uses ORDER BY clause at the *end* of the *final* SELECT,
            # so we modify the final SELECT part of the chain, not add a CTE step.
            # This function's role is primarily CTE building, so sort logic is handled differently.
            # We'll return the *previous* chain but indicate sorting is needed.
            # OR: We can make sort *always* the final step, wrapping the chain.
            sort_clauses = []
            for item in params.get('sort_columns', []):
                col = _sanitize_identifier(item['column'])
                order = "DESC" if not item.get('ascending', True) else "ASC"
                sort_clauses.append(f"{col} {order}")
            if not sort_clauses: raise ValueError("Sort operation requires columns.")
            order_by_clause = f"ORDER BY {', '.join(sort_clauses)}" # Store it

            # The current step SQL is just selecting from the previous step
            current_step_sql = f"SELECT * FROM {source_relation}"
            # Note: The final chain builder needs adjustment for ORDER BY.

        elif operation == "rename":
            select_clauses = []
            # Get columns from the source relation
            try: # <<< Line 158 area - Added try
                 # Use previous chain directly if it exists and is not just the base table ref
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [c[0] for c in cols_info]
            except Exception as desc_err: # <<< Added except block
                 raise ValueError(f"Could not describe source for rename: {desc_err}")

            rename_map = {item['old_name']: item['new_name'] for item in params['renames']}
            for col in source_columns:
                s_col = _sanitize_identifier(col)
                if col in rename_map:
                    new_name = _sanitize_identifier(rename_map[col])
                    select_clauses.append(f"{s_col} AS {new_name}") # <<< Line 169 area (syntax looks ok)
                else:
                    select_clauses.append(s_col)
            current_step_sql = f"SELECT {', '.join(select_clauses)} FROM {source_relation}"

        elif operation == "drop_columns":
            select_clauses = []
            try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [c[0] for c in cols_info]
            except Exception as desc_err:
                 raise ValueError(f"Could not describe source for drop: {desc_err}")

            cols_to_drop = set(params['drop_columns'])
            for col in source_columns:
                if col not in cols_to_drop:
                    select_clauses.append(_sanitize_identifier(col))
            if not select_clauses: raise ValueError("Cannot drop all columns.")
            current_step_sql = f"SELECT {', '.join(select_clauses)} FROM {source_relation}"

        elif operation == "groupby_multi_agg":
            group_cols = [_sanitize_identifier(c) for c in params['group_columns']]
            agg_clauses = []
            select_list = list(group_cols) # Start select list with group columns

            for agg in params['aggregations']:
                func = agg['function'].upper()
                col = agg['column']
                s_col = _sanitize_identifier(col, allow_star=True) # Allow '*' for COUNT
                alias_col_name = f"{func.lower()}_{col}".replace('*','all') # Basic alias
                s_alias = _sanitize_identifier(alias_col_name)

                # Map common functions to SQL functions
                sql_func = func
                if func == 'MEAN': sql_func = 'AVG'
                if func == 'STD': sql_func = 'STDDEV_SAMP' # Or STDDEV_POP? Sample is common.
                if func == 'VAR': sql_func = 'VAR_SAMP' # Or VAR_POP?
                if func == 'NUNIQUE': sql_func = 'COUNT(DISTINCT {})'.format(s_col)
                elif col == '*': # Handle COUNT(*)
                     if func == 'COUNT': sql_func = 'COUNT(*)'
                     else: raise ValueError(f"Function '{func}' cannot be applied to '*'. Use COUNT.")
                else: # Standard functions
                    # Ensure function is valid SQL aggregate
                    valid_aggs = ['SUM', 'AVG', 'MEDIAN', 'MIN', 'MAX', 'COUNT', 'FIRST', 'LAST', 'LIST', 'MODE', 'STDDEV_SAMP', 'VAR_SAMP']
                    if sql_func not in valid_aggs:
                        raise ValueError(f"Unsupported SQL aggregation function: {func}")
                    sql_func = f"{sql_func}({s_col})" # Apply function to column

                agg_clauses.append(f"{sql_func} AS {s_alias}")
                select_list.append(f"{sql_func} AS {s_alias}") # Add to select list

            # Construct the query
            current_step_sql = f"SELECT {', '.join(select_list)} FROM {source_relation} GROUP BY {', '.join(group_cols)}"

        elif operation == "fillna":
            # SQL fillna is complex, often done with COALESCE or CASE
            # Simple case: Fill specific columns with a single value
            fill_value = params.get('value')
            fill_method = params.get('method') # ffill/bfill are harder in standard SQL (need window functions)
            columns_to_fill = params.get('columns') # Optional list of columns

            if fill_method and fill_method in ['ffill', 'bfill']:
                 raise NotImplementedError("SQL fillna with ffill/bfill requires window functions and is not implemented here.")
            if fill_value is None:
                 raise ValueError("SQL fillna requires a 'value' to fill with.")

            # Quote fill value if string
            sql_fill_val = ""
            if isinstance(fill_value, (int, float)) and not isinstance(fill_value, bool):
                sql_fill_val = str(fill_value)
            else:
                escaped_fill_value = str(fill_value).replace("'", "''")
                sql_fill_val = f"'{escaped_fill_value}'"

            try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [c[0] for c in cols_info]
            except Exception as desc_err:
                 raise ValueError(f"Could not describe source for fillna: {desc_err}")

            select_clauses = []
            target_cols = set(columns_to_fill) if columns_to_fill else set(source_columns)

            for col in source_columns:
                s_col = _sanitize_identifier(col)
                if col in target_cols:
                    # Use COALESCE to replace NULLs
                    select_clauses.append(f"COALESCE({s_col}, {sql_fill_val}) AS {s_col}")
                else:
                    select_clauses.append(s_col) # Keep other columns as is

            current_step_sql = f"SELECT {', '.join(select_clauses)} FROM {source_relation}"

        elif operation == "dropna":
            # SQL dropna uses WHERE clause to filter rows with NULLs
            subset = params.get('subset') # Optional list of columns to check
            # 'how' ('any'/'all') and 'thresh' are more complex

            try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [c[0] for c in cols_info]
            except Exception as desc_err:
                 raise ValueError(f"Could not describe source for dropna: {desc_err}")

            target_cols = subset if subset else source_columns
            where_clauses = [f"{_sanitize_identifier(col)} IS NOT NULL" for col in target_cols]

            if not where_clauses:
                 current_step_sql = f"SELECT * FROM {source_relation}" # No columns to check? Return all.
            else:
                 # Default 'how' is 'any' -> drop if *any* target column is NULL
                 current_step_sql = f"SELECT * FROM {source_relation} WHERE {' AND '.join(where_clauses)}"

        elif operation == "astype":
            col = params['column']
            new_type = params['new_type'].upper()
            # Map common names to SQL types (DuckDB specific might be needed)
            type_map = {
                "INTEGER": "INTEGER", "INT": "INTEGER",
                "FLOAT": "DOUBLE", "DOUBLE": "DOUBLE", "NUMERIC": "DOUBLE",
                "STRING": "VARCHAR", "STR": "VARCHAR", "TEXT": "VARCHAR",
                "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN",
                "DATETIME": "TIMESTAMP", "TIMESTAMP": "TIMESTAMP",
                "DATE": "DATE",
                # Category is not a standard SQL type
            }
            sql_type = type_map.get(new_type)
            if not sql_type: raise ValueError(f"Unsupported type for SQL CAST: {new_type}")

            try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [c[0] for c in cols_info]
            except Exception as desc_err:
                 raise ValueError(f"Could not describe source for astype: {desc_err}")

            select_clauses = []
            s_target_col = _sanitize_identifier(col)
            for c in source_columns:
                s_c = _sanitize_identifier(c)
                if c == col:
                    select_clauses.append(f"CAST({s_c} AS {sql_type}) AS {s_c}")
                else:
                    select_clauses.append(s_c)
            current_step_sql = f"SELECT {', '.join(select_clauses)} FROM {source_relation}"

        elif operation == "drop_duplicates":
             subset = params.get('subset') # Optional list of columns
             keep = params.get('keep', 'first') # first, last, none (none not standard SQL)
             if keep != 'first':
                 raise NotImplementedError("SQL drop_duplicates 'keep' != 'first' requires window functions.")

             partition_cols = "*" # Default to all columns if no subset
             if subset:
                 partition_cols = ", ".join([_sanitize_identifier(c) for c in subset])

             # Use DISTINCT ON (DuckDB specific) or ROW_NUMBER()
             # DISTINCT ON is simpler if available and keep='first'
             # Need an ordering for DISTINCT ON to be deterministic, use all columns?
             try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 all_cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 order_by_cols = ", ".join([_sanitize_identifier(c[0]) for c in all_cols_info])
             except Exception as desc_err:
                 raise ValueError(f"Could not describe source for drop_duplicates ordering: {desc_err}")


             if partition_cols == "*": # Distinct on all columns is just DISTINCT
                 current_step_sql = f"SELECT DISTINCT * FROM {source_relation}"
             else:
                 # Requires ordering to define 'first'
                 current_step_sql = f"SELECT DISTINCT ON ({partition_cols}) * FROM {source_relation} ORDER BY {partition_cols}, {order_by_cols}" # Order by partition cols first, then all others

        elif operation == "sample":
            n = params.get('n')
            frac = params.get('frac')
            replace = params.get('replace', False)
            method = params.get('method', 'system') # system or bernoulli
            seed = params.get('seed')

            sample_clause = ""
            if n is not None:
                sample_clause = f"USING SAMPLE {int(n)} ROWS" # Ensure n is int
            elif frac is not None:
                sample_percentage = float(frac) * 100 # Ensure frac is float
                sample_clause = f"USING SAMPLE {sample_percentage:.2f} PERCENT"
            else:
                raise ValueError("Sample requires 'n' or 'frac'.")

            if method.lower() == 'bernoulli':
                sample_clause += " (BERNOULLI)"
            else:
                sample_clause += " (SYSTEM)" # Default

            if replace:
                 sample_clause += " WITH REPLACEMENT"
                 print("Warning: SQL Sample WITH REPLACEMENT might not be fully supported or behave as expected.")

            if seed is not None:
                 sample_clause += f" REPEATABLE ({int(seed)})"

            current_step_sql = f"SELECT * FROM {source_relation} {sample_clause}"

        elif operation == "shuffle":
             # SQL shuffle uses ORDER BY RANDOM()
             seed = params.get('seed') # DuckDB RANDOM() doesn't accept seed directly
             if seed is not None:
                 print(f"Warning: SQL Shuffle (ORDER BY RANDOM()) ignores provided seed {seed}.")
             current_step_sql = f"SELECT * FROM {source_relation} ORDER BY RANDOM()"

        elif operation == "apply_lambda":
             # Very basic translation for simple SQL expressions
             col = params['column']
             lambda_str = params['lambda_str'] # e.g., "x + 5", "UPPER(x::TEXT)"
             new_col_name = params.get('new_column_name')

             # Replace 'x' with the sanitized column name
             # This is highly insecure if lambda_str is complex or malicious
             # Only allow very simple replacements or raise error
             # Allow alphanumeric, underscores, basic math, comparisons, parens, commas, colons (for casting), periods, spaces, single quotes
             if not re.match(r"^[a-zA-Z0-9_+\-*/=<> ()',:.%|&^ ]+$", lambda_str):
                  raise ValueError("SQL Lambda contains potentially unsafe characters.")

             sql_expr = lambda_str.replace('x', _sanitize_identifier(col))

             try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [c[0] for c in cols_info]
             except Exception as desc_err:
                 raise ValueError(f"Could not describe source for apply_lambda: {desc_err}")

             select_clauses = []
             target_col_found = False
             for c in source_columns:
                 s_c = _sanitize_identifier(c)
                 if c == col:
                     target_col_found = True
                     alias = _sanitize_identifier(new_col_name) if new_col_name else s_c
                     select_clauses.append(f"({sql_expr}) AS {alias}")
                     # If not creating a new column, don't add the original column again
                     if new_col_name and new_col_name != col:
                          select_clauses.append(s_c) # Keep original if new name is different
                 else:
                     select_clauses.append(s_c)

             if not target_col_found and not new_col_name:
                  # If modifying existing column and it wasn't found (e.g., after rename)
                  raise ValueError(f"Column '{col}' not found for apply_lambda modification.")

             # If creating a new column, add it regardless of whether 'col' exists (expr might use others)
             if new_col_name:
                 # Ensure the new column isn't already selected if we kept the original
                 current_selection = {clause.split(' AS ')[-1].strip('"') for clause in select_clauses if ' AS ' in clause} | \
                                     {clause.strip('"') for clause in select_clauses if ' AS ' not in clause}
                 if new_col_name not in current_selection:
                     select_clauses.append(f"({sql_expr}) AS {_sanitize_identifier(new_col_name)}")
                 elif not target_col_found: # New name exists, but target col didn't - likely adding new col based on others
                      select_clauses = [_sanitize_identifier(c) for c in source_columns] # Select all original
                      select_clauses.append(f"({sql_expr}) AS {_sanitize_identifier(new_col_name)}") # Add new one


             current_step_sql = f"SELECT {', '.join(select_clauses)} FROM {source_relation}"

        # --- Add New Operations Here ---
        elif operation == "string_operation":
             col = params['column']
             string_func = params.get("string_function")
             new_col_name = params.get("new_column_name", f"{col}_{string_func}")
             delimiter = params.get("delimiter")
             part_index = params.get("part_index") # 1-based for SQL string split usually

             if not string_func: raise ValueError("string_operation requires 'string_function'.")

             sql_expr = ""
             s_col = _sanitize_identifier(col)
             func_lower = string_func.lower()

             if func_lower == 'upper': sql_expr = f"UPPER({s_col}::VARCHAR)"
             elif func_lower == 'lower': sql_expr = f"LOWER({s_col}::VARCHAR)"
             elif func_lower == 'strip': sql_expr = f"TRIM({s_col}::VARCHAR)" # Removes leading/trailing whitespace
             elif func_lower == 'length': sql_expr = f"LENGTH({s_col}::VARCHAR)"
             elif func_lower == 'split':
                 if delimiter is None or part_index is None:
                     raise ValueError("SQL String split requires 'delimiter' and 'part_index' (1-based).")
                 # DuckDB string_split returns a list, access with list_extract
                 escaped_delimiter = str(delimiter).replace("'", "''")
                 sql_expr = f"list_extract(string_split({s_col}::VARCHAR, '{escaped_delimiter}'), {int(part_index)})"
             else:
                 raise ValueError(f"Unsupported string_function for SQL: {string_func}")

             # Build SELECT statement, adding the new column
             try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [_sanitize_identifier(c[0]) for c in cols_info]
             except Exception as desc_err:
                 raise ValueError(f"Could not describe source for string_operation: {desc_err}")

             select_list = ", ".join(source_columns)
             current_step_sql = f"SELECT {select_list}, ({sql_expr}) AS {_sanitize_identifier(new_col_name)} FROM {source_relation}"

        elif operation == "date_extract":
             col = params['column']
             part = params.get("part")
             new_col_name = params.get("new_column_name", f"{col}_{part}")

             if not part: raise ValueError("date_extract requires 'part'.")

             # Map to DuckDB date functions/keywords
             part_lower = part.lower()
             sql_expr = ""
             s_col = _sanitize_identifier(col)

             # Use DATE_PART or EXTRACT
             valid_parts = ['year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond',
                            'millisecond', 'epoch', 'isodow', 'week', 'quarter', 'doy'] # dayofyear -> doy
             if part_lower == 'weekday': part_lower = 'isodow' # Monday=1..Sunday=7
             if part_lower == 'ordinal_day': part_lower = 'doy'
             if part_lower == 'weekofyear': part_lower = 'week'

             if part_lower in valid_parts:
                 sql_expr = f"DATE_PART('{part_lower}', {s_col}::TIMESTAMP)" # Cast to timestamp for safety
             else:
                 raise ValueError(f"Unsupported date part for SQL: {part}. Valid: {valid_parts}")

             # Build SELECT statement
             try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [_sanitize_identifier(c[0]) for c in cols_info]
             except Exception as desc_err:
                 raise ValueError(f"Could not describe source for date_extract: {desc_err}")

             select_list = ", ".join(source_columns)
             current_step_sql = f"SELECT {select_list}, ({sql_expr}) AS {_sanitize_identifier(new_col_name)} FROM {source_relation}"

        elif operation == "create_column":
             new_col_name = params.get("new_column_name")
             expression_str = params.get("expression") # SQL expression string

             if not new_col_name or not expression_str:
                 raise ValueError("create_column requires 'new_column_name' and 'expression' string.")

             # The expression MUST be a valid SQL expression referencing existing columns
             # We assume columns are referred to by their sanitized names within the expression
             # No 'eval' needed here, just embed the SQL expression directly.
             # User needs to ensure the expression is valid SQL.

             # Build SELECT statement
             try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [_sanitize_identifier(c[0]) for c in cols_info]
             except Exception as desc_err:
                 raise ValueError(f"Could not describe source for create_column: {desc_err}")

             select_list = ", ".join(source_columns)
             # Basic check for injection - disallow semicolons within the expression
             if ';' in expression_str:
                  raise ValueError("SQL expression cannot contain semicolons for safety.")

             current_step_sql = f"SELECT {select_list}, ({expression_str}) AS {_sanitize_identifier(new_col_name)} FROM {source_relation}"

        elif operation == "window_function":
             func = params.get("window_function")
             target_column = params.get("target_column")
             order_by_specs = params.get("order_by_columns", []) # List of {'column': 'c', 'descending': False}
             partition_by_columns = params.get("partition_by_columns", [])
             new_col_name = params.get("new_column_name", f"{func}_window")
             # Specific params
             rank_method = params.get("rank_method", "average")
             offset = params.get("offset", 1)
             default_value = params.get("default_value")

             if not func: raise ValueError("Window function requires 'window_function' name.")

             # --- Build Window Clause ---
             partition_clause = ""
             if partition_by_columns:
                 partition_cols = [_sanitize_identifier(c) for c in partition_by_columns]
                 partition_clause = f"PARTITION BY {', '.join(partition_cols)}"

             order_clause = ""
             if order_by_specs:
                 order_items = []
                 for spec in order_by_specs:
                     col = _sanitize_identifier(spec['column'])
                     direction = "DESC" if spec.get('descending', False) else "ASC"
                     order_items.append(f"{col} {direction}")
                 order_clause = f"ORDER BY {', '.join(order_items)}"

             window_spec = f"({partition_clause} {order_clause})".strip()
             if not window_spec:
                 # Some functions like global count might not need partition/order, but most do
                 print(f"Warning: Window function '{func}' applied without PARTITION or ORDER BY.")
                 # raise ValueError("Window functions typically require ORDER BY and optionally PARTITION BY.")

             # --- Build Window Function Call ---
             sql_func_call = ""
             func_lower = func.lower()
             s_target_col = _sanitize_identifier(target_column) if target_column else None

             rank_map = {'average': 'RANK', 'min': 'RANK', 'max': 'RANK', # RANK uses order
                         'dense': 'DENSE_RANK', 'ordinal': 'ROW_NUMBER', 'first': 'ROW_NUMBER'}

             if func_lower in ['rank', 'dense_rank', 'row_number']:
                 mapped_rank = rank_map.get(rank_method if func_lower=='rank' else func_lower)
                 if not mapped_rank: raise ValueError(f"Invalid rank method: {rank_method}")
                 if not order_clause: raise ValueError("Rank functions require ORDER BY.")
                 sql_func_call = f"{mapped_rank}() OVER {window_spec}"
             elif func_lower in ['lead', 'lag']:
                 if not s_target_col: raise ValueError(f"{func} requires 'target_column'.")
                 if not order_clause: raise ValueError(f"{func} requires ORDER BY.")
                 offset_val = int(offset)
                 default_clause = ""
                 if default_value is not None:
                     # Quote default if string
                     sql_default = ""
                     if isinstance(default_value, (int, float)) and not isinstance(default_value, bool): sql_default = str(default_value)
                     else: 
                        escaped_default = str(default_value).replace("'", "''")
                        sql_default = f"'{escaped_default}'"
                     default_clause = f", {sql_default}"
                 sql_func_call = f"{func.upper()}({s_target_col}, {offset_val}{default_clause}) OVER {window_spec}"
             elif func_lower in ['sum', 'avg', 'mean', 'min', 'max', 'count', 'stddev_samp', 'var_samp', 'median', 'first_value', 'last_value']:
                 sql_agg_func = func.upper()
                 if sql_agg_func == 'MEAN': sql_agg_func = 'AVG'
                 if sql_agg_func == 'STD': sql_agg_func = 'STDDEV_SAMP' # Corrected from STD
                 if sql_agg_func == 'VAR': sql_agg_func = 'VAR_SAMP' # Corrected from VAR
                 # FIRST_VALUE/LAST_VALUE require ORDER BY
                 if sql_agg_func in ['FIRST_VALUE', 'LAST_VALUE'] and not order_clause:
                      raise ValueError(f"{sql_agg_func} requires ORDER BY.")

                 target = s_target_col if s_target_col else '*' # Allow COUNT(*)
                 if target == '*' and sql_agg_func != 'COUNT':
                      raise ValueError(f"Cannot apply {sql_agg_func} to '*'. Use COUNT.")
                 if target != '*' and not s_target_col:
                      raise ValueError(f"{sql_agg_func} requires 'target_column'.")

                 sql_func_call = f"{sql_agg_func}({target}) OVER {window_spec}"
             else:
                 raise ValueError(f"Unsupported window function for SQL: {func}")

             # Build SELECT statement
             try:
                 describe_source = previous_sql_chain if step_number > 0 else source_relation
                 cols_info = con.execute(f"DESCRIBE ({describe_source});").fetchall()
                 source_columns = [_sanitize_identifier(c[0]) for c in cols_info]
             except Exception as desc_err:
                 raise ValueError(f"Could not describe source for window_function: {desc_err}")

             select_list = ", ".join(source_columns)
             current_step_sql = f"SELECT {select_list}, {sql_func_call} AS {_sanitize_identifier(new_col_name)} FROM {source_relation}"

        else:
            raise ValueError(f"Unsupported SQL operation: {operation}")

    except KeyError as e:
        raise ValueError(f"Missing parameter for operation '{operation}': {e}")
    except Exception as e:
        print(f"Error generating SQL for '{operation}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise ValueError(f"Failed to generate SQL for '{operation}': {e}") # Re-raise as ValueError

    if not current_step_sql:
        raise ValueError(f"Failed to generate SQL snippet for operation '{operation}'.")

    # --- Build the full CTE chain ---
    new_full_sql_chain, sql_snippet = _build_cte_chain(previous_sql_chain, current_step_sql, step_number)

    # --- Apply ORDER BY if it was requested in this step ---
    final_query_for_execution = new_full_sql_chain
    if order_by_clause:
        # Find the final SELECT * FROM alias and append ORDER BY
        select_pos = new_full_sql_chain.upper().rfind("SELECT * FROM")
        if select_pos != -1:
            # Ensure we are not appending inside a CTE definition parenthesis
            # Find the end of the chain before appending ORDER BY
            final_query_for_execution = new_full_sql_chain + " " + order_by_clause
            sql_snippet += f"\n-- Final ORDER BY: {order_by_clause}" # Add note to snippet
        else:
            # Fallback: Wrap the whole chain
            final_query_for_execution = f"SELECT * FROM ({new_full_sql_chain}) AS final_ordered {order_by_clause}"
            sql_snippet += f"\n-- Final ORDER BY: {order_by_clause} (applied to wrapped chain)"

    # --- Execute and Get Preview ---
    try:
        print(f"Executing SQL for preview:\n{final_query_for_execution}\n---")
        preview_result = con.execute(f"{final_query_for_execution} LIMIT 101").fetchdf() # Fetch 101 to check if > 100
        preview_data = preview_result.head(100).replace({pd.NA: None, pd.NaT: None}).to_dict(orient="records") # Replace pandas NAs
        result_columns = list(preview_result.columns)

        # Get total row count (can be expensive)
        # Use COUNT(*) on the final step definition for better performance than fetching all
        count_sql = f"SELECT COUNT(*) FROM ({final_query_for_execution}) AS final_count"
        total_rows = con.execute(count_sql).fetchone()[0]

    except Exception as exec_err:
        print(f"Error executing generated SQL: {type(exec_err).__name__}: {exec_err}")
        traceback.print_exc()
        raise ValueError(f"Generated SQL failed execution: {exec_err}\nSQL:\n{final_query_for_execution}")

    # Return the chain *without* the final ORDER BY for further CTE building,
    # but the executed query included it.
    return preview_data, result_columns, total_rows, new_full_sql_chain, sql_snippet


def apply_sql_join(
    con: duckdb.DuckDBPyConnection,
    previous_sql_chain_left: str,
    right_table_ref: str, # Sanitized name of the right table registered in DuckDB
    params: Dict[str, Any],
    base_table_ref_left: str # Original registered name of the left base table
) -> Tuple[List[Dict], List[str], int, str, str]:
    """
    Applies a SQL JOIN operation, extending the CTE chain for the left side.
    """
    step_number = 0
    left_source_relation = _sanitize_identifier(base_table_ref_left) # Start with base table

    if previous_sql_chain_left:
        match = re.search(r"SELECT\s+\*\s+FROM\s+([\w\"`']+)\s*$", previous_sql_chain_left, re.IGNORECASE)
        if match:
            left_source_relation = match.group(1)
            num_match = re.search(r"(\d+)$", left_source_relation.strip('"`'))
            if num_match: step_number = int(num_match.group(1)) + 1
            else: step_number = 0 # Assume base if no number
        else:
            left_source_relation = f"( {previous_sql_chain_left} ) AS prev_step_left"
            step_number = 1

    # --- Parameters ---
    left_on = _sanitize_identifier(params['left_on'])
    right_on = _sanitize_identifier(params['right_on'])
    join_type = params.get('join_type', 'inner').upper()
    # Suffixes are handled implicitly by column naming or explicit selection if needed

    # --- Build Join SQL ---
    # Assume selecting all columns from left source ('l') and right source ('r')
    # Need to handle potential column name collisions - use aliases
    # For simplicity, select all for now. User can use 'select_columns' later.
    # TODO: Implement smarter column selection/renaming for joins if needed.
    current_step_sql = f"""
        SELECT l.*, r.*
        FROM {left_source_relation} AS l
        {join_type} JOIN {right_table_ref} AS r
        ON l.{left_on} = r.{right_on}
    """
    # Refine SELECT list if specific columns are needed or to avoid collisions explicitly

    # --- Build CTE Chain ---
    new_full_sql_chain, sql_snippet = _build_cte_chain(previous_sql_chain_left, current_step_sql, step_number)

    # --- Execute and Get Preview ---
    try:
        print(f"Executing SQL Join for preview:\n{new_full_sql_chain}\n---")
        preview_result = con.execute(f"{new_full_sql_chain} LIMIT 101").fetchdf()
        preview_data = preview_result.head(100).replace({pd.NA: None, pd.NaT: None}).to_dict(orient="records")
        result_columns = list(preview_result.columns) # May contain duplicate names if not handled

        count_sql = f"SELECT COUNT(*) FROM ({new_full_sql_chain}) AS final_count"
        total_rows = con.execute(count_sql).fetchone()[0]

    except Exception as exec_err:
        print(f"Error executing generated SQL Join: {type(exec_err).__name__}: {exec_err}")
        traceback.print_exc()
        # Ensure f-string here is correct and variables are defined
        raise ValueError(f"Generated SQL Join failed execution: {exec_err}\nSQL:\n{new_full_sql_chain}") # <<< Line 791 area (syntax looks ok)

    # Ensure the function returns correctly
    return preview_data, result_columns, total_rows, new_full_sql_chain, sql_snippet