# backend/app/services/relational_algebra_service.py
import duckdb
import pandas as pd
import io
import re 
from typing import Dict, Any, Tuple, List, Optional
import json
import uuid 

# --- Utility Functions (Can potentially be shared with sql_service) ---

def _sanitize_identifier(identifier: Optional[str]) -> Optional[str]:
    """
    Sanitizes table or column names for safe use in SQL queries by double-quoting.
    Handles potentially pre-quoted input and escapes internal double quotes.
    """
    if identifier is None:
        return None
    identifier_str = str(identifier).strip() # Ensure string and remove outer whitespace

    # Strip existing double quotes ONLY if they wrap the entire string
    if len(identifier_str) >= 2 and identifier_str.startswith('"') and identifier_str.endswith('"'):
        identifier_str = identifier_str[1:-1]

    # Now escape any internal double quotes that remain
    escaped_identifier = identifier_str.replace('"', '""')

    # Always wrap the final result in double quotes
    return f'"{escaped_identifier}"'

def _load_ra_data(con: duckdb.DuckDBPyConnection, table_name: str, content: bytes):
    """Loads data from CSV bytes content into a DuckDB table using Pandas."""
    # Use the corrected sanitizer
    sanitized_table_name = _sanitize_identifier(table_name)
    if not sanitized_table_name:
        raise ValueError("Invalid table name provided for loading RA data.")
    try:
        # Read CSV bytes into a Pandas DataFrame
        df_temp = pd.read_csv(io.BytesIO(content))
        # Register the DataFrame as a temporary view in DuckDB
        # Make view name safer by removing potentially problematic chars
        safe_base_name = re.sub(r'\W|^(?=\d)', '_', table_name) # Replace non-word chars, ensure not starting with digit
        view_name = f'__temp_ra_view_{safe_base_name}_{uuid.uuid4().hex[:4]}'
        con.register(view_name, df_temp) # Use unique view name
        # Create the final table from the view
        con.execute(f"CREATE OR REPLACE TABLE {sanitized_table_name} AS SELECT * FROM {view_name};")
        # Clean up the temporary view
        con.unregister(view_name)
        print(f"Successfully loaded data into DuckDB table: {sanitized_table_name}") # Add confirmation log
    except (pd.errors.ParserError, pd.errors.EmptyDataError, duckdb.Error, Exception) as e:
        raise ValueError(f"Failed to load data for RA op into table {table_name}: {type(e).__name__} - {e}")

def _format_sql_value(value: Any) -> str:
    """ Formats Python values for safe insertion into SQL strings (basic). """
    if isinstance(value, str):
        # Perform the replacement *before* the f-string
        escaped_value = value.replace("'", "''") # Use double quotes for args is fine here
        return f"'{escaped_value}'" # Put the result in the f-string
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif value is None:
        return 'NULL'
    else:
        # Fallback for other types - ensure string conversion
        # Perform the replacement *before* the f-string
        escaped_value = str(value).replace("'", "''") # Use double quotes for args
        return f"'{escaped_value}'" # Put the result in the f-string
    
def _generate_sql_snippet(operation: str, params: Dict[str, Any], input_alias_or_table: str) -> str:
    """
    Generates the SQL snippet for a single RA operation, correctly handling
    base tables and subqueries (previous SQL results).
    """
    op_lower = operation.lower()

    # --- Determine if the input is a base table or a subquery ---
    is_subquery = False
    # Simple heuristic: check for core SQL keywords (case-insensitive)
    # More robust parsing could be used, but this covers many cases.
    sql_keywords = ['SELECT ', ' FROM ', ' WHERE ', ' JOIN ', ' UNION ', ' EXCEPT ', ' INTERSECT ', ' WITH ']
    if any(keyword in input_alias_or_table.strip().upper() for keyword in sql_keywords):
         # Check if it's just a quoted identifier that happens to contain a keyword substring
         # This check is basic, might need refinement if identifiers can contain keywords naturally
         if not (input_alias_or_table.strip().startswith('"') and input_alias_or_table.strip().endswith('"')):
              is_subquery = True

    if is_subquery:
        # If it's likely a subquery, wrap it in parentheses and assign a unique alias for the FROM clause
        # Using a unique alias prevents conflicts in potential self-joins later, though less readable.
        source_alias = f"subq_{uuid.uuid4().hex[:6]}"
        source_for_from = f"({input_alias_or_table}) AS {source_alias}"
    else:
        # If it's likely a table name, sanitize it as before
        source_for_from = _sanitize_identifier(input_alias_or_table)
        # We might still need an alias for consistency, especially for joins later
        # source_alias = _sanitize_identifier(input_alias_or_table) # Or derive one

    print(f"Generating SQL for '{op_lower}'. Input is {'subquery' if is_subquery else 'table'}. Source for FROM: {source_for_from}") # Debug log

    try:
        # --- Generate SQL based on operation, using `source_for_from` ---

        if op_lower == "select":
            predicate = params.get("predicate")
            if not predicate: raise ValueError("Select needs 'predicate'.")
            # Keep the warning, predicate remains unsanitized for flexibility (but risky)
            print(f"Warning: Generating SELECT with potentially unsafe predicate: {predicate}")
            # The predicate applies to the columns output by 'source_for_from'
            return f"SELECT * FROM {source_for_from} WHERE {predicate}"

        elif op_lower == "project":
            attributes = params.get("attributes")
            if not attributes or not isinstance(attributes, list) or not attributes:
                raise ValueError("Project needs a non-empty list of 'attributes'.")
            # Select specific attributes from the 'source_for_from'
            s_attributes = ", ".join([_sanitize_identifier(attr) for attr in attributes])
            return f"SELECT {s_attributes} FROM {source_for_from}"

        elif op_lower == "rename":
            # Rename applies *after* the subquery/table source
            renames_map_str = params.get("renaming_map")
            all_columns = params.get("all_columns") # IMPORTANT: Still needed!

            # ** Crucial Point for Rename **
            # If `input_alias_or_table` is a subquery, `all_columns` MUST accurately reflect
            # the columns *output* by that subquery *before* the rename.
            # The caller (main.py or frontend) needs to track/provide these columns.
            if not renames_map_str:
                 raise ValueError("Rename needs 'renaming_map' string.")
            if not all_columns:
                 raise ValueError(f"Rename needs 'all_columns' list, representing columns *before* renaming (from source: {'subquery' if is_subquery else 'table'}).")


            rename_map = {}
            try:
                pairs = [pair.strip() for pair in renames_map_str.split(',')]
                for pair in pairs:
                    old, new = map(str.strip, pair.split('=', 1))
                    if old and new: rename_map[old] = new
            except Exception:
                raise ValueError("Invalid format for 'renaming_map'. Use 'old1=new1, old2=new2'.")

            if not rename_map: raise ValueError("No valid renames found in 'renaming_map'.")

            select_clauses = []
            for col in all_columns:
                # Reference the column from the source (subquery or table)
                s_old = _sanitize_identifier(col)
                if col in rename_map:
                    s_new = _sanitize_identifier(rename_map[col]) # The new alias
                    select_clauses.append(f"{s_old} AS {s_new}")
                else:
                    select_clauses.append(s_old) # Keep the column as is
            select_list = ", ".join(select_clauses)
            # Apply the select list (with renames) to the source
            return f"SELECT {select_list} FROM {source_for_from}"

        # --- Placeholder for Binary Operations (Need Careful Handling of Subqueries/Aliases) ---
        elif op_lower in ["union", "difference", "intersection", "product", "join"]:
             # These require TWO inputs. The logic in apply_ra_operation needs refactoring
             # to handle binary ops correctly, likely not using _generate_sql_snippet directly.
             # This snippet generator is primarily for unary ops on a single source.
             # For now, raise NotImplemented or handle based on how apply_ra_operation calls it.
             raise NotImplementedError(f"Binary RA operation '{operation}' snippet generation needs different handling.")

        else:
            raise ValueError(f"Unsupported RA operation for snippet generation: {operation}")

    except Exception as e:
        # Catch errors during snippet generation itself
        raise ValueError(f"Error generating SQL snippet for '{operation}' (source type: {'subquery' if is_subquery else 'table'}): {e}")

def _execute_preview_query(con: duckdb.DuckDBPyConnection, query: str, preview_limit: int = 100) -> Tuple[List[Dict], List[str], int]:
    """Executes a full SQL query, gets preview data, columns, and total row count."""
    print(f"Executing RA Preview Query: {query}") # Log the query being executed
    try:
        # Use CTE for efficiency and correctness
        count_query = f"WITH result_set AS ({query}) SELECT COUNT(*) FROM result_set;"
        total_rows_result = con.execute(count_query).fetchone()
        total_rows = total_rows_result[0] if total_rows_result else 0

        preview_query = f"WITH result_set AS ({query}) SELECT * FROM result_set LIMIT {preview_limit};"
        preview_result = con.execute(preview_query)

        columns = [desc[0] for desc in preview_result.description]
        preview_df = preview_result.fetchdf() # Fetch as DataFrame first

        # Convert df to dicts, handling non-serializable types robustly
        data_dicts = []
        for record in preview_df.to_dict(orient='records'):
            formatted_record = {}
            for col, val in record.items():
                if pd.isna(val):
                    formatted_record[col] = None # Consistent null representation
                elif isinstance(val, (bytes, bytearray)):
                     # Try decoding, fallback to placeholder
                    try: formatted_record[col] = val.decode('utf-8', errors='replace')
                    except: formatted_record[col] = f"<binary data len={len(val)}>"
                else:
                    # Attempt direct JSON serialization check for complex types
                    try:
                        json.dumps(val) # Test if serializable
                        formatted_record[col] = val
                    except TypeError:
                        # If not serializable, convert known types or fallback to string
                        if hasattr(val, 'isoformat'): formatted_record[col] = val.isoformat() # Date/Time
                        elif isinstance(val, (list, dict)): formatted_record[col] = str(val) # Simple string for lists/dicts
                        else: formatted_record[col] = str(val) # Generic fallback
            data_dicts.append(formatted_record)

        return data_dicts, columns, total_rows

    # --- CORRECTED EXCEPTION HANDLING ---
    except (duckdb.Error, AttributeError, TypeError, ValueError) as e:
        print(f"RA SQL Execution/Preview Error: {type(e).__name__} - {e}\nQuery: {query}")
        # Provide more specific error info if it's a known DuckDB error type
        error_prefix = "Failed to execute RA SQL"
        if isinstance(e, duckdb.BinderException): error_prefix = f"{error_prefix} (Binder Error): Check column/table names and types."
        elif isinstance(e, duckdb.CatalogException): error_prefix = f"{error_prefix} (Catalog Error): Check if table/view exists."
        elif isinstance(e, duckdb.ParserException): error_prefix = f"{error_prefix} (Parser Error): Check SQL syntax."
        # Add back duckdb.IOException if needed for file access errors during query
        # elif isinstance(e, duckdb.IOException): error_prefix = f"{error_prefix} (IO Error): Check file paths/permissions."
        else: error_prefix = f"{error_prefix}:"
        raise ValueError(f"{error_prefix} {e}") # Re-raise as ValueError for the main handler
# --- Relational Algebra Operation Implementations ---

def _ra_select(params: Dict[str, Any]) -> str:
    """Generates SQL for Select (σ)."""
    dataset = params.get("dataset")
    predicate = params.get("predicate") # e.g., "age > 30 AND city = 'London'"
    if not dataset or not predicate:
        raise ValueError("Select (σ) requires 'dataset' and 'predicate' parameters.")

    # !!! WARNING: PREDICATE IS NOT SANITIZED !!!
    print(f"Warning: Executing RA Select with potentially unsafe predicate: {predicate}")

    s_dataset = _sanitize_identifier(dataset)
    return f"SELECT * FROM {s_dataset} WHERE {predicate}"

def _ra_project(params: Dict[str, Any]) -> str:
    """Generates SQL for Project (π)."""
    dataset = params.get("dataset")
    attributes = params.get("attributes") # List of column names
    if not dataset or not attributes or not isinstance(attributes, list):
        raise ValueError("Project (π) requires 'dataset' and a list of 'attributes'.")
    if not attributes:
         raise ValueError("Project (π) requires at least one attribute.")

    s_dataset = _sanitize_identifier(dataset)
    s_attributes = ", ".join([_sanitize_identifier(attr) for attr in attributes])
    return f"SELECT {s_attributes} FROM {s_dataset}"

def _ra_rename(params: Dict[str, Any]) -> str:
    """Generates SQL for Rename (ρ) - focusing on column renaming."""
    dataset = params.get("dataset")
    renames_list = params.get("renames", [])
    all_columns = params.get("all_columns") # Need all original columns

    if not dataset or not renames_list or not all_columns:
        raise ValueError("Rename (ρ) requires 'dataset', 'renames' list, and 'all_columns' list.")

    rename_map = {item['old_name']: item['new_name'] for item in renames_list if item.get('old_name') and item.get('new_name')}
    if not rename_map:
        raise ValueError("Invalid 'renames' list provided for Rename (ρ).")

    s_dataset = _sanitize_identifier(dataset)
    select_clauses = []
    for col in all_columns:
        s_old = _sanitize_identifier(col)
        if col in rename_map:
            s_new = _sanitize_identifier(rename_map[col])
            select_clauses.append(f"{s_old} AS {s_new}")
        else:
            select_clauses.append(s_old) # Keep original name

    select_list = ", ".join(select_clauses)
    return f"SELECT {select_list} FROM {s_dataset}"


def _ra_union(params: Dict[str, Any]) -> str:
    """Generates SQL for Union (∪)."""
    left = params.get("left_dataset")
    right = params.get("right_dataset")
    if not left or not right:
        raise ValueError("Union (∪) requires 'left_dataset' and 'right_dataset' parameters.")
    s_left = _sanitize_identifier(left)
    s_right = _sanitize_identifier(right)
    return f"(SELECT * FROM {s_left}) UNION (SELECT * FROM {s_right})"

def _ra_difference(params: Dict[str, Any]) -> str:
    """Generates SQL for Set Difference (-)."""
    left = params.get("left_dataset")
    right = params.get("right_dataset")
    if not left or not right:
        raise ValueError("Difference (-) requires 'left_dataset' and 'right_dataset' parameters.")
    s_left = _sanitize_identifier(left)
    s_right = _sanitize_identifier(right)
    return f"(SELECT * FROM {s_left}) EXCEPT (SELECT * FROM {s_right})"

def _ra_intersection(params: Dict[str, Any]) -> str:
    """Generates SQL for Intersection (∩)."""
    left = params.get("left_dataset")
    right = params.get("right_dataset")
    if not left or not right:
        raise ValueError("Intersection (∩) requires 'left_dataset' and 'right_dataset' parameters.")
    s_left = _sanitize_identifier(left)
    s_right = _sanitize_identifier(right)
    return f"(SELECT * FROM {s_left}) INTERSECT (SELECT * FROM {s_right})"

def _ra_product(params: Dict[str, Any]) -> str:
    """Generates SQL for Cartesian Product (×)."""
    left = params.get("left_dataset")
    right = params.get("right_dataset")
    if not left or not right:
        raise ValueError("Product (×) requires 'left_dataset' and 'right_dataset' parameters.")
    s_left = _sanitize_identifier(left)
    s_right = _sanitize_identifier(right)
    # Using aliases to prevent column name collisions
    return f"SELECT l.*, r.* FROM {s_left} AS l CROSS JOIN {s_right} AS r" # Adjusted for safety

def _ra_join(params: Dict[str, Any]) -> str:
    """Generates SQL for Join (⋈) - Natural or Theta."""
    left = params.get("left_dataset")
    right = params.get("right_dataset")
    join_type = params.get("join_type", "natural").lower() # 'natural' or 'theta'/'condition'
    condition = params.get("condition") # e.g., "l.id = r.user_id" - Assuming aliases l, r

    if not left or not right:
        raise ValueError("Join (⋈) requires 'left_dataset' and 'right_dataset' parameters.")

    s_left = _sanitize_identifier(left)
    s_right = _sanitize_identifier(right)

    if join_type == "natural":
        # Warning: NATURAL JOIN can be risky. Requires common column names.
        # SELECT * is implicit.
        return f"SELECT * FROM {s_left} NATURAL JOIN {s_right}"
    elif join_type in ["theta", "condition"]:
        if not condition:
             raise ValueError("Theta Join requires a 'condition' parameter.")
        # !!! WARNING: CONDITION IS NOT SANITIZED !!!
        print(f"Warning: Executing RA Theta Join with potentially unsafe condition: {condition}")
        # Use aliases 'l' and 'r' for the tables in the condition
        # Selecting * from both can still cause duplicate columns if keys have same name AND aren't the only common cols.
        # Let's explicitly select aliased columns similar to the merge logic for robustness.
        # This deviates slightly from pure RA notation but is safer in SQL.
        # This would require fetching column names first... complex.
        # Reverting to simpler SELECT * but WITH aliases for the ON clause.
        return f"SELECT l.*, r.* FROM {s_left} AS l INNER JOIN {s_right} AS r ON {condition}"
    else:
        raise ValueError(f"Unsupported join_type for Join (⋈): {join_type}. Use 'natural' or 'condition'.")


# --- Main Dispatcher ---

def apply_ra_operation(
    con: duckdb.DuckDBPyConnection,
    operation: str,
    params: Dict[str, Any],
    # NEW: Add parameter for the input SQL or table name from the previous step
    previous_step_sql_or_table: str,
    preview_limit: int = 100 # Add preview limit here
) -> Tuple[List[Dict], List[str], int, str]:
    """
    Generates and executes SQL for a specified relational algebra operation,
    using the result of the previous step as input.
    """
    generated_sql = ""
    op_lower = operation.lower()

    try:
        # --- Handle UNARY operations using the snippet generator ---
        if op_lower in ["select", "project", "rename"]:
            # Special handling for rename to get columns if needed (and possible)
            if op_lower == "rename" and "all_columns" not in params:
                # Attempt to infer columns from the previous step's SQL - THIS IS HARD
                # Requires executing a DESCRIBE or LIMIT 0 on the previous_step_sql_or_table
                # This adds complexity and overhead. It's often better to require
                # the caller (frontend/main.py) to track and provide 'all_columns'.
                print(f"Warning: 'all_columns' not provided for rename operation on potentially complex input. Relying on caller or previous state.")
                # If you absolutely must try to infer:
                try:
                    # Use CTE for safety if previous step was complex
                    describe_sql = f"WITH prev_step AS ({previous_step_sql_or_table}) DESCRIBE prev_step;"
                    # Or simpler for basic cases: describe_sql = f"DESCRIBE ({previous_step_sql_or_table})" # Might fail
                    print(f"Attempting to DESCRIBE previous step: {describe_sql}")
                    cols_result = con.execute(describe_sql).fetchall()
                    params["all_columns"] = [col[0] for col in cols_result]
                    print(f"Inferred columns for rename: {params['all_columns']}")
                except duckdb.Error as desc_err:
                    raise ValueError(f"Rename requires 'all_columns'. Could not automatically determine columns from previous step: {desc_err}. Input SQL/Table: {previous_step_sql_or_table}")

            # Generate the SQL snippet for the current operation using the previous result
            generated_sql = _generate_sql_snippet(operation, params, previous_step_sql_or_table)

        # --- Handle BINARY operations separately (require different params) ---
        elif op_lower in ["union", "difference", "intersection", "product", "join"]:
            # These operations need 'left_source' and 'right_source' (which could be table names or SQL strings)
            # The current `params` structure might not fit well.
            # This part needs significant design based on how binary ops are represented in the request.
            # Assuming params contains 'left_sql_or_table' and 'right_sql_or_table' for now.
            left_source = params.get("left_sql_or_table")
            right_source = params.get("right_sql_or_table")
            if not left_source or not right_source:
                raise ValueError(f"Binary operation '{operation}' requires 'left_sql_or_table' and 'right_sql_or_table' in params.")

            # Wrap sources if they are subqueries, sanitize if they are tables
            def prepare_binary_source(source_str):
                 source_strip = source_str.strip()
                 is_sq = any(kw in source_strip.upper() for kw in sql_keywords) and \
                         not (source_strip.startswith('"') and source_strip.endswith('"'))
                 if is_sq:
                     # Use fixed aliases for clarity in binary ops
                     return f"({source_str})" # Alias might be added by JOIN/UNION etc. itself
                 else:
                     return _sanitize_identifier(source_str)

            s_left = prepare_binary_source(left_source)
            s_right = prepare_binary_source(right_source)

            if op_lower == "union": generated_sql = f"{s_left} UNION {s_right}"
            elif op_lower == "difference": generated_sql = f"{s_left} EXCEPT {s_right}"
            elif op_lower == "intersection": generated_sql = f"{s_left} INTERSECT {s_right}"
            elif op_lower == "product":
                 # Need aliases to avoid column name collisions
                 # Inferring aliases automatically is tricky. Use fixed ones.
                 generated_sql = f"SELECT t1.*, t2.* FROM {s_left} AS t1 CROSS JOIN {s_right} AS t2"
            elif op_lower == "join":
                join_type = params.get("join_type", "natural").lower()
                condition = params.get("condition") # Assume condition uses t1.col, t2.col

                if join_type == "natural":
                     generated_sql = f"SELECT * FROM {s_left} NATURAL JOIN {s_right}" # Still risky
                elif join_type in ["theta", "condition"]:
                     if not condition: raise ValueError("Theta Join requires a 'condition'.")
                     print(f"Warning: Executing RA Theta Join with potentially unsafe condition: {condition}")
                     # Use aliases t1, t2 assumed in the condition
                     generated_sql = f"SELECT t1.*, t2.* FROM {s_left} AS t1 INNER JOIN {s_right} AS t2 ON {condition}"
                else:
                     raise ValueError(f"Unsupported join_type: {join_type}")
            else:
                 raise ValueError(f"Unhandled binary operation: {operation}") # Should not happen

        else:
            raise ValueError(f"Unsupported Relational Algebra operation: {operation}")

        # --- Execute the final generated query ---
        # Use the existing preview function
        preview_data, result_columns, total_rows = _execute_preview_query(con, generated_sql, preview_limit)

        return preview_data, result_columns, total_rows, generated_sql

    except Exception as e:
        # Improved error context
        print(f"Error during RA operation '{operation}' (Input: {previous_step_sql_or_table[:200]}...): {type(e).__name__}: {e}")
        # Avoid re-wrapping known specific errors raised earlier
        if isinstance(e, (ValueError, duckdb.Error, NotImplementedError)):
             raise e
        else:
             # Wrap unexpected errors
             raise ValueError(f"An unexpected error occurred during RA operation '{operation}': {e}")