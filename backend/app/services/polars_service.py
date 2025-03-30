# backend/app/services/polars_service.py
import polars as pl
import io
from typing import Dict, Any, Tuple, List

# --- Helper ---
def _is_numeric_dtype_pl(df: pl.DataFrame, col_name: str) -> bool:
    """Checks if a Polars column dtype is numeric."""
    if col_name not in df.columns:
        return False
    return pl.datatypes.is_numeric(df[col_name].dtype)

def apply_polars_operation(df: pl.DataFrame, operation: str, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    """
    Applies a specified polars operation to the DataFrame.
    """
    try:
        if operation == "filter":
            return _filter_rows_pl(df, params)
        elif operation == "select_columns":
            return _select_columns_pl(df, params)
        elif operation == "sort":
            return _sort_values_pl(df, params)
        elif operation == "rename":
            return _rename_columns_pl(df, params)
        elif operation == "drop_columns":
            return _drop_columns_pl(df, params)
        elif operation == "groupby":
             return _group_by_pl(df, params)
        elif operation == "groupby_multi":
             return _group_by_multi_pl(df, params)
        elif operation == "groupby_multi_agg":
             return _group_by_multi_agg_pl(df, params)
        elif operation == "pivot":
             return _pivot_pl(df, params)
        elif operation == "melt":
             return _melt_pl(df, params)
        elif operation == "set_index":
            return _set_index_pl(df, params)
        elif operation == "reset_index":
             return _reset_index_pl(df, params)
        else:
            raise ValueError(f"Unsupported polars operation: {operation}")
    except Exception as e:
        # Let main.py handle specific exceptions, just log and re-raise
        print(f"Error executing polars operation '{operation}': {type(e).__name__}: {e}")
        raise e

# --- Specific Operations ---

def _filter_rows_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    column = params.get("column")
    operator = params.get("operator")
    value = params.get("value")

    if not all([column, operator]): # Value can be empty/None sometimes
        raise ValueError("Column and operator are required for filter operation")
    if column not in df.columns:
        # Raise error that main.py can catch nicely
        raise ValueError(f"Filter column '{column}' not found in DataFrame")

    original_value = value
    pl_filter_expr = None
    condition_expr_str = ""
    col_expr = f"pl.col('{column}')"

    # Attempt type hint for comparison values based on column dtype
    try:
        dtype = df[column].dtype
        target_val = value
        if operator in ['==', '!=', '>', '<', '>=', '<=']:
            if pl.datatypes.is_numeric(dtype):
                if isinstance(value, str): # Only try conversion if input is string
                    if '.' in value or 'e' in value.lower(): target_val = float(value)
                    else: target_val = int(value)
            elif pl.datatypes.is_temporal(dtype):
                 # Polars is often good at auto-parsing, but explicit could be added
                 # target_val = pl.lit(value).str.strptime(...)
                 pass # Keep as string literal for now
            else: # String or other types
                 target_val = str(value) # Ensure string comparison
        else: # String operations
            target_val = str(value)

        value_repr = repr(target_val) # For code generation

        # Build Polars expression and code string
        if operator == "==":
            condition_expr_str = f"{col_expr} == {value_repr}"
            pl_filter_expr = pl.col(column) == target_val
        elif operator == "!=":
            condition_expr_str = f"{col_expr} != {value_repr}"
            pl_filter_expr = pl.col(column) != target_val
        elif operator == ">":
            condition_expr_str = f"{col_expr} > {value_repr}"
            pl_filter_expr = pl.col(column) > target_val
        elif operator == "<":
            condition_expr_str = f"{col_expr} < {value_repr}"
            pl_filter_expr = pl.col(column) < target_val
        elif operator == ">=":
            condition_expr_str = f"{col_expr} >= {value_repr}"
            pl_filter_expr = pl.col(column) >= target_val
        elif operator == "<=":
            condition_expr_str = f"{col_expr} <= {value_repr}"
            pl_filter_expr = pl.col(column) <= target_val
        elif operator == "contains":
            value_str = str(original_value)
            # Add cast for safety, although Polars often infers
            condition_expr_str = f"{col_expr}.cast(pl.Utf8).str.contains({repr(value_str)})"
            pl_filter_expr = pl.col(column).cast(pl.Utf8).str.contains(value_str)
        elif operator == "startswith":
             value_str = str(original_value)
             condition_expr_str = f"{col_expr}.cast(pl.Utf8).str.starts_with({repr(value_str)})"
             pl_filter_expr = pl.col(column).cast(pl.Utf8).str.starts_with(value_str)
        elif operator == "endswith":
             value_str = str(original_value)
             condition_expr_str = f"{col_expr}.cast(pl.Utf8).str.ends_with({repr(value_str)})"
             pl_filter_expr = pl.col(column).cast(pl.Utf8).str.ends_with(value_str)
        elif operator == "regex":
             value_str = str(original_value)
             condition_expr_str = f"{col_expr}.cast(pl.Utf8).str.contains({repr(value_str)}, literal=False)"
             pl_filter_expr = pl.col(column).cast(pl.Utf8).str.contains(value_str, literal=False)
        else:
            raise ValueError(f"Unsupported filter operator: {operator}")

    except (ValueError, TypeError) as e:
         # Catch potential errors during type conversion
         raise ValueError(f"Invalid value '{original_value}' for comparison with column '{column}': {e}")


    code = f"# Filter rows where {column} {operator} {original_value}\ndf = df.filter({condition_expr_str})"
    result_df = df.filter(pl_filter_expr) # Execute the Polars filter
    return result_df, code

def _select_columns_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    selected_columns = params.get("selected_columns", [])
    if not selected_columns:
        raise ValueError("No columns selected for 'select_columns' operation")

    missing = [col for col in selected_columns if col not in df.columns]
    if missing:
        # Use ValueError consistent with filter
        raise ValueError(f"Columns not found: {', '.join(missing)}")

    code = f"# Select specific columns\ndf = df.select({repr(selected_columns)})"
    result_df = df.select(selected_columns)
    return result_df, code

def _sort_values_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    sort_column = params.get("sort_column")
    sort_order = params.get("sort_order", "ascending")
    if not sort_column:
        raise ValueError("Sort column parameter is required")
    if sort_column not in df.columns:
        raise ValueError(f"Sort column '{sort_column}' not found")

    descending = sort_order == "descending"
    code = f"# Sort DataFrame by {sort_column} in {'ascending' if not descending else 'descending'} order\n"
    code += f"df = df.sort('{sort_column}', descending={descending})"
    result_df = df.sort(sort_column, descending=descending)
    return result_df, code

def _rename_columns_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    renames = params.get("renames", [])
    if not renames:
        raise ValueError("No rename mappings provided")

    rename_dict = {item['old_name']: item['new_name'] for item in renames if item.get('old_name') and item.get('new_name')}
    if not rename_dict:
         raise ValueError("Invalid rename parameters. Need list of {'old_name': '...', 'new_name': '...'}")

    missing = [old for old in rename_dict if old not in df.columns]
    if missing:
        raise ValueError(f"Columns to rename not found: {', '.join(missing)}")

    code = f"# Rename columns\nrename_map = {repr(rename_dict)}\ndf = df.rename(rename_map)"
    result_df = df.rename(rename_dict)
    return result_df, code

def _drop_columns_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    drop_columns = params.get("drop_columns", [])
    if not drop_columns:
        raise ValueError("No columns specified for dropping")

    # Check for existence *before* attempting drop
    missing = [col for col in drop_columns if col not in df.columns]
    if missing:
        # Warn or raise? Raise for consistency.
        raise ValueError(f"Columns to drop not found: {', '.join(missing)}")

    code = f"# Drop specified columns\ndf = df.drop({repr(drop_columns)})"
    result_df = df.drop(drop_columns)
    return result_df, code

def _map_agg_func_pl(func_name: str, col_name: str) -> Tuple[pl.Expr, str]:
    """Maps function name string to Polars aggregation expression and code string part."""
    alias = f"{col_name}_{func_name}"
    expr = None
    code_part = ""

    if func_name == 'mean':
        expr = pl.col(col_name).mean().alias(alias)
        code_part = f"pl.col('{col_name}').mean().alias('{alias}')"
    elif func_name == 'sum':
        expr = pl.col(col_name).sum().alias(alias)
        code_part = f"pl.col('{col_name}').sum().alias('{alias}')"
    elif func_name == 'count': # Special case: counts non-nulls in column
        expr = pl.col(col_name).count().alias(alias)
        code_part = f"pl.col('{col_name}').count().alias('{alias}')" # Or pl.count().alias(...) for total rows in group
    elif func_name == 'min':
        expr = pl.col(col_name).min().alias(alias)
        code_part = f"pl.col('{col_name}').min().alias('{alias}')"
    elif func_name == 'max':
        expr = pl.col(col_name).max().alias(alias)
        code_part = f"pl.col('{col_name}').max().alias('{alias}')"
    elif func_name == 'median':
        expr = pl.col(col_name).median().alias(alias)
        code_part = f"pl.col('{col_name}').median().alias('{alias}')"
    elif func_name == 'std':
        expr = pl.col(col_name).std().alias(alias)
        code_part = f"pl.col('{col_name}').std().alias('{alias}')"
    elif func_name == 'var':
        expr = pl.col(col_name).var().alias(alias)
        code_part = f"pl.col('{col_name}').var().alias('{alias}')"
    elif func_name == 'first':
        expr = pl.col(col_name).first().alias(alias)
        code_part = f"pl.col('{col_name}').first().alias('{alias}')"
    elif func_name == 'last':
        expr = pl.col(col_name).last().alias(alias)
        code_part = f"pl.col('{col_name}').last().alias('{alias}')"
    elif func_name == 'nunique':
        expr = pl.col(col_name).n_unique().alias(alias)
        code_part = f"pl.col('{col_name}').n_unique().alias('{alias}')"
    else:
        raise ValueError(f"Unsupported polars aggregation function: {func_name}")

    return expr, code_part

def _group_by_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    group_column = params.get("group_column")
    agg_column = params.get("agg_column")
    agg_function = params.get("agg_function", "count") # Default

    if not all([group_column, agg_column, agg_function]):
        raise ValueError("Group column, aggregation column, and function are required")
    if group_column not in df.columns: raise ValueError(f"Group column '{group_column}' not found")
    if agg_column not in df.columns: raise ValueError(f"Aggregation column '{agg_column}' not found")

    numeric_only_funcs = ['mean', 'median', 'std', 'var', 'sum']
    if agg_function in numeric_only_funcs and not _is_numeric_dtype_pl(df, agg_column):
        raise ValueError(f"Aggregation function '{agg_function}' requires a numeric column, but '{agg_column}' is not.")

    agg_expr, agg_expr_str = _map_agg_func_pl(agg_function, agg_column)

    code = f"# Group by {group_column} and aggregate {agg_column} using {agg_function}\n"
    code += f"df = df.group_by('{group_column}').agg({agg_expr_str})"

    result_df = df.group_by(group_column).agg(agg_expr)
    return result_df, code

def _group_by_multi_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    group_columns = params.get("group_columns") # List
    agg_column = params.get("agg_column")
    agg_function = params.get("agg_function", "count") # Default

    if not all([group_columns, agg_column, agg_function]):
        raise ValueError("Group columns (list), aggregation column, and function are required")
    if not isinstance(group_columns, list) or len(group_columns) == 0:
         raise ValueError("group_columns must be a non-empty list")

    missing_group = [col for col in group_columns if col not in df.columns]
    if missing_group: raise ValueError(f"Group columns not found: {', '.join(missing_group)}")
    if agg_column not in df.columns: raise ValueError(f"Aggregation column '{agg_column}' not found")

    numeric_only_funcs = ['mean', 'median', 'std', 'var', 'sum']
    if agg_function in numeric_only_funcs and not _is_numeric_dtype_pl(df, agg_column):
         raise ValueError(f"Aggregation function '{agg_function}' requires a numeric column, but '{agg_column}' is not.")

    agg_expr, agg_expr_str = _map_agg_func_pl(agg_function, agg_column)

    code = f"# Group by {group_columns} and aggregate {agg_column} using {agg_function}\n"
    code += f"df = df.group_by({repr(group_columns)}).agg({agg_expr_str})"

    result_df = df.group_by(group_columns).agg(agg_expr)
    return result_df, code

def _group_by_multi_agg_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    group_columns = params.get("group_columns") # String or list
    aggregations = params.get("aggregations") # List of {'column': 'c', 'function': 'f'}

    if not group_columns or not aggregations:
        raise ValueError("Group column(s) and aggregations list are required")
    if isinstance(group_columns, str): group_columns = [group_columns]
    if not isinstance(group_columns, list) or not group_columns: # Check again after potential conversion
         raise ValueError("group_columns must be a non-empty list")

    missing_group = [col for col in group_columns if col not in df.columns]
    if missing_group: raise ValueError(f"Group columns not found: {', '.join(missing_group)}")

    agg_expressions = []
    agg_expressions_str = []
    numeric_only_funcs = ['mean', 'median', 'std', 'var', 'sum']

    for agg_spec in aggregations:
        col = agg_spec.get("column")
        func = agg_spec.get("function")
        if not col or not func: raise ValueError(f"Invalid aggregation spec: {agg_spec}")
        if col not in df.columns: raise ValueError(f"Aggregation column '{col}' not found")

        if func in numeric_only_funcs and not _is_numeric_dtype_pl(df, col):
             raise ValueError(f"Aggregation function '{func}' on column '{col}' requires numeric type.")

        expr, code_part = _map_agg_func_pl(func, col)
        agg_expressions.append(expr)
        agg_expressions_str.append(f"    {code_part}") # Indent for code readability

    if not agg_expressions: raise ValueError("No valid aggregations provided")

    code = f"# Group by {group_columns} with multiple aggregations\n"
    code += f"df = df.group_by({repr(group_columns)}).agg([\n"
    code += ",\n".join(agg_expressions_str)
    code += "\n])"

    result_df = df.group_by(group_columns).agg(agg_expressions)
    return result_df, code

def _pivot_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    index_col = params.get("index_col")
    columns_col = params.get("columns_col")
    values_col = params.get("values_col")
    aggfunc_str = params.get("pivot_agg_function", "first")

    if not all([index_col, columns_col, values_col]):
        raise ValueError("Index, columns, and values are required for pivot")

    # Validate columns exist
    required_cols = []
    if isinstance(index_col, list): required_cols.extend(index_col)
    else: required_cols.append(index_col)
    required_cols.extend([columns_col, values_col])
    missing = [col for col in required_cols if col not in df.columns]
    if missing: raise ValueError(f"Columns not found for pivot: {', '.join(missing)}")

    # Note: Polars pivot aggregate_function expects specific strings ('first', 'sum', 'min', 'max', 'mean', 'median', 'count')
    # We might need to validate aggfunc_str against Polars' expectations if UI allows more options.
    valid_pivot_aggs = ['first', 'sum', 'min', 'max', 'mean', 'median', 'count']
    if aggfunc_str not in valid_pivot_aggs:
        # Try mapping common pandas names? Or just raise error.
        raise ValueError(f"Unsupported pivot aggregate function for Polars: '{aggfunc_str}'. Use one of {valid_pivot_aggs}")

    code = f"# Create pivot table\n"
    code += f"df = df.pivot(index={repr(index_col)},\n"
    code += f"              columns='{columns_col}',\n"
    code += f"              values='{values_col}',\n"
    code += f"              aggregate_function='{aggfunc_str}')"

    # Polars pivot might create multi-level columns if index is a list, but often flattens.
    # No explicit reset_index needed usually, unlike pandas pivot_table.
    result_df = df.pivot(index=index_col, columns=columns_col, values=values_col, aggregate_function=aggfunc_str)
    return result_df, code

def _melt_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    id_vars = params.get("id_vars")
    value_vars = params.get("value_vars")
    var_name = params.get("var_name", "variable")
    value_name = params.get("value_name", "value")

    if id_vars is None or value_vars is None:
         raise ValueError("id_vars and value_vars lists are required for melt")

    required_cols = (id_vars or []) + (value_vars or []) # Handle potential empty lists
    missing = [col for col in required_cols if col not in df.columns]
    if missing: raise ValueError(f"Columns not found for melt: {', '.join(missing)}")

    code = f"# Melt DataFrame (wide to long)\n"
    code += f"df = df.melt(id_vars={repr(id_vars)},\n"
    code += f"             value_vars={repr(value_vars)},\n" # Polars can infer value_vars if None
    code += f"             variable_name='{var_name}',\n"
    code += f"             value_name='{value_name}')"

    result_df = df.melt(id_vars=id_vars, value_vars=value_vars, variable_name=var_name, value_name=value_name)
    return result_df, code

def _set_index_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    index_column = params.get("index_column")
    if not index_column:
        raise ValueError("Index column parameter is required (used for sorting)")
    if index_column not in df.columns:
        raise ValueError(f"Index column '{index_column}' not found")

    # Polars doesn't have index, simulate via sort
    code = f"# Polars: Simulating 'set_index' by sorting by '{index_column}'.\n"
    code += f"df = df.sort('{index_column}')"
    result_df = df.sort(index_column)
    return result_df, code

def _reset_index_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    # Add a row count column if it doesn't exist
    row_count_col_name = "index"
    if row_count_col_name in df.columns:
         # Find alternative name if 'index' exists
         i = 0
         while f"index_{i}" in df.columns: i += 1
         row_count_col_name = f"index_{i}"

    code = f"# Polars: Simulating 'reset_index' by adding row count column '{row_count_col_name}'.\n"
    code += f"df = df.with_row_count(name='{row_count_col_name}', offset=0)"
    result_df = df.with_row_count(name=row_count_col_name, offset=0)
    return result_df, code


# Needs main API endpoint change for join
def apply_polars_join(left_df: pl.DataFrame, right_df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    """Applies a polars join operation between two DataFrames."""
    how = params.get("join_type", "inner")
    left_on = params.get("left_on")
    right_on = params.get("right_on")

    if not left_on or not right_on:
         raise ValueError("Both left_on and right_on key columns are required for join")

    # Validate columns exist
    if left_on not in left_df.columns: raise ValueError(f"Left key '{left_on}' not found in left dataset")
    if right_on not in right_df.columns: raise ValueError(f"Right key '{right_on}' not found in right dataset")

    valid_joins = ['inner', 'left', 'outer', 'semi', 'anti', 'cross']
    if how not in valid_joins:
        raise ValueError(f"Invalid join type: {how}. Must be one of {valid_joins}")

    code = f"# Join DataFrames\n"
    # Use different variable names in code example
    code += f"df = df_left.join(df_right,\n"
    code += f"                  left_on='{left_on}',\n"
    code += f"                  right_on='{right_on}',\n"
    code += f"                  how='{how}')"

    result_df = left_df.join(right_df, left_on=left_on, right_on=right_on, how=how)
    return result_df, code