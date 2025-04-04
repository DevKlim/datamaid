# backend/app/services/polars_service.py
import polars as pl
import polars.selectors as cs # Import selectors for convenience
import io
import numpy as np
import re # Import re for regex
import traceback
from typing import Dict, Any, Tuple, List, Optional

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
        # --- Existing Operations ---
        if operation == "filter": return _filter_rows_pl(df, params)
        elif operation == "select_columns": return _select_columns_pl(df, params)
        elif operation == "sort": return _sort_values_pl(df, params)
        elif operation == "rename": return _rename_columns_pl(df, params)
        elif operation == "drop_columns": return _drop_columns_pl(df, params)
        elif operation == "groupby": return _group_by_pl(df, params)
        elif operation == "groupby_multi": return _group_by_multi_pl(df, params)
        elif operation == "groupby_multi_agg": return _group_by_multi_agg_pl(df, params)
        elif operation == "pivot": return _pivot_pl(df, params)
        elif operation == "melt": return _melt_pl(df, params)
        elif operation == "set_index": return _set_index_pl(df, params) # Simulated
        elif operation == "reset_index": return _reset_index_pl(df, params) # Simulated
        elif operation == "fillna": return _fillna_pl(df, params)
        elif operation == "dropna": return _dropna_pl(df, params)
        elif operation == "astype": return _astype_pl(df, params)
        elif operation == "string_operation": return _string_op_pl(df, params)
        elif operation == "date_extract": return _date_extract_pl(df, params)
        elif operation == "drop_duplicates": return _drop_duplicates_pl(df, params)
        elif operation == "create_column": return _create_column_pl(df, params) # Uses eval! Risky.
        elif operation == "window_function": return _window_function_pl(df, params)
        elif operation == "sample": return _sample_pl(df, params)
        # --- NEW OPERATION ---
        elif operation == "shuffle": return _shuffle_pl(df, params)
        # Regex ops handled by apply_polars_regex
        elif operation.startswith("regex_"): # e.g., regex_filter, regex_extract, regex_replace
             return apply_polars_regex(df, operation.split('_', 1)[1], params)
        elif operation == "_apply_lambda": return _apply_lambda_pl(df, params)
        elif operation == "string_operation": return _string_op_pl(df, params)
        elif operation == "date_extract": return _date_extract_pl(df, params)
        elif operation == "create_column": return _create_column_pl(df, params) # Uses eval! Risky.
        elif operation == "window_function": return _window_function_pl(df, params)

        else:
            raise ValueError(f"Unsupported polars operation: {operation}")
    except Exception as e:
        print(f"Error executing polars operation '{operation}': {type(e).__name__}: {e}")
        traceback.print_exc() # Print traceback
        raise e

# --- Specific Operations ---

def _filter_rows_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    column = params.get("column")
    operator = params.get("operator")
    value = params.get("value")

    if not all([column, operator]): # Value can be empty/None
        raise ValueError("Column and operator are required for filter operation")
    if column not in df.columns:
        raise ValueError(f"Filter column '{column}' not found in DataFrame")

    original_value = value
    pl_filter_expr = None
    condition_expr_str = ""
    col_expr_str = f"pl.col('{column}')"
    target_dtype = df[column].dtype # Get column type

    # Attempt type coercion of the *value* based on column dtype
    try:
        target_val = value
        if operator in ['==', '!=', '>', '<', '>=', '<=']:
            if pl.datatypes.is_numeric(target_dtype) and isinstance(value, (str, int, float)):
                # Safely try to cast value to column's numeric type
                try: target_val = pl.lit(value).cast(target_dtype).item() # Get scalar value
                except: target_val = str(value) # Fallback to string if cast fails
            elif pl.datatypes.is_temporal(target_dtype) and isinstance(value, str):
                 # Try casting string value to the temporal type
                 try: target_val = pl.lit(value).cast(target_dtype).item()
                 except: pass # Keep as string lit if cast fails, polars might handle it
            elif pl.datatypes.is_string(target_dtype):
                 target_val = str(value) # Ensure string comparison
            # Handle boolean?
            elif pl.datatypes.is_boolean(target_dtype):
                 if isinstance(value, str):
                     v_lower = value.lower()
                     if v_lower in ['true', '1', 't']: target_val = True
                     elif v_lower in ['false', '0', 'f']: target_val = False
                     # else keep as original? Or error? Let's error.
                     else: raise ValueError(f"Cannot interpret '{value}' as boolean.")
                 elif isinstance(value, (int, bool)):
                      target_val = bool(value)
        else: # String operations
            target_val = str(value)

        value_repr = repr(target_val) # For code generation

        # Build Polars expression and code string
        # Using pl.lit() ensures correct type handling in the expression
        lit_val = pl.lit(target_val)

        if operator == "==":
            condition_expr_str = f"{col_expr_str} == {value_repr}"
            pl_filter_expr = pl.col(column) == lit_val
        elif operator == "!=":
            condition_expr_str = f"{col_expr_str} != {value_repr}"
            pl_filter_expr = pl.col(column) != lit_val
        elif operator == ">":
            condition_expr_str = f"{col_expr_str} > {value_repr}"
            pl_filter_expr = pl.col(column) > lit_val
        elif operator == "<":
            condition_expr_str = f"{col_expr_str} < {value_repr}"
            pl_filter_expr = pl.col(column) < lit_val
        elif operator == ">=":
            condition_expr_str = f"{col_expr_str} >= {value_repr}"
            pl_filter_expr = pl.col(column) >= lit_val
        elif operator == "<=":
            condition_expr_str = f"{col_expr_str} <= {value_repr}"
            pl_filter_expr = pl.col(column) <= lit_val
        # String ops need cast for safety if column isn't already Utf8
        elif operator == "contains":
            str_val = str(original_value)
            condition_expr_str = f"{col_expr_str}.cast(pl.Utf8).str.contains({repr(str_val)})"
            pl_filter_expr = pl.col(column).cast(pl.Utf8).str.contains(str_val)
        elif operator == "startswith":
             str_val = str(original_value)
             condition_expr_str = f"{col_expr_str}.cast(pl.Utf8).str.starts_with({repr(str_val)})"
             pl_filter_expr = pl.col(column).cast(pl.Utf8).str.starts_with(str_val)
        elif operator == "endswith":
             str_val = str(original_value)
             condition_expr_str = f"{col_expr_str}.cast(pl.Utf8).str.ends_with({repr(str_val)})"
             pl_filter_expr = pl.col(column).cast(pl.Utf8).str.ends_with(str_val)
        # Note: 'regex' filter handled by apply_polars_regex now
        else:
            raise ValueError(f"Unsupported filter operator: {operator}")

    except (ValueError, TypeError, pl.PolarsError) as e:
         raise ValueError(f"Invalid value '{original_value}' or type mismatch for comparison with column '{column}' ({target_dtype}): {e}")

    code = f"# Filter rows where {column} {operator} {original_value}\ndf = df.filter({condition_expr_str})"
    result_df = df.filter(pl_filter_expr)
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

def _map_str_to_pl_dtype(type_str: str) -> Optional[pl.DataType]:
    """Maps common type strings to Polars DataTypes."""
    type_str = type_str.lower()
    mapping = {
        "integer": pl.Int64, "int": pl.Int64, "int64": pl.Int64, "int32": pl.Int32,
        "float": pl.Float64, "double": pl.Float64, "float64": pl.Float64, "float32": pl.Float32,
        "numeric": pl.Float64, # Default numeric to float
        "string": pl.Utf8, "text": pl.Utf8, "varchar": pl.Utf8, "str": pl.Utf8,
        "boolean": pl.Boolean, "bool": pl.Boolean,
        "datetime": pl.Datetime, "timestamp": pl.Datetime, # Add timezone awareness option later?
        "date": pl.Date,
        "time": pl.Time,
        "category": pl.Categorical,
        "binary": pl.Binary, "blob": pl.Binary,
    }
    return mapping.get(type_str)

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
    suffix = params.get("suffix", "_right") # Suffix for duplicate columns

    if not left_on or not right_on: raise ValueError("Both left_on and right_on key columns required")
    if left_on not in left_df.columns: raise ValueError(f"Left key '{left_on}' not found")
    if right_on not in right_df.columns: raise ValueError(f"Right key '{right_on}' not found")

    valid_joins = ['inner', 'left', 'outer', 'semi', 'anti', 'cross', 'outer_coalesce'] # outer_coalesce added
    if how not in valid_joins:
        raise ValueError(f"Invalid join type: {how}. Must be one of {valid_joins}")

    join_args = {
        "right_df": right_df,
        "left_on": left_on,
        "right_on": right_on,
        "how": how,
        "suffix": suffix,
    }

    code_args_list = [f"df_right", f"left_on='{left_on}'", f"right_on='{right_on}'", f"how='{how}'", f"suffix='{suffix}'"]
    code = f"# Join DataFrames\ndf = df_left.join({', '.join(code_args_list)})"

    # Polars join call signature is slightly different (df.join(other_df, ...))
    result_df = left_df.join(**join_args)
    return result_df, code

def _fillna_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    columns = params.get("columns") # Optional list/selector
    value = params.get("value") # Fill with literal value
    strategy = params.get("method") # Polars uses 'strategy': 'forward', 'backward', 'mean', 'median', 'min', 'max', 'zero'

    if value is None and strategy is None:
        raise ValueError("Either 'value' or 'method' (strategy) must be provided.")
    if value is not None and strategy is not None:
        raise ValueError("Provide only 'value' or 'method' (strategy), not both.")

    fill_expr = None
    code_fill_part = ""
    if value is not None:
        # Use pl.lit() for literal values
        fill_expr = pl.lit(value)
        code_fill_part = f"value={repr(value)}"
    elif strategy:
        valid_strategies = ['forward', 'backward', 'mean', 'median', 'min', 'max', 'zero']
        if strategy not in valid_strategies:
             raise ValueError(f"Invalid fill method/strategy: {strategy}. Use one of {valid_strategies}")
        fill_expr = strategy # Pass strategy string directly to polars fill_null
        code_fill_part = f"strategy='{strategy}'"

    code = f"# Fill missing values\n"
    if columns:
        if not isinstance(columns, list): columns = [columns]
        missing = [col for col in columns if col not in df.columns]
        if missing: raise ValueError(f"Columns to fill not found: {', '.join(missing)}")
        # Use with_columns to fill specific columns
        code += f"df = df.with_columns(pl.col({repr(columns)}).fill_null({code_fill_part}))"
        result_df = df.with_columns(pl.col(columns).fill_null(fill_expr))
    else:
        # Fill all columns
        code += f"df = df.fill_null({code_fill_part})"
        result_df = df.fill_null(fill_expr) # fill_null can take strategy or value

    return result_df, code

def _dropna_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    subset = params.get("subset") # Optional list of columns

    if subset and not isinstance(subset, list):
        raise ValueError("'subset' must be a list.")
    if subset:
        missing = [col for col in subset if col not in df.columns]
        if missing: raise ValueError(f"Columns in dropna subset not found: {', '.join(missing)}")

    code = f"# Drop rows with missing values\n"
    if subset:
        code += f"df = df.drop_nulls(subset={repr(subset)})"
        result_df = df.drop_nulls(subset=subset)
    else:
        code += f"df = df.drop_nulls()"
        result_df = df.drop_nulls()

    return result_df, code

def _astype_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    column = params.get("column")
    new_type_str = params.get("new_type")
    pl_dtype = _map_str_to_pl_dtype(new_type_str)

    if not column or not new_type_str:
        raise ValueError("astype requires 'column' and 'new_type'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")
    if pl_dtype is None:
        raise ValueError(f"Unsupported polars type string: '{new_type_str}'.")

    code = f"# Cast column '{column}' to type {pl_dtype}\n"
    code += f"import polars as pl\n" # Add import for code snippet
    code += f"df = df.with_columns(pl.col('{column}').cast(pl.{pl_dtype}))"
    # Add strict=False? Default is True, raises error on invalid cast.
    # code += f"df = df.with_columns(pl.col('{column}').cast(pl.{pl_dtype}, strict=False))"

    try:
        result_df = df.with_columns(pl.col(column).cast(pl_dtype, strict=True))
    except pl.ComputeError as e:
        # Provide more specific error for casting failure
        raise ValueError(f"Failed to cast column '{column}' to {pl_dtype}. Check data compatibility. Error: {e}")

    return result_df, code


def _string_op_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    column = params.get("column")
    string_func = params.get("string_function") # 'upper', 'lower', 'strip', 'split'
    new_col_name = params.get("new_column_name", f"{column}_{string_func}")
    delimiter = params.get("delimiter")
    part_index = params.get("part_index") # 0-based for polars list slicing

    if not column or not string_func:
        raise ValueError("string_operation requires 'column' and 'string_function'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")

    code = f"# Apply string operation '{string_func}' to column '{column}'\n"
    target_col_expr = f"pl.col('{column}').cast(pl.Utf8).str" # Ensure Utf8 and access str namespace
    result_expr_str = ""
    pl_expr = None

    func_lower = string_func.lower()
    if func_lower == 'upper':
        result_expr_str = f"{target_col_expr}.to_uppercase()"
        pl_expr = pl.col(column).cast(pl.Utf8).str.to_uppercase()
    elif func_lower == 'lower':
        result_expr_str = f"{target_col_expr}.to_lowercase()"
        pl_expr = pl.col(column).cast(pl.Utf8).str.to_lowercase()
    elif func_lower == 'strip':
        result_expr_str = f"{target_col_expr}.strip_chars()" # Strips whitespace by default
        pl_expr = pl.col(column).cast(pl.Utf8).str.strip_chars()
    elif func_lower == 'split':
        if delimiter is None or part_index is None:
             raise ValueError("String split requires 'delimiter' and 'part_index' (0-based).")
        # .str.split() returns list, .list.get() accesses element
        result_expr_str = f"{target_col_expr}.split({repr(delimiter)}).list.get({int(part_index)})"
        pl_expr = pl.col(column).cast(pl.Utf8).str.split(delimiter).list.get(int(part_index))
    else:
        raise ValueError(f"Unsupported string_function for polars: {string_func}")

    code += f"df = df.with_columns({result_expr_str}.alias('{new_col_name}'))"
    try:
        result_df = df.with_columns(pl_expr.alias(new_col_name))
    except Exception as e: # Catch errors like index out of bounds for split
        raise ValueError(f"Error applying polars string function '{string_func}': {e}")

    return result_df, code

def _date_extract_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    column = params.get("column")
    part = params.get("part") # year, month, day, hour, minute, second, ordinal_day, weekday, week, quarter
    new_col_name = params.get("new_column_name", f"{column}_{part}")

    if not column or not part:
        raise ValueError("date_extract requires 'column' and 'part'.")
    if column not in df.columns:
         raise ValueError(f"Column '{column}' not found.")

    # Map to polars dt methods
    part_map = {
        'year': 'year()', 'month': 'month()', 'day': 'day()',
        'hour': 'hour()', 'minute': 'minute()', 'second': 'second()',
        'dow': 'weekday()', # Monday=1, Sunday=7
        'weekday': 'weekday()',
        'doy': 'ordinal_day()',
        'ordinal_day': 'ordinal_day()',
        'week': 'week()', # ISO week number
        'weekofyear': 'week()',
        'quarter': 'quarter()',
    }
    part_lower = part.lower()
    if part_lower not in part_map:
        raise ValueError(f"Invalid date part '{part}' for polars. Valid: {list(part_map.keys())}")

    pl_method = part_map[part_lower]

    code = f"# Extract '{part}' from date/time column '{column}'\n"
    # Assume column is already temporal or cast attempt needed? Polars usually needs explicit type.
    # Let's assume UI ensures compatible column or add cast.
    target_expr = f"pl.col('{column}').dt.{pl_method}"
    code += f"df = df.with_columns({target_expr}.alias('{new_col_name}'))"

    try:
        # Ensure column is appropriate type before accessing .dt
        # If not date/datetime, this will fail. Consider adding cast here?
        # result_df = df.with_columns(pl.col(column).cast(pl.Datetime).dt...) # Example cast
        result_df = df.with_columns(getattr(pl.col(column).dt, pl_method[:-2])().alias(new_col_name))
    except Exception as e: # Catches attribute errors if not datetime etc.
         raise ValueError(f"Failed to extract date part '{part}'. Is column '{column}' a Date/Datetime type? Error: {e}")

    return result_df, code

def _drop_duplicates_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    subset = params.get("subset") # Optional list
    keep = params.get("keep", "first") # 'first', 'last', 'none' (polars uses 'none')

    if subset and not isinstance(subset, list):
        raise ValueError("'subset' must be a list.")
    if subset:
        missing = [col for col in subset if col not in df.columns]
        if missing: raise ValueError(f"Columns in drop_duplicates subset not found: {', '.join(missing)}")
    if keep is False: keep = "none" # Map pandas False to polars 'none'
    valid_keeps = ['first', 'last', 'none', 'any'] # 'any' is also valid in polars unique
    if keep not in valid_keeps:
         raise ValueError(f"Invalid keep value: {keep}. Use 'first', 'last', or 'none'.")

    code = f"# Drop duplicate rows\n"
    if subset:
        code += f"df = df.unique(subset={repr(subset)}, keep='{keep}')"
        result_df = df.unique(subset=subset, keep=keep)
    else:
        code += f"df = df.unique(keep='{keep}')"
        result_df = df.unique(keep=keep)

    return result_df, code


def _create_column_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    new_col_name = params.get("new_column_name")
    expression_str = params.get("expression") # Polars expression as a string

    if not new_col_name or not expression_str:
        raise ValueError("create_column requires 'new_column_name' and 'expression' string.")

    code = f"# Create new column '{new_col_name}' using expression\n"
    code += f"import polars as pl\n" # Ensure pl is available in snippet context
    # The expression string MUST be a valid Polars expression string
    # e.g., "pl.col('a') + pl.col('b')", "pl.lit(5)", "pl.when(pl.col('x') > 0).then(pl.lit('pos')).otherwise(pl.lit('neg'))"
    code += f"df = df.with_columns( ({expression_str}).alias('{new_col_name}') )"

    # !!! WARNING: EVALUATING ARBITRARY STRING EXPRESSIONS IS RISKY !!!
    # This uses eval(), which can execute arbitrary code if not carefully controlled.
    # A safer approach involves building expressions programmatically based on UI input,
    # OR using a very restricted evaluation context.
    # For now, proceed with eval, assuming the expression_str is trusted/validated upstream.
    print(f"Warning: Executing create_column with potentially unsafe eval of expression: {expression_str}")
    local_env = {'pl': pl} # Provide polars in the eval context
    try:
        polars_expr = eval(expression_str, {"__builtins__": {}}, local_env) # Restricted builtins
        if not isinstance(polars_expr, pl.Expr):
             # Allow literals directly? Wrap them in pl.lit()
             if isinstance(polars_expr, (str, int, float, bool)):
                 polars_expr = pl.lit(polars_expr)
             else:
                 raise ValueError("Expression did not evaluate to a valid Polars expression or literal.")
        result_df = df.with_columns(polars_expr.alias(new_col_name))
    except SyntaxError as se:
         raise ValueError(f"Invalid syntax in expression: {expression_str}. Error: {se}")
    except Exception as e:
         raise ValueError(f"Failed to evaluate or apply expression '{expression_str}': {e}")

    return result_df, code

def _window_function_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    func = params.get("window_function") # 'rank', 'lead', 'lag'
    target_column = params.get("target_column") # For lead/lag
    order_by_column = params.get("order_by_column") # For ordering within window
    order_descending = not params.get("order_ascending", True) # Polars uses 'descending' flag
    partition_by_columns = params.get("partition_by_columns") # Optional list
    new_col_name = params.get("new_column_name", f"{func}_{target_column or order_by_column}")
    # Rank specific
    rank_method = params.get("rank_method", "average") # 'average', 'min', 'max', 'ordinal'(dense), 'random'
    # Lead/Lag specific
    offset = params.get("offset", 1)
    default_value = params.get("default_value") # Optional fill value for lead/lag

    if not func or not order_by_column:
        raise ValueError("window_function requires 'window_function' and 'order_by_column'.")
    func_lower = func.lower()
    if func_lower in ['lead', 'lag'] and not target_column:
        raise ValueError("Lead/Lag requires 'target_column'.")

    # Validate columns
    if order_by_column not in df.columns: raise ValueError(f"Order by column '{order_by_column}' not found.")
    if target_column and target_column not in df.columns: raise ValueError(f"Target column '{target_column}' not found.")
    if partition_by_columns:
         if not isinstance(partition_by_columns, list): raise ValueError("partition_by_columns must be a list.")
         missing_part = [c for c in partition_by_columns if c not in df.columns]
         if missing_part: raise ValueError(f"Partition columns not found: {', '.join(missing_part)}")

    code = f"# Apply window function '{func}'\n"
    over_args = repr(partition_by_columns) if partition_by_columns else repr(order_by_column)
    if partition_by_columns and len(partition_by_columns) == 1:
        over_args = f"'{partition_by_columns[0]}'" # Use string if single column partition

    window_expr_str = ""
    pl_expr = None

    # Build the window expression
    try:
        if func_lower == 'rank':
            # Map rank methods
            polars_rank_method = rank_method
            if rank_method == 'first': polars_rank_method = 'ordinal' # Map 'first' to 'ordinal' (row number) - might differ slightly
            if rank_method == 'dense': polars_rank_method = 'dense'
            # rank() operates implicitly on the column it's called on (or needs target)
            # Let's rank based on the order_by column
            window_expr_str = f"pl.col('{order_by_column}').rank(method='{polars_rank_method}', descending={order_descending})"
            pl_expr = pl.col(order_by_column).rank(method=polars_rank_method, descending=order_descending)
        elif func_lower in ['lead', 'lag']:
            fill_null_expr = f".fill_null({repr(default_value)})" if default_value is not None else ""
            window_expr_str = f"pl.col('{target_column}').{func_lower}(n={int(offset)}){fill_null_expr}"
            pl_expr = getattr(pl.col(target_column), func_lower)(n=int(offset))
            if default_value is not None:
                 pl_expr = pl_expr.fill_null(default_value)
        else:
            raise ValueError(f"Unsupported window_function for polars: {func}. Try 'rank', 'lead', 'lag'.")

        # Add the OVER clause
        over_clause_str = f".over({over_args})" # Simplified: assumes order_by handled implicitly or needs explicit sort before?
        # Polars window functions often imply ordering by group/partition key, but explicit sort might be safer depending on exact need.
        # Let's rely on .over() for partitioning. Explicit sort might be needed for global lead/lag.
        if partition_by_columns:
             pl_expr = pl_expr.over(partition_by_columns)
             window_expr_str += f".over({repr(partition_by_columns)})"
        else:
             # Global lead/lag requires sorting first
             code += f"# NOTE: Global lead/lag requires sorting first for predictable results\n"
             code += f"df = df.sort('{order_by_column}', descending={order_descending})\n"
             # No .over() needed for global shift after sort
             pass


        code += f"df = df.with_columns({window_expr_str}.alias('{new_col_name}'))"
        # Execute
        if partition_by_columns:
            result_df = df.with_columns(pl_expr.alias(new_col_name))
        else: # Global, sort first
             result_df = df.sort(order_by_column, descending=order_descending).with_columns(pl_expr.alias(new_col_name))


    except Exception as e:
        raise ValueError(f"Error applying polars window function '{func}': {e}")

    return result_df, code


def _sample_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    n = params.get("n")
    frac = params.get("frac")
    replace = params.get("replace", False) # Named 'replace' from UI
    shuffle = params.get("shuffle", True) # Default shuffle=True makes sense for sampling
    seed = params.get("seed") # Named 'seed' from UI

    if n is None and frac is None: raise ValueError("Sample requires 'n' or 'frac'.")
    if n is not None and frac is not None: raise ValueError("Provide 'n' or 'frac', not both.")

    # Map UI names to Polars names
    sample_args = {"with_replacement": replace, "shuffle": shuffle}
    if n is not None: sample_args["n"] = int(n)
    if frac is not None: sample_args["fraction"] = float(frac)
    if seed is not None: sample_args["seed"] = int(seed)

    code_args_list = []
    # Polars uses n/fraction keyword args positionally first if not None
    if n is not None: code_args_list.append(f"n={n}")
    elif frac is not None: code_args_list.append(f"fraction={frac}")

    # Add other args explicitly by keyword name
    # Use the polars kwarg names in the code string
    polars_kwargs = {
        "with_replacement": replace,
        "shuffle": shuffle,
        "seed": seed
    }
    code_args_list.extend([f"{k}={repr(v)}" for k, v in polars_kwargs.items() if v is not None and k != 'seed'] + ([f"seed={seed}"] if seed is not None else [])) # handle seed separately for clarity

    code_args = ', '.join(code_args_list)
    code = f"# Sample rows (Polars)\ndf = df.sample({code_args})"
    try:
        result_df = df.sample(**sample_args)
    except Exception as e: # Catch errors like negative n etc.
         raise ValueError(f"Error during polars sampling: {e}")

    return result_df, code

# --- NEW SHUFFLE IMPLEMENTATION ---
def _shuffle_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    """Shuffles all rows of the DataFrame using Polars."""
    seed = params.get("seed") # Optional seed

    sample_args = {"fraction": 1.0, "shuffle": True}
    if seed is not None:
        sample_args["seed"] = int(seed)
        code = f"# Shuffle all rows with seed {seed} (Polars)\ndf = df.sample(fraction=1.0, shuffle=True, seed={seed})"
    else:
        code = f"# Shuffle all rows randomly (Polars)\ndf = df.sample(fraction=1.0, shuffle=True)"

    try:
        result_df = df.sample(**sample_args)
    except Exception as e:
        raise ValueError(f"Error during polars shuffling: {e}")

    return result_df, code

def apply_polars_regex(df: pl.DataFrame, operation: str, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    """Handles regex operations using Polars expressions."""
    column = params.get("column")
    regex = params.get("regex")
    # Polars regex is implicitly case-sensitive unless flags like (?i) are in pattern

    if not column or regex is None:
        raise ValueError("Regex operations require 'column' and 'regex'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found for regex operation.")

    result_df = df
    code = f"# Polars Regex operation '{operation}' on column '{column}'\n"
    target_col_expr = f"pl.col('{column}').cast(pl.Utf8)" # Ensure Utf8

    try:
        if operation == "filter":
            # Use str.contains with literal=False for regex mode
            code += f"df = df.filter({target_col_expr}.str.contains({repr(regex)}, literal=False))"
            result_df = df.filter(pl.col(column).cast(pl.Utf8).str.contains(regex, literal=False))
        elif operation == "extract":
            new_column = params.get("new_column", f"{column}_extracted")
            # Use str.extract with group_index=0 for full match
            code += f"df = df.with_columns({target_col_expr}.str.extract({repr(regex)}, group_index=0).alias('{new_column}'))"
            result_df = df.with_columns(pl.col(column).cast(pl.Utf8).str.extract(regex, group_index=0).alias(new_column))
        elif operation == "extract_group":
            new_column = params.get("new_column", f"{column}_group_{params.get('group', 1)}")
            group_idx = params.get("group", 1) # Polars uses 1-based index for groups
            code += f"df = df.with_columns({target_col_expr}.str.extract({repr(regex)}, group_index={int(group_idx)}).alias('{new_column}'))"
            result_df = df.with_columns(pl.col(column).cast(pl.Utf8).str.extract(regex, group_index=int(group_idx)).alias(new_column))
        elif operation == "replace":
            replacement = params.get("replacement", "")
            new_column = params.get("new_column") # Optional new column
            # Use str.replace_all for regex replace
            replace_expr = f"{target_col_expr}.str.replace_all({repr(regex)}, {repr(replacement)})"
            if new_column:
                code += f"df = df.with_columns({replace_expr}.alias('{new_column}'))"
                result_df = df.with_columns(pl.col(column).cast(pl.Utf8).str.replace_all(regex, replacement).alias(new_column))
            else: # Replace in place using with_columns
                code += f"df = df.with_columns({replace_expr}.alias('{column}'))"
                result_df = df.with_columns(pl.col(column).cast(pl.Utf8).str.replace_all(regex, replacement).alias(column))
        else:
             raise ValueError(f"Unsupported regex operation type: {operation}")

    except pl.ComputeError as comp_err:
        # Catch polars specific compute errors (e.g., invalid regex pattern)
        raise ValueError(f"Polars compute error during regex '{operation}': {comp_err}")
    except Exception as e:
         raise ValueError(f"Error during polars regex '{operation}': {e}")

    return result_df, code

def _apply_lambda_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    """Applies a lambda function defined as a string to a column."""
    column = params.get("column")
    lambda_str = params.get("lambda_str")
    new_column_name = params.get("new_column_name") # Optional

    if not column or not lambda_str:
        raise ValueError("apply_lambda requires 'column' and 'lambda_str' parameters.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found for apply_lambda.")

    # Basic validation of lambda string (very limited)
    if not lambda_str.strip().startswith("lambda"):
        raise ValueError("lambda_str must start with 'lambda'.")

    result_df = df.copy()
    target_column = new_column_name if new_column_name else column
    code = f"# Apply lambda function to column '{column}'\n"
    # Ensure necessary imports might be available in the lambda context (pd, np)
    code += f"# Make sure 'pl' and 'np' are available if used in the lambda\n"
    code += f"lambda_func = {lambda_str}\n" # Show the lambda definition
    code += f"df['{target_column}'] = df['{column}'].apply(lambda_func)"

    try:
        # --- SECURITY WARNING: Using eval is risky! ---
        # Create the actual lambda function from the string
        # Provide pandas and numpy in the eval context for convenience
        lambda_func = eval(lambda_str, {"pl": pl, "np": np})

        # Apply the lambda function
        result_df[target_column] = result_df[column].apply(lambda_func)

    except SyntaxError as se:
        raise ValueError(f"Invalid lambda function syntax: {se}") from se
    except Exception as e:
        # Catch errors during lambda execution (e.g., TypeError on incompatible data)
        print(f"Error executing lambda on column '{column}': {type(e).__name__}: {e}")
        traceback.print_exc()
        raise ValueError(f"Error applying lambda to column '{column}': {e}. Check function logic and column data type.") from e

    return result_df, code

def generate_polars_code_snippet(operation: str, params: Dict[str, Any], df_var: str = "df") -> str:
    """Generates a single line/snippet of polars code for a given operation."""
    # --- Implement based on apply_polars_operation logic ---
    if operation == "filter":
        col = params['column']
        op = params['operator']
        val = params['value']
        # Basic example, needs proper quoting/type handling
        op_map = {"==": "eq", "!=": "neq", ">": "gt", "<": "lt", ">=": "gt_eq", "<=": "lt_eq"} # etc.
        if op in op_map:
            lit_val = f"pl.lit({repr(val)})" # Use repr for literal representation
            return f"{df_var} = {df_var}.filter(pl.col('{col}').{op_map[op]}({lit_val}))"
        # Add other operations (select, groupby, rename, drop, join etc.)
        elif operation == "join": # Polars uses join
             right_df_name = params.get("right_dataset_name", "df_right")
             left_on = params['left_on']
             right_on = params['right_on']
             how = params.get('join_type', 'inner')
             return f"{df_var} = {df_var}.join({right_df_name}, left_on='{left_on}', right_on='{right_on}', how='{how}')"
        # ... other operations
        else:
             return f"# Polars code for {operation} with params {params}"
    elif operation == "custom_code":
        return str(params)
    # Add other operations...
    else:
        return f"# TODO: Implement polars code snippet for {operation}"


def replay_polars_operations(original_content: bytes, history: List[Dict[str, Any]]) -> Tuple[bytes, str]:
    """
    Replays a list of polars operations from the original content.
    Returns the final content (bytes) and the cumulative code string.
    """
    if not history:
        return original_content, "# No operations applied"

    try:
        # Use scan_csv for potentially better performance? Or read_csv is fine.
        current_df = pl.read_csv(io.BytesIO(original_content))
    except Exception as e:
        raise ValueError(f"Failed to load original data for Polars replay: {e}")

    cumulative_code_lines = ["import polars as pl", "import io", "# Load initial data (replace with actual loading if needed)"]
    # cumulative_code_lines.append(f"df = pl.read_csv(io.BytesIO(original_content_placeholder))")

    loaded_dataframes = {"df": current_df}

    for i, step in enumerate(history):
        op = step["operation"]
        params = step["params_or_code"]
        df_var = "df"

        code_snippet = ""
        try:
            if op == 'join': # Polars uses join
                right_dataset_name = params.get("right_dataset")
                if not right_dataset_name: raise ValueError("Join step missing 'right_dataset' name.")
                right_df_var = f"df_{right_dataset_name}"
                if right_df_var not in loaded_dataframes:
                     raise ValueError(f"Right dataframe '{right_dataset_name}' for join step {i+1} not pre-loaded.")

                step_params_for_snippet = params.copy()
                step_params_for_snippet["right_dataset_name"] = right_df_var
                code_snippet = generate_polars_code_snippet(op, step_params_for_snippet, df_var="df")

                current_df = current_df.join(
                    loaded_dataframes[right_df_var],
                    left_on=params['left_on'],
                    right_on=params['right_on'],
                    how=params.get('join_type', 'inner')
                )
                loaded_dataframes[df_var] = current_df

            elif op == 'custom_code':
                 code_snippet = str(params)
                 local_vars = {'df': current_df, 'pl': pl, 'io': io}
                 # Polars often uses expression API, exec might be less common, but possible
                 exec(code_snippet, local_vars)
                 current_df = local_vars.get('df')
                 if not isinstance(current_df, pl.DataFrame):
                     raise ValueError(f"Custom code at step {i+1} did not produce a Polars DataFrame named 'df'.")
                 loaded_dataframes[df_var] = current_df

            else:
                code_snippet = generate_polars_code_snippet(op, params, df_var="df")
                # Apply using exec (simpler for replay, less safe)
                local_vars = {'df': current_df, 'pl': pl, 'io': io}
                exec(code_snippet, local_vars)
                current_df = local_vars.get('df')
                if not isinstance(current_df, pl.DataFrame):
                     raise ValueError(f"Operation '{op}' at step {i+1} did not produce a Polars DataFrame.")
                loaded_dataframes[df_var] = current_df

            cumulative_code_lines.append(code_snippet)
            step["generated_code_snippet"] = code_snippet

        except Exception as e:
            raise ValueError(f"Error replaying polars step {i+1} ({op}): {e}\nCode: {code_snippet}")

    # Convert final DataFrame back to bytes
    try:
        with io.BytesIO() as buffer:
            current_df.write_csv(buffer)
            final_content = buffer.getvalue()
    except Exception as e:
        raise ValueError(f"Failed to serialize final polars DataFrame: {e}")

    cumulative_code = "\n".join(cumulative_code_lines)
    return final_content, cumulative_code

def _string_op_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    column = params.get("column")
    string_func = params.get("string_function") # 'upper', 'lower', 'strip', 'split', 'length'
    new_col_name = params.get("new_column_name", f"{column}_{string_func}")
    delimiter = params.get("delimiter")
    part_index = params.get("part_index") # 0-based for polars list slicing

    if not column or not string_func:
        raise ValueError("string_operation requires 'column' and 'string_function'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")

    code = f"# Apply string operation '{string_func}' to column '{column}'\n"
    # Ensure Utf8 and access str namespace. Cast needed for safety.
    target_col_expr_str = f"pl.col('{column}').cast(pl.Utf8).str"
    result_expr_str = ""
    pl_expr = None

    func_lower = string_func.lower()
    try:
        if func_lower == 'upper':
            result_expr_str = f"{target_col_expr_str}.to_uppercase()"
            pl_expr = pl.col(column).cast(pl.Utf8).str.to_uppercase()
        elif func_lower == 'lower':
            result_expr_str = f"{target_col_expr_str}.to_lowercase()"
            pl_expr = pl.col(column).cast(pl.Utf8).str.to_lowercase()
        elif func_lower == 'strip':
            result_expr_str = f"{target_col_expr_str}.strip_chars()" # Strips whitespace
            pl_expr = pl.col(column).cast(pl.Utf8).str.strip_chars()
        elif func_lower == 'length':
            result_expr_str = f"{target_col_expr_str}.len_bytes()" # Or len_chars() ? len_bytes is usually faster
            pl_expr = pl.col(column).cast(pl.Utf8).str.len_bytes() # Use len_bytes
        elif func_lower == 'split':
            if delimiter is None or part_index is None:
                raise ValueError("String split requires 'delimiter' and 'part_index' (0-based).")
            # .str.split() returns list, .list.get() accesses element
            result_expr_str = f"{target_col_expr_str}.split({repr(delimiter)}).list.get({int(part_index)})"
            pl_expr = pl.col(column).cast(pl.Utf8).str.split(delimiter).list.get(int(part_index))
        else:
            raise ValueError(f"Unsupported string_function for polars: {string_func}")

        code += f"df = df.with_columns({result_expr_str}.alias('{new_col_name}'))"
        result_df = df.with_columns(pl_expr.alias(new_col_name))

    except Exception as e: # Catch errors like index out of bounds for split
        raise ValueError(f"Error applying polars string function '{string_func}': {e}")

    return result_df, code

def _date_extract_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    column = params.get("column")
    part = params.get("part") # year, month, day, hour, minute, second, ordinal_day, weekday, week, quarter
    new_col_name = params.get("new_column_name", f"{column}_{part}")

    if not column or not part:
        raise ValueError("date_extract requires 'column' and 'part'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")

    # Map UI part names to polars dt methods/attributes
    part_map = {
        'year': 'year()', 'month': 'month()', 'day': 'day()',
        'hour': 'hour()', 'minute': 'minute()', 'second': 'second()',
        'millisecond': 'millisecond()', 'microsecond': 'microsecond()', 'nanosecond': 'nanosecond()',
        'weekday': 'weekday()', # Monday=1, Sunday=7
        'week': 'week()', # ISO week number
        'ordinal_day': 'ordinal_day()', # Day of year
        'quarter': 'quarter()',
        'iso_year': 'iso_year()', # ISO 8601 year
        # Add more as needed: 'timestamp', 'epoch', 'days', 'seconds', etc.
    }
    part_lower = part.lower()
    if part_lower not in part_map:
        raise ValueError(f"Invalid date part '{part}' for polars. Valid examples: {list(part_map.keys())}")

    pl_method_call = part_map[part_lower]

    code = f"# Extract '{part}' from date/time column '{column}'\n"
    # Access the .dt namespace and call the appropriate method
    target_expr_str = f"pl.col('{column}').dt.{pl_method_call}"
    code += f"df = df.with_columns({target_expr_str}.alias('{new_col_name}'))"

    try:
        # Ensure column is appropriate type before accessing .dt
        # Polars raises explicit error if type is wrong (e.g., accessing .dt on Int)
        # Extract the method name (e.g., 'year') from the call string ('year()')
        method_name = pl_method_call.replace('()', '')
        pl_expr = getattr(pl.col(column).dt, method_name)() # Dynamically call the method

        result_df = df.with_columns(pl_expr.alias(new_col_name))
    except AttributeError:
         # More specific error if .dt namespace isn't available
         raise ValueError(f"Failed to extract date part '{part}'. Column '{column}' is not a Date or Datetime type.")
    except Exception as e:
         raise ValueError(f"Failed to extract date part '{part}' from column '{column}'. Error: {e}")

    return result_df, code

def _create_column_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    new_col_name = params.get("new_column_name")
    expression_str = params.get("expression") # Polars expression as a string

    if not new_col_name or not expression_str:
        raise ValueError("create_column requires 'new_column_name' and 'expression' string.")

    code = f"# Create new column '{new_col_name}' using Polars expression\n"
    code += f"import polars as pl\n" # Ensure pl is available in snippet context
    # The expression string MUST be a valid Polars expression string
    # e.g., "pl.col('a') + pl.col('b')", "pl.lit(5)", "pl.when(pl.col('x') > 0).then(pl.lit('pos')).otherwise(pl.lit('neg'))"
    code += f"df = df.with_columns( ({expression_str}).alias('{new_col_name}') )"

    # !!! SECURITY WARNING: EVALUATING ARBITRARY STRING EXPRESSIONS IS RISKY !!!
    # This uses eval(), which can execute arbitrary code if not carefully controlled.
    # Use only in trusted environments or replace with a safer evaluation method.
    print(f"⚠️ SECURITY WARNING: Executing create_column_pl with eval() on expression: {expression_str}")
    local_env = {'pl': pl, 'cs': cs} # Provide polars and selectors in the eval context
    try:
        # Evaluate the expression string to get a Polars expression object
        polars_expr = eval(expression_str, {"__builtins__": {}}, local_env) # Restricted builtins

        # Check if the result is a valid Polars expression or literal
        if not isinstance(polars_expr, pl.Expr):
            # Allow literals directly? Wrap them in pl.lit() for safety
            if isinstance(polars_expr, (str, int, float, bool, list, tuple)): # Allow basic literals and sequences
                polars_expr = pl.lit(polars_expr)
            else:
                raise ValueError(f"Expression '{expression_str}' did not evaluate to a valid Polars expression or literal. Got type: {type(polars_expr)}")

        # Apply the expression using with_columns
        result_df = df.with_columns(polars_expr.alias(new_col_name))
    except SyntaxError as se:
        raise ValueError(f"Invalid syntax in expression: {expression_str}. Error: {se}")
    except Exception as e:
        # Catch errors during evaluation or application (e.g., column not found in expression)
        raise ValueError(f"Failed to evaluate or apply expression '{expression_str}': {e}")

    return result_df, code

def _window_function_pl(df: pl.DataFrame, params: Dict[str, Any]) -> Tuple[pl.DataFrame, str]:
    func = params.get("window_function") # e.g., 'rank', 'lead', 'lag', 'sum', 'avg', 'row_number'
    target_column = params.get("target_column") # Column to apply func on (e.g., for sum, lead, lag)
    order_by_columns = params.get("order_by_columns") # List of dicts: [{'column': 'c', 'descending': False}]
    partition_by_columns = params.get("partition_by_columns") # Optional list of column names
    new_col_name = params.get("new_column_name", f"{func}_window")
    # Function-specific params
    rank_method = params.get("rank_method", "average") # For rank: 'average', 'min', 'max', 'dense', 'ordinal'
    offset = params.get("offset", 1) # For lead/lag
    default_value = params.get("default_value") # Optional fill for lead/lag

    # --- Validation ---
    if not func: raise ValueError("Window function requires 'window_function' name.")
    # Some functions don't need target_column (rank, row_number)
    agg_funcs = ['sum', 'mean', 'avg', 'min', 'max', 'median', 'std', 'var', 'count', 'first', 'last']
    lead_lag = ['lead', 'lag']
    rank_funcs = ['rank', 'dense_rank', 'row_number'] # Polars uses rank(method=...)

    if func.lower() in agg_funcs + lead_lag and not target_column:
        raise ValueError(f"Window function '{func}' requires a 'target_column'.")
    # Order by is generally recommended for meaningful window results, but not strictly required by Polars syntax
    # if not order_by_columns: raise ValueError("Window function requires 'order_by_columns'.")

    cols_to_check = []
    if target_column: cols_to_check.append(target_column)
    if order_by_columns: cols_to_check.extend([spec['column'] for spec in order_by_columns])
    if partition_by_columns: cols_to_check.extend(partition_by_columns)
    missing = [col for col in cols_to_check if col not in df.columns]
    if missing: raise ValueError(f"Columns not found for window function: {', '.join(missing)}")

    # --- Build Expression ---
    code = f"# Apply window function '{func}'\n"
    pl_expr = None
    window_expr_str = ""

    try:
        # Base expression (the function itself)
        if func.lower() in rank_funcs:
            # Map UI rank names to Polars rank methods if needed
            polars_rank_method = rank_method
            if func.lower() == 'dense_rank': polars_rank_method = 'dense'
            elif func.lower() == 'row_number': polars_rank_method = 'ordinal'
            # Rank needs a column to rank *by*, usually the order_by column
            if not order_by_columns: raise ValueError("Rank functions require 'order_by_columns'.")
            rank_col = order_by_columns[0]['column'] # Rank by the first order_by column
            is_desc = order_by_columns[0].get('descending', False)
            window_expr_str = f"pl.col('{rank_col}').rank(method='{polars_rank_method}', descending={is_desc})"
            pl_expr = pl.col(rank_col).rank(method=polars_rank_method, descending=is_desc)
        elif func.lower() in lead_lag:
            fill_null_expr_str = f".fill_null({repr(default_value)})" if default_value is not None else ""
            window_expr_str = f"pl.col('{target_column}').{func.lower()}(n={int(offset)}){fill_null_expr_str}"
            pl_expr = getattr(pl.col(target_column), func.lower())(n=int(offset))
            if default_value is not None:
                pl_expr = pl_expr.fill_null(default_value)
        elif func.lower() in agg_funcs:
            # Map common names
            polars_agg_func = func.lower()
            if polars_agg_func == 'avg': polars_agg_func = 'mean'
            # Apply aggregate function over the window
            window_expr_str = f"pl.col('{target_column}').{polars_agg_func}()"
            pl_expr = getattr(pl.col(target_column), polars_agg_func)()
        else:
            raise ValueError(f"Unsupported window_function for polars: {func}.")

        # Add OVER clause (partitioning)
        if partition_by_columns:
            window_expr_str += f".over({repr(partition_by_columns)})"
            pl_expr = pl_expr.over(partition_by_columns)
        # Note: Polars applies window functions *after* grouping/partitioning.
        # Sorting is handled implicitly by some functions (like rank) or needs to be done *before* if needed globally (e.g., global lead/lag).
        # For partitioned windows, Polars calculates within each partition.

        # --- Sorting (Important!) ---
        # If order_by is specified, sort *before* applying the window function for consistent results, especially for rank/lead/lag.
        # If partitioning, sort within partitions? Polars handles this via `over`.
        # If global (no partition), sort the whole dataframe.
        if order_by_columns:
             sort_cols = [spec['column'] for spec in order_by_columns]
             sort_desc = [spec.get('descending', False) for spec in order_by_columns]
             code += f"# Ensure data is sorted for predictable window results\n"
             code += f"df = df.sort(by={repr(sort_cols)}, descending={repr(sort_desc)})\n"
             df = df.sort(by=sort_cols, descending=sort_desc) # Apply sort before with_columns

        # Final code snippet and execution
        code += f"df = df.with_columns({window_expr_str}.alias('{new_col_name}'))"
        result_df = df.with_columns(pl_expr.alias(new_col_name))

    except Exception as e:
        raise ValueError(f"Error applying polars window function '{func}': {e}")

    return result_df, code