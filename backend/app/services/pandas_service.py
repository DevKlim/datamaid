# backend/app/services/pandas_service.py
import pandas as pd
import io
import numpy as np
import traceback
import re # Import re for regex operations
from typing import Dict, Any, Tuple, List, Optional

def _is_numeric_col(df: pd.DataFrame, col_name: str) -> bool:
    if col_name not in df.columns:
        return False
    if pd.api.types.is_numeric_dtype(df[col_name]):
        return True
    if df[col_name].dtype == 'object':
        try:
            # Use infer_objects().to_numeric() for potentially mixed types
            pd.to_numeric(df[col_name].dropna().infer_objects(), errors='raise')
            return True
        except (ValueError, TypeError):
            return False
    return False

def apply_pandas_operation(df: pd.DataFrame, operation: str, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    """
    Applies a specified pandas operation to the DataFrame and returns the result
    along with the equivalent pandas code.
    """
    try:
        # --- Existing Operations ---
        if operation == "filter":
            return _filter_rows_pd(df, params)
        elif operation == "select_columns":
            return _select_columns_pd(df, params)
        elif operation == "sort":
            return _sort_values_pd(df, params)
        elif operation == "rename":
            return _rename_columns_pd(df, params)
        elif operation == "drop_columns":
            return _drop_columns_pd(df, params)
        elif operation == "groupby":
            return _group_by_pd(df, params)
        elif operation == "groupby_multi":
            return _group_by_multi_pd(df, params)
        elif operation == "groupby_multi_agg":
             return _group_by_multi_agg_pd(df, params)
        elif operation == "pivot_table":
            return _pivot_table_pd(df, params)
        elif operation == "melt":
            return _melt_pd(df, params)
        elif operation == "set_index":
            return _set_index_pd(df, params)
        elif operation == "reset_index":
            return _reset_index_pd(df, params)
        elif operation == "fillna":
            return _fillna_pd(df, params)
        elif operation == "dropna":
             return _dropna_pd(df, params)
        elif operation == "astype":
             return _astype_pd(df, params)
        elif operation == "string_operation":
             return _string_op_pd(df, params)
        elif operation == "date_extract":
             return _date_extract_pd(df, params)
        elif operation == "drop_duplicates":
             return _drop_duplicates_pd(df, params)
        elif operation == "create_column":
             return _create_column_pd(df, params)
        elif operation == "window_function":
             return _window_function_pd(df, params)
        elif operation == "sample":
             return _sample_pd(df, params)
        elif operation == "apply_lambda":
             return _apply_lambda_pd(df, params)
        elif operation == "shuffle": return _shuffle_pd(df, params)
        elif operation.startswith("regex_"): # e.g., regex_filter, regex_extract, regex_replace
             return apply_pandas_regex(df, operation.split('_', 1)[1], params)
        elif operation == "string_operation": return _string_op_pd(df, params)
        elif operation == "date_extract": return _date_extract_pd(df, params)
        elif operation == "create_column": return _create_column_pd(df, params) # Uses eval! Risky.
        elif operation == "window_function": return _window_function_pd(df, params)

        else:
            raise ValueError(f"Unsupported pandas operation: {operation}")
    except Exception as e:
        print(f"Error executing pandas operation '{operation}': {type(e).__name__}: {e}")
        raise e # Re-raise for main.py handler


def _filter_rows_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    column = params.get("column")
    operator = params.get("operator")
    value = params.get("value")

    if not all([column, operator]):
        raise ValueError("Column, operator, and value are required for filter operation")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")

    # Attempt type conversion for comparison operators
    original_value = value
    is_numeric_target = False
    try:
        # Try to determine if the target column is numeric-like *before* filtering
        if pd.api.types.is_numeric_dtype(df[column]):
             is_numeric_target = True
        # Check object type more carefully
        elif df[column].dtype == 'object' and df[column].dropna().iloc[0] is not None:
             # Attempt conversion on first non-null value
             pd.to_numeric(df[column].dropna().iloc[0])
             is_numeric_target = True # Looks numeric
    except (ValueError, TypeError, IndexError, KeyError):
         is_numeric_target = False # Treat as string if check fails or empty

    try:
        if operator in ['==', '!=', '>', '<', '>=', '<='] and is_numeric_target:
             if isinstance(value, str): # Only convert if input is string
                 if '.' in value or 'e' in value.lower(): value = float(value)
                 else: value = int(value)
             elif isinstance(value, (int, float)):
                 pass # Already numeric
             else:
                 value = str(value) # Fallback to string comparison if conversion ambiguous
    except (ValueError, TypeError):
        # Keep as string if explicit conversion fails
        value = str(original_value)

    code_value = repr(value) # Use repr for code generation

    condition_str = ""
    result_df = None
    col_expr = f"df['{column}']"
    # Handle potential type mismatches more robustly
    try:
        if operator == "==":
            condition_str = f"{col_expr} == {code_value}"
            result_df = df[df[column] == value]
        elif operator == "!=":
            condition_str = f"{col_expr} != {code_value}"
            result_df = df[df[column] != value]
        # For comparisons, ensure types match or handle error
        elif operator in ['>', '<', '>=', '<=']:
             if is_numeric_target and isinstance(value, (int, float)):
                 # Convert column to numeric if needed, coercing errors
                 numeric_col = pd.to_numeric(df[column], errors='coerce')
                 if operator == ">": condition_str, result_df = f"pd.to_numeric({col_expr}, errors='coerce') > {code_value}", df[numeric_col > value]
                 elif operator == "<": condition_str, result_df = f"pd.to_numeric({col_expr}, errors='coerce') < {code_value}", df[numeric_col < value]
                 elif operator == ">=": condition_str, result_df = f"pd.to_numeric({col_expr}, errors='coerce') >= {code_value}", df[numeric_col >= value]
                 elif operator == "<=": condition_str, result_df = f"pd.to_numeric({col_expr}, errors='coerce') <= {code_value}", df[numeric_col <= value]
             else: # Treat as string comparison if types incompatible
                 str_col = df[column].astype(str)
                 str_val = str(value)
                 if operator == ">": condition_str, result_df = f"{col_expr}.astype(str) > {repr(str_val)}", df[str_col > str_val]
                 elif operator == "<": condition_str, result_df = f"{col_expr}.astype(str) < {repr(str_val)}", df[str_col < str_val]
                 elif operator == ">=": condition_str, result_df = f"{col_expr}.astype(str) >= {repr(str_val)}", df[str_col >= str_val]
                 elif operator == "<=": condition_str, result_df = f"{col_expr}.astype(str) <= {repr(str_val)}", df[str_col <= str_val]

        # String operations
        elif operator == "contains":
            value_str = str(original_value)
            condition_str = f"{col_expr}.astype(str).str.contains({repr(value_str)}, na=False)"
            result_df = df[df[column].astype(str).str.contains(value_str, na=False)]
        elif operator == "startswith":
            value_str = str(original_value)
            condition_str = f"{col_expr}.astype(str).str.startswith({repr(value_str)}, na=False)"
            result_df = df[df[column].astype(str).str.startswith(value_str, na=False)]
        elif operator == "endswith":
            value_str = str(original_value)
            condition_str = f"{col_expr}.astype(str).str.endswith({repr(value_str)}, na=False)"
            result_df = df[df[column].astype(str).str.endswith(value_str, na=False)]
        # Note: 'regex' filter handled by apply_pandas_regex now
        else:
            raise ValueError(f"Unsupported filter operator: {operator}")

    except TypeError as te: # Catch type errors during comparison
         raise TypeError(f"Cannot compare column '{column}' with value '{original_value}' using operator '{operator}'. Check data types. Error: {te}")

    code = f"# Filter rows where {column} {operator} {original_value}\ndf = df[{condition_str}]"
    return result_df, code

def _select_columns_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    selected_columns = params.get("selected_columns", [])
    if not selected_columns:
        raise ValueError("No columns selected for 'select_columns' operation")

    missing = [col for col in selected_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {', '.join(missing)}")

    code = f"# Select specific columns\ndf = df[{repr(selected_columns)}]"
    result_df = df[selected_columns]
    return result_df, code

def _sort_values_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    sort_column = params.get("sort_column")
    sort_order = params.get("sort_order", "ascending") # Default to ascending
    if not sort_column:
        raise ValueError("Sort column parameter is required")
    if sort_column not in df.columns:
        raise ValueError(f"Sort column '{sort_column}' not found")

    ascending = sort_order == "ascending"
    code = f"# Sort DataFrame by {sort_column} in {'ascending' if ascending else 'descending'} order\n"
    code += f"df = df.sort_values('{sort_column}', ascending={ascending})"
    result_df = df.sort_values(sort_column, ascending=ascending)
    return result_df, code

def _rename_columns_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    renames = params.get("renames", []) # Expecting list of {"old_name": "x", "new_name": "y"}
    if not renames:
        raise ValueError("No rename mappings provided")

    rename_dict = {item['old_name']: item['new_name'] for item in renames if item.get('old_name') and item.get('new_name')}
    if not rename_dict:
         raise ValueError("Invalid rename parameters. Need list of {'old_name': '...', 'new_name': '...'}")

    missing = [old for old in rename_dict if old not in df.columns]
    if missing:
        raise ValueError(f"Columns to rename not found: {', '.join(missing)}")

    code = f"# Rename columns\nrename_map = {repr(rename_dict)}\ndf = df.rename(columns=rename_map)"
    result_df = df.rename(columns=rename_dict)
    return result_df, code

def _drop_columns_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    drop_columns = params.get("drop_columns", [])
    if not drop_columns:
        raise ValueError("No columns specified for dropping")

    missing = [col for col in drop_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Columns to drop not found: {', '.join(missing)}")

    code = f"# Drop specified columns\ndf = df.drop(columns={repr(drop_columns)})"
    result_df = df.drop(columns=drop_columns)
    return result_df, code

def _group_by_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    group_column = params.get("group_column")
    agg_column = params.get("agg_column")
    # Make sure agg_function has a default if not provided by UI yet
    agg_function = params.get("agg_function", "count") # Default to 'count' as it works on all types

    if not all([group_column, agg_column, agg_function]):
        raise ValueError("Group column, aggregation column, and function are required")
    if group_column not in df.columns:
         raise KeyError(f"Group column '{group_column}' not found") # Use KeyError
    if agg_column not in df.columns:
         raise KeyError(f"Aggregation column '{agg_column}' not found") # Use KeyError

    valid_funcs = ['mean', 'sum', 'count', 'min', 'max', 'median', 'std', 'var', 'first', 'last', 'nunique']
    if agg_function not in valid_funcs:
         raise ValueError(f"Unsupported aggregation function: {agg_function}")

    # --- Add Validation ---
    numeric_only_funcs = ['mean', 'median', 'std', 'var', 'sum']
    is_numeric = _is_numeric_col(df, agg_column)

    if agg_function in numeric_only_funcs and not is_numeric:
        raise ValueError(f"Aggregation function '{agg_function}' requires a numeric column, but '{agg_column}' is not numeric.")
    # --- End Validation ---

    code = f"# Group by {group_column} and aggregate {agg_column} using {agg_function}\n"
    # Use agg syntax for consistency, even for single aggregation
    agg_spec = {agg_column: agg_function}
    code += f"df = df.groupby('{group_column}').agg({repr(agg_spec)}).reset_index()"

    # Using .agg() handles potential errors more gracefully sometimes
    result_df = df.groupby(group_column).agg(agg_spec).reset_index()
    # Pandas might create multi-index columns if agg_column was already the index name, handle it
    if isinstance(result_df.columns, pd.MultiIndex):
         result_df.columns = ['_'.join(col).strip('_') for col in result_df.columns.values]

    return result_df, code

def _group_by_multi_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    group_columns = params.get("group_columns") # Expecting a list
    agg_column = params.get("agg_column")
    agg_function = params.get("agg_function", "count") # Default

    if not all([group_columns, agg_column, agg_function]):
        raise ValueError("Group columns (list), aggregation column, and function are required")
    if not isinstance(group_columns, list) or len(group_columns) == 0:
         raise ValueError("group_columns must be a non-empty list")

    missing_group = [col for col in group_columns if col not in df.columns]
    if missing_group:
         raise KeyError(f"Group columns not found: {', '.join(missing_group)}") # Use KeyError
    if agg_column not in df.columns:
         raise KeyError(f"Aggregation column '{agg_column}' not found") # Use KeyError

    valid_funcs = ['mean', 'sum', 'count', 'min', 'max', 'median', 'std', 'var', 'first', 'last', 'nunique']
    if agg_function not in valid_funcs:
         raise ValueError(f"Unsupported aggregation function: {agg_function}")

    # --- Add Validation ---
    numeric_only_funcs = ['mean', 'median', 'std', 'var', 'sum']
    is_numeric = _is_numeric_col(df, agg_column)
    if agg_function in numeric_only_funcs and not is_numeric:
        raise ValueError(f"Aggregation function '{agg_function}' requires a numeric column, but '{agg_column}' is not numeric.")
    # --- End Validation ---

    code = f"# Group by {group_columns} and aggregate {agg_column} using {agg_function}\n"
    agg_spec = {agg_column: agg_function}
    code += f"df = df.groupby({repr(group_columns)}).agg({repr(agg_spec)}).reset_index()"

    result_df = df.groupby(group_columns).agg(agg_spec).reset_index()
    if isinstance(result_df.columns, pd.MultiIndex):
        result_df.columns = ['_'.join(col).strip('_') for col in result_df.columns.values]

    return result_df, code

def _group_by_multi_agg_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    group_columns = params.get("group_columns") # Can be string or list
    aggregations = params.get("aggregations") # Expecting [{'column': 'col1', 'function': 'mean'}, ...]

    if not group_columns or not aggregations:
        raise ValueError("Group column(s) and aggregations list are required")

    if isinstance(group_columns, str):
        group_columns = [group_columns] # Ensure it's a list

    missing_group = [col for col in group_columns if col not in df.columns]
    if missing_group:
        raise KeyError(f"Group columns not found: {', '.join(missing_group)}") # Use KeyError

    agg_dict = {}
    valid_funcs = ['mean', 'sum', 'count', 'min', 'max', 'median', 'std', 'var', 'first', 'last', 'nunique']
    numeric_only_funcs = ['mean', 'median', 'std', 'var', 'sum']

    for agg_spec in aggregations:
        col = agg_spec.get("column")
        func = agg_spec.get("function")
        if not col or not func:
            raise ValueError(f"Invalid aggregation spec: {agg_spec}. Need 'column' and 'function'.")
        if col not in df.columns:
            raise KeyError(f"Aggregation column '{col}' not found") # Use KeyError
        if func not in valid_funcs:
            raise ValueError(f"Unsupported aggregation function: {func}")

        # --- Add Validation ---
        is_numeric = _is_numeric_col(df, col)
        if func in numeric_only_funcs and not is_numeric:
             raise ValueError(f"Aggregation function '{func}' requires a numeric column, but '{col}' is not numeric.")
        # --- End Validation ---

        if col not in agg_dict:
            agg_dict[col] = []
        # Allow multiple aggregations on the same column (e.g., mean and count)
        if func not in agg_dict[col]: # Avoid duplicate functions for same column in spec
            agg_dict[col].append(func)


    # Check if agg_dict is empty after validation and deduplication
    if not agg_dict:
        raise ValueError("No valid aggregations provided or remaining after validation.")

    code = f"# Group by {group_columns} with multiple aggregations\n"
    code += f"agg_spec = {repr(agg_dict)}\n"
    code += f"df = df.groupby({repr(group_columns)}).agg(agg_spec).reset_index()\n"
    code += "# Note: Column names might be multi-level depending on pandas version.\n"
    code += "# Flattening columns if necessary:\n"
    code += "if isinstance(df.columns, pd.MultiIndex):\n"
    code += "    df.columns = ['_'.join(col).strip('_') for col in df.columns.values]"


    result_df = df.groupby(group_columns).agg(agg_dict)
    # Flatten MultiIndex columns if pandas creates them
    if isinstance(result_df.columns, pd.MultiIndex):
        result_df.columns = ['_'.join(col).strip('_') for col in result_df.columns.values]

    result_df = result_df.reset_index()

    return result_df, code

def _pivot_table_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    index_col = params.get("index_col") # Can be list or string
    columns_col = params.get("columns_col") # Can be list or string
    values_col = params.get("values_col") # Can be list or string
    aggfunc = params.get("pivot_agg_function", "mean")

    if not all([index_col, columns_col, values_col]):
        raise ValueError("Index, columns, and values are required for pivot_table")

    # Basic validation
    required_cols = []
    if isinstance(index_col, list): required_cols.extend(index_col)
    else: required_cols.append(index_col)
    if isinstance(columns_col, list): required_cols.extend(columns_col)
    else: required_cols.append(columns_col)
    if isinstance(values_col, list): required_cols.extend(values_col)
    else: required_cols.append(values_col)

    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found for pivot: {', '.join(missing)}")

    code = f"# Create pivot table\n"
    code += f"df = pd.pivot_table(df,\n"
    code += f"                    index={repr(index_col)},\n"
    code += f"                    columns={repr(columns_col)},\n"
    code += f"                    values={repr(values_col)},\n"
    code += f"                    aggfunc='{aggfunc}')\n"
    code += "# Reset index if you want index columns back as regular columns\n"
    code += "# df = df.reset_index()"

    result_df = pd.pivot_table(df, index=index_col, columns=columns_col, values=values_col, aggfunc=aggfunc)
    # Resetting index is common after pivot, but keep it as index for now.
    # result_df = result_df.reset_index()

    return result_df, code

def _melt_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    id_vars = params.get("id_vars") # List of columns to keep
    value_vars = params.get("value_vars") # List of columns to melt
    var_name = params.get("var_name", "variable")
    value_name = params.get("value_name", "value")

    if id_vars is None or value_vars is None: # Allow empty lists if intended
         raise ValueError("id_vars and value_vars lists are required for melt")

    required_cols = id_vars + value_vars
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Columns not found for melt: {', '.join(missing)}")

    code = f"# Melt DataFrame (wide to long)\n"
    code += f"df = pd.melt(df,\n"
    code += f"             id_vars={repr(id_vars)},\n"
    code += f"             value_vars={repr(value_vars)},\n"
    code += f"             var_name='{var_name}',\n"
    code += f"             value_name='{value_name}')"

    result_df = pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name)
    return result_df, code

def _set_index_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    index_column = params.get("index_column")
    drop = params.get("drop", True) # Default to dropping the column after setting as index

    if not index_column:
        raise ValueError("Index column parameter is required")
    if index_column not in df.columns:
        raise ValueError(f"Index column '{index_column}' not found")

    code = f"# Set '{index_column}' as the DataFrame index\n"
    code += f"df = df.set_index('{index_column}', drop={drop})"
    result_df = df.set_index(index_column, drop=drop)
    return result_df, code

def _reset_index_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    drop = params.get("drop_index", False) # Default to keeping the index as a column

    code = f"# Reset the DataFrame index\n"
    code += f"df = df.reset_index(drop={drop})"
    result_df = df.reset_index(drop=drop)
    return result_df, code

# Note: Merge/Join needs handling in the main API endpoint to load the second DataFrame
def apply_pandas_merge(left_df: pd.DataFrame, right_df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    """
    Applies a pandas merge operation between two DataFrames.
    """
    # ... (keep existing implementation) ...
    how = params.get("join_type", "inner")
    left_on = params.get("left_on")
    right_on = params.get("right_on") # Assumes joining on columns with potentially different names
    indicator = params.get("indicator", False) # Add merge indicator?

    if not left_on or not right_on:
         raise ValueError("Both left_on and right_on key columns are required for merge")
    if left_on not in left_df.columns: raise ValueError(f"Left key column '{left_on}' not found in left dataset")
    if right_on not in right_df.columns: raise ValueError(f"Right key column '{right_on}' not found in right dataset")

    valid_joins = ['left', 'right', 'outer', 'inner', 'cross']
    if how not in valid_joins:
        raise ValueError(f"Invalid join type: {how}. Must be one of {valid_joins}")

    merge_args = {
        "how": how,
        "left_on": left_on,
        "right_on": right_on,
        "indicator": indicator,
        # Add suffixes handling if needed later: suffixes=('_left', '_right')
    }

    code_args_str = f',\n                   '.join([f"{k}={repr(v)}" for k, v in merge_args.items()])
    code = f"# Merge DataFrames\n"
    code += f"df = pd.merge(df_left,\n" # Assuming df names in context
    code += f"                   df_right,\n"
    code += f"                   {code_args_str})"


    result_df = pd.merge(left_df, right_df, **merge_args)
    return result_df, code

# --- Implementations for NEW Pandas Operations ---

def _fillna_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    columns = params.get("columns") # Optional: if None, fill all specified, else fill selection
    value = params.get("value")
    method = params.get("method") # 'ffill', 'bfill'

    if value is None and method is None:
        raise ValueError("Either 'value' or 'method' must be provided for fillna")
    if value is not None and method is not None:
        raise ValueError("Provide only 'value' or 'method', not both")

    fill_args = {}
    if value is not None: fill_args['value'] = value
    if method is not None: fill_args['method'] = method

    code_args = ', '.join(f'{k}={repr(v)}' for k, v in fill_args.items())
    result_df = df.copy() # Work on a copy

    if columns:
        if not isinstance(columns, list): columns = [columns]
        missing = [col for col in columns if col not in df.columns]
        if missing: raise ValueError(f"Columns to fill not found: {', '.join(missing)}")

        code = f"# Fill missing values in columns {columns}\n"
        # Fill specific columns - need to assign back
        for col in columns:
            # Add type check? If filling numeric with string, pandas might change dtype
            result_df[col] = result_df[col].fillna(**fill_args)
        code += f"df[{repr(columns)}] = df[{repr(columns)}].fillna({code_args})" # Simplified code representation
    else:
        code = f"# Fill missing values in entire DataFrame\n"
        code += f"df = df.fillna({code_args})"
        result_df = result_df.fillna(**fill_args) # Apply to whole DataFrame copy

    return result_df, code

def _dropna_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    subset = params.get("subset") # Optional list of columns
    how = params.get("how", "any") # 'any' or 'all'
    thresh = params.get("thresh") # int, min non-NA values to keep row

    if subset and not isinstance(subset, list):
        raise ValueError("'subset' parameter for dropna must be a list.")
    if subset:
        missing = [col for col in subset if col not in df.columns]
        if missing: raise ValueError(f"Columns in dropna subset not found: {', '.join(missing)}")

    dropna_args = {"how": how}
    if subset: dropna_args["subset"] = subset
    if thresh is not None: dropna_args["thresh"] = int(thresh) # Ensure int

    code_args = ', '.join(f'{k}={repr(v)}' for k, v in dropna_args.items())
    code = f"# Drop rows with missing values\ndf = df.dropna({code_args})"
    result_df = df.dropna(**dropna_args)
    return result_df, code

def _astype_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    column = params.get("column")
    new_type = params.get("new_type")
    # Map common names to pandas dtypes if needed
    dtype_map = {
        "integer": "int64", "int": "int64",
        "float": "float64", "double": "float64", "numeric": "float64",
        "string": "object", "text": "object", "varchar": "object",
        "boolean": "bool",
        "datetime": "datetime64[ns]", "timestamp": "datetime64[ns]",
        "date": "datetime64[ns]", # Convert to datetime first, then maybe .dt.date
    }
    pandas_dtype = dtype_map.get(new_type.lower(), new_type) # Use provided if not in map

    if not column or not new_type:
        raise ValueError("astype requires 'column' and 'new_type' parameters.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found for astype.")

    # Special handling for date conversion
    is_date_only = new_type.lower() == "date"
    if is_date_only:
        pandas_dtype = "datetime64[ns]" # Convert to datetime first

    code = f"# Convert column '{column}' type to {pandas_dtype}\n"
    result_df = df.copy()
    try:
        result_df[column] = result_df[column].astype(pandas_dtype)
        code += f"df['{column}'] = df['{column}'].astype('{pandas_dtype}')"
        # Apply .dt.date if original request was just for date part
        if is_date_only:
            result_df[column] = result_df[column].dt.date
            code += "\n# Extract only the date part\ndf['{column}'] = df['{column}'].dt.date"

    except Exception as e: # Catch potential conversion errors
        raise ValueError(f"Failed to convert column '{column}' to type '{pandas_dtype}': {e}")

    return result_df, code

def _string_op_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    column = params.get("column")
    string_func = params.get("string_function") # 'upper', 'lower', 'strip', 'split'
    new_col_name = params.get("new_column_name", f"{column}_{string_func}")
    # Split specific params
    delimiter = params.get("delimiter")
    part_index = params.get("part_index") # 0-based for pandas str.split().str.get()

    if not column or not string_func:
        raise ValueError("string_operation requires 'column' and 'string_function'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")

    result_df = df.copy()
    code = f"# Apply string operation '{string_func}' to column '{column}'\n"
    target_col_expr = f"df['{column}'].astype(str).str" # Ensure string access
    result_expr = ""

    func_lower = string_func.lower()
    if func_lower == 'upper':
        result_expr = f"{target_col_expr}.upper()"
    elif func_lower == 'lower':
        result_expr = f"{target_col_expr}.lower()"
    elif func_lower == 'strip': # Removes leading/trailing whitespace
        result_expr = f"{target_col_expr}.strip()"
    elif func_lower == 'split':
        if delimiter is None or part_index is None:
             raise ValueError("String split requires 'delimiter' and 'part_index' (0-based).")
        # .str.split() returns list, .str.get() accesses element by index
        result_expr = f"{target_col_expr}.split({repr(delimiter)}, expand=False).str.get({int(part_index)})"
    else:
        raise ValueError(f"Unsupported string_function for pandas: {string_func}")

    code += f"df['{new_col_name}'] = {result_expr}"
    # Execute the operation
    try:
        if func_lower == 'split':
             result_df[new_col_name] = result_df[column].astype(str).str.split(delimiter, expand=False).str.get(int(part_index))
        elif func_lower == 'upper':
             result_df[new_col_name] = result_df[column].astype(str).str.upper()
        elif func_lower == 'lower':
             result_df[new_col_name] = result_df[column].astype(str).str.lower()
        elif func_lower == 'strip':
             result_df[new_col_name] = result_df[column].astype(str).str.strip()

    except Exception as e:
        raise ValueError(f"Error applying string function '{string_func}': {e}")

    return result_df, code


def _date_extract_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    column = params.get("column")
    part = params.get("part") # 'year', 'month', 'day', 'hour', 'minute', 'second', 'dayofweek', 'dayofyear', 'weekofyear'
    new_col_name = params.get("new_column_name", f"{column}_{part}")

    if not column or not part:
        raise ValueError("date_extract requires 'column' and 'part'.")
    if column not in df.columns:
         raise ValueError(f"Column '{column}' not found.")

    # Map parts to pandas dt properties
    part_map = {
        'year': 'year', 'month': 'month', 'day': 'day',
        'hour': 'hour', 'minute': 'minute', 'second': 'second',
        'dow': 'dayofweek', # Monday=0, Sunday=6
        'dayofweek': 'dayofweek',
        'doy': 'dayofyear',
        'dayofyear': 'dayofyear',
        'week': 'isocalendar().week', # ISO week number
        'weekofyear': 'isocalendar().week',
        'quarter': 'quarter',
    }
    part_lower = part.lower()
    if part_lower not in part_map:
        raise ValueError(f"Invalid date part '{part}'. Valid: {list(part_map.keys())}")

    pd_part = part_map[part_lower]

    result_df = df.copy()
    code = f"# Extract '{part}' from date/time column '{column}'\n"
    code += f"# Ensure column is datetime type first\n"
    code += f"df['{column}'] = pd.to_datetime(df['{column}'], errors='coerce')\n"
    code += f"df['{new_col_name}'] = df['{column}'].dt.{pd_part}"

    try:
        # Ensure column is datetime
        datetime_col = pd.to_datetime(result_df[column], errors='coerce')
        if pd_part == 'isocalendar().week':
            # Handle multi-column output of isocalendar if needed, just take week
             result_df[new_col_name] = datetime_col.dt.isocalendar().week
        else:
             result_df[new_col_name] = getattr(datetime_col.dt, pd_part)

    except AttributeError:
         raise ValueError(f"Could not access datetime property '{pd_part}'. Is column '{column}' a date/time type?")
    except Exception as e:
         raise ValueError(f"Error extracting date part '{part}': {e}")

    return result_df, code

def _drop_duplicates_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    subset = params.get("subset") # Optional list
    keep = params.get("keep", "first") # 'first', 'last', False

    if subset and not isinstance(subset, list):
        raise ValueError("'subset' must be a list.")
    if subset:
        missing = [col for col in subset if col not in df.columns]
        if missing: raise ValueError(f"Columns in drop_duplicates subset not found: {', '.join(missing)}")

    drop_args = {"keep": keep}
    if subset: drop_args["subset"] = subset

    code_args = ', '.join(f'{k}={repr(v)}' for k, v in drop_args.items())
    code = f"# Drop duplicate rows\ndf = df.drop_duplicates({code_args})"
    result_df = df.drop_duplicates(**drop_args)
    return result_df, code

def _create_column_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    new_col_name = params.get("new_column_name")
    expression = params.get("expression") # Pandas eval compatible string or assignment value

    if not new_col_name or expression is None: # Allow expression to be 0, False etc.
        raise ValueError("create_column requires 'new_column_name' and 'expression'.")

    result_df = df.copy()
    code = f"# Create new column '{new_col_name}'\n"

    # Try using df.eval for expressions involving existing columns
    # This is safer than raw exec but limited.
    try:
        # Check if expression refers to existing columns (simple check)
        if any(col in expression for col in df.columns):
             result_df[new_col_name] = result_df.eval(expression)
             code += f"df['{new_col_name}'] = df.eval('{expression}')"
        else: # Treat as a literal assignment
             result_df[new_col_name] = expression
             code += f"df['{new_col_name}'] = {repr(expression)}"
    except Exception as e:
         # Fallback or error - maybe allow simple assignments like df['new'] = df['old'] * 2?
         # This requires more complex parsing or using assign.
         # Let's stick to eval for column-based expressions and literals for now.
         # Alternative: Use assign
         # try:
         #     result_df = df.assign(**{new_col_name: lambda x: x.eval(expression)})
         #     code += f"df = df.assign({new_col_name}=lambda x: x.eval('{expression}'))"
         # except Exception as assign_e:
         #     raise ValueError(f"Failed to evaluate expression '{expression}': {e} / {assign_e}")
         raise ValueError(f"Failed to evaluate expression '{expression}' using df.eval or assign literal: {e}")


    return result_df, code


def _window_function_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    func = params.get("window_function") # 'rank', 'lead', 'lag'
    target_column = params.get("target_column") # For lead/lag
    order_by_column = params.get("order_by_column") # Required for ordering within window
    order_ascending = params.get("order_ascending", True) # Order for sort before window
    partition_by_columns = params.get("partition_by_columns") # Optional list
    new_col_name = params.get("new_column_name", f"{func}_{target_column or order_by_column}")
    # Rank specific
    rank_method = params.get("rank_method", "average") # 'average', 'min', 'max', 'first', 'dense' for rank()
    # Lead/Lag specific
    offset = params.get("offset", 1)

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

    result_df = df.copy()
    # Sort is often needed before applying simple shift-based lead/lag if not using groupby().shift()
    # For rank/lead/lag within groups, groupby is the way.
    code = f"# Apply window function '{func}'\n"
    group_expr = f"df.groupby({repr(partition_by_columns)})" if partition_by_columns else "df"
    target_expr = ""

    try:
        if func_lower == 'rank':
            # Pandas rank is slightly different from SQL RANK(). It ranks values within a Series.
            # Apply rank on the order_by column within partitions.
            if partition_by_columns:
                 target_expr = f"{group_expr}['{order_by_column}'].rank(method='{rank_method}', ascending={order_ascending})"
                 code += f"df['{new_col_name}'] = {target_expr}\n"
                 result_df[new_col_name] = result_df.groupby(partition_by_columns)[order_by_column].rank(method=rank_method, ascending=order_ascending)
            else:
                 target_expr = f"df['{order_by_column}'].rank(method='{rank_method}', ascending={order_ascending})"
                 code += f"df['{new_col_name}'] = {target_expr}\n"
                 result_df[new_col_name] = result_df[order_by_column].rank(method=rank_method, ascending=order_ascending)

        elif func_lower in ['lead', 'lag']:
            shift_periods = int(offset) if func_lower == 'lead' else -int(offset)
            if partition_by_columns:
                # Use groupby().shift()
                target_expr = f"{group_expr}['{target_column}'].shift(periods={shift_periods})"
                code += f"df['{new_col_name}'] = {target_expr}\n"
                result_df[new_col_name] = result_df.groupby(partition_by_columns)[target_column].shift(periods=shift_periods)
            else:
                # Simple Series.shift() - requires pre-sorting for meaningful result
                code += f"# Ensure data is sorted by '{order_by_column}' first for meaningful lead/lag without group\n"
                code += f"df = df.sort_values('{order_by_column}', ascending={order_ascending})\n"
                target_expr = f"df['{target_column}'].shift(periods={shift_periods})"
                code += f"df['{new_col_name}'] = {target_expr}\n"
                result_df = result_df.sort_values(order_by_column, ascending=order_ascending) # Sort first
                result_df[new_col_name] = result_df[target_column].shift(periods=shift_periods) # Then shift
        else:
            raise ValueError(f"Unsupported window_function for pandas: {func}. Try 'rank', 'lead', 'lag'.")

    except Exception as e:
        raise ValueError(f"Error applying window function '{func}': {e}")

    return result_df, code


def _sample_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    n = params.get("n") # Number of rows
    frac = params.get("frac") # Fraction of rows
    replace = params.get("replace", False) # Sample with replacement
    random_state = params.get("seed") # Optional seed (named 'seed' from UI)

    if n is None and frac is None:
        raise ValueError("Sample requires either 'n' or 'frac'.")
    if n is not None and frac is not None:
         raise ValueError("Provide either 'n' or 'frac', not both.")

    sample_args = {"replace": replace}
    if n is not None: sample_args["n"] = int(n)
    if frac is not None: sample_args["frac"] = float(frac)
    if random_state is not None: sample_args["random_state"] = int(random_state)

    code_args = ', '.join(f'{k}={repr(v)}' for k, v in sample_args.items())
    code = f"# Sample rows from DataFrame\ndf = df.sample({code_args})"
    try:
        result_df = df.sample(**sample_args)
    except ValueError as e:
         # Catch errors like n > population size when replace=False
         raise ValueError(f"Error during sampling: {e}")

    return result_df, code

# --- NEW SHUFFLE IMPLEMENTATION ---
def _shuffle_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    """Shuffles all rows of the DataFrame."""
    # Params might include seed in the future
    random_state = params.get("seed") # Optional seed for reproducibility

    sample_args = {"frac": 1} # Sample 100% of rows
    if random_state is not None:
        sample_args["random_state"] = int(random_state)
        code = f"# Shuffle all rows with seed {random_state}\ndf = df.sample(frac=1, random_state={random_state}).reset_index(drop=True)"
    else:
        code = f"# Shuffle all rows randomly\ndf = df.sample(frac=1).reset_index(drop=True)"

    try:
        # Shuffling is sampling frac=1, then reset index to avoid old index
        result_df = df.sample(**sample_args).reset_index(drop=True)
    except Exception as e:
        raise ValueError(f"Error during shuffling: {e}")

    return result_df, code

# --- Regex Operations ---
def apply_pandas_regex(df: pd.DataFrame, operation: str, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    """Handles regex operations: filter, extract, replace."""
    column = params.get("column")
    regex = params.get("regex")
    flags = 0
    if not params.get("case_sensitive", True):
        flags |= re.IGNORECASE

    if not column or regex is None:
        raise ValueError("Regex operations require 'column' and 'regex'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found for regex operation.")

    result_df = df
    code = f"# Regex operation '{operation}' on column '{column}'\n"
    code += f"import re\n"
    code_flags = f", flags=re.IGNORECASE" if not params.get("case_sensitive", True) else ""

    try:
        # Ensure target column is string type for regex ops
        string_series = df[column].astype(str)

        if operation == "filter":
            code += f"df = df[df['{column}'].astype(str).str.contains({repr(regex)}, regex=True, na=False{code_flags})]"
            result_df = df[string_series.str.contains(regex, regex=True, na=False, flags=flags)]
        elif operation == "extract":
            new_column = params.get("new_column", f"{column}_extracted")
            # Extract first match (group 0)
            code += f"df['{new_column}'] = df['{column}'].astype(str).str.extract(f'({regex})', expand=False{code_flags})"
            result_df = df.copy() # Modify copy
            result_df[new_column] = string_series.str.extract(f'({regex})', expand=False, flags=flags)
        elif operation == "extract_group":
            new_column = params.get("new_column", f"{column}_group_{params.get('group', 1)}")
            group = params.get("group", 1) # Default to group 1
            # Construct regex to capture the group correctly within extract
            # Note: str.extract requires the *entire* pattern, group is implicitly returned
            code += f"# Ensure regex pattern captures the desired group {group}\n"
            code += f"df['{new_column}'] = df['{column}'].astype(str).str.extract({repr(regex)}, expand=False{code_flags})" # Note: extract returns the specified group directly if regex uses capture groups
            # Extract might need adjustment based on how group capture works with the pattern
            result_df = df.copy()
            # Use pandas str.extract which correctly extracts specified groups
            # If regex has one group, it returns Series. If multiple, DataFrame.
            extracted = string_series.str.extract(regex, expand=True, flags=flags)

            if isinstance(extracted, pd.DataFrame):
                if int(group) - 1 < extracted.shape[1]: # Check if group index is valid (0-based for iloc)
                     result_df[new_column] = extracted.iloc[:, int(group)-1]
                else:
                     raise ValueError(f"Group index {group} out of range for extracted groups.")
            elif isinstance(extracted, pd.Series): # Single group captured or only one group in regex
                 if int(group) == 1: # Only group 1 is possible if Series is returned
                     result_df[new_column] = extracted
                 else:
                     raise ValueError(f"Requested group {group}, but regex only captured one group.")
            else: # No match or other issue
                 result_df[new_column] = None # Or should this raise error? Assigning None for no match.

        elif operation == "replace":
            replacement = params.get("replacement", "")
            new_column = params.get("new_column") # Optional: if provided, create new col
            # regex_compiled = re.compile(regex, flags=flags) # Compile for efficiency? Optional.

            if new_column:
                code += f"df['{new_column}'] = df['{column}'].astype(str).str.replace({repr(regex)}, {repr(replacement)}, regex=True{code_flags})"
                result_df = df.copy()
                result_df[new_column] = string_series.str.replace(regex, replacement, regex=True, flags=flags)
            else: # Replace in place (on copy)
                code += f"df['{column}'] = df['{column}'].astype(str).str.replace({repr(regex)}, {repr(replacement)}, regex=True{code_flags})"
                result_df = df.copy()
                result_df[column] = string_series.str.replace(regex, replacement, regex=True, flags=flags)
        else:
             raise ValueError(f"Unsupported regex operation type: {operation}")

    except re.error as regex_err:
         raise ValueError(f"Invalid regular expression: {regex_err}")
    except Exception as e:
         raise ValueError(f"Error during regex '{operation}': {e}")

    return result_df, code

def _string_op_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    column = params.get("column")
    string_func = params.get("string_function") # 'upper', 'lower', 'strip', 'split', 'len'
    new_col_name = params.get("new_column_name", f"{column}_{string_func}")
    delimiter = params.get("delimiter")
    part_index = params.get("part_index") # 0-based for pandas .str[] access

    if not column or not string_func:
        raise ValueError("string_operation requires 'column' and 'string_function'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")

    result_df = df.copy()
    code = f"# Apply string operation '{string_func}' to column '{column}'\n"
    op_series = None
    op_code_str = ""

    # Ensure column is string type for .str accessor
    str_series_code = f"df['{column}'].astype(str).str"

    try:
        func_lower = string_func.lower()
        if func_lower == 'upper':
            op_code_str = f"{str_series_code}.upper()"
            op_series = result_df[column].astype(str).str.upper()
        elif func_lower == 'lower':
            op_code_str = f"{str_series_code}.lower()"
            op_series = result_df[column].astype(str).str.lower()
        elif func_lower == 'strip':
            op_code_str = f"{str_series_code}.strip()"
            op_series = result_df[column].astype(str).str.strip()
        elif func_lower == 'len':
            op_code_str = f"{str_series_code}.len()"
            op_series = result_df[column].astype(str).str.len()
        elif func_lower == 'split':
            if delimiter is None or part_index is None:
                raise ValueError("String split requires 'delimiter' and 'part_index' (0-based).")
            idx = int(part_index)
            op_code_str = f"{str_series_code}.split({repr(delimiter)}, expand=True)[{idx}]"
            # Use expand=True and select column index. Fill NA for rows where split doesn't yield enough parts.
            op_series = result_df[column].astype(str).str.split(delimiter, expand=True).get(idx) # .get(idx) handles missing index gracefully -> None
        else:
            raise ValueError(f"Unsupported string_function for pandas: {string_func}")

        code += f"df['{new_col_name}'] = {op_code_str}"
        result_df[new_col_name] = op_series

    except Exception as e:
        raise ValueError(f"Error applying pandas string function '{string_func}': {e}")

    return result_df, code

def _date_extract_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    column = params.get("column")
    part = params.get("part") # year, month, day, hour, minute, second, dayofweek, dayofyear, weekofyear, quarter
    new_col_name = params.get("new_column_name", f"{column}_{part}")

    if not column or not part:
        raise ValueError("date_extract requires 'column' and 'part'.")
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found.")

    result_df = df.copy()
    code = f"# Extract '{part}' from date/time column '{column}'\n"
    part_lower = part.lower()
    # Map UI part names to pandas dt accessor attributes
    valid_parts = ['year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond', 'nanosecond',
                'dayofweek', 'weekday', 'dayofyear', 'weekofyear', 'week', 'quarter']
    if part_lower not in valid_parts:
         # Allow direct attribute access if user provides valid pandas dt attribute
         print(f"Warning: Date part '{part}' not in predefined list, attempting direct access.")
         # raise ValueError(f"Invalid date part '{part}' for pandas. Valid examples: {valid_parts}")

    dt_series_code = f"pd.to_datetime(df['{column}'], errors='coerce').dt"
    op_code_str = f"{dt_series_code}.{part_lower}" # Access attribute directly

    try:
        # Convert to datetime, coercing errors to NaT (Not a Time)
        dt_series = pd.to_datetime(result_df[column], errors='coerce').dt
        # Access the requested attribute
        extracted_part = getattr(dt_series, part_lower)

        code += f"df['{new_col_name}'] = {op_code_str}"
        result_df[new_col_name] = extracted_part

    except AttributeError:
         raise ValueError(f"Invalid date part '{part}' or column '{column}' is not convertible to datetime.")
    except Exception as e:
         raise ValueError(f"Failed to extract date part '{part}' from column '{column}'. Error: {e}")

    return result_df, code

def _create_column_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    new_col_name = params.get("new_column_name")
    expression_str = params.get("expression") # Python expression string

    if not new_col_name or not expression_str:
        raise ValueError("create_column requires 'new_column_name' and 'expression' string.")

    result_df = df.copy() # Work on a copy
    code = f"# Create new column '{new_col_name}' using Python expression\n"
    # The expression string can involve 'df', 'pd', 'np'
    code += f"# Ensure 'pd' and 'np' are available if used in the expression\n"
    code += f"df['{new_col_name}'] = {expression_str}"

    # !!! SECURITY WARNING: EVALUATING ARBITRARY STRING EXPRESSIONS IS RISKY !!!
    # This uses eval(), which can execute arbitrary code if not carefully controlled.
    # Use only in trusted environments or replace with a safer evaluation method.
    print(f"⚠️ SECURITY WARNING: Executing create_column_pd with eval() on expression: {expression_str}")
    exec_globals = {'pd': pd, 'np': np}
    exec_locals = {'df': result_df} # Pass the DataFrame copy to the eval context
    try:
        # Evaluate the expression string within the context of the DataFrame
        # We use exec to handle assignments within the expression if needed,
        # but eval is more common for calculating a value. Let's try eval first.
        # result_series = eval(expression_str, exec_globals, exec_locals)

        # Using exec is safer if the expression itself performs the assignment
        exec(f"result_df['{new_col_name}'] = {expression_str}", exec_globals, {'result_df': result_df})

        # Verify the column was created
        if new_col_name not in result_df.columns:
             raise ValueError(f"Expression did not successfully create column '{new_col_name}'. Check expression logic.")

    except SyntaxError as se:
        raise ValueError(f"Invalid syntax in expression: {expression_str}. Error: {se}")
    except Exception as e:
        # Catch errors during evaluation (e.g., column not found, type errors)
        raise ValueError(f"Failed to evaluate or apply expression '{expression_str}': {e}")

    return result_df, code

def _window_function_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    func = params.get("window_function") # e.g., 'rank', 'lead', 'lag', 'sum', 'mean', 'row_number'
    target_column = params.get("target_column") # Column for lead/lag/aggregates
    order_by_columns = params.get("order_by_columns") # List of dicts: [{'column': 'c', 'ascending': True}]
    partition_by_columns = params.get("partition_by_columns") # Optional list of column names
    new_col_name = params.get("new_column_name", f"{func}_window")
    # Function-specific params
    rank_method = params.get("rank_method", "average") # For rank: 'average', 'min', 'max', 'dense', 'first'
    offset = params.get("offset", 1) # For lead/lag (pandas uses shift)
    default_value = params.get("default_value")
    # Pandas rolling/expanding windows might need 'window_size' or 'min_periods' params later

    # --- Validation ---
    if not func: raise ValueError("Window function requires 'window_function' name.")
    agg_funcs = ['sum', 'mean', 'median', 'std', 'var', 'count', 'min', 'max'] # Rolling/Expanding
    lead_lag = ['lead', 'lag']
    rank_funcs = ['rank', 'dense_rank', 'row_number'] # Map to rank methods

    if func.lower() in agg_funcs + lead_lag and not target_column:
        raise ValueError(f"Window function '{func}' requires a 'target_column'.")
    # Order by is crucial for most window functions in pandas
    if not order_by_columns and func.lower() != 'count': # Count might not strictly need order
         raise ValueError("Window function requires 'order_by_columns' for meaningful results in Pandas.")

    cols_to_check = []
    if target_column: cols_to_check.append(target_column)
    if order_by_columns: cols_to_check.extend([spec['column'] for spec in order_by_columns])
    if partition_by_columns: cols_to_check.extend(partition_by_columns)
    missing = [col for col in cols_to_check if col not in df.columns]
    if missing: raise ValueError(f"Columns not found for window function: {', '.join(missing)}")

    # --- Apply ---
    result_df = df.copy()
    code = f"# Apply window function '{func}'\n"
    window_result = None

    try:
        # --- Sorting ---
        # Pandas window functions often require data to be sorted first, especially rank/shift
        sort_cols = []
        sort_asc = []
        if order_by_columns:
            sort_cols = [spec['column'] for spec in order_by_columns]
            sort_asc = [spec.get('ascending', True) for spec in order_by_columns]
            code += f"# Ensure data is sorted for window function\n"
            code += f"df_sorted = df.sort_values(by={repr(sort_cols)}, ascending={repr(sort_asc)})\n"
            result_df = result_df.sort_values(by=sort_cols, ascending=sort_asc)
        else:
             # code += f"# Warning: Applying window function without explicit sort order\n"
             pass # Proceed without sort if not specified, results might be unpredictable

        # --- Grouping (Partitioning) ---
        grouped = result_df
        group_cols_repr = ""
        if partition_by_columns:
            group_cols_repr = repr(partition_by_columns)
            code += f"# Partition data for window function\n"
            code += f"grouped = df_sorted.groupby({group_cols_repr})\n"
            grouped = result_df.groupby(partition_by_columns)
        else:
             # Apply globally if no partition columns
             code += f"# Apply window function globally (no partitioning)\n"
             grouped = result_df # Operate on the whole (sorted) DataFrame

        # --- Apply Window Logic ---
        func_lower = func.lower()
        target_col_repr = f"['{target_column}']" if target_column else "" # For accessing column on group/df

        if func_lower in rank_funcs:
            pandas_rank_method = rank_method
            if func_lower == 'dense_rank': pandas_rank_method = 'dense'
            elif func_lower == 'row_number': pandas_rank_method = 'first' # Row number is like rank(method='first')

            # Rank is applied to a column, typically the first sort column
            rank_col = sort_cols[0] if sort_cols else target_column # Need a column to rank
            if not rank_col: raise ValueError("Rank requires a column to rank by (specify target_column or order_by_columns).")

            rank_col_repr = f"['{rank_col}']"
            rank_args = f"method='{pandas_rank_method}', ascending={sort_asc[0] if sort_asc else True}"
            code += f"df['{new_col_name}'] = grouped{rank_col_repr}.rank({rank_args})\n"
            # Apply rank on the grouped object or the whole df
            window_result = grouped[rank_col].rank(method=pandas_rank_method, ascending=(sort_asc[0] if sort_asc else True))

        elif func_lower in lead_lag:
            period = int(offset) if func_lower == 'lag' else -int(offset) # shift uses negative for lead
            shift_args = f"periods={period}"
            code += f"df['{new_col_name}'] = grouped{target_col_repr}.shift({shift_args})\n"
            # Apply shift on the grouped object or the whole df
            window_result = grouped[target_column].shift(periods=period)
            # Handle default_value (fillna after shift)
            if default_value is not None:
                 code += f"df['{new_col_name}'] = df['{new_col_name}'].fillna({repr(default_value)})\n"
                 window_result = window_result.fillna(default_value)

        elif func_lower in agg_funcs:
             # Requires rolling or expanding window - More params needed (window size)
             # For simplicity, let's implement expanding (cumulative) versions first
             # Example: Cumulative Sum
             if func_lower == 'sum':
                 code += f"df['{new_col_name}'] = grouped{target_col_repr}.cumsum()\n"
                 window_result = grouped[target_column].cumsum()
             elif func_lower == 'mean':
                  code += f"df['{new_col_name}'] = grouped{target_col_repr}.expanding().mean()\n"
                  window_result = grouped[target_column].expanding().mean()
             # Add other expanding functions (min, max, count etc.) or rolling if needed
             else:
                 raise NotImplementedError(f"Pandas window aggregation '{func}' (expanding/rolling) requires more parameters or is not yet implemented.")
        else:
            raise ValueError(f"Unsupported window_function for pandas: {func}.")

        # Assign result back to the original DataFrame index (important if grouped)
        result_df[new_col_name] = window_result

    except Exception as e:
        raise ValueError(f"Error applying pandas window function '{func}': {e}")

    return result_df, code


def generate_pandas_code_snippet(operation: str, params: Dict[str, Any], df_var: str = "df") -> str:
    """Generates a single line/snippet of pandas code for a given operation."""
    # --- This function needs to be implemented based on apply_pandas_operation logic ---
    # Example for filter:
    if operation == "filter":
        col = params['column']
        op = params['operator']
        val = params['value']
        # Basic example, needs proper quoting/type handling like in apply_pandas_operation
        if isinstance(val, str): val = f"'{val}'"
        op_map = {"==": "==", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="} # etc.
        if op in op_map:
             return f"{df_var} = {df_var}[{df_var}['{col}'] {op_map[op]} {val}]"
        # Add other operations (select, groupby, rename, drop, merge etc.)
        # For merge: needs left/right df names passed in
        elif operation == "merge":
             right_df_name = params.get("right_dataset_name", "df_right") # Need to pass this
             left_on = params['left_on']
             right_on = params['right_on']
             how = params.get('join_type', 'inner')
             return f"{df_var} = pd.merge({df_var}, {right_df_name}, left_on='{left_on}', right_on='{right_on}', how='{how}')"
        # ... other operations
        else:
             # Fallback or raise error
             return f"# Pandas code for {operation} with params {params}"
    elif operation == "custom_code":
        return str(params) # params is the code itself
    # Add other operations...
    else:
        return f"# TODO: Implement pandas code snippet for {operation}"


def replay_pandas_operations(original_content: bytes, history: List[Dict[str, Any]]) -> Tuple[bytes, str]:
    """
    Replays a list of pandas operations from the original content.
    Returns the final content (bytes) and the cumulative code string.
    """
    if not history:
        return original_content, "# No operations applied"

    try:
        current_df = pd.read_csv(io.BytesIO(original_content))
    except Exception as e:
        raise ValueError(f"Failed to load original data for replay: {e}")

    cumulative_code_lines = ["import pandas as pd", "import io", "import numpy as np", "# Load initial data (replace with actual loading if needed)"]
    # Simulate loading - in real script, user would load their CSV
    # We don't include the actual read_csv in the *generated* script usually
    # cumulative_code_lines.append(f"df = pd.read_csv(io.BytesIO(original_content_placeholder))")

    # Placeholder for merge operations needing the right dataframe
    loaded_dataframes = {"df": current_df} # Tracks df variables

    for i, step in enumerate(history):
        op = step["operation"]
        params = step["params_or_code"]
        df_var = "df" # Assume we operate on 'df' unless it's a merge result

        code_snippet = ""
        try:
            # Handle merge specifically - needs the right df loaded
            if op == 'merge':
                right_dataset_name = params.get("right_dataset") # Name from UI params
                if not right_dataset_name: raise ValueError("Merge step missing 'right_dataset' name in history params.")
                # We assume the right dataset's *original* content is needed
                # This is a simplification - ideally, we'd replay the right dataset's history too!
                # For now, let's assume the user merges with another *original* dataset available in `datasets`
                # This requires access to the main `datasets` dict - pass it in? Or handle in main.py?
                # Let's assume main.py pre-loads required merge DFs into loaded_dataframes
                right_df_var = f"df_{right_dataset_name}"
                if right_df_var not in loaded_dataframes:
                    raise ValueError(f"Right dataframe '{right_dataset_name}' for merge step {i+1} not pre-loaded.")

                # Generate snippet with correct variable names
                step_params_for_snippet = params.copy()
                step_params_for_snippet["right_dataset_name"] = right_df_var # Pass variable name
                code_snippet = generate_pandas_code_snippet(op, step_params_for_snippet, df_var="df") # Apply merge to 'df'

                # Apply the operation to the actual DataFrame
                current_df = pd.merge(
                    loaded_dataframes[df_var],
                    loaded_dataframes[right_df_var],
                    left_on=params['left_on'],
                    right_on=params['right_on'],
                    how=params.get('join_type', 'inner')
                )
                loaded_dataframes[df_var] = current_df # Update 'df' variable

            elif op == 'custom_code':
                 code_snippet = str(params) # The code is the param
                 local_vars = {'df': current_df, 'pd': pd, 'np': np, 'io': io}
                 exec(code_snippet, local_vars)
                 current_df = local_vars.get('df')
                 if not isinstance(current_df, pd.DataFrame):
                     raise ValueError(f"Custom code at step {i+1} did not produce a pandas DataFrame named 'df'.")
                 loaded_dataframes[df_var] = current_df

            else:
                # Generate snippet for this step
                code_snippet = generate_pandas_code_snippet(op, params, df_var="df")
                # Apply the operation using the existing single-step logic (or re-implement here)
                # For simplicity, let's re-apply using exec with the snippet (less safe, but mirrors code)
                local_vars = {'df': current_df, 'pd': pd, 'np': np, 'io': io}
                exec(code_snippet, local_vars)
                current_df = local_vars.get('df')
                if not isinstance(current_df, pd.DataFrame):
                     raise ValueError(f"Operation '{op}' at step {i+1} did not produce a pandas DataFrame.")
                loaded_dataframes[df_var] = current_df


            cumulative_code_lines.append(code_snippet)
            step["generated_code_snippet"] = code_snippet # Store the snippet back in history entry

        except Exception as e:
            raise ValueError(f"Error replaying pandas step {i+1} ({op}): {e}\nCode: {code_snippet}")

    # Convert final DataFrame back to bytes
    try:
        with io.BytesIO() as buffer:
            current_df.to_csv(buffer, index=False)
            final_content = buffer.getvalue()
    except Exception as e:
        raise ValueError(f"Failed to serialize final pandas DataFrame: {e}")

    cumulative_code = "\n".join(cumulative_code_lines)
    return final_content, cumulative_code


def _apply_lambda_pd(df: pd.DataFrame, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
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
    code += f"# Make sure 'pd' and 'np' are available if used in the lambda\n"
    code += f"lambda_func = {lambda_str}\n" # Show the lambda definition
    code += f"df['{target_column}'] = df['{column}'].apply(lambda_func)"

    try:
        # --- SECURITY WARNING: Using eval is risky! ---
        # Create the actual lambda function from the string
        # Provide pandas and numpy in the eval context for convenience
        lambda_func = eval(lambda_str, {"pd": pd, "np": np})

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