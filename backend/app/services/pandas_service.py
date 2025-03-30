# backend/app/services/pandas_service.py
import pandas as pd
import io
from typing import Dict, Any, Tuple, List

def _is_numeric_col(df: pd.DataFrame, col_name: str) -> bool:
    if col_name not in df.columns:
        return False # Or raise error earlier
    # Check if dtype is numeric, handle potential 'object' dtypes that might contain numbers
    if pd.api.types.is_numeric_dtype(df[col_name]):
        return True
    # Attempt conversion for object columns, be cautious with performance on large data
    if df[col_name].dtype == 'object':
        try:
            pd.to_numeric(df[col_name].dropna(), errors='raise')
            return True # Convertible to numeric
        except (ValueError, TypeError):
            return False # Contains non-numeric strings
    return False

def apply_pandas_operation(df: pd.DataFrame, operation: str, params: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    """
    Applies a specified pandas operation to the DataFrame and returns the result
    along with the equivalent pandas code.

    Args:
        df: The input pandas DataFrame.
        operation: The name of the operation to perform (e.g., 'filter', 'groupby').
        params: A dictionary of parameters for the operation.

    Returns:
        A tuple containing:
        - The resulting pandas DataFrame after the operation.
        - A string representing the pandas code for the operation.
    """
    try:
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
            return _group_by_pd(df, params) # Single column, single agg
        elif operation == "groupby_multi":
            return _group_by_multi_pd(df, params) # Multiple columns, single agg
        elif operation == "groupby_multi_agg":
             return _group_by_multi_agg_pd(df, params) # Single/multi column, multi agg
        elif operation == "pivot_table":
            return _pivot_table_pd(df, params)
        elif operation == "melt":
            return _melt_pd(df, params)
        elif operation == "set_index":
            return _set_index_pd(df, params)
        elif operation == "reset_index":
            return _reset_index_pd(df, params)
        else:
            raise ValueError(f"Unsupported pandas operation: {operation}")
    except Exception as e:
        # Add more specific error handling if needed
        # The main.py handler should catch most things now
        print(f"Error executing pandas operation '{operation}': {type(e).__name__}: {e}")
        # Re-raise the original error for main.py to handle
        raise e


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
    try:
        if operator in ['==', '!=', '>', '<', '>=', '<='] and pd.api.types.is_numeric_dtype(df[column].dropna().iloc[0]):
             if '.' in str(value):
                 value = float(value)
             else:
                 value = int(value)
    except (ValueError, TypeError, IndexError):
        # Keep as string if conversion fails or column is empty/non-numeric
        value = str(value)


    code_value = repr(value) # Use repr for code generation to handle strings correctly

    condition_str = ""
    result_df = None

    if operator == "==":
        condition_str = f"df['{column}'] == {code_value}"
        result_df = df[df[column] == value]
    elif operator == "!=":
        condition_str = f"df['{column}'] != {code_value}"
        result_df = df[df[column] != value]
    elif operator == ">":
        condition_str = f"df['{column}'] > {code_value}"
        result_df = df[df[column] > value]
    elif operator == "<":
        condition_str = f"df['{column}'] < {code_value}"
        result_df = df[df[column] < value]
    elif operator == ">=":
        condition_str = f"df['{column}'] >= {code_value}"
        result_df = df[df[column] >= value]
    elif operator == "<=":
        condition_str = f"df['{column}'] <= {code_value}"
        result_df = df[df[column] <= value]
    elif operator == "contains":
        value_str = str(original_value)
        condition_str = f"df['{column}'].astype(str).str.contains({repr(value_str)}, na=False)"
        result_df = df[df[column].astype(str).str.contains(value_str, na=False)]
    elif operator == "startswith":
        value_str = str(original_value)
        condition_str = f"df['{column}'].astype(str).str.startswith({repr(value_str)}, na=False)"
        result_df = df[df[column].astype(str).str.startswith(value_str, na=False)]
    elif operator == "endswith":
        value_str = str(original_value)
        condition_str = f"df['{column}'].astype(str).str.endswith({repr(value_str)}, na=False)"
        result_df = df[df[column].astype(str).str.endswith(value_str, na=False)]
    elif operator == "regex":
        value_str = str(original_value)
        condition_str = f"df['{column}'].astype(str).str.contains({repr(value_str)}, regex=True, na=False)"
        result_df = df[df[column].astype(str).str.contains(value_str, regex=True, na=False)]
    else:
        raise ValueError(f"Unsupported filter operator: {operator}")

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
    how = params.get("join_type", "inner")
    left_on = params.get("left_on")
    right_on = params.get("right_on") # Assumes joining on columns with potentially different names

    if not left_on or not right_on:
         raise ValueError("Both left_on and right_on key columns are required for merge")

    if left_on not in left_df.columns:
         raise ValueError(f"Left key column '{left_on}' not found in left dataset")
    if right_on not in right_df.columns:
         raise ValueError(f"Right key column '{right_on}' not found in right dataset")

    valid_joins = ['left', 'right', 'outer', 'inner'] # 'cross' is also valid
    if how not in valid_joins:
        raise ValueError(f"Invalid join type: {how}. Must be one of {valid_joins}")

    code = f"# Merge DataFrames\n"
    code += f"merged_df = pd.merge(left_df,\n" # Assuming df names in context
    code += f"                   right_df,\n"
    code += f"                   how='{how}',\n"
    code += f"                   left_on='{left_on}',\n"
    code += f"                   right_on='{right_on}')"

    result_df = pd.merge(left_df, right_df, how=how, left_on=left_on, right_on=right_on)
    return result_df, code