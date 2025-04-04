import React, { useState, useEffect } from 'react';
import { Tooltip } from 'react-tooltip';

const OperationsPanel = ({ columns = [], onOperationSubmit, availableDatasets = [], currentDataset, isLoading, currentEngine = 'pandas' }) => {
  // Ensure columns is always an array
  const safeColumns = Array.isArray(columns) ? columns : [];

  const [operation, setOperation] = useState('filter');
  const [params, setParams] = useState({});
  const [showAdvanced, setShowAdvanced] = useState(false); // Controls visibility of operation groups

  // Reset params when operation or *currentDataset* changes
  useEffect(() => {
    setParams({});
  }, [operation, currentDataset]); // Also reset if the target dataset changes


  const handleParamChange = (name, value) => {
    setParams(prev => ({ ...prev, [name]: value }));
  };

  // Specific handler for multi-select rename/drop
  const handleMultiSelectChange = (name, selectedOptions) => {
      const values = Array.from(selectedOptions).map(option => option.value);
      handleParamChange(name, values);
  };

  // Handler for dynamic rename rows
  const handleRenameChange = (index, field, value) => {
    const updatedRenames = [...(params.renames || [{ old_name: '', new_name: '' }])];
    updatedRenames[index][field] = value;
    // Automatically add a new empty row if the last one is being filled
    if (index === updatedRenames.length - 1 && updatedRenames[index].old_name && updatedRenames[index].new_name) {
        updatedRenames.push({ old_name: '', new_name: '' });
    }
    handleParamChange('renames', updatedRenames);
  };

  const removeRenameRow = (index) => {
      const updatedRenames = (params.renames || []).filter((_, i) => i !== index);
      // Ensure there's always at least one row, even if empty
      if (updatedRenames.length === 0) {
          handleParamChange('renames', [{ old_name: '', new_name: '' }]);
      } else {
          handleParamChange('renames', updatedRenames);
      }
  };

  // Handler for GroupBy Multi Agg
  const handleMultiAggChange = (index, field, value) => {
     const updatedAggs = [...(params.aggregations || [{ column: '', function: '' }])];
     updatedAggs[index][field] = value;
     if (index === updatedAggs.length - 1 && updatedAggs[index].column && updatedAggs[index].function) {
         updatedAggs.push({ column: '', function: '' });
     }
     handleParamChange('aggregations', updatedAggs);
  };
  const removeMultiAggRow = (index) => {
      const updatedAggs = (params.aggregations || []).filter((_, i) => i !== index);
      if (updatedAggs.length === 0) {
          handleParamChange('aggregations', [{ column: '', function: '' }]);
      } else {
          handleParamChange('aggregations', updatedAggs);
      }
  };

  // Handler for multi-select columns (Sort)
   const handleMultiSortChange = (index, field, value) => {
       const updatedSorts = [...(params.sort_columns || [{ column: '', ascending: true }])];
       if (field === 'ascending') {
           updatedSorts[index][field] = value === 'true'; // Convert string to boolean
       } else {
           updatedSorts[index][field] = value;
       }
       // Add new row if last is filled
       if (index === updatedSorts.length - 1 && updatedSorts[index].column) {
           updatedSorts.push({ column: '', ascending: true });
       }
       handleParamChange('sort_columns', updatedSorts);
   };

   const removeMultiSortRow = (index) => {
       const updatedSorts = (params.sort_columns || []).filter((_, i) => i !== index);
       if (updatedSorts.length === 0) {
           handleParamChange('sort_columns', [{ column: '', ascending: true }]);
       } else {
           handleParamChange('sort_columns', updatedSorts);
       }
   };


   const handleWindowOrderByChange = (index, field, value) => {
    const updatedOrders = [...(params.order_by_columns || [{ column: '', descending: false }])];
    if (field === 'descending') {
        updatedOrders[index][field] = value === 'true'; // Convert string to boolean
    } else {
        updatedOrders[index][field] = value;
    }
    // Add new row if last is filled
    if (index === updatedOrders.length - 1 && updatedOrders[index].column) {
        updatedOrders.push({ column: '', descending: false });
    }
    handleParamChange('order_by_columns', updatedOrders);
};

const removeWindowOrderByRow = (index) => {
    const updatedOrders = (params.order_by_columns || []).filter((_, i) => i !== index);
    if (updatedOrders.length === 0) {
        handleParamChange('order_by_columns', [{ column: '', descending: false }]);
    } else {
        handleParamChange('order_by_columns', updatedOrders);
    }
};

  const handleSubmit = (e) => {
    e.preventDefault();
    console.log(`DEBUG: Submitting operation '${operation}' for dataset '${currentDataset}' with engine '${currentEngine}' and params:`, params);

    if (!currentDataset) {
        alert("Please select a dataset first.");
        return;
    }

    // --- Clean up empty rows from dynamic forms ---
    let finalParams = { ...params };
    if (operation === 'rename' && Array.isArray(finalParams.renames)) {
        finalParams.renames = finalParams.renames.filter(r => r.old_name && r.new_name);
        if (finalParams.renames.length === 0) {
             alert("Please specify at least one column rename mapping."); return;
        }
    }
    if (operation === 'groupby_multi_agg' && Array.isArray(finalParams.aggregations)) {
        finalParams.aggregations = finalParams.aggregations.filter(a => a.column && a.function);
         if (finalParams.aggregations.length === 0) {
             alert("Please specify at least one aggregation."); return;
         }
    }
    if (operation === 'sort' && currentEngine === 'sql' && Array.isArray(finalParams.sort_columns)) {
         finalParams.sort_columns = finalParams.sort_columns.filter(s => s.column);
          if (finalParams.sort_columns.length === 0) {
              alert("Please specify at least one column to sort by."); return;
          }
    }
    // --- Add other specific validations ---
    if (operation === 'filter' && (!finalParams.column || !finalParams.operator)) { alert("Filter requires Column and Operator."); return; }
    if (operation === 'select_columns' && (!finalParams.selected_columns || finalParams.selected_columns.length === 0)) { alert("Select Columns requires selecting at least one column."); return; }
    if (operation === 'drop_columns' && (!finalParams.drop_columns || finalParams.drop_columns.length === 0)) { alert("Drop Columns requires selecting at least one column."); return; }
    if (operation === 'groupby' && (!finalParams.group_column || !finalParams.agg_column || !finalParams.agg_function)) { alert("GroupBy requires Group Column, Agg Column, and Function."); return; }
    if (operation === 'groupby_multi_agg' && (!finalParams.group_columns || finalParams.group_columns.length === 0)) { alert("GroupBy Multi-Agg requires at least one Group Column."); return; }
    if (operation === 'merge' && (!finalParams.right_dataset || !finalParams.left_on || !finalParams.right_on)) { alert("Merge requires Right Dataset, Left Key, and Right Key."); return; }
    if (operation === 'apply_lambda' && (!finalParams.column || !finalParams.lambda_str)) { alert("Apply Lambda requires Column and Lambda String."); return; }
    if (operation === 'sample' && finalParams.sample_by === 'n' && (finalParams.n === undefined || finalParams.n === '')) { alert("Sample by N requires specifying the number of rows."); return; }
    if (operation === 'sample' && finalParams.sample_by === 'frac' && (finalParams.frac === undefined || finalParams.frac === '')) { alert("Sample by Fraction requires specifying the fraction (0-1)."); return; }
    if (operation === 'string_operation') {
      if (!finalParams.column || !finalParams.string_function || !finalParams.new_column_name) { alert("String Operation requires Target Column, Function, and New Column Name."); return; }
      if (finalParams.string_function === 'split' && (finalParams.delimiter === undefined || finalParams.part_index === undefined)) { alert("String Split requires Delimiter and Part Index."); return; }
  }
  if (operation === 'date_extract') {
      if (!finalParams.column || !finalParams.part || !finalParams.new_column_name) { alert("Date Extraction requires Column, Part to Extract, and New Column Name."); return; }
  }
  if (operation === 'create_column') {
      if (!finalParams.new_column_name || !finalParams.expression) { alert("Create Column requires New Column Name and Expression."); return; }
  }
  if (operation === 'window_function') {
      if (!finalParams.window_function || !finalParams.new_column_name) { alert("Window Function requires selecting a Function and specifying a New Column Name."); return; }
      // Clean up empty order_by rows
      if (Array.isArray(finalParams.order_by_columns)) {
          finalParams.order_by_columns = finalParams.order_by_columns.filter(s => s.column);
      }
      // Validate required fields based on function type
      const needsTarget = ['lead', 'lag', 'sum', 'mean', 'min', 'max', 'count'].includes(finalParams.window_function);
      const needsOrder = ['rank', 'dense_rank', 'row_number', 'lead', 'lag', 'sum', 'mean'].includes(finalParams.window_function); // Most need order
      if (needsTarget && !finalParams.target_column) { alert(`Window function '${finalParams.window_function}' requires a Target Column.`); return; }
      if (needsOrder && (!finalParams.order_by_columns || finalParams.order_by_columns.length === 0)) { alert(`Window function '${finalParams.window_function}' requires at least one Order By column.`); return; }
      if (['lead', 'lag'].includes(finalParams.window_function) && (finalParams.offset === undefined || finalParams.offset < 1)) { alert("Lead/Lag requires a positive Offset (>= 1)."); return; }
  }
    // Add more...

    onOperationSubmit(operation, finalParams);
  };

  // Aggregation function options
  const aggFunctions = [
    { value: "count", label: "Count" }, { value: "sum", label: "Sum" },
    { value: "mean", label: "Mean" }, { value: "median", label: "Median" },
    { value: "min", label: "Min" }, { value: "max", label: "Max" },
    { value: "std", label: "Std Dev" }, { value: "var", label: "Variance" },
    { value: "nunique", label: "Count Distinct" }, { value: "first", label: "First" },
    { value: "last", label: "Last" },
    // SQL/DuckDB specific (conditionally enable/show?)
    { value: "list", label: "List Agg (SQL)" }, { value: "mode", label: "Mode (SQL)"}
  ];

  // Filter operator options
  const filterOperators = [
    { value: "==", label: "==" }, { value: "!=", label: "!=" },
    { value: ">", label: ">" }, { value: "<", label: "<" },
    { value: ">=", label: ">=" }, { value: "<=", label: "<=" },
    { value: "contains", label: "Contains (string)" },
    { value: "startswith", label: "Starts With (string)" },
    { value: "endswith", label: "Ends With (string)" },
    { value: "isnull", label: "Is Null (SQL only)", engines: ['sql'] }, // Engine specific example
    { value: "notnull", label: "Is Not Null (SQL only)", engines: ['sql'] }, // Engine specific example
    { value: "regex", label: "Matches Regex" }, // Regex needs separate handling or specific endpoint
  ];

  // Define operation groups including the new one
  const operationGroups = [
    {
      groupName: "Basic",
      operations: [
        { value: "filter", label: "Filter Rows" },
        { value: "select_columns", label: "Select Columns" },
        { value: "sort", label: "Sort Values" },
        { value: "rename", label: "Rename Columns" },
        { value: "drop_columns", label: "Drop Columns" },
        { value: "drop_duplicates", label: "Drop Duplicates" },
      ]
    },
    {
      groupName: "Cleaning",
      operations: [
         { value: "fillna", label: "Fill Missing (NA)" },
         { value: "dropna", label: "Drop Missing (NA)" },
         { value: "astype", label: "Change Type" },
      ]
    },
    {
      groupName: "Aggregation",
      operations: [
        // { value: "groupby", label: "Group By (Single Agg)" }, // Use multi-agg for consistency
        { value: "groupby_multi_agg", label: "Group By" }
      ]
    },
    {
      groupName: "Transformation",
      operations: [
          { value: "string_operation", label: "String Operation" }, // ADDED
          { value: "date_extract", label: "Date Extraction" }, // ADDED
          { value: "create_column", label: "Create Column (Expr)" }, // ADDED
          { value: "window_function", label: "Window Function" }, // ADDED
          { value: "apply_lambda", label: "Apply Lambda (Pandas/SQL)", engines: ['pandas', 'sql'] },
      ]
    },
     {
       groupName: "Reshaping", // Pivot/Melt complex for SQL UI
       operations: [
         { value: "pivot_table", label: "Pivot Table (Pandas/Polars)", engines: ['pandas', 'polars'] },
         { value: "melt", label: "Melt (Pandas/Polars)", engines: ['pandas', 'polars'] },
         // { value: "set_index", label: "Set Index (Pandas)", engines: ['pandas']}, // Less relevant for Polars/SQL
         // { value: "reset_index", label: "Reset Index (Pandas)", engines: ['pandas']},
       ]
     },
    {
      groupName: "Joining",
      operations: [
        { value: "merge", label: "Merge / Join Datasets" }
      ]
    },
    // --- NEW GROUP ---
    {
        groupName: "Sampling",
        operations: [
            { value: "sample", label: "Sample Rows" },
            { value: "shuffle", label: "Shuffle Rows" }
        ]
    }
  ];

  // Filter operations based on the current engine
  const getFilteredOperations = (groupName) => {
      const group = operationGroups.find(g => g.groupName === groupName);
      if (!group) return [];
      return group.operations.filter(op => !op.engines || op.engines.includes(currentEngine));
  };


  // --- Lambda Presets ---
  const lambdaPresets = [
      { label: "x * 2", value: "lambda x: x * 2" },
      { label: "x + 10", value: "lambda x: x + 10" },
      { label: "Square (x**2)", value: "lambda x: x ** 2" },
      { label: "To Uppercase", value: "lambda x: str(x).upper()", engines: ['pandas'] }, // Pandas specific
      { label: "To Lowercase", value: "lambda x: str(x).lower()", engines: ['pandas'] }, // Pandas specific
      { label: "String Length", value: "lambda x: len(str(x))", engines: ['pandas'] }, // Pandas specific
      { label: "Extract Year (Date)", value: "lambda x: pd.to_datetime(x, errors='coerce').year", engines: ['pandas'] }, // Pandas specific
      { label: "Extract Month (Date)", value: "lambda x: pd.to_datetime(x, errors='coerce').month", engines: ['pandas'] }, // Pandas specific
      { label: "Is Null?", value: "lambda x: pd.isna(x)", engines: ['pandas'] }, // Pandas specific
      // SQL Lambda Presets (Simple Examples)
      { label: "x + 5 (SQL)", value: "x + 5", engines: ['sql'] },
      { label: "UPPER(x) (SQL)", value: "UPPER(x::TEXT)", engines: ['sql'] },
      { label: "x IS NULL (SQL)", value: "x IS NULL", engines: ['sql'] },
  ];

  const applyLambdaPreset = (presetValue) => {
      handleParamChange('lambda_str', presetValue);
  };

  // Helper to render common column selector
  const renderColumnSelector = (paramName, label, required = true, isMulti = false, options = safeColumns) => (
    <div className="mb-4">
      <label className="input-label">{label}:</label>
      <select
        className="select-base"
        value={isMulti ? params[paramName] || [] : params[paramName] || ''}
        onChange={(e) => isMulti ? handleMultiSelectChange(paramName, e.target.selectedOptions) : handleParamChange(paramName, e.target.value)}
        required={required}
        multiple={isMulti}
        size={isMulti ? Math.min(options.length, 5) : 1} // Show scroll for multi-select
      >
        {!isMulti && <option value="">Select a column</option>}
        {options.map((col, index) => (
          <option key={index} value={col}>{col}</option>
        ))}
      </select>
    </div>
  );

  const renderOperationForm = () => {
    const currentColumns = safeColumns; // Use the safe array

    // Filter options based on engine
    const availableOperators = filterOperators.filter(op => !op.engines || op.engines.includes(currentEngine));

    switch (operation) {
      case 'filter':
        return (
          <>
            {renderColumnSelector('column', 'Column')}
            <div className="mb-4">
              <label className="input-label">Operator:</label>
              <select
                className="select-base"
                value={params.operator || ''}
                onChange={(e) => handleParamChange('operator', e.target.value)}
                required
              >
                <option value="">Select operator</option>
                {availableOperators.map((op, index) => (
                  <option key={index} value={op.value}>{op.label}</option>
                ))}
              </select>
            </div>
             {/* Value input only needed for certain operators */}
            {params.operator && !['isnull', 'notnull'].includes(params.operator) && (
                <div className="mb-4">
                <label className="input-label">Value:</label>
                <input
                    className="input-base"
                    type="text"
                    placeholder="Enter filter value"
                    value={params.value ?? ''} // Use ?? for null/undefined
                    onChange={(e) => handleParamChange('value', e.target.value)}
                    required
                />
                </div>
            )}
          </>
        );

      case 'select_columns':
         return renderColumnSelector('selected_columns', 'Columns to Select', true, true);

      case 'sort':
          if (currentEngine === 'sql') { // SQL supports multi-column sort easily via UI
               const sortCols = params.sort_columns || [{ column: '', ascending: true }];
               return (
                   <>
                       <label className="input-label">Sort By:</label>
                       {sortCols.map((sortItem, index) => (
                           <div key={index} className="flex items-center space-x-2 mb-2">
                               <select
                                   className="select-base select-sm flex-grow"
                                   value={sortItem.column}
                                   onChange={(e) => handleMultiSortChange(index, 'column', e.target.value)}
                               >
                                   <option value="">Select column...</option>
                                   {currentColumns.map(col => <option key={col} value={col}>{col}</option>)}
                               </select>
                               <select
                                   className="select-base select-sm w-28"
                                   value={String(sortItem.ascending)}
                                   onChange={(e) => handleMultiSortChange(index, 'ascending', e.target.value)}
                               >
                                   <option value="true">Ascending</option>
                                   <option value="false">Descending</option>
                               </select>
                               {sortCols.length > 1 && (
                                   <button type="button" onClick={() => removeMultiSortRow(index)} className="btn btn-ghost btn-xs text-red-500">✕</button>
                               )}
                           </div>
                       ))}
                   </>
               );
          } else { // Pandas / Polars - Simple sort for now
              return (
                  <>
                      {renderColumnSelector('sort_column', 'Sort by Column')}
                      <div className="mb-4">
                          <label className="input-label">Order:</label>
                          <select
                              className="select-base"
                              value={params.sort_order || 'ascending'}
                              onChange={(e) => handleParamChange('sort_order', e.target.value)}
                          >
                              <option value="ascending">Ascending</option>
                              <option value="descending">Descending</option>
                          </select>
                      </div>
                  </>
              );
          }

        case 'rename':
            const renames = params.renames || [{ old_name: '', new_name: '' }];
            return (
                <>
                    <label className="input-label">Rename Mappings:</label>
                    {renames.map((item, index) => (
                        <div key={index} className="flex items-center space-x-2 mb-2">
                            <select
                                className="select-base select-sm flex-grow"
                                value={item.old_name}
                                onChange={(e) => handleRenameChange(index, 'old_name', e.target.value)}
                                required={index < renames.length - 1} // Required only if not the last empty row
                            >
                                <option value="">Select old name...</option>
                                {currentColumns.map(col => <option key={col} value={col}>{col}</option>)}
                            </select>
                            <span className="text-maid-gray-dark">to</span>
                            <input
                                type="text"
                                className="input-base input-sm flex-grow"
                                placeholder="New name"
                                value={item.new_name}
                                onChange={(e) => handleRenameChange(index, 'new_name', e.target.value)}
                                required={index < renames.length - 1}
                            />
                            {/* Allow removing rows only if it's not the very last (potentially empty) row */}
                            {(renames.length > 1 || item.old_name || item.new_name) && index < renames.length -1 && (
                                <button type="button" onClick={() => removeRenameRow(index)} className="btn btn-ghost btn-xs text-red-500">✕</button>
                            )}
                        </div>
                    ))}
                </>
            );

      case 'drop_columns':
          return renderColumnSelector('drop_columns', 'Columns to Drop', true, true);

      case 'groupby_multi_agg': // Renamed GroupBy
            const groupCols = params.group_columns || [];
            const aggs = params.aggregations || [{ column: '', function: '' }];
            const availableAggFunctions = aggFunctions.filter(f => !f.engines || f.engines.includes(currentEngine));

            return (
                <>
                    <div className="mb-4">
                       <label className="input-label">Group By Columns:</label>
                       <select
                           className="select-base"
                           value={groupCols}
                           onChange={(e) => handleMultiSelectChange('group_columns', e.target.selectedOptions)}
                           required
                           multiple
                           size={Math.min(currentColumns.length, 5)}
                       >
                           {currentColumns.map((col) => (
                             <option key={col} value={col}>{col}</option>
                           ))}
                       </select>
                    </div>
                    <label className="input-label">Aggregations:</label>
                    {aggs.map((aggItem, index) => (
                        <div key={index} className="flex items-center space-x-2 mb-2">
                            <select // Aggregation Function
                                className="select-base select-sm flex-grow-[2]" // Give more space
                                value={aggItem.function}
                                onChange={(e) => handleMultiAggChange(index, 'function', e.target.value)}
                                required={index < aggs.length -1}
                            >
                                <option value="">Select function...</option>
                                {availableAggFunctions.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
                            </select>
                            <span className="text-maid-gray-dark">of</span>
                             <select // Aggregation Column
                                className="select-base select-sm flex-grow-[3]" // More space
                                value={aggItem.column}
                                onChange={(e) => handleMultiAggChange(index, 'column', e.target.value)}
                                required={index < aggs.length -1}
                            >
                                <option value="">Select column...</option>
                                 {/* Allow COUNT(*) */}
                                {aggItem.function === 'count' && <option value="*">* (All Rows)</option>}
                                {currentColumns.map(col => <option key={col} value={col}>{col}</option>)}
                            </select>
                             { (aggs.length > 1 || aggItem.column || aggItem.function) && index < aggs.length - 1 && (
                                <button type="button" onClick={() => removeMultiAggRow(index)} className="btn btn-ghost btn-xs text-red-500">✕</button>
                             )}
                        </div>
                    ))}
                </>
            );

        case 'fillna':
            return (
                <>
                    {renderColumnSelector('columns', 'Columns to Fill (Optional)', false, true)}
                    <p className="text-xs text-maid-gray-dark mb-2">Leave blank to fill all columns.</p>
                    <div className="mb-4">
                      <label className="input-label">Fill With Value:</label>
                      <input
                        className="input-base" type="text"
                        placeholder="e.g., 0, 'Unknown'"
                        value={params.value ?? ''}
                        onChange={(e) => { handleParamChange('value', e.target.value); handleParamChange('method', undefined); }} // Clear method if value is used
                      />
                    </div>
                     {/* Add Method (Pandas/Polars only?) */}
                     {currentEngine !== 'sql' && (
                         <div className="mb-4">
                            <label className="input-label">Or Fill Method (Pandas/Polars):</label>
                            <select
                                className="select-base"
                                value={params.method || ''}
                                onChange={(e) => { handleParamChange('method', e.target.value); handleParamChange('value', undefined); }} // Clear value if method is used
                            >
                                <option value="">Select method...</option>
                                <option value="ffill">ffill (forward)</option>
                                <option value="bfill">bfill (backward)</option>
                                {/* Polars specific strategies */}
                                {currentEngine === 'polars' && <>
                                    <option value="mean">Mean</option>
                                    <option value="median">Median</option>
                                    <option value="min">Min</option>
                                    <option value="max">Max</option>
                                    <option value="zero">Zero</option>
                                </>}
                            </select>
                         </div>
                     )}
                </>
            );

        case 'dropna':
             return (
                 <>
                     {renderColumnSelector('subset', 'Check Columns (Optional)', false, true)}
                     <p className="text-xs text-maid-gray-dark mb-2">Leave blank to check all columns.</p>
                     {/* Add 'how' and 'thresh' if needed */}
                 </>
             );

        case 'astype':
            // Define common types, backend handles mapping
            const commonTypes = ["integer", "float", "string", "boolean", "datetime", "date", "category (polars)"];
            return (
                <>
                    {renderColumnSelector('column', 'Column to Convert')}
                    <div className="mb-4">
                      <label className="input-label">New Type:</label>
                      <select
                        className="select-base" value={params.new_type || ''}
                        onChange={(e) => handleParamChange('new_type', e.target.value)} required
                      >
                        <option value="">Select type...</option>
                        {commonTypes.map(t => <option key={t} value={t}>{t}</option>)}
                      </select>
                    </div>
                </>
            );

      case 'merge':
        return (
          <>
            <div className="mb-4">
              <label className="input-label">Merge With Dataset:</label>
              <select
                className="select-base"
                value={params.right_dataset || ''}
                onChange={(e) => handleParamChange('right_dataset', e.target.value)}
                required
              >
                <option value="">Select dataset...</option>
                {availableDatasets.filter(ds => ds !== currentDataset).map((ds) => (
                  <option key={ds} value={ds}>{ds}</option>
                ))}
              </select>
            </div>
            {renderColumnSelector('left_on', 'Left Key Column')}
            {/* Right key selector needs columns from the *other* dataset - tricky for UI */}
            {/* For now, use text input for right key */}
             <div className="mb-4">
                <label className="input-label">Right Key Column:</label>
                <input
                    className="input-base" type="text"
                    placeholder="Enter column name from the other dataset"
                    value={params.right_on || ''}
                    onChange={(e) => handleParamChange('right_on', e.target.value)}
                    required
                />
                <p className="text-xs text-maid-gray-dark mt-1">Ensure this column exists in the selected 'Merge With' dataset.</p>
             </div>

            <div className="mb-4">
              <label className="input-label">Join Type:</label>
              <select
                className="select-base"
                value={params.join_type || 'inner'}
                onChange={(e) => handleParamChange('join_type', e.target.value)}
              >
                <option value="inner">Inner</option>
                <option value="left">Left</option>
                <option value="right">Right</option>
                <option value="outer">Outer</option>
                 {/* Polars/SQL specific */}
                {(currentEngine === 'polars' || currentEngine === 'sql') && <option value="cross">Cross</option>}
                {(currentEngine === 'polars' || currentEngine === 'sql') && <option value="semi">Semi</option>}
                {(currentEngine === 'polars' || currentEngine === 'sql') && <option value="anti">Anti</option>}
                {currentEngine === 'polars' && <option value="outer_coalesce">Outer Coalesce (Polars)</option>}
              </select>
            </div>
          </>
        );

      // --- Sampling & Shuffle ---
      case 'sample':
        return (
          <>
            <div className="mb-2">
                <span className="input-label mr-4">Sample By:</span>
                <label className="mr-4">
                    <input type="radio" name="sample_by" value="n" className="mr-1"
                        checked={params.sample_by === 'n'}
                        onChange={() => handleParamChange('sample_by', 'n')} /> N Rows
                </label>
                <label>
                    <input type="radio" name="sample_by" value="frac" className="mr-1"
                        checked={params.sample_by === 'frac'}
                        onChange={() => handleParamChange('sample_by', 'frac')} /> Fraction
                </label>
            </div>

            {params.sample_by === 'n' && (
                 <div className="mb-4">
                   <label className="input-label">Number of Rows (N):</label>
                   <input
                     className="input-base" type="number" min="1" step="1"
                     placeholder="e.g., 100"
                     value={params.n ?? ''}
                     onChange={(e) => handleParamChange('n', e.target.value ? parseInt(e.target.value) : undefined)}
                     required
                   />
                 </div>
            )}
             {params.sample_by === 'frac' && (
                 <div className="mb-4">
                   <label className="input-label">Fraction (0.0 to 1.0):</label>
                   <input
                     className="input-base" type="number" min="0" max="1" step="0.01"
                     placeholder="e.g., 0.1 for 10%"
                     value={params.frac ?? ''}
                     onChange={(e) => handleParamChange('frac', e.target.value ? parseFloat(e.target.value) : undefined)}
                     required
                   />
                 </div>
            )}

            <div className="mb-4">
                <label className="flex items-center">
                    <input type="checkbox" className="mr-2"
                        checked={params.replace || false}
                        onChange={(e) => handleParamChange('replace', e.target.checked)}
                    /> Sample with Replacement
                </label>
                 {currentEngine === 'sql' && params.replace && <p className="text-xs text-orange-600 ml-6">Note: SQL sample often ignores replacement.</p>}
            </div>

            {/* SQL Specific Sample Method */}
            {currentEngine === 'sql' && (
                 <div className="mb-4">
                   <label className="input-label">SQL Sample Method:</label>
                    <select className="select-base" value={params.method || 'system'} onChange={(e) => handleParamChange('method', e.target.value)}>
                        <option value="system">System (Faster)</option>
                        <option value="bernoulli">Bernoulli (More Random)</option>
                    </select>
                 </div>
            )}

            <div className="mb-4">
                <label className="input-label">Random Seed (Optional):</label>
                <input
                    className="input-base" type="number" placeholder="For reproducibility"
                    value={params.seed ?? ''}
                    onChange={(e) => handleParamChange('seed', e.target.value ? parseInt(e.target.value) : undefined)}
                />
            </div>
          </>
        );

        case 'shuffle':
            return (
                <>
                    <p className="text-maid-gray-dark mb-4">Shuffles all rows in the dataset randomly.</p>
                    <div className="mb-4">
                        <label className="input-label">Random Seed (Optional):</label>
                        <input
                            className="input-base" type="number" placeholder="For reproducibility"
                            value={params.seed ?? ''}
                            onChange={(e) => handleParamChange('seed', e.target.value ? parseInt(e.target.value) : undefined)}
                        />
                         {currentEngine === 'sql' && params.seed && <p className="text-xs text-orange-600 mt-1">Note: SQL ORDER BY RANDOM() seed ignored.</p>}
                    </div>
                </>
            );

      // --- Lambda Function (Conditionally Render based on Engine) ---
      case 'apply_lambda':
        // Check if supported by current engine
        const lambdaSupported = ['pandas', 'sql'].includes(currentEngine);
        if (!lambdaSupported) {
             return <p className="text-red-600">Apply Lambda is not directly supported for the '{currentEngine}' engine via this panel. Use Custom Code or Create Column.</p>;
        }
        const filteredPresets = lambdaPresets.filter(p => !p.engines || p.engines.includes(currentEngine));
        return (
          <>
             <p className="text-sm text-orange-600 mb-3">
                <strong>Warning:</strong> Executes code on server ({currentEngine}).{' '}
                {currentEngine === 'sql' && <span>SQL translation is limited.</span>}
             </p>
            {renderColumnSelector('column', 'Apply to Column')}
            <div className="mb-4">
              <label className="input-label">Lambda Function:</label>
              <textarea
                className="input-base font-mono text-sm"
                rows="3"
                placeholder={currentEngine === 'pandas' ? "e.g., lambda x: x * 2" : "e.g., x + 5 OR UPPER(x::TEXT)"}
                value={params.lambda_str || ''}
                onChange={(e) => handleParamChange('lambda_str', e.target.value)}
                required
              />
              <p className="text-xs text-maid-gray-dark mt-1">
                {currentEngine === 'pandas' && "Use 'x' as the variable. `pd` and `np` available."}
                {currentEngine === 'sql' && "Use 'x' for the column value. Basic Python ops might translate."}
              </p>
            </div>
            {/* Lambda Presets */}
            <div className="mb-4">
                <label className="input-label text-xs">Presets ({currentEngine}):</label>
                <div className="flex flex-wrap gap-1">
                    {filteredPresets.map(preset => (
                        <button
                            key={preset.label}
                            type="button"
                            onClick={() => applyLambdaPreset(preset.value)}
                            className="btn btn-outline btn-xs"
                            title={preset.value}
                            data-tooltip-id="lambda-preset-tooltip"
                            data-tooltip-content={preset.value}
                        >
                            {preset.label}
                        </button>
                    ))}
                </div>
                 <Tooltip id="lambda-preset-tooltip" place="top" effect="solid" />
            </div>
            <div className="mb-4">
              <label className="input-label">New Column Name (Optional):</label>
              <input
                className="input-base" type="text"
                placeholder="Leave blank to modify original column"
                value={params.new_column_name || ''}
                onChange={(e) => handleParamChange('new_column_name', e.target.value)}
              />
            </div>
          </>
        );

        case 'string_operation':
        const stringFunctions = [
            { value: 'upper', label: 'To Uppercase' },
            { value: 'lower', label: 'To Lowercase' },
            { value: 'strip', label: 'Strip Whitespace' },
            { value: 'length', label: 'Get Length' },
            { value: 'split', label: 'Split String' },
        ];
        return (
            <>
                {renderColumnSelector('column', 'Target Column')}
                <div className="mb-4">
                    <label className="input-label">String Function:</label>
                    <select
                        className="select-base"
                        value={params.string_function || ''}
                        onChange={(e) => handleParamChange('string_function', e.target.value)}
                        required
                    >
                        <option value="">Select function...</option>
                        {stringFunctions.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
                    </select>
                </div>
                {/* Conditional fields for 'split' */}
                {params.string_function === 'split' && (
                    <>
                        <div className="mb-4">
                            <label className="input-label">Delimiter:</label>
                            <input
                                className="input-base" type="text"
                                placeholder="e.g., ',', '-', ' '"
                                value={params.delimiter ?? ''}
                                onChange={(e) => handleParamChange('delimiter', e.target.value)}
                                required
                            />
                        </div>
                        <div className="mb-4">
                            <label className="input-label">Part Index (0-based):</label>
                            <input
                                className="input-base" type="number" min="0" step="1"
                                placeholder="e.g., 0 for first part"
                                value={params.part_index ?? ''}
                                onChange={(e) => handleParamChange('part_index', e.target.value ? parseInt(e.target.value) : undefined)}
                                required
                            />
                        </div>
                    </>
                )}
                <div className="mb-4">
                    <label className="input-label">New Column Name:</label>
                    <input
                        className="input-base" type="text"
                        placeholder={`e.g., ${params.column || 'col'}_${params.string_function || 'str'}`}
                        value={params.new_column_name || ''}
                        onChange={(e) => handleParamChange('new_column_name', e.target.value)}
                        required
                    />
                </div>
            </>
        );

      case 'date_extract':
        const dateParts = [
            'year', 'month', 'day', 'hour', 'minute', 'second',
            'millisecond', 'microsecond', 'nanosecond', // Engine specific?
            'weekday', 'dayofweek', // Pandas/Polars differ slightly
            'dayofyear', 'ordinal_day', // Pandas/Polars differ slightly
            'week', 'weekofyear', // Pandas/Polars differ slightly
            'quarter', 'iso_year' // Polars specific?
        ];
        return (
            <>
                {renderColumnSelector('column', 'Date/Datetime Column')}
                <div className="mb-4">
                    <label className="input-label">Part to Extract:</label>
                    <select
                        className="select-base"
                        value={params.part || ''}
                        onChange={(e) => handleParamChange('part', e.target.value)}
                        required
                    >
                        <option value="">Select part...</option>
                        {dateParts.map(p => <option key={p} value={p}>{p}</option>)}
                    </select>
                     <p className="text-xs text-maid-gray-dark mt-1">Ensure column is a date/datetime type. Availability varies by engine.</p>
                </div>
                <div className="mb-4">
                    <label className="input-label">New Column Name:</label>
                    <input
                        className="input-base" type="text"
                        placeholder={`e.g., ${params.column || 'col'}_${params.part || 'part'}`}
                        value={params.new_column_name || ''}
                        onChange={(e) => handleParamChange('new_column_name', e.target.value)}
                        required
                    />
                </div>
            </>
        );

      case 'create_column':
        const exprPlaceholder = currentEngine === 'polars'
            ? "e.g., pl.col('a') + pl.col('b')\npl.when(pl.col('x') > 0).then(1).otherwise(0)"
            : "e.g., df['a'] * df['b']\nnp.where(df['x'] > 10, 'High', 'Low')";
        return (
            <>
                 <p className="text-sm text-orange-600 mb-3">
                    <strong>Warning:</strong> Executes code via `eval()` on server ({currentEngine}). Use with caution.
                 </p>
                <div className="mb-4">
                    <label className="input-label">New Column Name:</label>
                    <input
                        className="input-base" type="text"
                        placeholder="e.g., new_col, calculated_value"
                        value={params.new_column_name || ''}
                        onChange={(e) => handleParamChange('new_column_name', e.target.value)}
                        required
                    />
                </div>
                <div className="mb-4">
                    <label className="input-label">Expression ({currentEngine} syntax):</label>
                    <textarea
                        className="input-base font-mono text-sm"
                        rows="4"
                        placeholder={exprPlaceholder}
                        value={params.expression || ''}
                        onChange={(e) => handleParamChange('expression', e.target.value)}
                        required
                    />
                     <p className="text-xs text-maid-gray-dark mt-1">
                        {currentEngine === 'polars' && "Use `pl` for polars functions (e.g., `pl.col`, `pl.lit`)."}
                        {currentEngine === 'pandas' && "Use `df` to reference the dataframe, `pd`, `np` available."}
                     </p>
                </div>
            </>
        );

      case 'window_function':
        // Define available window functions (can vary by engine)
        const windowFuncs = [
            { value: 'rank', label: 'Rank' },
            { value: 'dense_rank', label: 'Dense Rank' },
            { value: 'row_number', label: 'Row Number' },
            { value: 'lead', label: 'Lead' },
            { value: 'lag', label: 'Lag' },
            { value: 'sum', label: 'Cumulative Sum' }, // Simplified for now
            { value: 'mean', label: 'Cumulative Mean' }, // Simplified for now
            // Add more basic aggregates: min, max, count (cumulative)
            // Rolling/complex aggregates might need more UI elements (window size etc.)
        ];
        const needsTargetCol = ['lead', 'lag', 'sum', 'mean', 'min', 'max', 'count']; // Functions requiring a target column
        const showTargetCol = needsTargetCol.includes(params.window_function);
        const showLeadLagParams = ['lead', 'lag'].includes(params.window_function);
        const showRankParams = ['rank', 'dense_rank'].includes(params.window_function); // 'row_number' doesn't need method

        const orderCols = params.order_by_columns || [{ column: '', descending: false }];
        const partitionCols = params.partition_by_columns || [];

        return (
            <>
                <div className="mb-4">
                    <label className="input-label">Window Function:</label>
                    <select
                        className="select-base"
                        value={params.window_function || ''}
                        onChange={(e) => handleParamChange('window_function', e.target.value)}
                        required
                    >
                        <option value="">Select function...</option>
                        {windowFuncs.map(f => <option key={f.value} value={f.value}>{f.label}</option>)}
                    </select>
                </div>

                {/* Target Column (Conditional) */}
                {showTargetCol && renderColumnSelector('target_column', 'Apply Function To Column', true)}

                {/* Partition By */}
                <div className="mb-4">
                    <label className="input-label">Partition By (Optional):</label>
                    <select
                        className="select-base"
                        value={partitionCols}
                        onChange={(e) => handleMultiSelectChange('partition_by_columns', e.target.selectedOptions)}
                        multiple
                        size={Math.min(currentColumns.length, 3)}
                    >
                        {currentColumns.map((col) => (
                            <option key={col} value={col}>{col}</option>
                        ))}
                    </select>
                    <p className="text-xs text-maid-gray-dark mt-1">Apply function independently within each group.</p>
                </div>

                {/* Order By */}
                <div className="mb-4">
                    <label className="input-label">Order By (Required for most functions):</label>
                    {orderCols.map((orderItem, index) => (
                        <div key={index} className="flex items-center space-x-2 mb-2">
                            <select
                                className="select-base select-sm flex-grow"
                                value={orderItem.column}
                                onChange={(e) => handleWindowOrderByChange(index, 'column', e.target.value)}
                                required={index < orderCols.length - 1 || params.window_function?.includes('rank')} // Rank needs order
                            >
                                <option value="">Select column...</option>
                                {currentColumns.map(col => <option key={col} value={col}>{col}</option>)}
                            </select>
                            <select
                                className="select-base select-sm w-28"
                                value={String(orderItem.descending)} // Use descending for Polars/SQL consistency
                                onChange={(e) => handleWindowOrderByChange(index, 'descending', e.target.value)}
                            >
                                <option value="false">Ascending</option>
                                <option value="true">Descending</option>
                            </select>
                            {orderCols.length > 1 && (
                                <button type="button" onClick={() => removeWindowOrderByRow(index)} className="btn btn-ghost btn-xs text-red-500">✕</button>
                            )}
                        </div>
                    ))}
                     <p className="text-xs text-maid-gray-dark mt-1">Determines order for rank, lead, lag, cumulative calcs.</p>
                </div>

                 {/* Function Specific Params */}
                 {showRankParams && (
                     <div className="mb-4">
                        <label className="input-label">Rank Method:</label>
                        <select className="select-base" value={params.rank_method || 'average'} onChange={(e) => handleParamChange('rank_method', e.target.value)}>
                            <option value="average">Average</option>
                            <option value="min">Min</option>
                            <option value="max">Max</option>
                            <option value="dense">Dense</option>
                            <option value="ordinal">Ordinal (Row Number)</option>
                            {currentEngine === 'pandas' && <option value="first">First (Pandas)</option>}
                        </select>
                     </div>
                 )}
                 {showLeadLagParams && (
                     <>
                        <div className="mb-4">
                            <label className="input-label">Offset:</label>
                            <input className="input-base" type="number" min="1" step="1" value={params.offset ?? 1} onChange={(e) => handleParamChange('offset', e.target.value ? parseInt(e.target.value) : 1)} />
                        </div>
                         {/* Polars/SQL might support default fill */}
                         {(currentEngine === 'polars' || currentEngine === 'sql') && (
                             <div className="mb-4">
                                <label className="input-label">Default Value (Optional):</label>
                                <input className="input-base" type="text" placeholder="Fill value for nulls" value={params.default_value ?? ''} onChange={(e) => handleParamChange('default_value', e.target.value)} />
                             </div>
                         )}
                     </>
                 )}

                {/* New Column Name */}
                <div className="mb-4">
                    <label className="input-label">New Column Name:</label>
                    <input
                        className="input-base" type="text"
                        placeholder={`e.g., ${params.window_function || 'window'}_result`}
                        value={params.new_column_name || ''}
                        onChange={(e) => handleParamChange('new_column_name', e.target.value)}
                        required
                    />
                </div>
            </>
        );

      default:
        // Check if operation is valid for the current engine
        const selectedOpInfo = operationGroups.flatMap(g => g.operations).find(op => op.value === operation);
        const opSupported = !selectedOpInfo || !selectedOpInfo.engines || selectedOpInfo.engines.includes(currentEngine);
        if (selectedOpInfo && !opSupported) {
            return (
                <div className="text-red-600 italic text-center p-4">
                    Operation '{selectedOpInfo.label}' is not available for the '{currentEngine}' engine in this panel.
                </div>
            );
        }
        return (
          <div className="text-maid-gray-dark italic text-center p-4">
            Select an operation or parameters might appear here.
          </div>
        );
    }
  };

  // Filter operation groups based on whether they contain *any* operation valid for the current engine
  const availableOperationGroups = operationGroups.filter(group =>
     getFilteredOperations(group.groupName).length > 0
  );

  return (
    <div className="card bg-white p-4 rounded-lg shadow-soft border border-maid-gray-light">
      <h2 className="card-header">Operations Panel ({currentEngine})</h2>

      <form onSubmit={handleSubmit}>
        {/* Operation Group Selection */}
        <div className="mb-4">
          <label className="input-label">Operation Category:</label>
          <div className="flex flex-wrap gap-2 mb-2">
            {availableOperationGroups.map((group, index) => (
              <button
                key={index} type="button"
                className={`py-1 px-3 rounded-md text-sm font-medium ${ showAdvanced === group.groupName ? 'bg-coffee text-white' : 'bg-maid-cream text-maid-choco hover:bg-maid-cream-dark' }`}
                onClick={() => setShowAdvanced( showAdvanced === group.groupName ? false : group.groupName )}
              >
                {group.groupName}
              </button>
            ))}
          </div>
        </div>

        {/* Operation Selection within Group */}
        {showAdvanced && (
          <div className="mb-4">
            <label className="input-label">Select Operation:</label>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
              {getFilteredOperations(showAdvanced).map((op, index) => (
                <div
                  key={index}
                  className={`p-2 border rounded-md cursor-pointer text-sm ${ operation === op.value ? 'bg-coffee-light bg-opacity-20 border-coffee' : 'hover:bg-maid-cream border-maid-gray-light' }`}
                  onClick={() => setOperation(op.value)} // Params reset handled by useEffect
                >
                  {op.label}
                </div>
              ))}
            </div>
          </div>
        )}


        {/* Render the form for the selected operation */}
        <div className="mb-6 p-4 bg-maid-cream-light rounded-md border border-maid-gray-light min-h-[150px]">
          {renderOperationForm()}
        </div>

        {/* Submit Button */}
        <div className="flex items-center justify-end">
          <button
            type="submit"
            className="btn btn-coffee"
            disabled={isLoading || !currentDataset} // Disable if no dataset selected
            title={!currentDataset ? "Select a dataset first" : `Apply ${operation} using ${currentEngine}`}
          >
            {isLoading ? 'Processing...' : 'Apply Operation'}
          </button>
        </div>
      </form>
    </div>
  );
};

export default OperationsPanel;