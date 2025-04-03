// src/components/EnhancedOperationsPanel.jsx
import React, { useState, useEffect } from 'react';

const EnhancedOperationsPanel = ({ columns = [], onOperationSubmit, availableDatasets = [], currentDataset, isLoading }) => {
  // Ensure columns is always an array
  const safeColumns = Array.isArray(columns) ? columns : [];

  const [operation, setOperation] = useState('filter');
  const [params, setParams] = useState({});
  const [showAdvanced, setShowAdvanced] = useState(false); // Keep track of which group is shown

  // Reset params when operation changes
  useEffect(() => {
    setParams({});
  }, [operation]);

  const handleParamChange = (name, value) => {
    setParams(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    console.log('DEBUG: Submitting operation:', operation, 'with params:', params);
    // Basic validation before submitting
    if (!currentDataset) {
        alert("Please select a dataset first.");
        return;
    }
    if (operation === 'apply_lambda' && (!params.column || !params.lambda_str)) {
        alert("Please select a column and provide a lambda function string.");
        return;
    }
    // Add other necessary validations here
    onOperationSubmit(operation, params);
  };

  // Define operation groups including the new one
  const operationGroups = [
    {
      groupName: "Basic Operations",
      operations: [
        { value: "filter", label: "Filter Rows" },
        { value: "select_columns", label: "Select Columns" },
        { value: "sort", label: "Sort Values" },
        { value: "rename", label: "Rename Columns" },
        { value: "drop_columns", label: "Drop Columns" }
      ]
    },
    {
      groupName: "Aggregation",
      operations: [
        { value: "groupby", label: "Group By (Single Column)" },
        { value: "groupby_multi", label: "Group By (Multiple Columns)" },
        // { value: "groupby_multi_agg", label: "Group By with Multiple Aggregations" } // Keep if implemented
      ]
    },
    {
      groupName: "Reshaping",
      operations: [
        { value: "pivot_table", label: "Pivot Table" }, // Keep if implemented
        { value: "melt", label: "Melt (Wide to Long)" } // Keep if implemented
      ]
    },
    {
      groupName: "Joining & Merging",
      operations: [
        { value: "merge", label: "Merge Datasets" }
      ]
    },
    // --- NEW GROUP ---
    {
        groupName: "Custom Function",
        operations: [
            { value: "apply_lambda", label: "Apply Lambda Function" }
        ]
    }
  ];

  // --- Lambda Presets ---
  const lambdaPresets = [
      { label: "x * 2", value: "lambda x: x * 2" },
      { label: "x + 10", value: "lambda x: x + 10" },
      { label: "Square (x**2)", value: "lambda x: x ** 2" },
      { label: "To Uppercase", value: "lambda x: str(x).upper()" },
      { label: "To Lowercase", value: "lambda x: str(x).lower()" },
      { label: "String Length", value: "lambda x: len(str(x))" },
      { label: "Extract Year (Date)", value: "lambda x: pd.to_datetime(x, errors='coerce').year" },
      { label: "Extract Month (Date)", value: "lambda x: pd.to_datetime(x, errors='coerce').month" },
      { label: "Is Null?", value: "lambda x: pd.isna(x)" },
  ];

  const applyLambdaPreset = (presetValue) => {
      handleParamChange('lambda_str', presetValue);
  };

  const renderOperationForm = () => {
    // Use safeColumns which is guaranteed to be an array
    const currentColumns = safeColumns;

    switch (operation) {
      case 'filter':
        return (
          <>
            {/* ... (existing filter form - use currentColumns) ... */}
             <div className="mb-4">
              <label className="input-label">Column:</label>
              <select
                className="select-base"
                value={params.column || ''}
                onChange={(e) => handleParamChange('column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {currentColumns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            {/* ... rest of filter form ... */}
          </>
        );

      case 'select_columns':
         return (
           <>
             <div className="mb-4">
               <label className="input-label">Columns to Select:</label>
               <div className="max-h-60 overflow-y-auto border rounded-md p-2 bg-white">
                 {currentColumns.map((col, index) => ( // Use currentColumns
                   <div key={index} className="flex items-center mb-2">
                     <input
                       type="checkbox"
                       id={`col-${index}`}
                       value={col}
                       checked={params.selected_columns?.includes(col) || false}
                       onChange={(e) => {
                         const selectedCols = params.selected_columns || [];
                         if (e.target.checked) {
                           handleParamChange('selected_columns', [...selectedCols, col]);
                         } else {
                           handleParamChange('selected_columns', selectedCols.filter(c => c !== col));
                         }
                       }}
                       className="mr-2 h-4 w-4 text-coffee focus:ring-coffee-light border-maid-gray"
                     />
                     <label htmlFor={`col-${index}`} className="text-sm text-maid-choco">{col}</label>
                   </div>
                 ))}
               </div>
             </div>
           </>
         );

      // ... (other existing cases - ensure they use currentColumns) ...

      // --- NEW CASE for apply_lambda ---
      case 'apply_lambda':
        return (
          <>
            <div className="mb-4">
              <label className="input-label">Apply to Column:</label>
              <select
                className="select-base"
                value={params.column || ''}
                onChange={(e) => handleParamChange('column', e.target.value)}
                required
              >
                <option value="">Select target column</option>
                {currentColumns.map((col, index) => ( // Use currentColumns
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div className="mb-4">
              <label className="input-label">Lambda Function:</label>
              <textarea
                className="input-base font-mono text-sm"
                rows="3"
                placeholder="e.g., lambda x: x * 100 / df['total'].sum()"
                value={params.lambda_str || ''}
                onChange={(e) => handleParamChange('lambda_str', e.target.value)}
                required
              />
              <p className="text-xs text-maid-gray-dark mt-1">
                Define a function applied to each element in the column. Use 'x' as the variable.
                You can use `pd` and `np`.
                <strong className="text-red-600"> Security Note: Executes on the server.</strong>
              </p>
            </div>
            {/* Lambda Presets */}
            <div className="mb-4">
                <label className="input-label text-xs">Presets:</label>
                <div className="flex flex-wrap gap-1">
                    {lambdaPresets.map(preset => (
                        <button
                            key={preset.label}
                            type="button"
                            onClick={() => applyLambdaPreset(preset.value)}
                            className="btn btn-outline btn-xs"
                            title={preset.value}
                        >
                            {preset.label}
                        </button>
                    ))}
                </div>
            </div>
            <div className="mb-4">
              <label className="input-label">New Column Name (Optional):</label>
              <input
                className="input-base"
                type="text"
                placeholder="Leave blank to modify original column"
                value={params.new_column_name || ''}
                onChange={(e) => handleParamChange('new_column_name', e.target.value)}
              />
            </div>
          </>
        );

      default:
        return (
          <div className="text-maid-gray-dark italic text-center p-4">
            Select an operation to see parameters.
          </div>
        );
    }
  };

  return (
    <div className="card bg-white p-4 rounded-lg shadow-soft border border-maid-gray-light">
      <h2 className="card-header">Data Operations Panel</h2>

      <form onSubmit={handleSubmit}>
        {/* Operation Group Selection */}
        <div className="mb-4">
          <label className="input-label">Operation Category:</label>
          <div className="flex flex-wrap gap-2 mb-2">
            {operationGroups.map((group, index) => (
              <button
                key={index}
                type="button"
                className={`py-1 px-3 rounded-md text-sm font-medium ${
                  showAdvanced === group.groupName
                    ? 'bg-coffee text-white'
                    : 'bg-maid-cream text-maid-choco hover:bg-maid-cream-dark'
                }`}
                onClick={() => setShowAdvanced(
                  showAdvanced === group.groupName ? false : group.groupName
                )}
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
              {operationGroups.find(g => g.groupName === showAdvanced)?.operations.map((op, index) => (
                <div
                  key={index}
                  className={`p-2 border rounded-md cursor-pointer text-sm ${
                    operation === op.value
                      ? 'bg-coffee-light bg-opacity-20 border-coffee'
                      : 'hover:bg-maid-cream border-maid-gray-light'
                  }`}
                  onClick={() => {
                    setOperation(op.value);
                    // setParams({}); // Reset params when changing operation
                  }}
                >
                  {op.label}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Fallback Simple Selector (Optional) */}
        {!showAdvanced && (
          <div className="mb-4">
            <label className="input-label">Common Operations:</label>
            <select
              className="select-base"
              value={operation}
              onChange={(e) => {
                setOperation(e.target.value);
                // setParams({}); // Reset params when changing operation
                // Automatically show the relevant group?
                const group = operationGroups.find(g => g.operations.some(op => op.value === e.target.value));
                setShowAdvanced(group?.groupName || false);
              }}
            >
              {/* Populate with common ops or all ops */}
              {operationGroups.flatMap(g => g.operations).map(op => (
                  <option key={op.value} value={op.value}>{op.label}</option>
              ))}
            </select>
          </div>
        )}

        {/* Render the form for the selected operation */}
        <div className="mb-6 p-4 bg-maid-cream-light rounded-md border border-maid-gray-light">
          {renderOperationForm()}
        </div>

        {/* Submit Button */}
        <div className="flex items-center justify-end">
          <button
            type="submit"
            className="btn btn-coffee"
            disabled={isLoading || !currentDataset} // Disable if no dataset selected
            title={!currentDataset ? "Select a dataset first" : `Apply ${operation}`}
          >
            {isLoading ? 'Processing...' : 'Apply Operation'}
          </button>
        </div>
      </form>
    </div>
  );
};

export default EnhancedOperationsPanel;