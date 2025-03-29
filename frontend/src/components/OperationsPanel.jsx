import React, { useState } from 'react';

const EnhancedOperationsPanel = ({ columns, onOperationSubmit }) => {
  const [operation, setOperation] = useState('filter');
  const [params, setParams] = useState({});
  const [showAdvanced, setShowAdvanced] = useState(false);

  const handleParamChange = (name, value) => {
    setParams(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    onOperationSubmit(operation, params);
  };

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
        { value: "groupby_multi_agg", label: "Group By with Multiple Aggregations" },
        { value: "groupby_select", label: "Select from Group By Result" }
      ]
    },
    {
      groupName: "Reshaping",
      operations: [
        { value: "pivot_table", label: "Pivot Table" },
        { value: "melt", label: "Melt (Wide to Long)" },
        { value: "pivot", label: "Pivot (Long to Wide)" },
        { value: "stack", label: "Stack Columns" },
        { value: "unstack", label: "Unstack Rows" }
      ]
    },
    {
      groupName: "Index Operations",
      operations: [
        { value: "set_index", label: "Set Index" },
        { value: "reset_index", label: "Reset Index" }
      ]
    },
    {
      groupName: "Joining & Merging",
      operations: [
        { value: "join", label: "Join Datasets" },
        { value: "merge", label: "Merge Datasets" }
      ]
    }
  ];

  const renderOperationForm = () => {
    switch (operation) {
      case 'filter':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Column:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.column || ''}
                onChange={(e) => handleParamChange('column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Operator:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.operator || ''}
                onChange={(e) => handleParamChange('operator', e.target.value)}
                required
              >
                <option value="">Select an operator</option>
                <option value="==">Equals (==)</option>
                <option value="!=">Not equals (!=)</option>
                <option value=">">Greater than (&gt;)</option>
                <option value="<">Less than (&lt;)</option>
                <option value=">=">Greater than or equal (&gt;=)</option>
                <option value="<=">Less than or equal (&lt;=)</option>
                <option value="contains">Contains</option>
                <option value="startswith">Starts with</option>
                <option value="endswith">Ends with</option>
                <option value="regex">Regular Expression</option>
              </select>
            </div>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Value:
              </label>
              <input
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                type="text"
                value={params.value || ''}
                onChange={(e) => handleParamChange('value', e.target.value)}
                required
              />
              {params.operator === 'regex' && (
                <div className="mt-1 text-sm text-gray-500">
                  Enter a valid regular expression (e.g., ^A.*e$ matches strings starting with 'A' and ending with 'e')
                </div>
              )}
            </div>
          </>
        );
      
      case 'select_columns':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Columns to Select:
              </label>
              <div className="max-h-60 overflow-y-auto border rounded p-2">
                {columns.map((col, index) => (
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
                      className="mr-2"
                    />
                    <label htmlFor={`col-${index}`}>{col}</label>
                  </div>
                ))}
              </div>
            </div>
          </>
        );

      case 'sort':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Sort by Column:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.sort_column || ''}
                onChange={(e) => handleParamChange('sort_column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Sort Order:
              </label>
              <div className="flex items-center">
                <input
                  type="radio"
                  id="ascending"
                  name="sort_order"
                  value="ascending"
                  checked={params.sort_order === 'ascending'}
                  onChange={() => handleParamChange('sort_order', 'ascending')}
                  className="mr-2"
                />
                <label htmlFor="ascending" className="mr-4">Ascending</label>
                
                <input
                  type="radio"
                  id="descending"
                  name="sort_order"
                  value="descending"
                  checked={params.sort_order === 'descending'}
                  onChange={() => handleParamChange('sort_order', 'descending')}
                  className="mr-2"
                />
                <label htmlFor="descending">Descending</label>
              </div>
            </div>
          </>
        );
      
      case 'groupby':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Group By Column:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.group_column || ''}
                onChange={(e) => handleParamChange('group_column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Aggregation Column:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.agg_column || ''}
                onChange={(e) => handleParamChange('agg_column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Aggregation Function:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.agg_function || 'mean'}
                onChange={(e) => handleParamChange('agg_function', e.target.value)}
                required
              >
                <option value="mean">Mean</option>
                <option value="sum">Sum</option>
                <option value="count">Count</option>
                <option value="min">Min</option>
                <option value="max">Max</option>
                <option value="median">Median</option>
                <option value="std">Standard Deviation</option>
                <option value="var">Variance</option>
              </select>
            </div>
          </>
        );
      
      case 'groupby_multi':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Group By Columns:
              </label>
              <div className="max-h-40 overflow-y-auto border rounded p-2">
                {columns.map((col, index) => (
                  <div key={index} className="flex items-center mb-2">
                    <input
                      type="checkbox"
                      id={`grpcol-${index}`}
                      value={col}
                      checked={params.group_columns?.includes(col) || false}
                      onChange={(e) => {
                        const selectedCols = params.group_columns || [];
                        if (e.target.checked) {
                          handleParamChange('group_columns', [...selectedCols, col]);
                        } else {
                          handleParamChange('group_columns', selectedCols.filter(c => c !== col));
                        }
                      }}
                      className="mr-2"
                    />
                    <label htmlFor={`grpcol-${index}`}>{col}</label>
                  </div>
                ))}
              </div>
            </div>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Aggregation Column:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.agg_column || ''}
                onChange={(e) => handleParamChange('agg_column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Aggregation Function:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.agg_function || 'mean'}
                onChange={(e) => handleParamChange('agg_function', e.target.value)}
                required
              >
                <option value="mean">Mean</option>
                <option value="sum">Sum</option>
                <option value="count">Count</option>
                <option value="min">Min</option>
                <option value="max">Max</option>
                <option value="median">Median</option>
                <option value="std">Standard Deviation</option>
                <option value="var">Variance</option>
              </select>
            </div>
          </>
        );
      
      case 'groupby_multi_agg':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Group By Column:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.group_column || ''}
                onChange={(e) => handleParamChange('group_column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Aggregations:
              </label>
              
              {(params.aggregations || []).map((agg, index) => (
                <div key={index} className="flex items-center mb-2 space-x-2">
                  <select
                    className="shadow border rounded py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                    value={agg.column || ''}
                    onChange={(e) => {
                      const newAggs = [...(params.aggregations || [])];
                      newAggs[index] = { ...newAggs[index], column: e.target.value };
                      handleParamChange('aggregations', newAggs);
                    }}
                    required
                  >
                    <option value="">Select column</option>
                    {columns.map((col, idx) => (
                      <option key={idx} value={col}>{col}</option>
                    ))}
                  </select>
                  
                  <select
                    className="shadow border rounded py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                    value={agg.function || ''}
                    onChange={(e) => {
                      const newAggs = [...(params.aggregations || [])];
                      newAggs[index] = { ...newAggs[index], function: e.target.value };
                      handleParamChange('aggregations', newAggs);
                    }}
                    required
                  >
                    <option value="">Select function</option>
                    <option value="mean">Mean</option>
                    <option value="sum">Sum</option>
                    <option value="count">Count</option>
                    <option value="min">Min</option>
                    <option value="max">Max</option>
                  </select>
                  
                  <button
                    type="button"
                    className="bg-red-500 hover:bg-red-700 text-white font-bold py-1 px-2 rounded focus:outline-none focus:shadow-outline"
                    onClick={() => {
                      const newAggs = [...(params.aggregations || [])];
                      newAggs.splice(index, 1);
                      handleParamChange('aggregations', newAggs);
                    }}
                  >
                    X
                  </button>
                </div>
              ))}
              
              <button
                type="button"
                className="mt-2 bg-green-500 hover:bg-green-700 text-white font-bold py-1 px-2 rounded focus:outline-none focus:shadow-outline"
                onClick={() => {
                  const currentAggs = params.aggregations || [];
                  handleParamChange('aggregations', [...currentAggs, { column: '', function: '' }]);
                }}
              >
                Add Aggregation
              </button>
            </div>
          </>
        );
      
      case 'rename':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Column Renaming:
              </label>
              
              {(params.renames || []).map((rename, index) => (
                <div key={index} className="flex items-center mb-2 space-x-2">
                  <select
                    className="shadow border rounded py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                    value={rename.old_name || ''}
                    onChange={(e) => {
                      const newRenames = [...(params.renames || [])];
                      newRenames[index] = { ...newRenames[index], old_name: e.target.value };
                      handleParamChange('renames', newRenames);
                    }}
                    required
                  >
                    <option value="">Select original column</option>
                    {columns.map((col, idx) => (
                      <option key={idx} value={col}>{col}</option>
                    ))}
                  </select>
                  
                  <span className="text-gray-600">→</span>
                  
                  <input
                    className="shadow border rounded py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                    type="text"
                    placeholder="New name"
                    value={rename.new_name || ''}
                    onChange={(e) => {
                      const newRenames = [...(params.renames || [])];
                      newRenames[index] = { ...newRenames[index], new_name: e.target.value };
                      handleParamChange('renames', newRenames);
                    }}
                    required
                  />
                  
                  <button
                    type="button"
                    className="bg-red-500 hover:bg-red-700 text-white font-bold py-1 px-2 rounded focus:outline-none focus:shadow-outline"
                    onClick={() => {
                      const newRenames = [...(params.renames || [])];
                      newRenames.splice(index, 1);
                      handleParamChange('renames', newRenames);
                    }}
                  >
                    X
                  </button>
                </div>
              ))}
              
              <button
                type="button"
                className="mt-2 bg-green-500 hover:bg-green-700 text-white font-bold py-1 px-2 rounded focus:outline-none focus:shadow-outline"
                onClick={() => {
                  const currentRenames = params.renames || [];
                  handleParamChange('renames', [...currentRenames, { old_name: '', new_name: '' }]);
                }}
              >
                Add Column Rename
              </button>
            </div>
          </>
        );
      
      case 'drop_columns':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Columns to Drop:
              </label>
              <div className="max-h-60 overflow-y-auto border rounded p-2">
                {columns.map((col, index) => (
                  <div key={index} className="flex items-center mb-2">
                    <input
                      type="checkbox"
                      id={`dropcol-${index}`}
                      value={col}
                      checked={params.drop_columns?.includes(col) || false}
                      onChange={(e) => {
                        const dropCols = params.drop_columns || [];
                        if (e.target.checked) {
                          handleParamChange('drop_columns', [...dropCols, col]);
                        } else {
                          handleParamChange('drop_columns', dropCols.filter(c => c !== col));
                        }
                      }}
                      className="mr-2"
                    />
                    <label htmlFor={`dropcol-${index}`}>{col}</label>
                  </div>
                ))}
              </div>
            </div>
          </>
        );
      
      case 'pivot_table':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Index Column (rows):
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.index_col || ''}
                onChange={(e) => handleParamChange('index_col', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Column (for column headers):
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.columns_col || ''}
                onChange={(e) => handleParamChange('columns_col', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Values Column:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.values_col || ''}
                onChange={(e) => handleParamChange('values_col', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Aggregation Function:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.pivot_agg_function || 'mean'}
                onChange={(e) => handleParamChange('pivot_agg_function', e.target.value)}
                required
              >
                <option value="mean">Mean</option>
                <option value="sum">Sum</option>
                <option value="count">Count</option>
                <option value="min">Min</option>
                <option value="max">Max</option>
              </select>
            </div>
          </>
        );
      
      case 'melt':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                ID Variables (columns to keep as is):
              </label>
              <div className="max-h-40 overflow-y-auto border rounded p-2">
                {columns.map((col, index) => (
                  <div key={index} className="flex items-center mb-2">
                    <input
                      type="checkbox"
                      id={`id-var-${index}`}
                      value={col}
                      checked={params.id_vars?.includes(col) || false}
                      onChange={(e) => {
                        const idVars = params.id_vars || [];
                        if (e.target.checked) {
                          handleParamChange('id_vars', [...idVars, col]);
                        } else {
                          handleParamChange('id_vars', idVars.filter(c => c !== col));
                        }
                      }}
                      className="mr-2"
                    />
                    <label htmlFor={`id-var-${index}`}>{col}</label>
                  </div>
                ))}
              </div>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Value Variables (columns to "unpivot"):
              </label>
              <div className="max-h-40 overflow-y-auto border rounded p-2">
                {columns.map((col, index) => (
                  <div key={index} className="flex items-center mb-2">
                    <input
                      type="checkbox"
                      id={`value-var-${index}`}
                      value={col}
                      checked={params.value_vars?.includes(col) || false}
                      onChange={(e) => {
                        const valueVars = params.value_vars || [];
                        if (e.target.checked) {
                          handleParamChange('value_vars', [...valueVars, col]);
                        } else {
                          handleParamChange('value_vars', valueVars.filter(c => c !== col));
                        }
                      }}
                      className="mr-2"
                    />
                    <label htmlFor={`value-var-${index}`}>{col}</label>
                  </div>
                ))}
              </div>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Variable Name (name for the column containing former column names):
              </label>
              <input
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                type="text"
                value={params.var_name || 'variable'}
                onChange={(e) => handleParamChange('var_name', e.target.value)}
                required
              />
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Value Name (name for the column containing values):
              </label>
              <input
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                type="text"
                value={params.value_name || 'value'}
                onChange={(e) => handleParamChange('value_name', e.target.value)}
                required
              />
            </div>
          </>
        );
      
      case 'set_index':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Set Column as Index:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.index_column || ''}
                onChange={(e) => handleParamChange('index_column', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={params.drop || false}
                  onChange={(e) => handleParamChange('drop', e.target.checked)}
                  className="mr-2"
                />
                <span className="text-gray-700 text-sm font-bold">
                  Drop column after setting as index
                </span>
              </label>
            </div>
          </>
        );
      
      case 'reset_index':
        return (
          <>
            <div className="mb-4">
              <label className="flex items-center">
                <input
                  type="checkbox"
                  checked={params.drop_index || false}
                  onChange={(e) => handleParamChange('drop_index', e.target.checked)}
                  className="mr-2"
                />
                <span className="text-gray-700 text-sm font-bold">
                  Drop the index column (don't include it in the result)
                </span>
              </label>
            </div>
          </>
        );
      
      case 'join':
      case 'merge':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Right Dataset:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.right_dataset || ''}
                onChange={(e) => handleParamChange('right_dataset', e.target.value)}
                required
              >
                <option value="">Select another dataset</option>
                {/* This would be populated with available datasets */}
                <option value="dataset1">Dataset 1</option>
                <option value="dataset2">Dataset 2</option>
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Join Type:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.join_type || 'inner'}
                onChange={(e) => handleParamChange('join_type', e.target.value)}
                required
              >
                <option value="inner">Inner Join</option>
                <option value="left">Left Join</option>
                <option value="right">Right Join</option>
                <option value="outer">Full Outer Join</option>
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Left Key (current dataset):
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.left_on || ''}
                onChange={(e) => handleParamChange('left_on', e.target.value)}
                required
              >
                <option value="">Select a column</option>
                {columns.map((col, index) => (
                  <option key={index} value={col}>{col}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Right Key (other dataset):
              </label>
              <input
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                type="text"
                placeholder="Column name in right dataset"
                value={params.right_on || ''}
                onChange={(e) => handleParamChange('right_on', e.target.value)}
                required
              />
            </div>
          </>
        );
      
      default:
        return (
          <div className="text-gray-600 italic">
            Please select an operation to see its parameters.
          </div>
        );
    }
  };

  return (
    <div className="bg-white p-4 rounded shadow">
      <h2 className="text-xl font-semibold mb-4">Data Operations</h2>
      
      <form onSubmit={handleSubmit}>
        <div className="mb-4">
          <label className="block text-gray-700 text-sm font-bold mb-2">
            Operation Category:
          </label>
          <div className="flex flex-wrap gap-2 mb-2">
            {operationGroups.map((group, index) => (
              <button
                key={index}
                type="button"
                className={`py-1 px-3 rounded text-sm font-medium ${
                  showAdvanced === group.groupName 
                    ? 'bg-blue-600 text-white' 
                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
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
        
        {showAdvanced && (
          <div className="mb-4">
            <label className="block text-gray-700 text-sm font-bold mb-2">
              Operation:
            </label>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
              {operationGroups.find(g => g.groupName === showAdvanced)?.operations.map((op, index) => (
                <div 
                  key={index}
                  className={`p-2 border rounded cursor-pointer ${
                    operation === op.value 
                      ? 'bg-blue-100 border-blue-500' 
                      : 'hover:bg-gray-100'
                  }`}
                  onClick={() => {
                    setOperation(op.value);
                    setParams({});
                  }}
                >
                  {op.label}
                </div>
              ))}
            </div>
          </div>
        )}
        
        {!showAdvanced && (
          <div className="mb-4">
            <label className="block text-gray-700 text-sm font-bold mb-2">
              Common Operations:
            </label>
            <select
              className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
              value={operation}
              onChange={(e) => {
                setOperation(e.target.value);
                setParams({});
              }}
            >
              <option value="filter">Filter Rows</option>
              <option value="select_columns">Select Columns</option>
              <option value="sort">Sort Values</option>
              <option value="groupby">Group By</option>
              <option value="rename">Rename Columns</option>
            </select>
          </div>
        )}
        
        <div className="mb-6 p-4 bg-gray-50 rounded">
          {renderOperationForm()}
        </div>
        
        <div className="flex items-center justify-end">
          <button
            type="submit"
            className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded focus:outline-none focus:shadow-outline"
          >
            Apply
          </button>
        </div>
      </form>
    </div>
  );
};

export default EnhancedOperationsPanel;