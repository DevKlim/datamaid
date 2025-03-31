import React, { useState } from 'react';

const EnhancedOperationsPanel = ({ columns, onOperationSubmit, availableDatasets, currentDataset, isLoading }) => {
  const [operation, setOperation] = useState('filter');
  const [params, setParams] = useState({});
  const [showAdvanced, setShowAdvanced] = useState(false);

  const handleParamChange = (name, value) => {
    setParams(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    console.log('DEBUG: Submitting operation:', operation, 'with params:', params);
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
        { value: "groupby_multi_agg", label: "Group By with Multiple Aggregations" }
      ]
    },
    {
      groupName: "Reshaping",
      operations: [
        { value: "pivot_table", label: "Pivot Table" },
        { value: "melt", label: "Melt (Wide to Long)" }
      ]
    },
    {
      groupName: "Joining & Merging",
      operations: [
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
              <label className="input-label">Column:</label>
              <select
                className="select-base"
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
              <label className="input-label">Operator:</label>
              <select
                className="select-base"
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
              <label className="input-label">Value:</label>
              <input
                className="input-base"
                type="text"
                value={params.value || ''}
                onChange={(e) => handleParamChange('value', e.target.value)}
                required
              />
              {params.operator === 'regex' && (
                <div className="mt-1 text-xs text-maid-gray-dark">
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
              <label className="input-label">Columns to Select:</label>
              <div className="max-h-60 overflow-y-auto border rounded-md p-2 bg-white">
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
                      className="mr-2 h-4 w-4 text-coffee focus:ring-coffee-light border-maid-gray"
                    />
                    <label htmlFor={`col-${index}`} className="text-sm text-maid-choco">{col}</label>
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
              <label className="input-label">Sort by Column:</label>
              <select
                className="select-base"
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
              <label className="input-label">Sort Order:</label>
              <div className="flex items-center mt-2">
                <input
                  type="radio"
                  id="ascending"
                  name="sort_order"
                  value="ascending"
                  checked={params.sort_order === 'ascending'}
                  onChange={() => handleParamChange('sort_order', 'ascending')}
                  className="mr-2 h-4 w-4 text-coffee focus:ring-coffee-light border-maid-gray"
                />
                <label htmlFor="ascending" className="text-sm text-maid-choco mr-4">Ascending</label>
                
                <input
                  type="radio"
                  id="descending"
                  name="sort_order"
                  value="descending"
                  checked={params.sort_order === 'descending'}
                  onChange={() => handleParamChange('sort_order', 'descending')}
                  className="mr-2 h-4 w-4 text-coffee focus:ring-coffee-light border-maid-gray"
                />
                <label htmlFor="descending" className="text-sm text-maid-choco">Descending</label>
              </div>
            </div>
          </>
        );
      
      case 'groupby':
        return (
          <>
            <div className="mb-4">
              <label className="input-label">Group By Column:</label>
              <select
                className="select-base"
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
              <label className="input-label">Aggregation Column:</label>
              <select
                className="select-base"
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
              <label className="input-label">Aggregation Function:</label>
              <select
                className="select-base"
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
              <label className="input-label">Group By Columns:</label>
              <div className="max-h-40 overflow-y-auto border rounded-md p-2 bg-white">
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
                      className="mr-2 h-4 w-4 text-coffee focus:ring-coffee-light border-maid-gray"
                    />
                    <label htmlFor={`grpcol-${index}`} className="text-sm text-maid-choco">{col}</label>
                  </div>
                ))}
              </div>
            </div>
            <div className="mb-4">
              <label className="input-label">Aggregation Column:</label>
              <select
                className="select-base"
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
              <label className="input-label">Aggregation Function:</label>
              <select
                className="select-base"
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
      
      case 'rename':
        return (
          <>
            <div className="mb-4">
              <label className="input-label">Column Renaming:</label>
              
              {(params.renames || []).map((rename, index) => (
                <div key={index} className="flex items-center mb-2 space-x-2">
                  <select
                    className="select-base"
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
                  
                  <span className="text-maid-choco">→</span>
                  
                  <input
                    className="input-base"
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
                    className="btn btn-red px-2 py-1"
                    onClick={() => {
                      const newRenames = [...(params.renames || [])];
                      newRenames.splice(index, 1);
                      handleParamChange('renames', newRenames);
                    }}
                  >
                    ✕
                  </button>
                </div>
              ))}
              
              <button
                type="button"
                className="mt-2 btn btn-coffee py-1 px-2 text-xs"
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
              <label className="input-label">Columns to Drop:</label>
              <div className="max-h-60 overflow-y-auto border rounded-md p-2 bg-white">
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
                      className="mr-2 h-4 w-4 text-coffee focus:ring-coffee-light border-maid-gray"
                    />
                    <label htmlFor={`dropcol-${index}`} className="text-sm text-maid-choco">{col}</label>
                  </div>
                ))}
              </div>
            </div>
          </>
        );
      
      case 'merge':
        return (
          <>
            <div className="mb-4">
              <label className="input-label">Right Dataset:</label>
              <select
                className="select-base"
                value={params.right_dataset || ''}
                onChange={(e) => handleParamChange('right_dataset', e.target.value)}
                required
              >
                <option value="">Select another dataset</option>
                {availableDatasets.map((ds, index) => (
                  <option key={index} value={ds}>{ds}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="input-label">Join Type:</label>
              <select
                className="select-base"
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
              <label className="input-label">Left Key (current dataset):</label>
              <select
                className="select-base"
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
              <label className="input-label">Right Key (other dataset):</label>
              <input
                className="input-base"
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
          <div className="text-maid-gray-dark italic text-center p-4">
            Please select an operation to see its parameters.
          </div>
        );
    }
  };

  return (
    <div className="card bg-white p-4 rounded-lg shadow-soft border border-maid-gray-light">
      <h2 className="card-header">Data Operations</h2>
      
      <form onSubmit={handleSubmit}>
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
        
        {showAdvanced && (
          <div className="mb-4">
            <label className="input-label">Operation:</label>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
              {operationGroups.find(g => g.groupName === showAdvanced)?.operations.map((op, index) => (
                <div 
                  key={index}
                  className={`p-2 border rounded-md cursor-pointer ${
                    operation === op.value 
                      ? 'bg-coffee-light bg-opacity-20 border-coffee' 
                      : 'hover:bg-maid-cream border-maid-gray-light'
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
            <label className="input-label">Common Operations:</label>
            <select
              className="select-base"
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
        
        <div className="mb-6 p-4 bg-maid-cream-light rounded-md border border-maid-gray-light">
          {renderOperationForm()}
        </div>
        
        <div className="flex items-center justify-end">
          <button
            type="submit"
            className="btn btn-coffee"
            disabled={isLoading}
          >
            {isLoading ? 'Processing...' : 'Apply'}
          </button>
        </div>
      </form>
    </div>
  );
};

export default EnhancedOperationsPanel;