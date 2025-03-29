import React, { useState } from 'react';

const OperationsPanel = ({ columns, onOperationSubmit }) => {
  const [operation, setOperation] = useState('filter');
  const [params, setParams] = useState({});

  const handleParamChange = (name, value) => {
    setParams(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    onOperationSubmit(operation, params);
  };

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
            </div>
          </>
        );
      
      case 'groupby':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Group By Columns:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.columns ? params.columns[0] || '' : ''}
                onChange={(e) => handleParamChange('columns', [e.target.value])}
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
              </select>
            </div>
          </>
        );
      
      case 'rename':
        return (
          <>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Old Column Name:
              </label>
              <select
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                value={params.old_name || ''}
                onChange={(e) => handleParamChange('old_name', e.target.value)}
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
                New Column Name:
              </label>
              <input
                className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                type="text"
                value={params.new_name || ''}
                onChange={(e) => handleParamChange('new_name', e.target.value)}
                required
              />
            </div>
          </>
        );
      
      default:
        return null;
    }
  };

  return (
    <div className="bg-white p-4 rounded shadow">
      <h2 className="text-xl font-semibold mb-4">Data Operations</h2>
      <form onSubmit={handleSubmit}>
        <div className="mb-4">
          <label className="block text-gray-700 text-sm font-bold mb-2">
            Operation:
          </label>
          <select
            className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
            value={operation}
            onChange={(e) => {
              setOperation(e.target.value);
              setParams({});
            }}
          >
            <option value="filter">Filter</option>
            <option value="groupby">Group By</option>
            <option value="rename">Rename Column</option>
          </select>
        </div>
        
        {renderOperationForm()}
        
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

export default OperationsPanel;