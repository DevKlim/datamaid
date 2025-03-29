import React, { useState, useEffect } from 'react';

const EnhancedDataTable = ({ data, columns, totalRows, isPreview = true }) => {
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [showAllRows, setShowAllRows] = useState(false);
  const [expandedView, setExpandedView] = useState(false);
  
  useEffect(() => {
    // Reset to first page when data changes
    setPage(0);
  }, [data]);

  if (!data || data.length === 0) {
    return (
      <div className="p-4 bg-white rounded shadow">
        <p className="text-gray-500">No data to display. Upload a dataset to get started.</p>
      </div>
    );
  }

  // Calculate pagination
  const pageCount = Math.ceil(data.length / rowsPerPage);
  const displayedData = showAllRows 
    ? data 
    : data.slice(page * rowsPerPage, (page * rowsPerPage) + rowsPerPage);

  const handlePrevPage = () => {
    setPage(prev => Math.max(0, prev - 1));
  };

  const handleNextPage = () => {
    setPage(prev => Math.min(pageCount - 1, prev + 1));
  };

  const getColumnWidth = (colName) => {
    // Set a minimum width
    return { minWidth: '120px' };
  };
  
  const formatCellValue = (value) => {
    if (value === null || value === undefined) return 'N/A';
    
    // Check if it's a number and format it
    if (typeof value === 'number') {
      // Format to 4 decimal places if it's a float
      if (value % 1 !== 0) {
        return value.toLocaleString(undefined, { 
          minimumFractionDigits: 0,
          maximumFractionDigits: 4 
        });
      }
      return value.toLocaleString();
    }
    
    // Return the value as a string
    return String(value);
  };

  return (
    <div className={`bg-white rounded shadow ${expandedView ? 'fixed inset-4 z-50 overflow-auto' : ''}`}>
      <div className="flex justify-between items-center p-3 border-b">
        <div className="flex items-center">
          <h2 className="text-lg font-semibold mr-2">
            Data Preview
          </h2>
          {isPreview && totalRows > data.length && (
            <span className="bg-blue-100 text-blue-800 text-xs font-medium px-2 py-0.5 rounded">
              Showing {data.length} of {totalRows.toLocaleString()} rows
            </span>
          )}
        </div>
        
        <div className="flex items-center space-x-2">
          {!showAllRows && (
            <div className="flex items-center">
              <button
                onClick={handlePrevPage}
                disabled={page === 0}
                className={`p-1 rounded ${page === 0 ? 'text-gray-400' : 'text-gray-600 hover:bg-gray-100'}`}
              >
                &lt; Prev
              </button>
              <span className="mx-2 text-sm">
                Page {page + 1} of {pageCount}
              </span>
              <button
                onClick={handleNextPage}
                disabled={page >= pageCount - 1}
                className={`p-1 rounded ${page >= pageCount - 1 ? 'text-gray-400' : 'text-gray-600 hover:bg-gray-100'}`}
              >
                Next &gt;
              </button>
            </div>
          )}
          
          <select
            value={rowsPerPage}
            onChange={(e) => setRowsPerPage(Number(e.target.value))}
            className="text-sm border rounded p-1"
            disabled={showAllRows}
          >
            <option value={10}>10 rows</option>
            <option value={25}>25 rows</option>
            <option value={50}>50 rows</option>
            <option value={100}>100 rows</option>
          </select>
          
          <button
            onClick={() => setShowAllRows(!showAllRows)}
            className="text-sm border rounded p-1 hover:bg-gray-100"
          >
            {showAllRows ? 'Show Pages' : 'Show All'}
          </button>
          
          <button
            onClick={() => setExpandedView(!expandedView)}
            className="text-sm border rounded p-1 hover:bg-gray-100"
          >
            {expandedView ? 'Exit Fullscreen' : 'Fullscreen'}
          </button>
        </div>
      </div>
      
      <div className={`overflow-x-auto ${expandedView ? 'h-[calc(100%-80px)]' : 'max-h-96'}`}>
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50 sticky top-0">
            <tr>
              {columns.map((column, index) => (
                <th 
                  key={index}
                  scope="col" 
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                  style={getColumnWidth(column)}
                >
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {displayedData.map((row, rowIndex) => (
              <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                {columns.map((column, colIndex) => (
                  <td 
                    key={`${rowIndex}-${colIndex}`} 
                    className="px-6 py-4 whitespace-nowrap text-sm text-gray-500"
                  >
                    {formatCellValue(row[column])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {expandedView && (
        <div className="absolute bottom-0 left-0 right-0 p-2 bg-gray-100 border-t">
          <button
            onClick={() => setExpandedView(false)}
            className="w-full py-2 bg-blue-500 hover:bg-blue-600 text-white rounded"
          >
            Close Fullscreen View
          </button>
        </div>
      )}
    </div>
  );
};

export default EnhancedDataTable;