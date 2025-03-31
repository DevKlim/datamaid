// src/components/DataTable.jsx
import React, { useState, useEffect } from 'react';

const EnhancedDataTable = ({
  data,
  columns,
  totalRows,
  isPreview = true,
  onHeaderClick
}) => {
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [showAllRows, setShowAllRows] = useState(false);
  const [expandedView, setExpandedView] = useState(false);

  useEffect(() => {
    setPage(0);
  }, [data]);

  // Ensure columns is always an array before using .map
  const safeColumns = Array.isArray(columns) ? columns : [];

  if (!data || data.length === 0) {
    return (
      <div className="card p-4 bg-white rounded-lg border border-maid-gray-light shadow-soft flex flex-col items-center justify-center h-48">
        <div className="text-maid-choco-dark text-center mb-2">No data to display</div>
        <div className="text-maid-gray-dark text-sm">Upload or select a dataset to begin ♡</div>
      </div>
    );
  }

  // Calculate pagination based on available data length
  const dataLength = data.length;
  const pageCount = rowsPerPage > 0 ? Math.ceil(dataLength / rowsPerPage) : 1;
  const displayedData = showAllRows
    ? data
    : data.slice(page * rowsPerPage, (page * rowsPerPage) + rowsPerPage);

  const handlePrevPage = () => {
    setPage(prev => Math.max(0, prev - 1));
  };

  const handleNextPage = () => {
    setPage(prev => Math.min(pageCount - 1, prev + 1));
  };

  // Handle page calculation correctly when showing all rows
  const currentPageInfo = showAllRows
    ? `Showing all ${dataLength.toLocaleString()} rows`
    : `Page ${page + 1} of ${pageCount}`;

  // Handle next button disabling correctly
  const isLastPage = showAllRows || (page + 1 >= pageCount);

  const getColumnWidth = (colName) => {
    const baseWidth = 120;
    const extraWidth = colName.length > 15 ? (colName.length - 15) * 6 : 0;
    const finalWidth = Math.min(300, baseWidth + extraWidth);
    return { minWidth: `${finalWidth}px` };
  };

  const formatCellValue = (value) => {
    if (value === null || value === undefined || value === 'NaN' || value === 'NaT') 
      return <span className="text-maid-gray-dark italic">N/A</span>;

    if (typeof value === 'number') {
      if (value % 1 !== 0) {
        return value.toLocaleString(undefined, {
          minimumFractionDigits: 0,
          maximumFractionDigits: 4
        });
      }
      return value.toLocaleString();
    }

    // Truncate long strings for preview
    const stringValue = String(value);
    if (stringValue.length > 100) {
       return <span title={stringValue}>{stringValue.substring(0, 100)}...</span>;
    }
    return stringValue;
  };

  return (
    <div className={`bg-white rounded-lg shadow-soft border border-maid-gray-light ${expandedView ? 'fixed inset-2 sm:inset-4 z-50 overflow-auto flex flex-col' : 'flex flex-col'}`}>
      {/* Header Controls */}
      <div className="flex flex-wrap justify-between items-center p-3 border-b border-maid-gray gap-2">
        <div className="flex items-center flex-shrink-0">
          <h2 className="text-lg font-semibold text-maid-choco-dark mr-2">
            Data Preview
          </h2>
          {isPreview && totalRows > dataLength && (
            <span className="bg-maid-blue-light text-maid-choco-dark text-xs font-medium px-2 py-0.5 rounded-full whitespace-nowrap">
              Showing {dataLength.toLocaleString()} of {totalRows.toLocaleString()} rows
            </span>
          )}
           {isPreview && totalRows <= dataLength && dataLength > 0 && (
             <span className="bg-green-100 text-green-800 text-xs font-medium px-2 py-0.5 rounded-full whitespace-nowrap">
               Showing all {totalRows.toLocaleString()} rows
             </span>
           )}
        </div>

        {/* Controls */}
        <div className="flex items-center flex-wrap justify-end space-x-2 gap-y-1 flex-grow">
          {!showAllRows && dataLength > 0 && (
            <div className="flex items-center">
              <button
                onClick={handlePrevPage}
                disabled={page === 0}
                className="p-1 rounded-md text-sm text-maid-choco hover:bg-maid-cream disabled:text-maid-gray-dark disabled:cursor-not-allowed"
              >
                &lt; Prev
              </button>
              <span className="mx-2 text-xs whitespace-nowrap text-maid-choco">
                {currentPageInfo}
              </span>
              <button
                onClick={handleNextPage}
                disabled={isLastPage}
                className="p-1 rounded-md text-sm text-maid-choco hover:bg-maid-cream disabled:text-maid-gray-dark disabled:cursor-not-allowed"
              >
                Next &gt;
              </button>
            </div>
          )}

          <select
            value={rowsPerPage}
            onChange={(e) => { setRowsPerPage(Number(e.target.value)); setPage(0); }}
            className="text-sm border rounded-md p-1 bg-white text-maid-choco border-maid-gray-light hover:border-maid-gray disabled:bg-maid-cream-light"
            disabled={showAllRows}
          >
            <option value={10}>10 rows</option>
            <option value={25}>25 rows</option>
            <option value={50}>50 rows</option>
            <option value={100}>100 rows</option>
          </select>

          <button
            onClick={() => setShowAllRows(!showAllRows)}
            className="text-sm border border-maid-gray-light rounded-md p-1 px-2 hover:bg-maid-cream-light text-maid-choco"
          >
            {showAllRows ? 'Paginate' : `Show All (${dataLength.toLocaleString()})`}
          </button>

          <button
            onClick={() => setExpandedView(!expandedView)}
            className="text-sm border border-maid-gray-light rounded-md p-1 px-2 hover:bg-maid-cream-light text-maid-choco"
          >
            {expandedView ? 'Exit Fullscreen' : 'Fullscreen'}
          </button>
        </div>
      </div>

      {/* Table Content */}
      <div className={`overflow-auto ${expandedView ? 'flex-grow' : 'max-h-96'}`}>
        <table className="min-w-full divide-y divide-maid-gray-light border-collapse">
          <thead className="bg-maid-cream-light sticky top-0 z-10">
            <tr>
              {safeColumns.map((column, index) => (
                <th
                  key={index}
                  scope="col"
                  className="table-header px-4 py-2 text-left text-xs font-bold text-maid-choco-dark uppercase tracking-wider sticky top-0 bg-maid-cream-light border-b border-maid-gray hover:bg-maid-cream transition-colors duration-150"
                  style={getColumnWidth(column)}
                  onClick={() => onHeaderClick && onHeaderClick(column)}
                  title={`Click for stats: ${column}`}
                >
                   <div className="flex items-center cursor-pointer">
                        {column}
                   </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-maid-gray-light">
            {displayedData.length > 0 ? displayedData.map((row, rowIndex) => (
              <tr key={rowIndex} className={`hover:bg-maid-cream-light transition-colors duration-150 ${rowIndex % 2 === 0 ? 'bg-white' : 'bg-maid-cream-light bg-opacity-30'}`}>
                {safeColumns.map((column, colIndex) => (
                  <td
                    key={`${rowIndex}-${colIndex}`}
                    className="table-cell px-4 py-2 whitespace-nowrap text-sm text-maid-choco border-b border-maid-gray-light"
                  >
                    {formatCellValue(row[column])}
                  </td>
                ))}
              </tr>
            )) : (
                 <tr>
                    <td colSpan={safeColumns.length || 1} className="text-center py-10 text-maid-gray-dark italic">
                        No data matching current view or filters.
                    </td>
                 </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Fullscreen Close Button */}
      {expandedView && (
        <div className="p-2 bg-maid-cream-light border-t mt-auto flex-shrink-0">
          <button
            onClick={() => setExpandedView(false)}
            className="w-full py-2 bg-maid-blue hover:bg-maid-blue-dark text-maid-choco-dark rounded-md text-sm"
          >
            Close Fullscreen View
          </button>
        </div>
      )}
    </div>
  );
};

export default EnhancedDataTable;