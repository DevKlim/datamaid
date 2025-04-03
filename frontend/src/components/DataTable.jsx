import React, { useState, useEffect } from 'react';

/**
 * EnhancedDataTable Component
 * Displays data (DataFrame or Series) in a paginated table with interactive headers.
 */
const EnhancedDataTable = ({
  data,             // Array of data objects for the current view
  columns,          // Array of column names (strings)
  totalRows,        // Total number of rows in the full dataset
  datasetType,      // 'dataframe' or 'series'
  isPreview = true, // Flag indicating if the data is a preview subset
  onHeaderClick,    // Callback function when a header is clicked (e.g., for stats)
  isLoading,        // Loading state from parent
  datasetName       // Name of the current dataset for context
}) => {
  // --- State for pagination and display ---
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [showAllRows, setShowAllRows] = useState(false); // Toggle between pagination and showing all loaded rows
  const [expandedView, setExpandedView] = useState(false); // Toggle fullscreen view

  // Reset page to 0 when data, datasetName, or datasetType changes
  useEffect(() => {
    setPage(0);
    setShowAllRows(false); // Reset show all toggle as well
  }, [data, datasetName, datasetType]); // Dependency: data array, name, type

  // Ensure columns is always an array to prevent errors if it's initially null/undefined
  const safeColumns = Array.isArray(columns) ? columns : [];

  // --- Render placeholder if no data or loading ---
   if (isLoading && (!data || data.length === 0)) {
     return (
       <div className="card p-4 bg-white rounded-lg border border-maid-gray-light shadow-soft flex flex-col items-center justify-center h-48">
         <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-coffee mb-3"></div>
         <div className="text-maid-choco-dark text-center">Loading data...</div>
       </div>
     );
   }

  if (!data || data.length === 0) {
    return (
      <div className="card p-4 bg-white rounded-lg border border-maid-gray-light shadow-soft flex flex-col items-center justify-center h-48">
        <div className="text-maid-choco-dark text-center mb-2">
            {datasetName ? `Dataset "${datasetName}" is empty` : "No data to display"}
        </div>
        <div className="text-maid-gray-dark text-sm">Upload or select a dataset to begin ♡</div>
      </div>
    );
  }

  // --- Pagination Logic ---
  const dataLength = data.length; // Length of the *currently loaded* data array
  const pageCount = rowsPerPage > 0 ? Math.ceil(dataLength / rowsPerPage) : 1;
  // Determine which slice of data to display based on pagination state
  const displayedData = showAllRows
    ? data // Show all loaded rows if toggled
    : data.slice(page * rowsPerPage, (page * rowsPerPage) + rowsPerPage); // Show current page slice

  // --- Event Handlers for Pagination ---
  const handlePrevPage = () => {
    setPage(prev => Math.max(0, prev - 1));
  };

  const handleNextPage = () => {
    // Ensure we don't go beyond the last page of the *loaded* data
    setPage(prev => Math.min(pageCount - 1, prev + 1));
  };

  // --- Display Information ---
  // Text indicating current page or total rows shown
  const currentPageInfo = showAllRows
    ? `Showing all ${dataLength.toLocaleString()} loaded rows`
    : `Page ${page + 1} of ${pageCount}`;

  // Determine if the 'Next' button should be disabled
  const isLastPage = showAllRows || (page + 1 >= pageCount);

  /**
   * Calculates a minimum width for table columns based on header text length.
   * @param {string} colName - The name of the column.
   * @returns {object} Style object with minWidth property.
   */
  const getColumnWidth = (colName) => {
    const baseWidth = datasetType === 'series' ? 180 : 120; // Wider base for series?
    const extraWidthPerChar = 6;
    const threshold = 15;
    const maxWidth = 350; // Slightly increased max width

    const nameLength = colName ? colName.length : 0;
    const extraWidth = nameLength > threshold
      ? (nameLength - threshold) * extraWidthPerChar
      : 0;
    const finalWidth = Math.min(maxWidth, baseWidth + extraWidth);
    return { minWidth: `${finalWidth}px` };
  };

  /**
   * Formats cell values for display. Handles null/undefined/NaN and truncates long strings.
   * @param {*} value - The raw cell value.
   * @returns {React.ReactNode} The formatted value for display.
   */
  const formatCellValue = (value) => {
    // Handle common null/missing representations
    if (value === null || value === undefined || value === 'NaN' || value === 'NaT' || Number.isNaN(value)) {
      return <span className="text-maid-gray-dark italic">N/A</span>;
    }

    // Format numbers with commas, limiting decimal places for floats
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) {
         return <span className="text-maid-gray-dark italic">{String(value)}</span>; // Show Infinity/-Infinity explicitly
      }
      // Use Intl.NumberFormat for better locale support and precision control
      const formatter = new Intl.NumberFormat(undefined, { // Use user's locale
          minimumFractionDigits: 0,
          maximumFractionDigits: 4 // Show up to 4 decimal places
      });
      return formatter.format(value);
    }

    // Convert other types to string and truncate if very long
    const stringValue = String(value);
    const maxLength = 150; // Max length before truncating
    if (stringValue.length > maxLength) {
       // Show truncated version with ellipsis, full value in tooltip
       return <span title={stringValue}>{stringValue.substring(0, maxLength)}...</span>;
    }
    return stringValue; // Return shorter strings as is
  };

  // --- Main Render ---
  const tableTitle = datasetType === 'series' ? 'Series Preview' : 'DataFrame Preview';

  return (
    <div className={`bg-white rounded-lg shadow-soft border border-maid-gray-light ${expandedView ? 'fixed inset-2 sm:inset-4 z-50 overflow-auto flex flex-col' : 'flex flex-col'}`}>
      {/* Header Section: Title, Row Info, Controls */}
      <div className="flex flex-wrap justify-between items-center p-3 border-b border-maid-gray gap-2">
        {/* Left Side: Title and Row Count Info */}
        <div className="flex items-center flex-shrink-0 gap-2">
          <h2 className="text-lg font-semibold text-maid-choco-dark">
            {tableTitle} {datasetName && <span className="text-base font-normal text-maid-gray-dark">({datasetName})</span>}
          </h2>
          {/* Display info about preview vs. full dataset */}
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
           {datasetType && (
             <span className="bg-purple-100 text-purple-800 text-xs font-medium px-2 py-0.5 rounded-full whitespace-nowrap">
               {datasetType}
             </span>
           )}
        </div>

        {/* Right Side: Pagination and View Controls */}
        <div className="flex items-center flex-wrap justify-end space-x-2 gap-y-1 flex-grow">
          {/* Pagination Controls (only show if not showing all rows) */}
          {!showAllRows && dataLength > 0 && pageCount > 1 && (
            <div className="flex items-center">
              <button
                onClick={handlePrevPage}
                disabled={page === 0} // Disable on first page
                className="p-1 rounded-md text-sm text-maid-choco hover:bg-maid-cream disabled:text-maid-gray-dark disabled:cursor-not-allowed"
                aria-label="Previous page"
              >
                &lt; Prev
              </button>
              <span className="mx-2 text-xs whitespace-nowrap text-maid-choco" aria-live="polite">
                {currentPageInfo}
              </span>
              <button
                onClick={handleNextPage}
                disabled={isLastPage} // Disable on last page
                className="p-1 rounded-md text-sm text-maid-choco hover:bg-maid-cream disabled:text-maid-gray-dark disabled:cursor-not-allowed"
                aria-label="Next page"
              >
                Next &gt;
              </button>
            </div>
          )}

          {/* Rows Per Page Selector */}
          <select
            value={rowsPerPage}
            onChange={(e) => { setRowsPerPage(Number(e.target.value)); setPage(0); }} // Reset page on change
            className="text-sm border rounded-md p-1 bg-white text-maid-choco border-maid-gray-light hover:border-maid-gray disabled:bg-maid-cream-light disabled:cursor-not-allowed"
            disabled={showAllRows} // Disable when showing all rows
            aria-label="Rows per page"
          >
            <option value={10}>10 rows</option>
            <option value={25}>25 rows</option>
            <option value={50}>50 rows</option>
            <option value={100}>100 rows</option>
          </select>

          {/* Show All / Paginate Toggle Button */}
          {dataLength > rowsPerPage && ( // Only show toggle if loaded data exceeds one page
             <button
               onClick={() => setShowAllRows(!showAllRows)}
               className="text-sm border border-maid-gray-light rounded-md p-1 px-2 hover:bg-maid-cream-light text-maid-choco"
               title={showAllRows ? "Switch back to paginated view" : `Show all ${dataLength.toLocaleString()} loaded rows`}
             >
               {showAllRows ? 'Paginate' : `Show All (${dataLength.toLocaleString()})`}
             </button>
          )}

          {/* Fullscreen Toggle Button */}
          <button
            onClick={() => setExpandedView(!expandedView)}
            className="text-sm border border-maid-gray-light rounded-md p-1 px-2 hover:bg-maid-cream-light text-maid-choco"
            title={expandedView ? "Exit fullscreen view" : "Enter fullscreen view"}
          >
            {expandedView ? 'Exit Fullscreen' : 'Fullscreen'}
          </button>
        </div>
      </div>

      {/* Table Content Area */}
      {/* Adjust max-height based on expanded view */}
      <div className={`overflow-auto ${expandedView ? 'flex-grow' : 'max-h-[32rem]'}`}> {/* Increased max-height */}
        <table className="min-w-full divide-y divide-maid-gray-light border-collapse">
          {/* Table Header */}
          <thead className="bg-maid-cream-light sticky top-0 z-10">
            <tr>
              {safeColumns.map((column, index) => (
                <th
                  key={index}
                  scope="col"
                  className="px-4 py-2 text-left text-xs font-bold text-maid-choco-dark tracking-wider sticky top-0 bg-maid-cream-light border-b border-maid-gray hover:bg-maid-cream transition-colors duration-150 cursor-pointer"
                  style={getColumnWidth(column)} // Apply dynamic width
                  onClick={() => onHeaderClick && onHeaderClick(column)} // Trigger callback on click
                  title={`Click for stats: ${column}`} // Tooltip for interaction hint
                >
                  {/* Display the column name directly */}
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          {/* Table Body */}
          <tbody className="bg-white divide-y divide-maid-gray-light">
            {displayedData.length > 0 ? displayedData.map((row, rowIndex) => (
              // Alternating row background for readability
              <tr key={rowIndex} className={`hover:bg-maid-cream-light transition-colors duration-150 ${rowIndex % 2 === 0 ? 'bg-white' : 'bg-maid-cream-light bg-opacity-30'}`}>
                {safeColumns.map((column, colIndex) => (
                  <td
                    key={`${rowIndex}-${colIndex}`}
                    className="table-cell px-4 py-2 whitespace-nowrap text-sm text-maid-choco border-b border-maid-gray-light"
                  >
                    {/* Format the cell value for display */}
                    {formatCellValue(row[column])}
                  </td>
                ))}
              </tr>
            )) : (
                 // Display message if no data matches the current view/filters
                 <tr>
                    <td colSpan={safeColumns.length || 1} className="text-center py-10 text-maid-gray-dark italic">
                        No data available for the current page or view.
                    </td>
                 </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Fullscreen Close Button (only shown when expanded) */}
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