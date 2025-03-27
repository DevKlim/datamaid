import React from 'react';

const DataTable = ({ data, columns }) => {
  if (!data || data.length === 0) {
    return (
      <div className="p-4 bg-white rounded shadow">
        <p className="text-gray-500">No data to display. Upload a dataset to get started.</p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto shadow-md sm:rounded-lg">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            {columns.map((column, index) => (
              <th 
                key={index}
                scope="col" 
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {data.map((row, rowIndex) => (
            <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
              {columns.map((column, colIndex) => (
                <td 
                  key={`${rowIndex}-${colIndex}`} 
                  className="px-6 py-4 whitespace-nowrap text-sm text-gray-500"
                >
                  {row[column] !== null && row[column] !== undefined ? String(row[column]) : 'N/A'}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default DataTable;