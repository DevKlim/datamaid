import React from 'react';

// Helper to format memory bytes
const formatBytes = (bytes, decimals = 2) => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

const DatasetInfoPanel = ({ info, onClose, onColumnSelect }) => {
  if (!info) return null;

  const renderValueCounts = (counts) => {
      if (!counts || typeof counts !== 'object') return 'N/A';
      return (
          <ul className="list-disc list-inside text-xs">
              {Object.entries(counts).slice(0, 5).map(([value, count]) => (
                  <li key={value}>{value}: {count}</li>
              ))}
              {Object.keys(counts).length > 5 && <li>... ({Object.keys(counts).length - 5} more)</li>}
          </ul>
      );
  }

  return (
    <div className="fixed inset-y-0 right-0 w-full max-w-md bg-white shadow-xl z-40 p-4 overflow-y-auto transform transition-transform duration-300 ease-in-out translate-x-0 border-l border-coffee-light">
         <div className="flex justify-between items-center mb-4 border-b border-maid-gray pb-2">
            <h2 className="text-xl font-semibold text-coffee-dark">Dataset Info</h2>
            <button onClick={onClose} className="text-maid-gray-dark hover:text-coffee text-2xl font-bold">×</button>
         </div>

        <div className="space-y-4 text-sm">
            {/* Basic Stats */}
            <div className="grid grid-cols-2 gap-2 p-3 bg-maid-cream-light rounded-md border border-maid-gray-light">
                <div className="font-medium text-maid-choco-dark">Rows:</div>
                <div className="text-maid-choco">{info.row_count?.toLocaleString() ?? 'N/A'}</div>
                
                <div className="font-medium text-maid-choco-dark">Columns:</div>
                <div className="text-maid-choco">{info.column_count?.toLocaleString() ?? 'N/A'}</div>
                
                <div className="font-medium text-maid-choco-dark">Memory:</div>
                <div className="text-maid-choco">{info.memory_usage ? formatBytes(info.memory_usage) : 'N/A'}</div>
            </div>

            {/* Column Types */}
            {info.column_types && (
                <div className="card-inset">
                    <h3 className="text-sm font-semibold text-coffee-dark mb-2">Column Types:</h3>
                    <div className="max-h-40 overflow-y-auto border rounded-md p-2 bg-white shadow-inner">
                        <table className="w-full text-xs">
                            <tbody>
                            {Object.entries(info.column_types).map(([col, type]) => (
                                <tr key={col} className="hover:bg-maid-cream-light transition-colors duration-150">
                                    <td className="py-1 px-2 font-medium cursor-pointer text-coffee hover:underline" onClick={() => onColumnSelect(col)}>{col}</td>
                                    <td className="py-1 px-2 text-maid-choco">{type}</td>
                                </tr>
                            ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

             {/* Missing Values */}
             {info.missing_values && (
                 <div className="card-inset">
                     <h3 className="text-sm font-semibold text-coffee-dark mb-2">Missing Values:</h3>
                     <div className="max-h-40 overflow-y-auto border rounded-md p-2 bg-white shadow-inner">
                         <table className="w-full text-xs">
                            <tbody>
                             {Object.entries(info.missing_values)
                                 .filter(([, count]) => count > 0) // Only show columns with missing values
                                 .sort(([, a], [, b]) => b - a) // Sort by count descending
                                 .map(([col, count]) => (
                                     <tr key={col} className="hover:bg-maid-cream-light transition-colors duration-150">
                                         <td className="py-1 px-2 font-medium cursor-pointer text-coffee hover:underline" onClick={() => onColumnSelect(col)}>{col}</td>
                                         <td className="py-1 px-2 text-red-600">{count.toLocaleString()} ({((count / info.row_count) * 100).toFixed(1)}%)</td>
                                     </tr>
                                 ))}
                             {Object.values(info.missing_values).every(count => count === 0) && (
                                 <tr><td colSpan="2" className="py-1 px-2 text-green-600 italic">No missing values found ♡</td></tr>
                             )}
                            </tbody>
                         </table>
                     </div>
                 </div>
             )}

            {/* Unique Value Counts (for categorical) */}
            {info.unique_value_counts && Object.keys(info.unique_value_counts).length > 0 && (
                <div className="card-inset">
                    <h3 className="text-sm font-semibold text-coffee-dark mb-2">Unique Values (Categorical - Top 5):</h3>
                     <div className="max-h-60 overflow-y-auto border rounded-md p-2 bg-white shadow-inner space-y-2">
                         {Object.entries(info.unique_value_counts).map(([col, data]) => (
                            <div key={col} className="border-b border-maid-gray-light pb-2 last:border-b-0 last:pb-0">
                                <div className="font-medium cursor-pointer text-coffee hover:underline" onClick={() => onColumnSelect(col)}>{col} <span className="text-maid-gray-dark text-xs">({data.total_unique} unique)</span></div>
                                {renderValueCounts(data.values)}
                            </div>
                        ))}
                    </div>
                </div>
             )}
        </div>
    </div>
  );
};

export default DatasetInfoPanel;