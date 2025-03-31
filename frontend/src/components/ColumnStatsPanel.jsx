import React from 'react';

// Basic Bar Chart Component
const SimpleBarChart = ({ data, labels }) => {
    if (!data || !labels || data.length === 0 || data.length !== labels.length) 
        return <div className="text-xs text-maid-gray-dark italic">Histogram data unavailable.</div>;
    
    const maxValue = Math.max(...data);
    const barWidth = 90 / data.length; // % width per bar

    return (
        <div className="w-full h-32 bg-maid-cream-light p-2 border rounded-md flex items-end space-x-1">
            {data.map((value, index) => (
                <div key={index} className="text-center flex-grow" style={{ width: `${barWidth}%` }}>
                     <div
                         className="bg-coffee-light hover:bg-coffee transition-colors duration-150 mx-auto"
                         style={{ height: `${maxValue > 0 ? (value / maxValue) * 100 : 0}%` }}
                         title={`Bin ${index + 1}: ${labels[index].toFixed(1)}-${labels[index+1]?.toFixed(1) || '∞'}\nCount: ${value}`}
                      >
                     </div>
                     <div className="text-xxs text-maid-gray-dark truncate mt-1">{labels[index].toFixed(1)}</div>
                </div>
            ))}
        </div>
    );
};


const ColumnStatsPanel = ({ stats, onClose }) => {
  if (!stats) return null;

  const isNumeric = stats.mean !== undefined || stats.min !== undefined;

  return (
    <div className="fixed inset-y-0 right-0 w-full max-w-sm bg-white shadow-xl z-40 p-4 overflow-y-auto transform transition-transform duration-300 ease-in-out translate-x-0 border-l border-coffee-light">
      <div className="flex justify-between items-center mb-4 border-b border-maid-gray-light pb-2">
        <h2 className="text-xl font-semibold text-coffee-dark">Column: {stats.column_name}</h2>
        <button onClick={onClose} className="text-maid-gray-dark hover:text-coffee text-2xl font-bold">×</button>
      </div>

      <div className="space-y-3 text-sm">
        {/* Basic Info */}
        <div className="p-3 bg-maid-cream-light rounded-md border border-maid-gray-light">
          <div className="flex justify-between mb-2">
            <span className="font-medium text-maid-choco-dark">Type:</span>
            <span className="text-maid-choco">{stats.dtype}</span>
          </div>
          <div className="flex justify-between">
            <span className="font-medium text-maid-choco-dark">Missing:</span>
            <span className={stats.missing_count > 0 ? "text-red-600" : "text-green-600"}>
              {stats.missing_count?.toLocaleString() ?? 'N/A'} ({stats.missing_percentage?.toFixed(1) ?? 'N/A'}%)
            </span>
          </div>
        </div>

        {/* Numeric Stats */}
        {isNumeric && (
          <div className="card-inset">
            <h3 className="text-sm font-semibold text-coffee-dark mb-2">Numeric Stats:</h3>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
              <div className="flex justify-between">
                <span className="font-medium text-maid-choco-dark">Min:</span>
                <span className="text-maid-choco">{stats.min?.toLocaleString() ?? 'N/A'}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-maid-choco-dark">Max:</span>
                <span className="text-maid-choco">{stats.max?.toLocaleString() ?? 'N/A'}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-maid-choco-dark">Mean:</span>
                <span className="text-maid-choco">{stats.mean?.toLocaleString(undefined, {maximumFractionDigits: 4}) ?? 'N/A'}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-maid-choco-dark">Median:</span>
                <span className="text-maid-choco">{stats.median?.toLocaleString() ?? 'N/A'}</span>
              </div>
              <div className="flex justify-between">
                <span className="font-medium text-maid-choco-dark">Std Dev:</span>
                <span className="text-maid-choco">{stats.std?.toLocaleString(undefined, {maximumFractionDigits: 4}) ?? 'N/A'}</span>
              </div>
            </div>
             {stats.quantiles && (
                <div className="mt-3 p-2 bg-white rounded-md border border-maid-gray-light text-xs">
                     <div className="font-medium text-maid-choco-dark mb-1">Quantiles:</div>
                     <div className="grid grid-cols-3 gap-2">
                       <div className="flex justify-between">
                         <span className="text-maid-choco-dark">25%:</span>
                         <span className="text-maid-choco">{stats.quantiles['25%']?.toLocaleString() ?? 'N/A'}</span>
                       </div>
                       <div className="flex justify-between">
                         <span className="text-maid-choco-dark">50%:</span>
                         <span className="text-maid-choco">{stats.quantiles['50%']?.toLocaleString() ?? 'N/A'}</span>
                       </div>
                       <div className="flex justify-between">
                         <span className="text-maid-choco-dark">75%:</span>
                         <span className="text-maid-choco">{stats.quantiles['75%']?.toLocaleString() ?? 'N/A'}</span>
                       </div>
                     </div>
                </div>
             )}
             {stats.histogram && (
                <div className="mt-3">
                     <h4 className="font-medium text-xs text-maid-choco-dark mb-1">Histogram (10 bins):</h4>
                     <SimpleBarChart data={stats.histogram.counts} labels={stats.histogram.bin_edges} />
                </div>
             )}
          </div>
        )}

        {/* Categorical Stats */}
        {!isNumeric && (
          <div className="card-inset">
             <h3 className="text-sm font-semibold text-coffee-dark mb-2">Categorical Stats:</h3>
             <div className="flex justify-between mb-2">
               <span className="font-medium text-maid-choco-dark">Unique Values:</span>
               <span className="text-maid-choco">{stats.unique_count?.toLocaleString() ?? 'N/A'}</span>
             </div>
             {stats.top_value !== undefined && (
               <div className="flex justify-between mb-2">
                 <span className="font-medium text-maid-choco-dark">Mode (Top Value):</span>
                 <span className="text-maid-choco">{String(stats.top_value)}</span>
               </div>
             )}
             {stats.top_values && (
                 <div className="mt-3">
                     <h4 className="font-medium text-xs text-maid-choco-dark mb-1">Top Values (Max 20):</h4>
                     <div className="max-h-48 overflow-y-auto border rounded-md p-2 text-xs bg-white shadow-inner">
                         <table className="w-full">
                            <thead>
                              <tr className="border-b border-maid-gray-light">
                                <th className="py-1 px-2 text-left text-coffee-dark">Value</th>
                                <th className="py-1 px-2 text-right text-coffee-dark">Count</th>
                              </tr>
                            </thead>
                            <tbody>
                             {Object.entries(stats.top_values).map(([value, count]) => (
                                 <tr key={value} className="hover:bg-maid-cream-light transition-colors duration-150">
                                     <td className="py-0.5 px-2 text-maid-choco">{value}</td>
                                     <td className="py-0.5 px-2 text-right text-maid-choco">{count.toLocaleString()}</td>
                                 </tr>
                             ))}
                            </tbody>
                         </table>
                     </div>
                 </div>
             )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ColumnStatsPanel;