import React, { useState, useEffect, useCallback } from 'react';
import apiService from '../services/api';
import EnhancedDataTable from './DataTable';

const UNARY_OPERATIONS = ['select', 'project', 'rename'];
const BINARY_OPERATIONS = ['union', 'difference', 'intersection', 'cartesian_product', 'natural_join', 'theta_join'];

const RelationalAlgebraPanel = ({
    availableDatasets = [],
    onDatasetSaved,
    isLoading: isAppLoading,
    onError, // Pass error handler from App
}) => {
    const [operation, setOperation] = useState('select');
    const [params, setParams] = useState({});
    const [baseDataset, setBaseDataset] = useState(''); // Store the initial dataset

    const [raPreviewData, setRaPreviewData] = useState([]);
    const [raPreviewColumns, setRaPreviewColumns] = useState([]);
    const [raPreviewRowCount, setRaPreviewRowCount] = useState(0);
    const [raOperationChain, setRaOperationChain] = useState([]);
    const [currentSqlState, setCurrentSqlState] = useState('');
    const [isChaining, setIsChaining] = useState(false);
    const [localLoading, setLocalLoading] = useState(false);
    const [localError, setLocalError] = useState(null); // Store error message string or object

    const [sourceColumns, setSourceColumns] = useState([]);

    const clearLocalError = () => setLocalError(null);

    // Updated error handler
    const handleLocalError = (err, defaultMessage = 'RA Operation Error') => {
         let errorToStore = defaultMessage; // Default to string
         if (typeof err === 'string') { errorToStore = err; }
         else if (err.response?.data) {
             // Store the data part which might contain { detail: ... } or just a string
             errorToStore = err.response.data;
         }
         else if (err.message) { errorToStore = err.message; }
         else if (typeof err === 'object' && err !== null) {
             errorToStore = err; // Store the object if no better message found
         }
         setLocalError(errorToStore); // Store potentially object or string
         setLocalLoading(false);
         // Also call the main error handler if provided
         if (onError) onError(err, defaultMessage);
    };

    useEffect(() => {
        handleResetChain();
    }, [operation]);

    const fetchSourceColumns = useCallback(async () => {
        // ... (keep existing fetchSourceColumns logic) ...
         if (!['project', 'rename'].includes(operation)) {
             setSourceColumns([]);
             return;
         }

         let columnsToSet = [];
         if (isChaining) {
             columnsToSet = raPreviewColumns;
         }
         else if (baseDataset) {
              setLocalLoading(true); clearLocalError();
              try {
                 const info = await apiService.getDatasetInfo(baseDataset);
                 columnsToSet = Object.keys(info?.column_types || {});
              } catch (err) {
                  console.error("Failed to fetch columns for RA base dataset:", err);
                  handleLocalError(err, `Could not fetch columns for ${baseDataset}`);
                  columnsToSet = ['Error fetching columns'];
              } finally {
                  setLocalLoading(false);
              }
         }
         setSourceColumns(columnsToSet);
    }, [operation, isChaining, baseDataset, raPreviewColumns]);

    useEffect(() => {
         fetchSourceColumns();
    }, [fetchSourceColumns]);

    const handleParamChange = (name, value) => {
        clearLocalError();
        setParams(prev => ({ ...prev, [name]: value }));
    };

    const handleAttributeChange = (selectedAttributes) => {
         handleParamChange('attributes', selectedAttributes);
    }


    const handleResetChain = () => {
         clearLocalError();
         setParams({});
         setBaseDataset('');
         setRaPreviewData([]);
         setRaPreviewColumns([]);
         setRaPreviewRowCount(0);
         setRaOperationChain([]);
         setCurrentSqlState('');
         setIsChaining(false);
         setSourceColumns([]); // Also reset source columns
         console.log("RA Chain Reset");
    }

    // --- Handle Apply Operation (Interactive Step) ---
    const handleApplyOperation = async (e) => {
         e.preventDefault();
         clearLocalError();

         // --- Basic Client-Side Validation ---
         let isValid = true;
         let missing = [];
         if (!isChaining && !baseDataset) { // First step requires base dataset
             isValid = false; missing.push('Base Dataset');
         }
         if (operation === 'select' && !params.predicate?.trim()) { isValid = false; missing.push('Predicate'); }
         if (operation === 'project' && (!params.attributes || params.attributes.length === 0)) { isValid = false; missing.push('Attributes'); }
         if (operation === 'rename' && !params.renaming_map?.trim()) { isValid = false; missing.push('Renaming Map'); }
         // Add more validation for other unary ops if supported later

         if (!isValid) {
            setLocalError(`Missing required parameters for ${operation}: ${missing.join(', ')}.`);
            return;
         }

         setLocalLoading(true);

         try {
            const apiParams = { ...params };
             // Pass current source columns for rename if needed
            if (operation === 'rename') {
                 apiParams.all_columns = sourceColumns;
             }

            const result = await apiService.performRelationalOperationPreview(
                operation,
                apiParams,
                isChaining ? currentSqlState : null, // Pass previous SQL state if chaining
                baseDataset                        // *** ALWAYS pass baseDataset ***
            );

             // Update preview state
             setRaPreviewData(result.data || []);
             setRaPreviewColumns(result.columns || []);
             setRaPreviewRowCount(result.row_count || 0);
             setCurrentSqlState(result.generated_sql_state || ''); // Store SQL for the *next* step

             // Update operation chain display
             const displayParams = {...params}; // Copy params for display
             const displayString = `${getRaSymbol(operation)}(${JSON.stringify(displayParams)})(${isChaining ? 'prev' : baseDataset})`;
             setRaOperationChain(prev => [...prev, { op: operation, params: displayParams, display: displayString }]);

             setIsChaining(true); // Now we are chaining
             setParams({}); // Clear params for next operation (optional, maybe keep some?)

             // Re-fetch columns for the *new* preview state if needed by next op
             if (['project', 'rename'].includes(operation)) {
                 setSourceColumns(result.columns || []); // Update immediately
             }

         } catch (err) {
             handleLocalError(err, `RA Operation '${operation}' failed.`);
         } finally {
             setLocalLoading(false);
         }
    };

    // --- Handle Save Operation ---
    const handleSave = async () => {
         if (!isChaining || !currentSqlState) {
             setLocalError("No operation performed or preview available to save.");
             return;
         }

         const newName = prompt("Enter a name for the new dataset derived from the preview:");
         if (!newName || !newName.trim()) {
             alert("Save cancelled or invalid name provided.");
             return;
         }

         setLocalLoading(true); clearLocalError();

         try {
             // We need the base dataset name(s) used in the chain. For now, assume single base dataset.
             if (!baseDataset) throw new Error("Base dataset name is missing for saving.");

             const result = await apiService.saveRaResult(
                 currentSqlState, // Pass the final SQL state
                 newName.trim(),
                 [baseDataset] // Pass the base dataset name(s) used
             );

             alert(result.message);
             if (onDatasetSaved) {
                 onDatasetSaved(result); // Notify App to refresh list and potentially update state
             }
             handleResetChain(); // Reset RA panel after successful save

         } catch (err) {
             handleLocalError(err, `Failed to save RA result as '${newName}'.`);
         } finally {
             setLocalLoading(false);
         }
    };

    // Helper to get RA symbol
    const getRaSymbol = (op) => {
        const symbols = { select: 'σ', project: 'π', rename: 'ρ', /* add others if needed */ };
        return symbols[op] || op;
    }

    // Determine if the apply button should be enabled
    const canApply = UNARY_OPERATIONS.includes(operation) && (isChaining || baseDataset);
                       // Add logic for binary ops later if implemented


    return (
        <div className="card bg-white p-4 rounded-lg shadow-soft border border-maid-gray-light space-y-4">
            <h2 className="card-header">Relational Algebra (Interactive)</h2>

             {/* Local Error Display */}
            {localError && (
              <div className="p-2 bg-red-100 text-red-700 text-xs rounded border border-red-200 flex justify-between items-center">
                <span>RA Error: {localError}</span>
                <button onClick={clearLocalError} className="ml-2 font-bold text-red-800 text-xs">✕</button>
              </div>
            )}

            {/* Operation Selection (Unary only for now) */}
            <div className="mb-4">
                 <label className="input-label">Operation:</label>
                 <div className="grid grid-cols-3 gap-2 mb-2">
                     {UNARY_OPERATIONS.map(op => (
                        <div key={op}
                            className={`p-2 text-center rounded-md cursor-pointer text-sm ${operation === op ? 'bg-coffee text-white' : 'bg-maid-cream-light hover:bg-maid-cream text-maid-choco'}`}
                            onClick={() => setOperation(op)}>
                            {op.charAt(0).toUpperCase() + op.slice(1)} ({getRaSymbol(op)})
                        </div>
                     ))}
                     {/* Placeholder for Binary Ops */}
                     {BINARY_OPERATIONS.map(op => (
                         <div key={op} title="Binary operations currently disabled for interactive chaining"
                             className={`p-2 text-center rounded-md text-sm bg-gray-200 text-gray-500 cursor-not-allowed`}>
                             {op.charAt(0).toUpperCase() + op.slice(1)}
                         </div>
                      ))}
                 </div>
            </div>

            {/* Form Area */}
            <form onSubmit={handleApplyOperation} className="space-y-3">
                 {/* Base Dataset Selection (Only for the first step) */}
                {!isChaining && UNARY_OPERATIONS.includes(operation) && (
                    <div>
                        <label className="input-label">Base Dataset:</label>
                        <select
                           required
                           value={baseDataset}
                           onChange={e => { setBaseDataset(e.target.value); clearLocalError(); }}
                           className="select-base"
                           disabled={isAppLoading || localLoading || isChaining}
                        >
                            <option value="">Select Initial Dataset</option>
                            {availableDatasets.map(ds => <option key={ds} value={ds}>{ds}</option>)}
                        </select>
                         {!baseDataset && <p className="text-xs text-red-500 mt-1">Select a dataset to start the operation chain.</p>}
                    </div>
                )}

                 {/* Operation Specific Fields */}
                 {operation === 'select' && (
                    <div>
                        <label className="input-label">Predicate (SQL WHERE clause):</label>
                        <input
                           required type="text" placeholder="e.g., value > 100 AND category = 'A'"
                           value={params.predicate || ''} onChange={e => handleParamChange('predicate', e.target.value)}
                           className="input-base font-mono text-xs"
                           disabled={isAppLoading || localLoading}
                         />
                         <div className="text-xs text-maid-gray-dark mt-1 italic">Enter SQL condition. Available columns depend on the current preview/base dataset.</div>
                    </div>
                )}
                 {operation === 'project' && (
                    <div>
                        <label className="input-label">Attributes (Columns) to Keep:</label>
                        <div className="border rounded-md p-2 max-h-32 overflow-y-auto text-xs bg-white shadow-inner">
                            {sourceColumns.length > 0 && !sourceColumns.includes("Error fetching columns") ? sourceColumns.map(col => (
                                <div key={col} className="flex items-center mb-1 last:mb-0">
                                    <input
                                       type="checkbox" id={`proj_${col}`} value={col}
                                       checked={params.attributes?.includes(col) || false}
                                       onChange={e => { const current = params.attributes || []; handleAttributeChange(e.target.checked ? [...current, col] : current.filter(c => c !== col)) }}
                                       className="mr-1 h-3 w-3 text-coffee focus:ring-coffee-light border-maid-gray-light"
                                       disabled={isAppLoading || localLoading}
                                     />
                                     <label htmlFor={`proj_${col}`} className="text-maid-choco">{col}</label>
                                 </div>
                            )) : (
                                <span className="text-maid-gray-dark italic">
                                    {isAppLoading || localLoading ? "Loading columns..." : (isChaining ? "Columns from preview" : (baseDataset ? "Fetching columns..." : "Select base dataset first."))}
                                    {sourceColumns.includes("Error fetching columns") && <span className="text-red-500"> Error loading columns.</span>}
                                </span>
                            )}
                        </div>
                        {params.attributes?.length === 0 && sourceColumns.length > 0 && <div className="text-red-600 text-xs mt-1">Please select at least one attribute.</div>}
                    </div>
                )}
                 {operation === 'rename' && (
                    <div>
                        <label className="input-label">Renaming (Format: old1=new1, old2=new2):</label>
                        <input
                           required type="text" placeholder="e.g., category=product_type, value=price"
                           value={params.renaming_map || ''} onChange={e => handleParamChange('renaming_map', e.target.value)}
                           className="input-base font-mono text-xs"
                           disabled={isAppLoading || localLoading}
                         />
                         <div className="text-xs text-maid-gray-dark mt-1 italic">
                             Available columns: {sourceColumns.length > 0 && !sourceColumns.includes("Error fetching columns") ? sourceColumns.join(', ') : (isAppLoading || localLoading ? "Loading..." : "(Select base dataset or apply previous step)")}
                             {sourceColumns.includes("Error fetching columns") && <span className="text-red-500"> Error loading columns.</span>}
                         </div>
                    </div>
                )}

                 {/* Apply Button */}
                 <div className="flex justify-end">
                     <button
                         type="submit"
                         className="btn btn-blue"
                         disabled={isAppLoading || localLoading || !canApply}
                     >
                         {localLoading ? 'Applying...' : (isChaining ? 'Apply to Preview' : 'Apply Operation')}
                     </button>
                 </div>
            </form>

            {/* Preview and Chain Display Area */}
            {isChaining && (
                <div className="mt-4 pt-4 border-t border-maid-gray-light space-y-4">
                    <h3 className="text-base font-semibold text-maid-choco">Operation Chain & Preview</h3>

                    {/* Chain Display */}
                    <div className="text-xs bg-maid-cream p-2 rounded border border-maid-gray-light overflow-x-auto">
                         <span className="font-semibold">Base:</span> {baseDataset}
                         {raOperationChain.map((item, index) => (
                             <div key={index}><span className="font-semibold">➔ {index+1}:</span> {item.display}</div>
                         ))}
                     </div>

                    {/* Preview Table */}
                    <div>
                        <h4 className="text-sm font-semibold text-maid-choco mb-1">Current Preview ({raPreviewRowCount} total rows)</h4>
                        <EnhancedDataTable
                            data={raPreviewData}
                            columns={raPreviewColumns}
                            totalRows={raPreviewRowCount}
                            isPreview={true}
                            isLoading={localLoading}
                            tableHeight="max-h-60" // Limit height of preview table
                        />
                    </div>

                    {/* Action Buttons for Preview */}
                     <div className="flex justify-between items-center mt-2">
                         <button
                             onClick={handleResetChain}
                             className="btn btn-sm btn-gray"
                             disabled={localLoading || isAppLoading}
                         >
                             Reset Chain
                         </button>
                         <button
                             onClick={handleSave}
                             className="btn btn-sm btn-green"
                             disabled={localLoading || isAppLoading || !currentSqlState}
                         >
                             {localLoading ? 'Saving...' : 'Save Preview As...'}
                         </button>
                     </div>
                </div>
            )}
        </div>
    );
};

export default RelationalAlgebraPanel;