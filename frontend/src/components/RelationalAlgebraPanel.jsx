// src/components/RelationalAlgebraPanel.jsx
import React, { useState, useEffect, useCallback } from 'react';
import apiService from '../services/api';
import EnhancedDataTable from './DataTable'; // Assuming this component can handle limited height

const UNARY_OPERATIONS = ['select', 'project', 'rename'];
const BINARY_OPERATIONS = ['union', 'difference', 'intersection', 'cartesian_product', 'natural_join', 'theta_join'];

const RelationalAlgebraPanel = ({
    availableDatasets = [],
    onDatasetSaved, // Handler from App.jsx when RA result is saved
    isLoading: isAppLoading, // Loading state from the main app
    onError, // Main error handler from App.jsx
}) => {
    // --- State ---
    const [operation, setOperation] = useState('select'); // Current operation type selected
    const [params, setParams] = useState({}); // Parameters for the current operation
    const [baseDataset, setBaseDataset] = useState(''); // Initial dataset selected for the chain

    // Preview State
    const [raPreviewData, setRaPreviewData] = useState([]);
    const [raPreviewColumns, setRaPreviewColumns] = useState([]);
    const [raPreviewRowCount, setRaPreviewRowCount] = useState(0);

    // Chain State
    const [raOperationChain, setRaOperationChain] = useState([]); // Stores { op, params, display } for UI
    const [currentSqlState, setCurrentSqlState] = useState(''); // Stores the SQL generated *by the last preview* (input for next)
    const [isChaining, setIsChaining] = useState(false); // Tracks if we are past the first step

    // UI State
    const [localLoading, setLocalLoading] = useState(false); // Loading specific to this panel
    const [localError, setLocalError] = useState(null); // Error specific to this panel (string or object)
    const [sourceColumns, setSourceColumns] = useState([]); // Columns available from the previous step/base dataset

    // --- Effects ---

    // Reset parameters when operation changes
    useEffect(() => {
        setParams({});
        clearLocalError();
        // Don't reset the whole chain on op change, just the params for the *next* step
    }, [operation]);

    // Fetch columns needed for certain operations (project, rename)
    const fetchSourceColumns = useCallback(async () => {
        // Only fetch if needed by the current operation selection
        if (!['project', 'rename'].includes(operation)) {
            setSourceColumns([]);
            return;
        }

        let columnsToSet = [];
        if (isChaining) {
            // If chaining, the columns are from the last preview result
            columnsToSet = raPreviewColumns;
        } else if (baseDataset) {
            // If first step, fetch columns from the selected base dataset
            setLocalLoading(true); clearLocalError();
            try {
                const info = await apiService.getDatasetInfo(baseDataset);
                columnsToSet = Object.keys(info?.column_types || {});
            } catch (err) {
                console.error("Failed to fetch columns for RA base dataset:", err);
                handleLocalError(err, `Could not fetch columns for ${baseDataset}`);
                columnsToSet = ['Error fetching columns']; // Indicate error
            } finally {
                setLocalLoading(false);
            }
        }
        setSourceColumns(columnsToSet);
    }, [operation, isChaining, baseDataset, raPreviewColumns]); // Dependencies

    // Trigger column fetch when relevant state changes
    useEffect(() => {
        fetchSourceColumns();
    }, [fetchSourceColumns]);


    // --- Handlers ---

    const clearLocalError = () => setLocalError(null);

    // Error handler specific to this panel
    const handleLocalError = (err, defaultMessage = 'RA Operation Error') => {
        let errorToStore = defaultMessage;
        if (typeof err === 'string') { errorToStore = err; }
        else if (err?.response?.data?.detail) { // FastAPI validation or detail string
            errorToStore = err.response.data.detail;
        } else if (err?.response?.data) { // Other backend error structures
             errorToStore = JSON.stringify(err.response.data);
        } else if (err?.message) { errorToStore = err.message; }
        else if (typeof err === 'object' && err !== null) { errorToStore = JSON.stringify(err); }

        setLocalError(errorToStore);
        setLocalLoading(false);
        // Optionally call the main error handler passed from App.jsx
        // if (onError) onError(err, defaultMessage); // Maybe too noisy? Panel error is usually sufficient.
        console.error("RA Panel Error:", err); // Log the full error object
    };

    // Update parameters for the current operation form
    const handleParamChange = (name, value) => {
        clearLocalError();
        setParams(prev => ({ ...prev, [name]: value }));
    };

    // Specific handler for multi-select checkbox (e.g., project attributes)
    const handleAttributeChange = (selectedAttributes) => {
        handleParamChange('attributes', selectedAttributes);
    }

    // Reset the entire RA chain state
    const handleResetChain = () => {
        clearLocalError();
        setParams({});
        setBaseDataset('');
        setRaPreviewData([]);
        setRaPreviewColumns([]);
        setRaPreviewRowCount(0);
        setRaOperationChain([]);
        setCurrentSqlState(''); // Clear the generated SQL chain
        setIsChaining(false); // Back to step 0
        setSourceColumns([]); // Reset available columns
        console.log("RA Chain Reset");
    }

    // Handle applying the currently configured operation step
    const handleApplyOperation = async (e) => {
        e.preventDefault();
        clearLocalError();

        // --- Basic Client-Side Validation ---
        let isValid = true;
        let missing = [];
        if (!isChaining && !baseDataset) { isValid = false; missing.push('Base Dataset'); }
        if (operation === 'select' && !params.predicate?.trim()) { isValid = false; missing.push('Predicate'); }
        if (operation === 'project' && (!params.attributes || params.attributes.length === 0)) { isValid = false; missing.push('Attributes'); }
        if (operation === 'rename' && !params.renaming_map?.trim()) { isValid = false; missing.push('Renaming Map'); }
        // Add more validation for other ops if needed

        if (!isValid) {
            setLocalError(`Missing required parameters for ${operation}: ${missing.join(', ')}.`);
            return;
        }

        setLocalLoading(true);

        try {
            const apiParams = { ...params };
            // Pass current source columns for rename if needed by backend validation
            if (operation === 'rename' && sourceColumns.length > 0 && !sourceColumns.includes("Error fetching columns")) {
                apiParams.all_columns = sourceColumns;
            }

            // Call the preview API
            const result = await apiService.performRelationalOperationPreview(
                operation,
                apiParams,
                isChaining ? currentSqlState : null, // Pass SQL state from *previous* step if chaining
                baseDataset                        // Base dataset is always needed by backend now
            );

            // --- Update State on Success ---
            setRaPreviewData(result.data || []);
            setRaPreviewColumns(result.columns || []);
            setRaPreviewRowCount(result.row_count || 0);
            // Store the SQL state returned by the preview - this represents the *full chain* up to this point
            setCurrentSqlState(result.generated_sql_state || '');

            // Update UI display of the chain
            const displayParams = {...params}; // Copy params for display string
            const displaySource = isChaining ? `Step ${raOperationChain.length}` : baseDataset;
            const displayString = `${getRaSymbol(operation)}(${JSON.stringify(displayParams)}) applied to [${displaySource}]`;
            setRaOperationChain(prev => [...prev, { op: operation, params: displayParams, display: displayString }]);

            setIsChaining(true); // Ensure we are marked as chaining
            setParams({}); // Clear params form for the next operation (optional)
            // Optionally reset operation selector? Maybe not.
            // setOperation('select');

            // Update source columns based on the *new* preview result for the next step
            setSourceColumns(result.columns || []);

        } catch (err) {
            handleLocalError(err, `RA Operation '${operation}' failed.`);
            // Keep existing preview data? Or clear it? Maybe keep it.
        } finally {
            setLocalLoading(false);
        }
    };

    // Handle saving the final result of the chain
    const handleSave = async () => {
        // Can only save if we have a valid chain result (currentSqlState)
        if (!isChaining || !currentSqlState) {
            setLocalError("No operation chain preview available to save.");
            return;
        }

        const newName = prompt("Enter a name for the new dataset derived from the preview:");
        if (!newName || !newName.trim()) {
            alert("Save cancelled or invalid name provided.");
            return;
        }

        setLocalLoading(true); clearLocalError();

        try {
            // Base dataset name is crucial for the backend to load data
            if (!baseDataset) throw new Error("Base dataset name is missing for saving.");

            // Call the save API, passing the *final SQL chain* generated by the last preview
            const result = await apiService.saveRaResult(
                currentSqlState, // This IS the final SQL chain
                newName.trim(),
                [baseDataset] // Pass the base dataset name(s) used
            );

            alert(result.message || `Successfully saved as ${result.dataset_name}`);
            if (onDatasetSaved) {
                onDatasetSaved(result); // Notify App.jsx to update dataset list etc.
            }
            handleResetChain(); // Reset RA panel after successful save

        } catch (err) {
            handleLocalError(err, `Failed to save RA result as '${newName}'.`);
        } finally {
            setLocalLoading(false);
        }
    };

    // Helper to get RA symbol for display
    const getRaSymbol = (op) => {
        const symbols = { select: 'σ', project: 'π', rename: 'ρ', /* add others */ };
        return symbols[op] || op;
    }

    // Determine if the apply button should be enabled
    const canApply = UNARY_OPERATIONS.includes(operation) && (isChaining || baseDataset);
                       // Add logic for binary ops later if implemented

    // --- Render ---
    return (
        <div className="card bg-white p-4 rounded-lg shadow-soft border border-maid-gray-light space-y-4">
            <h2 className="card-header">Relational Algebra Builder</h2>

             {/* Local Error Display */}
            {localError && (
              <div className="p-2 bg-red-100 text-red-700 text-xs rounded border border-red-200 flex justify-between items-center break-words">
                <span className="font-medium mr-2">RA Error:</span>
                {/* Display detail if available, otherwise the message */}
                <span>{typeof localError === 'object' ? localError.detail || JSON.stringify(localError) : localError}</span>
                <button onClick={clearLocalError} className="ml-2 font-bold text-red-800 text-xs flex-shrink-0">✕</button>
              </div>
            )}

            {/* Operation Selection (Unary only for now) */}
            <div className="mb-4">
                 <label className="input-label">Select Operation to Add:</label>
                 <div className="grid grid-cols-3 gap-2 mb-2">
                     {UNARY_OPERATIONS.map(op => (
                        <div key={op}
                            className={`p-2 text-center rounded-md cursor-pointer text-sm ${operation === op ? 'bg-coffee text-white shadow-inner' : 'bg-maid-cream-light hover:bg-maid-cream text-maid-choco border border-maid-gray-light'}`}
                            onClick={() => setOperation(op)}>
                            {op.charAt(0).toUpperCase() + op.slice(1)} ({getRaSymbol(op)})
                        </div>
                     ))}
                     {/* Placeholder for Binary Ops */}
                     {BINARY_OPERATIONS.map(op => (
                         <div key={op} title="Binary operations currently disabled for interactive chaining"
                             className={`p-2 text-center rounded-md text-sm bg-gray-200 text-gray-500 cursor-not-allowed border border-gray-300`}>
                             {op.charAt(0).toUpperCase() + op.slice(1)}
                         </div>
                      ))}
                 </div>
            </div>

            {/* Form Area */}
            <form onSubmit={handleApplyOperation} className="space-y-3 border-t border-maid-gray-light pt-3">
                 {/* Base Dataset Selection (Only for the first step) */}
                {!isChaining && UNARY_OPERATIONS.includes(operation) && (
                    <div>
                        <label className="input-label font-semibold">Step 1: Select Base Dataset</label>
                        <select
                           required
                           value={baseDataset}
                           onChange={e => { setBaseDataset(e.target.value); clearLocalError(); }}
                           className="select-base"
                           disabled={isAppLoading || localLoading || isChaining}
                        >
                            <option value="">Select Initial Dataset...</option>
                            {availableDatasets.map(ds => <option key={ds} value={ds}>{ds}</option>)}
                        </select>
                         {!baseDataset && <p className="text-xs text-red-500 mt-1">Select a dataset to start the operation chain.</p>}
                    </div>
                )}

                 {/* Operation Specific Fields */}
                 <div className="space-y-2">
                     <label className="input-label font-semibold">
                         {isChaining ? `Step ${raOperationChain.length + 1}: Configure ${operation}` : `Step 1: Configure ${operation}`}
                     </label>
                     {operation === 'select' && (
                        <div>
                            <label className="input-label text-xs">Predicate (SQL WHERE clause):</label>
                            <input
                               required type="text" placeholder="e.g., value > 100 AND category = 'A'"
                               value={params.predicate || ''} onChange={e => handleParamChange('predicate', e.target.value)}
                               className="input-base font-mono text-xs"
                               disabled={isAppLoading || localLoading}
                             />
                             <div className="text-xs text-maid-gray-dark mt-1 italic">Use SQL syntax. Available columns: {sourceColumns.join(', ') || '(from previous step)'}</div>
                        </div>
                    )}
                     {operation === 'project' && (
                        <div>
                            <label className="input-label text-xs">Attributes (Columns) to Keep:</label>
                            <div className="border rounded-md p-2 max-h-32 overflow-y-auto text-xs bg-white shadow-inner">
                                {sourceColumns.length > 0 && !sourceColumns.includes("Error fetching columns") ? sourceColumns.map(col => (
                                    <div key={col} className="flex items-center mb-1 last:mb-0">
                                        <input
                                           type="checkbox" id={`proj_${col}`} value={col}
                                           checked={params.attributes?.includes(col) || false}
                                           onChange={e => { const current = params.attributes || []; handleAttributeChange(e.target.checked ? [...current, col] : current.filter(c => c !== col)) }}
                                           className="mr-1.5 h-3 w-3 text-coffee focus:ring-coffee-light border-maid-gray-light rounded-sm"
                                           disabled={isAppLoading || localLoading}
                                         />
                                         <label htmlFor={`proj_${col}`} className="text-maid-choco select-none">{col}</label>
                                     </div>
                                )) : (
                                    <span className="text-maid-gray-dark italic">
                                        {isAppLoading || localLoading ? "Loading columns..." : (isChaining ? "Columns from preview" : (baseDataset ? "Fetching columns..." : "Select base dataset first."))}
                                        {sourceColumns.includes("Error fetching columns") && <span className="text-red-500"> Error loading columns.</span>}
                                    </span>
                                )}
                            </div>
                            {params.attributes?.length === 0 && sourceColumns.length > 0 && !sourceColumns.includes("Error fetching columns") && <div className="text-red-600 text-xs mt-1">Please select at least one attribute.</div>}
                        </div>
                    )}
                     {operation === 'rename' && (
                        <div>
                            <label className="input-label text-xs">Renaming (Format: old1=new1, old2=new2):</label>
                            <input
                               required type="text" placeholder="e.g., category=product_type, value=price"
                               value={params.renaming_map || ''} onChange={e => handleParamChange('renaming_map', e.target.value)}
                               className="input-base font-mono text-xs"
                               disabled={isAppLoading || localLoading}
                             />
                             <div className="text-xs text-maid-gray-dark mt-1 italic">
                                 Available columns: {sourceColumns.length > 0 && !sourceColumns.includes("Error fetching columns") ? sourceColumns.join(', ') : (isAppLoading || localLoading ? "Loading..." : "(from previous step)")}
                                 {sourceColumns.includes("Error fetching columns") && <span className="text-red-500"> Error loading columns.</span>}
                             </div>
                        </div>
                    )}
                 </div>

                 {/* Apply Button */}
                 <div className="flex justify-end">
                     <button
                         type="submit"
                         className="btn btn-blue"
                         disabled={isAppLoading || localLoading || !canApply}
                         title={!canApply ? "Select base dataset first" : `Apply ${operation} operation`}
                     >
                         {localLoading ? 'Applying...' : (isChaining ? `Add Step ${raOperationChain.length + 1}` : 'Apply Step 1')}
                     </button>
                 </div>
            </form>

            {/* Preview and Chain Display Area */}
            {isChaining && (
                <div className="mt-4 pt-4 border-t border-maid-gray-light space-y-4">
                    <h3 className="text-base font-semibold text-maid-choco">Current Chain & Preview</h3>

                    {/* Chain Display */}
                    <div className="text-xs bg-maid-cream p-2 rounded border border-maid-gray-light overflow-x-auto">
                         <div className="mb-1"><span className="font-semibold">Base:</span> {baseDataset}</div>
                         {raOperationChain.map((item, index) => (
                             <div key={index} className="ml-2 mb-0.5">
                                <span className="font-semibold text-coffee">➔ Step {index+1}:</span> {item.display}
                             </div>
                         ))}
                     </div>

                    {/* Preview Table */}
                    <div>
                        <h4 className="text-sm font-semibold text-maid-choco mb-1">Preview of Step {raOperationChain.length} Result ({raPreviewRowCount.toLocaleString()} total rows)</h4>
                        <EnhancedDataTable
                            data={raPreviewData}
                            columns={raPreviewColumns}
                            totalRows={raPreviewRowCount}
                            isPreview={true} // RA preview is always potentially partial
                            // isLoading={localLoading} // DataTable has its own internal loading indicator maybe?
                            // tableHeight="max-h-60" // Use Tailwind class directly if needed
                        />
                         {localLoading && <div className="text-center text-sm text-maid-gray-dark p-2">Loading preview...</div>}
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
                             title={!currentSqlState ? "Apply at least one operation first" : "Save the result of the current chain as a new dataset"}
                         >
                             {localLoading ? 'Saving...' : 'Save Chain Result As...'}
                         </button>
                     </div>
                </div>
            )}
        </div>
    );
};

export default RelationalAlgebraPanel;