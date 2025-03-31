// src/App.jsx
import React, { useState, useEffect, useCallback } from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom'; // Import Router components
import apiService from './services/api';

import NavBar from './components/NavBar';
import EnhancedDataTable from './components/DataTable';
import EnhancedOperationsPanel from './components/OperationsPanel';
import CodeDisplay from './components/CodeDisplay';
import CodeEditor from './components/CodeEditor';
import TextUploadModal from './components/TextUploadModal';
import DatasetInfoPanel from './components/DatasetInfoPanel';
import ColumnStatsPanel from './components/ColumnStatsPanel';
import RelationalAlgebraPanel from './components/RelationalAlgebraPanel';
import DatasetManagerPage from './components/DatasetManagerPage'; // Import the new page

function App() {
  // Core State (remain the same)
  const [currentDataset, setCurrentDataset] = useState(null);
  const [availableDatasets, setAvailableDatasets] = useState([]);
  const [engine, setEngine] = useState('pandas');
  const [columns, setColumns] = useState([]);
  const [data, setData] = useState([]);
  const [rowCount, setRowCount] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  // Transformation State (remain the same)
  const [canUndo, setCanUndo] = useState(false);
  const [canReset, setCanReset] = useState(false);
  const [lastCode, setLastCode] = useState('');
  const [currentEditorCode, setCurrentEditorCode] = useState('');

  // DB Upload State (remain the same)
  const [tempDbId, setTempDbId] = useState(null);
  const [dbTables, setDbTables] = useState([]);
  const [showDbModal, setShowDbModal] = useState(false);

  // Text Upload Modal State (remain the same)
  const [showTextModal, setShowTextModal] = useState(false);

  // Dataset Info Panel State (remain the same)
  const [datasetInfo, setDatasetInfo] = useState(null);
  const [showInfoPanel, setShowInfoPanel] = useState(false);

  // Column Stats Panel State (remain the same)
  const [columnStats, setColumnStats] = useState(null);
  const [selectedColumnForStats, setSelectedColumnForStats] = useState(null);
  const [showColumnStatsPanel, setShowColumnStatsPanel] = useState(false);

  // Relational Algebra Panel State (remain the same)
  const [showRaPanel, setShowRaPanel] = useState(false);

  // --- Utility Functions (remain the same) ---
  const clearError = () => setError(null);
  const handleError = (err, defaultMessage = 'An error occurred') => {
     console.error("Error caught in handler:", err);
     let message = defaultMessage;
     if (typeof err === 'string') { message = err; }
     else if (err?.response?.data?.detail) {
        // Handle FastAPI validation errors, strings, or other formats
        const detail = err.response.data.detail;
         if (typeof detail === 'string') message = detail;
         else if (Array.isArray(detail)) message = detail.map(d => `${d.loc?.join('.')} - ${d.msg}`).join('; ');
         else message = JSON.stringify(detail);
     } else if (err?.message) { message = err.message; }
     setError(message);
     setIsLoading(false); // Ensure loading is stopped on error
  };

  // --- Data Fetching and State Update (remain the same) ---
  const fetchDatasetsList = useCallback(async () => {
         // No setIsLoading here, can run in background
         clearError(); // Clear previous errors before fetching
         try {
             const datasets = await apiService.getDatasets();
             setAvailableDatasets(datasets || []);
         } catch (err) {
              console.error('Failed to fetch dataset list:', err);
             // Avoid setting a global error for background fetch failure
             // setError("Failed to fetch dataset list. Some operations might be affected.");
         }
    }, []);

    const updateStateFromResult = (result, origin = "load") => {
      if (!result) return;
      setData(result.data || []);
      setColumns(result.columns || []);
      setRowCount(result.row_count || 0);
      setCanUndo(result.can_undo || false);
      setCanReset(result.can_reset || false);
      setLastCode(result.last_code || result.code || ''); // Update last code display

      // Update editor only on specific actions to avoid overwriting user input
      if (['load', 'undo', 'reset', 'code_exec', 'upload', 'db_import', 'text_upload', 'save_ra'].includes(origin)) {
           setCurrentEditorCode(result.last_code || result.code || '');
      }
      // Reset info/stats panels when data changes significantly
      if (['load', 'reset', 'upload', 'db_import', 'text_upload', 'save_ra', 'rename', 'delete'].includes(origin)) {
          setShowInfoPanel(false); setDatasetInfo(null);
          setShowColumnStatsPanel(false); setColumnStats(null); setSelectedColumnForStats(null);
      }
  };

 const loadDataset = useCallback(async (datasetName, targetEngine = engine) => {
     // ... (keep existing implementation, uses updateStateFromResult) ...
      if (!datasetName) {
        setCurrentDataset(null);
        setData([]); setColumns([]); setRowCount(0);
        setCanUndo(false); setCanReset(false); setLastCode(''); setCurrentEditorCode('');
        setDatasetInfo(null); setShowInfoPanel(false);
        setColumnStats(null); setShowColumnStatsPanel(false); setSelectedColumnForStats(null);
        clearError();
        return;
      }
      setIsLoading(true);
      clearError();
      setShowInfoPanel(false); setShowColumnStatsPanel(false); // Close panels

      try {
        const result = await apiService.getDataset(datasetName, targetEngine);
        if (result) {
           updateStateFromResult(result, "load");
           setCurrentDataset(datasetName);
        } else {
            throw new Error(`Received no result when loading dataset '${datasetName}'.`);
        }
      } catch (err) {
        handleError(err, `Failed to load dataset '${datasetName}'.`);
        setCurrentDataset(null); // Reset selection on severe load failure
        // Fetch list again in case the dataset was deleted/unavailable
        fetchDatasetsList();
      } finally {
        setIsLoading(false);
      }
  }, [engine, fetchDatasetsList]); // Added fetchDatasetsList dependency

  useEffect(() => {
    fetchDatasetsList();
    apiService.testConnection().then(r => console.log(r.message)).catch(e => setError("Backend connection failed. Check if the server is running."));
 }, [fetchDatasetsList]);

// --- Re-load data when currentDataset or engine changes (keep existing) ---
 useEffect(() => {
     if (currentDataset && availableDatasets.includes(currentDataset)) {
         loadDataset(currentDataset, engine);
     } else if (currentDataset && !availableDatasets.includes(currentDataset)) {
          console.log(`Current dataset ${currentDataset} no longer available (maybe deleted/renamed). Resetting selection.`);
          setCurrentDataset(null); // Triggers clearing logic in loadDataset
     } else if (!currentDataset) {
         // Explicitly clear if currentDataset becomes null
         loadDataset(null);
     }
     // eslint-disable-next-line react-hooks/exhaustive-deps
 }, [currentDataset, availableDatasets, engine]); // loadDataset is stable due to useCallback

  // --- Handlers for UI Actions ---

  // handleDatasetUploaded, handleEngineChange, handleDatasetChange, handleOperationSubmit,
  // handleExecuteCode, handleUndo, handleReset, handleSaveTransform, handleExport,
  // DB Upload Handlers, Text Upload Handlers, Info/Stats Panel Handlers, RA Handlers
  // ... (keep existing implementations) ...

  const handleDatasetUploaded = (uploadResult, source = "upload") => {
    if (uploadResult && uploadResult.dataset_name) {
        fetchDatasetsList().then(() => { // Fetch list *then* update state
            updateStateFromResult({
                ...uploadResult,
                data: uploadResult.preview || [],
                can_undo: false,
                can_reset: false,
                last_code: '',
            }, source);
            setCurrentDataset(uploadResult.dataset_name); // Switch to the new dataset
            clearError();
            setShowTextModal(false);
            setShowDbModal(false);
        });
    } else {
        handleError(null, "Upload process did not return expected dataset information.");
    }
 };
  const handleEngineChange = (newEngine) => {
    setEngine(newEngine);
    // The useEffect hook dependent on 'engine' will handle reloading the preview
  };
  const handleDatasetChange = (datasetName) => {
    if (datasetName !== currentDataset) {
      setCurrentDataset(datasetName); // Triggers useEffect to load
    }
  };
   const handleOperationSubmit = async (operation, params) => {
    if (!currentDataset) return handleError("Please select a dataset first.");
    setIsLoading(true); clearError();
    try {
      let result;
      let apiFunction;
      let operationDesc = operation;

      if (operation === 'merge') {
        if (!params.right_dataset || !params.left_on || !params.right_on) throw new Error("Merge requires Right Dataset, Left Key, and Right Key.");
        apiFunction = () => apiService.mergeDatasets(currentDataset, params.right_dataset, params, engine);
        operationDesc = `Merge with ${params.right_dataset}`;
      } else if (operation === 'regex_operation') {
        if (!params.regex_action || !params.column || !params.regex) throw new Error("Regex operation requires Action, Column, and Pattern.");
        apiFunction = () => apiService.performRegexOperation(currentDataset, params.regex_action, params, engine);
        operationDesc = `Regex ${params.regex_action}`;
      } else {
        apiFunction = () => apiService.performOperation(currentDataset, operation, params, engine);
      }

      result = await apiFunction();
      updateStateFromResult(result, "operation");
      console.log(`${operationDesc} successful.`);
    } catch (err) {
      handleError(err, `Failed to perform operation: ${operation}.`);
    } finally {
      setIsLoading(false);
    }
  };
  const handleExecuteCode = async (codeToExecute) => {
    if (!currentDataset || !codeToExecute.trim()) return handleError("Select a dataset and write code.");
    setIsLoading(true); clearError();
    try {
      const result = await apiService.executeCustomCode(currentDataset, codeToExecute, engine);
      updateStateFromResult(result, "code_exec");
      setCurrentEditorCode(codeToExecute);
      setLastCode(codeToExecute);
      console.log("Custom code execution successful.");
    } catch (err) {
      handleError(err, 'Failed to execute custom code.');
    } finally {
      setIsLoading(false);
    }
  };
  const handleUndo = async () => {
    if (!currentDataset || !canUndo) return;
    setIsLoading(true); clearError();
    try {
      const result = await apiService.undoTransformation(currentDataset, engine);
      updateStateFromResult(result, "undo");
      console.log("Undo successful.");
    } catch (err) {
      handleError(err, 'Failed to undo operation.');
    } finally {
      setIsLoading(false);
    }
  };
  const handleReset = async () => {
    if (!currentDataset || !canReset) return;
    setIsLoading(true); clearError();
    try {
      const result = await apiService.resetTransformation(currentDataset, engine);
      updateStateFromResult(result, "reset");
      console.log("Reset successful.");
    } catch (err) {
      handleError(err, 'Failed to reset transformations.');
    } finally {
      setIsLoading(false);
    }
  };
  const handleSaveTransform = async () => {
    if (!currentDataset) return;
    const newName = prompt(`Enter a name for the new dataset (saving current state of '${currentDataset}'):`);
    if (!newName || !newName.trim()) return;

    setIsLoading(true); clearError();
    try {
      const result = await apiService.saveTransformation(currentDataset, newName.trim(), engine);
      console.log(result.message);
      fetchDatasetsList(); // Refresh list
      alert(`Successfully saved as '${result.dataset_name}'. Select it from the dropdown.`);
    } catch (err) {
      handleError(err, `Failed to save transformation as '${newName}'.`);
    } finally {
      setIsLoading(false);
    }
  };
  const handleExport = async (format) => {
    if (!currentDataset) return handleError("Please select a dataset to export.");
    setIsLoading(true); clearError();
    try {
      await apiService.exportDataset(currentDataset, format, engine);
    } catch (err) {
       console.error("Export trigger failed in App.jsx");
       handleError(err, `Export failed.`);
    } finally {
      setIsLoading(false);
    }
  };
  const handleDbFileUpload = async (file) => {
    if (!file) return;
    setIsLoading(true); clearError(); setTempDbId(null); setDbTables([]);
    try {
      const result = await apiService.uploadDbFile(file);
      setTempDbId(result.temp_db_id);
      const tablesResult = await apiService.listDbTables(result.temp_db_id);
      setDbTables(tablesResult.tables || []);
      setShowDbModal(true);
    } catch (err) {
      handleError(err, 'Failed to upload or process database file.');
      setShowDbModal(false);
    } finally {
      setIsLoading(false);
    }
  };
  const handleImportTable = async (tableName, newDatasetName) => {
    if (!tempDbId || !tableName || !newDatasetName) return;
    setIsLoading(true); clearError();
    try {
      const result = await apiService.importDbTable(tempDbId, tableName, newDatasetName);
      handleDatasetUploaded(result, "db_import");
    } catch (err) {
      handleError(err, `Failed to import table '${tableName}'.`);
    } finally {
      setIsLoading(false);
    }
  };
  const handleOpenTextModal = () => setShowTextModal(true);
  const handleCloseTextModal = () => setShowTextModal(false);
  const handleTextUploadSubmit = async (uploadParams) => {
      const { datasetName: textDatasetName, dataText, dataFormat } = uploadParams;
       if (!dataText.trim() || !textDatasetName.trim()) {
           return handleError("Please provide data and a dataset name.");
       }
       setIsLoading(true); clearError();
       try {
           const result = await apiService.uploadTextDataset(textDatasetName, dataText, dataFormat);
           handleDatasetUploaded(result, "text_upload");
       } catch (error) {
           handleError(error, 'Text Upload failed');
       } finally {
           setIsLoading(false);
       }
   };
   const handleFetchDatasetInfo = async () => {
       if (!currentDataset) return handleError("Select a dataset first.");
       setIsLoading(true); clearError(); setShowColumnStatsPanel(false);
       try {
           const info = await apiService.getDatasetInfo(currentDataset);
           setDatasetInfo(info);
           setShowInfoPanel(true);
       } catch (err) {
           handleError(err, "Failed to fetch dataset info.");
           setShowInfoPanel(false);
       } finally {
           setIsLoading(false);
       }
   };
   const handleFetchColumnStats = async (columnName) => {
        if (!currentDataset || !columnName) return;
        setSelectedColumnForStats(columnName);
        setIsLoading(true); clearError(); setShowInfoPanel(false);
        try {
            const stats = await apiService.getColumnStats(currentDataset, columnName, engine);
            setColumnStats(stats);
            setShowColumnStatsPanel(true);
        } catch (err) {
            handleError(err, `Failed to fetch stats for column '${columnName}'.`);
            setShowColumnStatsPanel(false);
            setSelectedColumnForStats(null);
        } finally {
            setIsLoading(false);
        }
    };
  const handleCloseInfoPanel = () => { setShowInfoPanel(false); setDatasetInfo(null); };
  const handleCloseColumnStatsPanel = () => { setShowColumnStatsPanel(false); setColumnStats(null); setSelectedColumnForStats(null); };
  const handleToggleRaPanel = () => setShowRaPanel(prev => !prev);
  const handleRelationalOperation = async (operation, params, newDatasetName) => {
       if (!newDatasetName) return handleError("Please provide a name for the new dataset.");
       setIsLoading(true); clearError();
       try {
           const result = await apiService.performRelationalOperation(operation, params, newDatasetName);
           fetchDatasetsList(); // Refresh list
           alert(`Relational operation '${operation}' successful. Result saved as '${result.dataset_name}'. Select it from the dropdown to view.`);
           setShowRaPanel(false);
       } catch (err) {
           handleError(err, `Relational Algebra operation '${operation}' failed.`);
       } finally {
           setIsLoading(false);
       }
   };

  // --- NEW Handler for Rename ---
  const handleRenameRequest = (oldName) => {
    if (!oldName) return;
    const newName = prompt(`Enter new name for dataset "${oldName}":`);
    if (!newName || !newName.trim()) {
        alert("Rename cancelled or invalid name provided.");
        return;
    }
    if (newName.trim() === oldName) {
        alert("New name is the same as the old name.");
        return;
    }
    handleRenameDataset(oldName, newName.trim());
  };

  const handleRenameDataset = async (oldName, newName) => {
    setIsLoading(true); clearError();
    try {
        const result = await apiService.renameDataset(oldName, newName);
        setAvailableDatasets(result.datasets || []); // Update list from backend result
        if (currentDataset === oldName) {
           // Important: Update currentDataset state *without* triggering immediate load yet
           // The useEffect watching availableDatasets will handle the load correctly
           // when the list updates AND the name changes.
           setCurrentDataset(newName);
        }
        alert(result.message || `Renamed ${oldName} to ${newName}`);
    } catch (err) {
        handleError(err, `Failed to rename dataset '${oldName}' to '${newName}'.`);
    } finally {
        setIsLoading(false);
    }
};

  // --- (Optional) NEW Handler for Delete ---
    const handleDeleteRequest = (datasetNameToDelete) => {
        if (!datasetNameToDelete) return;
        if (window.confirm(`Are you sure you want to permanently delete the dataset "${datasetNameToDelete}"? This action cannot be undone.`)) {
             handleDeleteDataset(datasetNameToDelete);
        }
    };

    const handleDeleteDataset = async (datasetName) => {
      setIsLoading(true); clearError();
      try {
          const result = await apiService.deleteDataset(datasetName);
          // Optimistic update? Or wait for backend list? Backend list is safer.
          setAvailableDatasets(result.datasets || []);
          if (currentDataset === datasetName) {
              setCurrentDataset(null); // Deselect if current dataset is deleted
          }
          alert(result.message || `Deleted ${datasetName}`);
      } catch (err) {
          handleError(err, `Failed to delete dataset '${datasetName}'.`);
      } finally {
          setIsLoading(false);
      }
  };

    const handleRaDatasetSaved = (saveResult) => {
      console.log("RA Dataset Saved:", saveResult);
      setAvailableDatasets(saveResult.datasets || []); // Update list immediately from save response
      // Optionally, switch view to the newly saved dataset
      // setCurrentDataset(saveResult.dataset_name);
      // updateStateFromResult({ // Update preview based on save result
      //      data: saveResult.preview || [],
      //      columns: saveResult.columns || [],
      //      row_count: saveResult.row_count || 0,
      //      can_undo: false, // New dataset has no history
      //      can_reset: false,
      //      last_code: '',
      // }, "save_ra");
      setShowRaPanel(false); // Optionally close panel after save
 };


  // --- Render ---
  return (
    <Router>
      <div className="min-h-screen bg-maid-cream-light flex flex-col">
        {/* Loading Overlay (Keep existing) */}
         {isLoading && ( /* ... overlay div ... */
            <div className="fixed inset-0 bg-black bg-opacity-40 flex items-center justify-center z-50">
               <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-coffee"></div>
               <span className="ml-4 text-white text-xl font-medium">Loading...</span>
            </div>
         )}

        {/* Pass rename/delete handlers down ONLY IF NEEDED IN NAVBAR (unlikely) */}
        {/* Typically these belong on the management page */}
        <NavBar
          availableDatasets={availableDatasets}
          currentDataset={currentDataset}
          currentEngine={engine}
          onDatasetUploaded={handleDatasetUploaded}
          onEngineChange={handleEngineChange}
          onDatasetChange={handleDatasetChange}
          onExport={handleExport}
          onUploadText={handleOpenTextModal}
          onUploadDbFile={handleDbFileUpload}
          isLoading={isLoading}
          // Removed rename/delete handlers from NavBar props
        />

         {/* Error Display (keep existing) */}
         {error && ( /* ... error div ... */
            <div className="m-4 p-3 bg-red-100 text-red-700 rounded-md border border-red-200 flex justify-between items-center">
               <span>Error: {error}</span>
               <button onClick={clearError} className="ml-4 font-bold text-red-800 hover:text-red-900">✕</button>
            </div>
         )}

        <Routes>
           {/* Route for the main application */}
           <Route path="/" element={
             <MainAppContent
               // Pass all necessary state and handlers for the main view
               currentDataset={currentDataset}
               engine={engine}
               columns={columns}
               data={data}
               rowCount={rowCount}
               isLoading={isLoading}
               error={error} // Pass error state if needed inside main content specifically
               clearError={clearError}
               canUndo={canUndo}
               canReset={canReset}
               lastCode={lastCode}
               currentEditorCode={currentEditorCode}
               setCurrentEditorCode={setCurrentEditorCode}
               availableDatasets={availableDatasets}
               showRaPanel={showRaPanel}
               handleOperationSubmit={handleOperationSubmit}
               handleExecuteCode={handleExecuteCode}
               handleUndo={handleUndo}
               handleReset={handleReset}
               handleSaveTransform={handleSaveTransform}
               handleFetchDatasetInfo={handleFetchDatasetInfo}
               handleToggleRaPanel={handleToggleRaPanel}
               handleFetchColumnStats={handleFetchColumnStats}
               handleRaDatasetSaved={handleRaDatasetSaved} // Pass the new handler
               handleError={handleError} // Pass error handler to RA panel
               // Panels state
               showInfoPanel={showInfoPanel}
               datasetInfo={datasetInfo}
               handleCloseInfoPanel={handleCloseInfoPanel}
               showColumnStatsPanel={showColumnStatsPanel}
               columnStats={columnStats}
               handleCloseColumnStatsPanel={handleCloseColumnStatsPanel}
             />
           }/>

           {/* Route for the Dataset Manager */}
           <Route path="/manage-datasets" element={
             <DatasetManagerPage
               datasets={availableDatasets}
               onRenameRequest={handleRenameRequest}
               onDeleteRequest={handleDeleteRequest}
               isLoading={isLoading}
             />
           }/>
        </Routes>

      {/* Modals (remain the same) */}
      {/* ... (TextUploadModal, DB Import Modal) ... */}
      <TextUploadModal
        isOpen={showTextModal}
        onClose={handleCloseTextModal}
        onSubmit={handleTextUploadSubmit}
        isLoading={isLoading}
        currentDatasetName={currentDataset}
      />
       {showDbModal && tempDbId && (
        <div className="modal-overlay">
            {/* ... (modal content remains the same) ... */}
          <div className="modal-content bg-white rounded-lg shadow-xl p-6 max-w-md w-full">
            <h3 className="modal-header text-xl font-semibold text-maid-choco mb-4">Import Table from Database</h3>
            <form onSubmit={(e) => {
              e.preventDefault();
              const formData = new FormData(e.target);
              const tableName = formData.get('tableName');
              const newDatasetName = formData.get('newDatasetName');
              handleImportTable(tableName, newDatasetName);
            }}>
              <div className="mb-4">
                <label htmlFor="db-table-select" className="input-label">Select Table:</label>
                <select id="db-table-select" name="tableName" className="select-base" required disabled={isLoading || dbTables.length === 0}>
                  <option value="">Choose a table...</option>
                  {dbTables.map(table => (
                    <option key={table} value={table}>{table}</option>
                  ))}
                </select>
                {dbTables.length === 0 && <p className="text-xs text-gray-500 mt-1">No tables found in the database file.</p>}
              </div>

              <div className="mb-4">
                <label htmlFor="db-new-name-input" className="input-label">Dataset Name:</label>
                <input
                  type="text"
                  id="db-new-name-input"
                  name="newDatasetName"
                  className="input-base"
                  placeholder="Enter name for the imported dataset"
                  required
                  disabled={isLoading}
                />
              </div>

              <div className="flex justify-end space-x-3 mt-6">
                <button type="button" onClick={() => setShowDbModal(false)} disabled={isLoading} className="btn btn-gray">Cancel</button>
                <button type="submit" disabled={isLoading || dbTables.length === 0} className="btn btn-coffee">
                  {isLoading ? 'Importing...' : 'Import'}
                </button>
              </div>
            </form>
            <button onClick={() => setShowDbModal(false)} className="modal-close-button" aria-label="Close">
              &times;
            </button>
            {error && <p className="mt-4 text-red-600 text-sm">Error: {error}</p>}
          </div>
        </div>
      )}
    </div>
    </Router>
  );
}

function MainAppContent({
  currentDataset, engine, columns, data, rowCount, isLoading, error, clearError,
  canUndo, canReset, lastCode, currentEditorCode, setCurrentEditorCode, availableDatasets,
  showRaPanel, handleOperationSubmit, handleExecuteCode, handleUndo, handleReset,
  handleSaveTransform, handleFetchDatasetInfo, handleToggleRaPanel, handleFetchColumnStats,
  handleRaDatasetSaved, handleError, // Added RA save handler and error handler
  showInfoPanel, datasetInfo, handleCloseInfoPanel,
  showColumnStatsPanel, columnStats, handleCloseColumnStatsPanel
}) {
return (
   <main className="flex-grow p-4 max-w-full mx-auto w-full">
       {/* Top Button Row (keep existing) */}
       <div className="mb-4 flex flex-wrap justify-start items-center gap-2">
          {/* Add Link to Manage Datasets */}
          <Link to="/manage-datasets" className="btn btn-outline">
             Manage Datasets
          </Link>
          {currentDataset && (
             <>
                 <button onClick={handleUndo} disabled={!canUndo || isLoading} className="btn btn-yellow">Undo Last</button>
                 <button onClick={handleReset} disabled={!canReset || isLoading} className="btn btn-red">Reset</button>
                 <button onClick={handleSaveTransform} disabled={isLoading} className="btn btn-green">Save State As...</button>
             </>
          )}
          <button onClick={handleFetchDatasetInfo} disabled={!currentDataset || isLoading} className="btn btn-blue">Dataset Info</button>
          <button onClick={handleToggleRaPanel} className={`btn ${showRaPanel ? 'bg-coffee text-white' : 'btn-outline'}`}>
             {showRaPanel ? 'Hide' : 'Show'} Relational Algebra
          </button>
       </div>

      {/* Main Grid (keep existing) */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left Panel: Operations */}
        <div className="lg:col-span-1 space-y-4">
           <EnhancedOperationsPanel
              columns={columns}
              onOperationSubmit={handleOperationSubmit}
              availableDatasets={availableDatasets.filter(ds => ds !== currentDataset)}
              currentDataset={currentDataset}
              isLoading={isLoading}
           />
           {showRaPanel && (
              <RelationalAlgebraPanel
                  availableDatasets={availableDatasets}
                  onDatasetSaved={handleRaDatasetSaved} // Pass save handler
                  isLoading={isLoading}
                  onError={handleError} // Pass error handler
              />
           )}
        </div>

        {/* Right Area: Data Table, Code Display, Code Editor */}
        <div className="lg:col-span-2 space-y-4">
           <EnhancedDataTable
              data={data}
              columns={columns}
              totalRows={rowCount}
              isPreview={data.length < rowCount}
              onHeaderClick={handleFetchColumnStats}
              isLoading={isLoading}
           />
           <CodeDisplay code={lastCode} engine={engine} />
           <CodeEditor
              key={currentDataset + engine + lastCode}
              currentDataset={currentDataset}
              engine={engine}
              initialCode={currentEditorCode}
              onCodeExecuted={handleExecuteCode}
              isLoading={isLoading}
              onCodeChange={(code) => setCurrentEditorCode(code)}
           />
        </div>
      </div>

      {/* Panels (keep existing) */}
       {showInfoPanel && datasetInfo && (
          <DatasetInfoPanel info={datasetInfo} onClose={handleCloseInfoPanel} onColumnSelect={handleFetchColumnStats} />
       )}
       {showColumnStatsPanel && columnStats && (
          <ColumnStatsPanel stats={columnStats} onClose={handleCloseColumnStatsPanel} />
       )}
   </main>
);
}


export default App;