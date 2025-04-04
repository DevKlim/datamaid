// src/App.jsx
import React, { useState, useEffect, useCallback } from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import apiService from './services/api';

import NavBar from './components/NavBar';
import EnhancedDataTable from './components/DataTable';
import EnhancedOperationsPanel from './components/OperationsPanel'; // <-- Import Operations Panel
import CodeDisplay from './components/CodeDisplay';
import CodeEditor from './components/CodeEditor';
import TextUploadModal from './components/TextUploadModal';
import DatasetInfoPanel from './components/DatasetInfoPanel';
import ColumnStatsPanel from './components/ColumnStatsPanel';
import RelationalAlgebraPanel from './components/RelationalAlgebraPanel';
import DatasetManagerPage from './components/DatasetManagerPage';

function App() {
  // --- Core State ---
  const [availableDatasets, setAvailableDatasets] = useState([]);
  const [currentViewName, setCurrentViewName] = useState(null);
  const [engine, setEngine] = useState('pandas'); // Default engine

  // State for the *currently viewed* dataset's data
  const [viewData, setViewData] = useState([]);
  const [viewColumns, setViewColumns] = useState([]);
  const [viewRowCount, setViewRowCount] = useState(0);
  const [viewCanUndo, setViewCanUndo] = useState(false);
  const [viewCanReset, setViewCanReset] = useState(false);

  // General App State
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastExecutedCode, setLastExecutedCode] = useState(''); // Store last executed code OR code from operation panel

  // DB Upload State
  const [tempDbId, setTempDbId] = useState(null);
  const [dbTables, setDbTables] = useState([]);
  const [showDbModal, setShowDbModal] = useState(false);

  // Text Upload Modal State
  const [showTextModal, setShowTextModal] = useState(false);

  // Dataset Info Panel State
  const [datasetInfo, setDatasetInfo] = useState(null);
  const [showInfoPanel, setShowInfoPanel] = useState(false);

  // Column Stats Panel State
  const [columnStats, setColumnStats] = useState(null);
  const [selectedColumnForStats, setSelectedColumnForStats] = useState(null);
  const [showColumnStatsPanel, setShowColumnStatsPanel] = useState(false);

  // Relational Algebra Panel State
  const [showRaPanel, setShowRaPanel] = useState(false);

  // --- Utility Functions ---
  const clearError = () => setError(null);
  const handleError = (err, defaultMessage = 'An error occurred') => {
     console.error("Error caught in handler:", err);
     let message = defaultMessage;
     if (typeof err === 'string') { message = err; }
     else if (err?.response?.data?.detail) {
        const detail = err.response.data.detail;
         if (typeof detail === 'string') message = detail;
         else if (Array.isArray(detail)) message = detail.map(d => `${d.loc?.join('.')} - ${d.msg}`).join('; ');
         else message = JSON.stringify(detail);
     } else if (err?.message) { message = err.message; }
     setError(message);
     setIsLoading(false);
  };

  // --- Data Fetching and State Update ---
  const fetchDatasetsList = useCallback(async () => {
         clearError();
         try {
             const datasets = await apiService.getDatasets();
             setAvailableDatasets(datasets || []);
             if (currentViewName && !datasets.includes(currentViewName)) {
                 console.log(`Current view '${currentViewName}' no longer available. Resetting view.`);
                 setCurrentViewName(null);
             }
         } catch (err) {
              console.error('Failed to fetch dataset list:', err);
         }
    }, [currentViewName]);

    // Updates the VIEW state based on API result for a specific dataset
    const updateViewState = (result, datasetName) => {
      // Ensure result is valid and matches the intended dataset
      if (!result || !datasetName || datasetName !== currentViewName) {
          console.warn("updateViewState called with invalid result or mismatched dataset name.", { result, datasetName, currentViewName });
          return;
      }

      console.log(`Updating view state for: ${datasetName}`);
      setViewData(result.data || []);
      setViewColumns(result.columns || []);
      setViewRowCount(result.row_count || 0);
      setViewCanUndo(result.can_undo || false);
      setViewCanReset(result.can_reset || false);

      // Optionally display generated code from operation panel
      if (result.generated_code) {
          setLastExecutedCode(result.generated_code);
      }

      // Reset info/stats panels when view data changes significantly
      setShowInfoPanel(false); setDatasetInfo(null);
      setShowColumnStatsPanel(false); setColumnStats(null); setSelectedColumnForStats(null);
  };

 // Loads data for the `currentViewName`
 const loadViewData = useCallback(async (viewName) => {
      if (!viewName) {
        setViewData([]); setViewColumns([]); setViewRowCount(0);
        setViewCanUndo(false); setViewCanReset(false);
        setDatasetInfo(null); setShowInfoPanel(false);
        setColumnStats(null); setShowColumnStatsPanel(false); setSelectedColumnForStats(null);
        clearError();
        return;
      }
      if (!availableDatasets.includes(viewName)) {
          console.warn(`Attempted to load view for unavailable dataset: ${viewName}. Clearing view.`);
          setCurrentViewName(null);
          return;
      }
      console.log(`Loading view data for: ${viewName}`);
      setIsLoading(true); clearError();
      setShowInfoPanel(false); setShowColumnStatsPanel(false);

      try {
        const result = await apiService.getDatasetView(viewName);
        if (result) {
           updateViewState(result, viewName);
        } else {
            throw new Error(`Received no result when loading view for dataset '${viewName}'.`);
        }
      } catch (err) {
        handleError(err, `Failed to load view for dataset '${viewName}'.`);
        setViewData([]); setViewColumns([]); setViewRowCount(0);
        setViewCanUndo(false); setViewCanReset(false);
      } finally {
        setIsLoading(false);
      }
  }, [availableDatasets]); // Dependency added

  // Initial load and fetch list
  useEffect(() => {
    fetchDatasetsList();
    apiService.testConnection().then(r => console.log(r.message)).catch(e => setError("Backend connection failed. Check if the server is running."));
 }, [fetchDatasetsList]);

 // Reload view data when the selected view name changes
 useEffect(() => {
     console.log(`Effect: currentViewName changed to '${currentViewName}'. Loading data.`);
     loadViewData(currentViewName);
     // eslint-disable-next-line react-hooks/exhaustive-deps
 }, [currentViewName]);


  // --- Handlers for UI Actions ---

  // Handles results from Upload, DB Import, RA Save
  const handleDatasetCreation = (result, source = "upload") => {
    if (result && result.dataset_name && result.datasets) {
        setAvailableDatasets(result.datasets);
        setCurrentViewName(result.dataset_name);
        setViewData(result.preview || []);
        setViewColumns(result.columns || []);
        setViewRowCount(result.row_count || 0);
        setViewCanUndo(false);
        setViewCanReset(false);
        setLastExecutedCode('');
        clearError();
        setShowTextModal(false);
        setShowDbModal(false);
        setShowRaPanel(false);
    } else {
        handleError(null, `${source} process did not return expected dataset information.`);
    }
 };

  const handleEngineChange = (newEngine) => {
    setEngine(newEngine);
  };

  const handleDatasetChange = (datasetName) => {
    if (datasetName !== currentViewName) {
      console.log(`Changing view from '${currentViewName}' to '${datasetName}'`);
      setCurrentViewName(datasetName);
      setLastExecutedCode('');
    }
  };

  // --- Handler for Operations Panel ---
  const handleOperationSubmit = async (operation, params) => {
    console.log("App.jsx: handleOperationSubmit called with:", operation, params, "Engine:", engine); // Log engine
    if (!currentViewName) {
      handleError("Please select a dataset first.");
      return;
    }
    setIsLoading(true); clearError();

    try {
      // Use the NEW endpoint and pass the engine
      const result = await apiService.applyStructuredOperation(
          currentViewName,
          operation,
          params, // apiService will stringify
          engine // Pass the current engine state
      );
      console.log("Operation result:", result);

      // Update view state based on the result
      updateViewState(result, currentViewName); // Update view with new state
      // Update the list of datasets if it changed (e.g., merge creating new?) - unlikely for ops panel
      // fetchDatasetsList(); // Maybe not needed here unless ops can create datasets

      // Store generated code/snippet if needed for display
      setLastExecutedCode(result.generated_code || '');

    } catch (err) {
      handleError(err, `Failed to apply operation '${operation}'.`);
    } finally {
      setIsLoading(false);
    }
  };


  const handleExecuteCode = async (codeToExecute) => {
    console.log("App.jsx: handleExecuteCode called with code:", codeToExecute);
    if (!codeToExecute.trim()) {
        handleError("Write some code to execute.");
        return;
    }
    setIsLoading(true); clearError();
    setLastExecutedCode(codeToExecute);

    try {
      const result = await apiService.executeCustomCode(codeToExecute, engine, currentViewName);
      console.log("Code execution result:", result);

      if (result.datasets) {
        setAvailableDatasets(result.datasets);
      }

      if (result.primary_result_name && result.primary_result_name === currentViewName) {
          updateViewState({
              data: result.preview,
              columns: result.columns,
              row_count: result.row_count,
              can_undo: result.can_undo ?? viewCanUndo,
              can_reset: result.can_reset ?? viewCanReset
          }, currentViewName);
      } else if (result.primary_result_name && result.primary_result_name !== currentViewName) {
          setCurrentViewName(result.primary_result_name);
          setViewData(result.preview || []);
          setViewColumns(result.columns || []);
          setViewRowCount(result.row_count || 0);
          setViewCanUndo(result.can_undo ?? false);
          setViewCanReset(result.can_reset ?? false);
      } else if (currentViewName) {
          loadViewData(currentViewName);
      }

      console.log("Custom code execution successful.");
    } catch (err) {
      handleError(err, 'Failed to execute custom code.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleUndo = async () => {
    if (!currentViewName || !viewCanUndo) return;
    setIsLoading(true); clearError();
    try {
      const result = await apiService.undoTransformation(currentViewName);
      updateViewState(result, currentViewName);
      console.log("Undo successful for:", currentViewName);
    } catch (err) {
      handleError(err, `Failed to undo operation for '${currentViewName}'.`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleReset = async () => {
    if (!currentViewName || !viewCanReset) return;
    setIsLoading(true); clearError();
    try {
      const result = await apiService.resetTransformation(currentViewName);
      updateViewState(result, currentViewName);
      console.log("Reset successful for:", currentViewName);
    } catch (err) {
      handleError(err, `Failed to reset transformations for '${currentViewName}'.`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleExport = async (format) => {
    if (!currentViewName) return handleError("Please select a dataset to export.");
    setIsLoading(true); clearError();
    try {
      await apiService.exportDataset(currentViewName, format);
    } catch (err) {
       console.error("Export trigger failed in App.jsx");
    } finally {
      setIsLoading(false);
    }
  };

  // DB Upload Handlers
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
      handleDatasetCreation(result, "db_import");
    } catch (err) {
      handleError(err, `Failed to import table '${tableName}'.`);
    } finally {
      setIsLoading(false);
    }
  };

  // Text Upload Handlers
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
           handleDatasetCreation(result, "text_upload");
       } catch (error) {
           handleError(error, 'Text Upload failed');
       } finally {
           setIsLoading(false);
       }
   };

   // Info/Stats Panel Handlers
   const handleFetchDatasetInfo = async () => {
       if (!currentViewName) return handleError("Select a dataset to view its info.");
       setIsLoading(true); clearError(); setShowColumnStatsPanel(false);
       try {
           const info = await apiService.getDatasetInfo(currentViewName);
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
        if (!currentViewName || !columnName) return;
        setSelectedColumnForStats(columnName);
        setIsLoading(true); clearError(); setShowInfoPanel(false);
        try {
            const stats = await apiService.getColumnStats(currentViewName, columnName);
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

  // RA Handlers
  const handleToggleRaPanel = () => setShowRaPanel(prev => !prev);
  const handleRaDatasetSaved = (saveResult) => {
      console.log("RA Dataset Saved:", saveResult);
      handleDatasetCreation(saveResult, "save_ra");
  };

  // Rename/Delete Handlers
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
        setAvailableDatasets(result.datasets || []);
        if (currentViewName === oldName) {
           setCurrentViewName(newName);
        }
        alert(result.message || `Renamed ${oldName} to ${newName}`);
    } catch (err) {
        handleError(err, `Failed to rename dataset '${oldName}' to '${newName}'.`);
    } finally {
        setIsLoading(false);
    }
};

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
          setAvailableDatasets(result.datasets || []);
          if (currentViewName === datasetName) {
              setCurrentViewName(null);
          }
          alert(result.message || `Deleted ${datasetName}`);
      } catch (err) {
          handleError(err, `Failed to delete dataset '${datasetName}'.`);
      } finally {
          setIsLoading(false);
      }
  };


  // --- Render ---
  return (
    <Router>
      <div className="min-h-screen bg-maid-cream-light flex flex-col">
         {isLoading && (
            <div className="fixed inset-0 bg-black bg-opacity-40 flex items-center justify-center z-50">
               <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-coffee"></div>
               <span className="ml-4 text-white text-xl font-medium">Loading...</span>
            </div>
         )}

        <NavBar
          availableDatasets={availableDatasets}
          currentDataset={currentViewName}
          currentEngine={engine}
          onDatasetUploaded={(result) => handleDatasetCreation(result, "upload")}
          onEngineChange={handleEngineChange}
          onDatasetChange={handleDatasetChange}
          onExport={handleExport}
          onUploadText={handleOpenTextModal}
          onUploadDbFile={handleDbFileUpload}
          isLoading={isLoading}
        />

         {error && (
            <div className="m-4 p-3 bg-red-100 text-red-700 rounded-md border border-red-200 flex justify-between items-center">
               <span>Error: {error}</span>
               <button onClick={clearError} className="ml-4 font-bold text-red-800 hover:text-red-900">✕</button>
            </div>
         )}

        <Routes>
           <Route path="/" element={
             <MainAppContent
               currentViewName={currentViewName}
               engine={engine}
               columns={viewColumns} // Pass viewColumns
               data={viewData}
               rowCount={viewRowCount}
               isLoading={isLoading}
               canUndo={viewCanUndo}
               canReset={viewCanReset}
               lastExecutedCode={lastExecutedCode}
               availableDatasets={availableDatasets}
               showRaPanel={showRaPanel}
               // Pass handlers
               handleOperationSubmit={handleOperationSubmit} // <-- Pass operation handler
               handleExecuteCode={handleExecuteCode}
               handleUndo={handleUndo}
               handleReset={handleReset}
               handleFetchDatasetInfo={handleFetchDatasetInfo}
               handleToggleRaPanel={handleToggleRaPanel}
               handleFetchColumnStats={handleFetchColumnStats}
               handleRaDatasetSaved={handleRaDatasetSaved}
               handleError={handleError}
               // Panels state
               showInfoPanel={showInfoPanel}
               datasetInfo={datasetInfo}
               handleCloseInfoPanel={handleCloseInfoPanel}
               showColumnStatsPanel={showColumnStatsPanel}
               columnStats={columnStats}
               handleCloseColumnStatsPanel={handleCloseColumnStatsPanel}
             />
           }/>

           <Route path="/manage-datasets" element={
             <DatasetManagerPage
               datasets={availableDatasets}
               onRenameRequest={handleRenameRequest}
               onDeleteRequest={handleDeleteRequest}
               isLoading={isLoading}
             />
           }/>
        </Routes>

      {/* Modals */}
      <TextUploadModal
        isOpen={showTextModal}
        onClose={handleCloseTextModal}
        onSubmit={handleTextUploadSubmit}
        isLoading={isLoading}
        currentDatasetName={currentViewName}
      />
       {showDbModal && tempDbId && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-40">
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
                <label htmlFor="db-table-select" className="input-label block text-sm font-medium text-maid-choco mb-1">Select Table:</label>
                <select id="db-table-select" name="tableName" className="select-base w-full p-2 border border-maid-gray rounded-md focus:ring-coffee-light focus:border-coffee-light" required disabled={isLoading || dbTables.length === 0}>
                  <option value="">Choose a table...</option>
                  {dbTables.map(table => (
                    <option key={table} value={table}>{table}</option>
                  ))}
                </select>
                {dbTables.length === 0 && <p className="text-xs text-gray-500 mt-1">No tables found in the database file.</p>}
              </div>

              <div className="mb-4">
                <label htmlFor="db-new-name-input" className="input-label block text-sm font-medium text-maid-choco mb-1">Dataset Name:</label>
                <input
                  type="text"
                  id="db-new-name-input"
                  name="newDatasetName"
                  className="input-base w-full p-2 border border-maid-gray rounded-md focus:ring-coffee-light focus:border-coffee-light"
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
             <button onClick={() => setShowDbModal(false)} className="absolute top-2 right-2 text-gray-500 hover:text-gray-700 text-2xl" aria-label="Close">
               ×
             </button>
            {error && <p className="mt-4 text-red-600 text-sm">Error: {error}</p>}
          </div>
        </div>
      )}
    </div>
    </Router>
  );
}

// --- MainAppContent Component ---
function MainAppContent({
  currentViewName, engine, columns, data, rowCount, isLoading,
  canUndo, canReset, lastExecutedCode, availableDatasets,
  showRaPanel,
  handleOperationSubmit, // <-- Receive handler
  handleExecuteCode, handleUndo, handleReset,
  handleFetchDatasetInfo, handleToggleRaPanel, handleFetchColumnStats,
  handleRaDatasetSaved, handleError,
  showInfoPanel, datasetInfo, handleCloseInfoPanel,
  showColumnStatsPanel, columnStats, handleCloseColumnStatsPanel
}) {
return (
   <main className="flex-grow p-4 max-w-full mx-auto w-full">
       <div className="mb-4 flex flex-wrap justify-start items-center gap-2">
          <Link to="/manage-datasets" className="btn btn-outline">
             Manage Datasets
          </Link>
          <button onClick={handleUndo} disabled={!currentViewName || !canUndo || isLoading} className="btn btn-yellow">Undo Last Change</button>
          <button onClick={handleReset} disabled={!currentViewName || !canReset || isLoading} className="btn btn-red">Reset History</button>
          <button onClick={handleFetchDatasetInfo} disabled={!currentViewName || isLoading} className="btn btn-blue">Dataset Info</button>
          <button onClick={handleToggleRaPanel} className={`btn ${showRaPanel ? 'bg-coffee text-white' : 'btn-outline'}`}>
             {showRaPanel ? 'Hide' : 'Show'} Relational Algebra
          </button>
       </div>

      {/* Main Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Left Panel: Operations Panel and RA Panel */}
        <div className="lg:col-span-1 space-y-4">
           {/* --- Add Operations Panel Back --- */}
           <EnhancedOperationsPanel
                columns={columns || []} // Pass current view columns
                onOperationSubmit={handleOperationSubmit} // Pass the handler
                availableDatasets={availableDatasets} // Pass for merge target selection
                currentDataset={currentViewName} // Pass current view name
                isLoading={isLoading} // Pass loading state
            />
           {showRaPanel && (
              <RelationalAlgebraPanel
                  availableDatasets={availableDatasets}
                  onDatasetSaved={handleRaDatasetSaved}
                  isLoading={isLoading}
                  onError={handleError}
              />
            )}
            {!showRaPanel && (
                <div className="card p-4 bg-white rounded-lg border border-maid-gray-light shadow-soft flex flex-col items-center justify-center h-48">
                    <div className="text-maid-choco-dark text-center mb-2">Relational Algebra</div>
                    <div className="text-maid-gray-dark text-sm">Click button above to show RA panel ♡</div>
                </div>
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
              datasetName={currentViewName}
           />
           <CodeDisplay code={lastExecutedCode} engine={engine} title="Last Action Code" />
           <CodeEditor
             key={currentViewName + engine}
             currentDataset={currentViewName}
             engine={engine}
             initialCode={""}
             onExecute={handleExecuteCode}
             isLoading={isLoading}
           />
        </div>
      </div>

      {/* Panels */}
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