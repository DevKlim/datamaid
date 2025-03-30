import React, { useState, useEffect } from 'react';
import NavBar from './components/NavBar';
import DataTable from './components/DataTable'; // Use your actual DataTable component name
import OperationsPanel from './components/OperationsPanel'; // Use your actual OperationsPanel component name
import CodeEditor from './components/CodeEditor';
import CodeDisplay from './components/CodeDisplay';
import {
  getDatasets,
  getDataset, // Updated to return flags/last_code
  performOperation, // Updated to return flags/last_code
  loadMoreRows,
  executeCustomCode, // Updated to return flags/last_code
  saveTransformation,
  exportDataset,
  undoTransformation, // New
  resetTransformation // New
} from './services/api'; // Make sure path is correct

function App() {
  const [datasets, setDatasets] = useState([]);
  const [currentDataset, setCurrentDataset] = useState(null);
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [rowCount, setRowCount] = useState(0);
  const [engine, setEngine] = useState('pandas');
  const [code, setCode] = useState(''); // Displays the *last* generated code
  const [loading, setLoading] = useState(false);
  const [viewMode, setViewMode] = useState('operations');
  const [showAllData, setShowAllData] = useState(false); // Consider removing if loadMore is primary
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [newDatasetName, setNewDatasetName] = useState('');
  const [canUndo, setCanUndo] = useState(false); // New state
  const [canReset, setCanReset] = useState(false); // New state

  // Fetch available datasets on mount
  useEffect(() => {
    const fetchDatasets = async () => {
      try {
        const datasets = await getDatasets();
        setDatasets(datasets);
      } catch (error) {
        console.error('Error fetching datasets:', error);
      }
    };
    fetchDatasets();
  }, []);

  // Load dataset when current dataset or engine changes
  useEffect(() => {
    if (currentDataset) {
      loadDataset(currentDataset);
    } else {
      // Clear state if no dataset selected
      setData([]);
      setColumns([]);
      setRowCount(0);
      setCode('');
      setCanUndo(false);
      setCanReset(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentDataset, engine]); // Dependency array is correct

  const loadDataset = async (datasetName) => {
    setLoading(true);
    try {
      // Fetch initial preview for the selected engine
      const result = await getDataset(datasetName, engine, 100); // Fetch initial 100 rows
      setData(result.data);
      setColumns(result.columns);
      setRowCount(result.row_count);
      setCode(result.last_code || ''); // Show code from last step if available
      setCanUndo(result.can_undo || false);
      setCanReset(result.can_reset || false);
      setShowAllData(false); // Reset view state
    } catch (error) {
      console.error('Error loading dataset:', error);
      alert(`Failed to load dataset: ${error.message}`);
      // Reset state on load failure
      setCurrentDataset(null);
      setData([]);
      setColumns([]);
      setRowCount(0);
      setCode('');
      setCanUndo(false);
      setCanReset(false);
    } finally {
      setLoading(false);
    }
  };

  const handleLoadMoreData = async () => {
    if (!currentDataset || data.length >= rowCount) return;
    setLoading(true);
    try {
      // Load more rows, appending to existing data
      const result = await loadMoreRows(currentDataset, engine, data.length, 50);
      setData(prevData => [...prevData, ...result.data]);
      // Note: loadMoreRows doesn't update flags or code display
    } catch (error) {
      console.error('Error loading more data:', error);
      alert(`Failed to load more data: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Helper to update state after an operation/undo/reset
  const updateStateFromResult = (result) => {
      setData(result.data || []);
      setColumns(result.columns || []);
      setRowCount(result.row_count || 0);
      // Use last_code from undo/reset, or generated code from operation
      setCode(result.last_code !== undefined ? result.last_code : (result.code || ''));
      setCanUndo(result.can_undo || false);
      setCanReset(result.can_reset || false);
  };

  const handleDatasetUploaded = (result) => {
    if (!datasets.includes(result.dataset_name)) {
      setDatasets(prev => [...prev, result.dataset_name]);
    }
    // Set current dataset *after* adding to list if needed
    setCurrentDataset(result.dataset_name);
    // The useEffect for currentDataset will trigger loadDataset
  };

  const handleEngineChange = (newEngine) => {
    setEngine(newEngine); // useEffect will reload data with the new engine
  };

  const handleDatasetChange = (datasetName) => {
    if (datasetName === currentDataset) return; // Avoid reload if same dataset selected
    setCurrentDataset(datasetName); // useEffect will reload data
  };

  const handleOperationSubmit = async (operation, params) => {
    if (!currentDataset) return;
    setLoading(true);
    try {
      const result = await performOperation(currentDataset, operation, params, engine);
      updateStateFromResult(result); // Use helper to update state
    } catch (error) {
      console.error('Error performing operation:', error);
      alert(`Operation failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleCodeExecution = async (customCode) => { // Pass the code from editor
      if (!currentDataset) return;
      setLoading(true);
      try {
        const result = await executeCustomCode(currentDataset, customCode, engine);
        updateStateFromResult(result); // Use helper
      } catch (error) {
        console.error('Error executing code:', error);
        alert(`Code execution failed: ${error.response?.data?.detail || error.message}`);
      } finally {
        setLoading(false);
      }
    };

  // --- Undo Handler ---
  const handleUndo = async () => {
    if (!currentDataset || !canUndo) return;
    setLoading(true);
    try {
      const result = await undoTransformation(currentDataset, engine); // Pass engine for preview
      updateStateFromResult(result); // Update state with reverted data/flags
      alert(result.message || 'Last operation undone.');
    } catch (error) {
      console.error('Error undoing operation:', error);
      alert(`Undo failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  // --- Reset Handler ---
  const handleReset = async () => {
    if (!currentDataset || !canReset) return;
    if (!window.confirm("Are you sure you want to discard all changes for this dataset?")) {
        return;
    }
    setLoading(true);
    try {
      const result = await resetTransformation(currentDataset, engine); // Pass engine for preview
      updateStateFromResult(result); // Update state with original data/flags
      alert(result.message || 'Dataset reset to original state.');
    } catch (error) {
      console.error('Error resetting dataset:', error);
      alert(`Reset failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleSaveTransformation = async () => {
    if (!currentDataset || !newDatasetName) return;
    setLoading(true);
    try {
      const result = await saveTransformation(currentDataset, newDatasetName, engine);
      if (!datasets.includes(result.dataset_name)) { // Avoid duplicates
          setDatasets(prev => [...prev, result.dataset_name]);
      }
      alert(`Transformation saved as ${result.dataset_name}`);
      setSaveDialogOpen(false);
      setNewDatasetName('');
      // Optionally, switch to the new dataset
      // setCurrentDataset(result.dataset_name);
    } catch (error) {
      console.error('Error saving transformation:', error);
      alert(`Failed to save transformation: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleExportDataset = async (format = 'csv') => {
    if (!currentDataset) return;
    setLoading(true);
    try {
      await exportDataset(currentDataset, format, engine);
      // Download is handled in api.js
    } catch (error) {
      console.error('Error exporting dataset:', error);
      alert(`Failed to export dataset: ${error.response?.data?.detail || error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const toggleViewMode = () => {
    setViewMode(viewMode === 'operations' ? 'code' : 'operations');
  };

  return (
    <div className="min-h-screen bg-gray-100 flex flex-col">
      <NavBar
        onDatasetUploaded={handleDatasetUploaded}
        onEngineChange={handleEngineChange}
        onDatasetChange={handleDatasetChange}
        availableDatasets={datasets}
        currentEngine={engine}
        currentDataset={currentDataset} // Pass current dataset
        onExport={handleExportDataset}
      />

      <main className="flex-grow max-w-full mx-auto px-4 py-6 w-full">
        <div className="mb-4">
          <div className="flex justify-between items-center mb-4 gap-4 flex-wrap">
            <h1 className="text-2xl font-bold text-gray-800 flex-shrink-0">
              {currentDataset ? `Dataset: ${currentDataset}` : 'DataMaid Analysis GUI'}
            </h1>

            {/* Action Buttons */}
            <div className="flex items-center space-x-2 flex-wrap">
              {currentDataset && (
                <>
                   <button
                    onClick={handleUndo}
                    disabled={!canUndo || loading}
                    className={`px-3 py-1 text-sm bg-yellow-500 hover:bg-yellow-600 text-white rounded disabled:opacity-50 disabled:cursor-not-allowed`}
                  >
                    Undo
                  </button>
                   <button
                    onClick={handleReset}
                    disabled={!canReset || loading}
                    className={`px-3 py-1 text-sm bg-red-500 hover:bg-red-600 text-white rounded disabled:opacity-50 disabled:cursor-not-allowed`}
                  >
                    Reset
                  </button>
                   <button
                    onClick={() => setSaveDialogOpen(true)}
                    disabled={!currentDataset || loading}
                    className="px-3 py-1 text-sm bg-green-500 hover:bg-green-600 text-white rounded disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Save As
                  </button>
                  <button
                    onClick={toggleViewMode}
                    disabled={!currentDataset || loading}
                    className="px-3 py-1 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {viewMode === 'operations' ? 'Code Editor' : 'Operations Panel'}
                  </button>
                </>
              )}
            </div>
          </div>

          {/* Save Dialog (Keep as is) */}
          {saveDialogOpen && (
             <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
               {/* ... your save dialog content ... */}
               <div className="bg-white p-6 rounded shadow-lg w-96">
                 <h2 className="text-xl font-bold mb-4">Save Transformation</h2>
                 <div className="mb-4">
                   <label className="block text-gray-700 text-sm font-bold mb-2">
                     New Dataset Name:
                   </label>
                   <input
                     type="text"
                     value={newDatasetName}
                     onChange={(e) => setNewDatasetName(e.target.value)}
                     className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                     placeholder="Enter a name for the new dataset"
                   />
                 </div>
                 <div className="flex justify-end space-x-2">
                   <button
                     onClick={() => setSaveDialogOpen(false)}
                     disabled={loading}
                     className="px-4 py-2 bg-gray-300 hover:bg-gray-400 rounded disabled:opacity-50"
                   >
                     Cancel
                   </button>
                   <button
                     onClick={handleSaveTransformation}
                     disabled={!newDatasetName || loading}
                     className={`px-4 py-2 bg-blue-500 hover:bg-blue-600 text-white rounded ${
                       !newDatasetName || loading ? 'opacity-50 cursor-not-allowed' : ''
                     }`}
                   >
                     {loading ? 'Saving...' : 'Save'}
                   </button>
                 </div>
               </div>
             </div>
          )}

          {/* Loading Indicator and Data Table */}
          {loading && (
            <div className="flex justify-center items-center py-12">
              <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
               <span className="ml-4 text-gray-600">Loading...</span>
            </div>
          )}

          {!loading && currentDataset && (
             <>
              <DataTable
                key={`${currentDataset}-${columns.join('-')}`} // Add key to force re-render on column change
                data={data}
                columns={columns}
                totalRows={rowCount}
                isPreview={!showAllData && data.length < rowCount}
              />
              {!showAllData && data.length < rowCount && (
                 <div className="mt-2 text-center">
                  <button
                    onClick={handleLoadMoreData}
                    disabled={loading}
                    className="px-4 py-2 bg-blue-500 hover:bg-blue-600 text-white rounded disabled:opacity-50"
                  >
                    Load More Rows ({rowCount.toLocaleString()} total)
                  </button>
                 </div>
               )}
             </>
          )}

          {!loading && !currentDataset && (
            <div className="bg-white p-6 rounded shadow text-center">
              <p className="text-lg text-gray-600 mb-4">
                Upload a CSV file or select an existing dataset to get started.
              </p>
              <p className="text-gray-500">
                Use the operations panel or code editor to transform your data. Use Undo/Reset to manage changes.
              </p>
            </div>
          )}
        </div>

        {/* Operations Panel / Code Editor */}
        {currentDataset && !loading && (
          <div className="grid grid-cols-1 gap-6 mt-6">
            {viewMode === 'operations' ? (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <OperationsPanel
                  columns={columns}
                  onOperationSubmit={handleOperationSubmit}
                  key={currentDataset + '-ops'} // Add key for potential reset needs
                />
                <CodeDisplay
                    code={code} // Now shows last operation's code
                    engine={engine}
                />
              </div>
            ) : (
              <CodeEditor
                currentDataset={currentDataset}
                engine={engine}
                initialCode={code} // Pass current code to editor potentially
                onCodeExecuted={handleCodeExecution} // Needs to accept code string
                key={currentDataset + '-editor'} // Add key
              />
            )}
          </div>
        )}
      </main>
    </div>
  );
}

export default App;