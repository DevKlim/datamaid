import React, { useState, useEffect } from 'react';
import NavBar from './components/NavBar';
import DataTable from './components/DataTable';
import OperationsPanel from './components/OperationsPanel';
import CodeEditor from './components/CodeEditor';
import CodeDisplay from './components/CodeDisplay';
import { 
  getDatasets, 
  getDataset, 
  performOperation, 
  loadMoreRows, 
  executeCustomCode,
  saveTransformation,
  exportDataset
} from './services/api';

function App() {
  const [datasets, setDatasets] = useState([]);
  const [currentDataset, setCurrentDataset] = useState(null);
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [rowCount, setRowCount] = useState(0);
  const [engine, setEngine] = useState('pandas');
  const [code, setCode] = useState('');
  const [loading, setLoading] = useState(false);
  const [viewMode, setViewMode] = useState('operations'); // 'operations' or 'code'
  const [showAllData, setShowAllData] = useState(false);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [newDatasetName, setNewDatasetName] = useState('');

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

  // Load dataset when current dataset changes
  useEffect(() => {
    if (currentDataset) {
      loadDataset(currentDataset);
    }
  }, [currentDataset, engine]);

  const loadDataset = async (datasetName) => {
    setLoading(true);
    try {
      const result = await getDataset(datasetName, engine);
      setData(result.data);
      setColumns(result.columns);
      setRowCount(result.row_count);
      setCode('');  // Reset code when loading a new dataset
    } catch (error) {
      console.error('Error loading dataset:', error);
      alert(`Failed to load dataset: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleLoadMoreData = async () => {
    if (!currentDataset) return;
    
    setLoading(true);
    try {
      const result = await loadMoreRows(currentDataset, engine, data.length, 50);
      // Append the new rows to existing data
      setData(prevData => [...prevData, ...result.data]);
    } catch (error) {
      console.error('Error loading more data:', error);
      alert(`Failed to load more data: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleDatasetUploaded = (result) => {
    // Add the new dataset to the list if not already there
    if (!datasets.includes(result.dataset_name)) {
      setDatasets(prev => [...prev, result.dataset_name]);
    }
    
    // Set the current dataset to the newly uploaded one
    setCurrentDataset(result.dataset_name);
    
    // Set the preview data
    setData(result.preview);
    setColumns(result.columns);
    setRowCount(result.row_count);
    setCode('');  // Reset code for new dataset
  };

  const handleEngineChange = (newEngine) => {
    setEngine(newEngine);
  };

  const handleDatasetChange = (datasetName) => {
    setCurrentDataset(datasetName);
  };

  const handleOperationSubmit = async (operation, params) => {
    if (!currentDataset) {
      alert('Please select a dataset first.');
      return;
    }

    setLoading(true);
    try {
      const result = await performOperation(currentDataset, operation, params, engine);
      setData(result.data);
      setColumns(result.columns);
      setRowCount(result.row_count);
      setCode(result.code);
    } catch (error) {
      console.error('Error performing operation:', error);
      alert(`Operation failed: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleCodeExecution = (result) => {
    setData(result.data);
    setColumns(result.columns);
    setRowCount(result.row_count);
    // No need to update code as it's already in the CodeEditor
  };

  const handleSaveTransformation = async () => {
    if (!currentDataset || !newDatasetName) return;
    
    setLoading(true);
    try {
      const result = await saveTransformation(currentDataset, newDatasetName, engine);
      
      // Add the new dataset to the list
      setDatasets(prev => [...prev, result.dataset_name]);
      alert(`Transformation saved as ${result.dataset_name}`);
      
      // Close the save dialog
      setSaveDialogOpen(false);
      setNewDatasetName('');
    } catch (error) {
      console.error('Error saving transformation:', error);
      alert(`Failed to save transformation: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleExportDataset = async (format = 'csv') => {
    if (!currentDataset) return;
    
    setLoading(true);
    try {
      await exportDataset(currentDataset, format, engine);
      // The download will be handled by the browser
    } catch (error) {
      console.error('Error exporting dataset:', error);
      alert(`Failed to export dataset: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const toggleViewMode = () => {
    setViewMode(viewMode === 'operations' ? 'code' : 'operations');
  };

  return (
    <div className="min-h-screen bg-gray-100">
      <NavBar 
        onDatasetUploaded={handleDatasetUploaded}
        onEngineChange={handleEngineChange}
        onDatasetChange={handleDatasetChange}
        availableDatasets={datasets}
        currentEngine={engine}
        onExport={handleExportDataset}
      />
      
      <div className="max-w-7xl mx-auto px-4 py-6">
        <div className="mb-4">
          <div className="flex justify-between items-center mb-4">
            <h1 className="text-2xl font-bold text-gray-800">
              {currentDataset ? `Dataset: ${currentDataset}` : 'Welcome to Data Analysis GUI'}
            </h1>
            
            <div className="flex items-center space-x-2">
              {currentDataset && (
                <>
                  <button
                    onClick={() => setSaveDialogOpen(true)}
                    className="px-3 py-1 text-sm bg-green-500 hover:bg-green-600 text-white rounded"
                  >
                    Save As
                  </button>
                  
                  <button
                    onClick={toggleViewMode}
                    className="px-3 py-1 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded"
                  >
                    {viewMode === 'operations' ? 'Switch to Code Editor' : 'Switch to Operations Panel'}
                  </button>
                </>
              )}
            </div>
          </div>
          
          {/* Save Dialog */}
          {saveDialogOpen && (
            <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
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
                    className="px-4 py-2 bg-gray-300 hover:bg-gray-400 rounded"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={handleSaveTransformation}
                    disabled={!newDatasetName}
                    className={`px-4 py-2 bg-blue-500 hover:bg-blue-600 text-white rounded ${
                      !newDatasetName ? 'opacity-50 cursor-not-allowed' : ''
                    }`}
                  >
                    Save
                  </button>
                </div>
              </div>
            </div>
          )}
          
          {loading ? (
            <div className="flex justify-center items-center py-12">
              <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
            </div>
          ) : currentDataset ? (
            <DataTable 
              data={data} 
              columns={columns} 
              totalRows={rowCount}
              isPreview={!showAllData}
            />
          ) : (
            <div className="bg-white p-6 rounded shadow text-center">
              <p className="text-lg text-gray-600 mb-4">
                Upload a CSV file or select an existing dataset to get started.
              </p>
              <p className="text-gray-500">
                This tool allows you to perform data operations through a user-friendly interface
                and generates the equivalent code in Pandas, Polars, or SQL.
              </p>
            </div>
          )}
          
          {currentDataset && !showAllData && data.length < rowCount && (
            <div className="mt-2 text-center">
              <button
                onClick={handleLoadMoreData}
                className="px-4 py-2 bg-blue-500 hover:bg-blue-600 text-white rounded"
              >
                Load More Rows
              </button>
            </div>
          )}
        </div>
        
        {currentDataset && (
          <div className="grid grid-cols-1 gap-6">
            {viewMode === 'operations' ? (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div>
                  <OperationsPanel 
                    columns={columns} 
                    onOperationSubmit={handleOperationSubmit} 
                  />
                </div>
                <div>
                  <CodeDisplay code={code} engine={engine} />
                </div>
              </div>
            ) : (
              <CodeEditor 
                currentDataset={currentDataset}
                engine={engine}
                onCodeExecuted={handleCodeExecution}
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default App;