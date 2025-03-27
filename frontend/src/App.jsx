import React, { useState, useEffect } from 'react';
import NavBar from './components/NavBar';
import DataTable from './components/DataTable';
import OperationsPanel from './components/OperationsPanel';
import CodeDisplay from './components/CodeDisplay';
import { getDatasets, getDataset, performOperation } from './services/api';

function App() {
  const [datasets, setDatasets] = useState([]);
  const [currentDataset, setCurrentDataset] = useState(null);
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [rowCount, setRowCount] = useState(0);
  const [engine, setEngine] = useState('pandas');
  const [code, setCode] = useState('');
  const [loading, setLoading] = useState(false);

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

  return (
    <div className="min-h-screen bg-gray-100">
      <NavBar 
        onDatasetUploaded={handleDatasetUploaded}
        onEngineChange={handleEngineChange}
        onDatasetChange={handleDatasetChange}
        availableDatasets={datasets}
      />
      
      <div className="max-w-7xl mx-auto px-4 py-6">
        <div className="mb-4">
          <div className="flex justify-between items-center mb-2">
            <h1 className="text-2xl font-bold text-gray-800">
              {currentDataset ? `Dataset: ${currentDataset}` : 'Welcome to Data Analysis GUI'}
            </h1>
            {rowCount > 0 && (
              <span className="text-gray-600">
                {rowCount.toLocaleString()} row{rowCount !== 1 ? 's' : ''}
                {data.length < rowCount ? ` (showing ${data.length})` : ''}
              </span>
            )}
          </div>
          
          {loading ? (
            <div className="flex justify-center items-center py-12">
              <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
            </div>
          ) : currentDataset ? (
            <DataTable data={data} columns={columns} />
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
        </div>
        
        {currentDataset && (
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
        )}
      </div>
    </div>
  );
}

export default App;