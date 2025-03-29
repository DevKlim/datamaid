import React, { useState } from 'react';
import { uploadDataset } from '../services/api';

const NavBar = ({ onDatasetUploaded, onEngineChange, onDatasetChange, availableDatasets }) => {
  const [uploading, setUploading] = useState(false);
  const [datasetName, setDatasetName] = useState('');
  const [engine, setEngine] = useState('pandas');

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    
    // Create a dataset name if not provided
    const name = datasetName || file.name.replace(/\.[^/.]+$/, "");
    
    setUploading(true);
    try {
      const result = await uploadDataset(file, name);
      onDatasetUploaded(result);
      // Reset the file input
      e.target.value = null;
      setDatasetName('');
    } catch (error) {
      console.error('Error uploading file:', error);
      alert('Failed to upload file. Please try again.');
    } finally {
      setUploading(false);
    }
  };

  const handleEngineChange = (e) => {
    const newEngine = e.target.value;
    setEngine(newEngine);
    onEngineChange(newEngine);
  };

  const handleDatasetSelect = (e) => {
    const dataset = e.target.value;
    if (dataset) {
      onDatasetChange(dataset);
    }
  };

  return (
    <nav className="bg-gray-800 p-4">
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex justify-between items-center">
          <div className="flex items-center">
            <span className="text-white text-lg font-semibold">
              Data Analysis GUI
            </span>
          </div>
          
          <div className="flex items-center space-x-4">
            {/* Dataset selector */}
            {availableDatasets && availableDatasets.length > 0 && (
              <select
                className="bg-gray-700 text-white rounded px-3 py-1.5"
                onChange={handleDatasetSelect}
              >
                <option value="">Select Dataset</option>
                {availableDatasets.map((dataset, index) => (
                  <option key={index} value={dataset}>{dataset}</option>
                ))}
              </select>
            )}
            
            {/* Engine selector */}
            <select
              className="bg-gray-700 text-white rounded px-3 py-1.5"
              value={engine}
              onChange={handleEngineChange}
            >
              <option value="pandas">Pandas</option>
              <option value="polars">Polars</option>
              <option value="sql">SQL</option>
            </select>
            
            {/* File upload */}
            <div className="relative inline-block">
              <input
                type="text"
                placeholder="Dataset name (optional)"
                className="bg-gray-700 text-white rounded px-3 py-1.5"
                value={datasetName}
                onChange={(e) => setDatasetName(e.target.value)}
              />
            </div>
            
            <div className="relative inline-block">
              <input
                type="file"
                id="file-upload"
                accept=".csv"
                onChange={handleFileUpload}
                className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                disabled={uploading}
              />
              <label
                htmlFor="file-upload"
                className={`bg-blue-500 hover:bg-blue-600 text-white px-4 py-1.5 rounded cursor-pointer ${
                  uploading ? 'opacity-50 cursor-not-allowed' : ''
                }`}
              >
                {uploading ? 'Uploading...' : 'Upload CSV'}
              </label>
            </div>
          </div>
        </div>
      </div>
    </nav>
  );
};

export default NavBar;