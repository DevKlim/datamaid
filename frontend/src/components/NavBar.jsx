// Replace your existing NavBar.jsx with this improved version

import React, { useState } from 'react';
import { uploadDataset } from '../services/api';

const NavBar = ({ 
  onDatasetUploaded, 
  onEngineChange, 
  onDatasetChange, 
  availableDatasets,
  currentEngine = 'pandas',
  onExport
}) => {
  const [uploading, setUploading] = useState(false);
  const [datasetName, setDatasetName] = useState('');
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [uploadError, setUploadError] = useState(null);
  const fileInputRef = React.useRef(null);

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    
    // Create a dataset name if not provided
    const name = datasetName || file.name.replace(/\.[^/.]+$/, "");
    
    setUploading(true);
    setUploadError(null);
    
    try {
      console.log("Starting file upload:", file.name, "size:", file.size);
      const result = await uploadDataset(file, name);
      console.log("Upload successful:", result);
      
      onDatasetUploaded(result);
      
      // Reset the file input and name
      if (fileInputRef.current) {
        fileInputRef.current.value = null;
      }
      setDatasetName('');
      
    } catch (error) {
      console.error('Error uploading file:', error);
      
      let errorMessage = 'Failed to upload file. Please try again.';
      if (error.response && error.response.data) {
        errorMessage += ` Server says: ${error.response.data.detail || JSON.stringify(error.response.data)}`;
      } else if (error.message) {
        errorMessage += ` Error: ${error.message}`;
      }
      
      setUploadError(errorMessage);
      alert(errorMessage);
      
    } finally {
      setUploading(false);
    }
  };

  const handleEngineChange = (e) => {
    const newEngine = e.target.value;
    onEngineChange(newEngine);
  };

  const handleDatasetSelect = (e) => {
    const dataset = e.target.value;
    if (dataset) {
      onDatasetChange(dataset);
    }
  };

  const handleExport = (format) => {
    onExport(format);
    setShowExportMenu(false);
  };

  return (
    <nav className="bg-gray-800 p-4">
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex flex-wrap justify-between items-center">
          <div className="flex items-center">
            <span className="text-white text-lg font-semibold mr-4">
              Data Analysis GUI
            </span>
            
            <div className="hidden md:flex items-center space-x-1">
              <a 
                href="https://github.com/yourusername/data-analysis-gui" 
                target="_blank" 
                rel="noopener noreferrer"
                className="px-3 py-2 rounded text-gray-300 hover:bg-gray-700 hover:text-white"
              >
                GitHub
              </a>
              <a 
                href="/docs" 
                className="px-3 py-2 rounded text-gray-300 hover:bg-gray-700 hover:text-white"
              >
                Documentation
              </a>
            </div>
          </div>
          
          <div className="flex flex-wrap items-center space-x-2 mt-2 md:mt-0">
            {/* Dataset selector */}
            {availableDatasets && availableDatasets.length > 0 && (
              <select
                className="bg-gray-700 text-white rounded px-3 py-1.5 max-w-xs"
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
              value={currentEngine}
              onChange={handleEngineChange}
            >
              <option value="pandas">Pandas</option>
              <option value="polars">Polars</option>
              <option value="sql">SQL</option>
            </select>
            
            {/* Export dropdown */}
            <div className="relative">
              <button 
                className="bg-gray-700 text-white rounded px-3 py-1.5 flex items-center"
                onClick={() => setShowExportMenu(!showExportMenu)}
              >
                Export
                <svg className="w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              
              {showExportMenu && (
                <div className="absolute right-0 mt-2 w-48 rounded-md shadow-lg bg-white z-10">
                  <div className="py-1">
                    <button
                      className="block w-full text-left px-4 py-2 text-gray-700 hover:bg-gray-100"
                      onClick={() => handleExport('csv')}
                    >
                      Export as CSV
                    </button>
                    <button
                      className="block w-full text-left px-4 py-2 text-gray-700 hover:bg-gray-100"
                      onClick={() => handleExport('json')}
                    >
                      Export as JSON
                    </button>
                    <button
                      className="block w-full text-left px-4 py-2 text-gray-700 hover:bg-gray-100"
                      onClick={() => handleExport('excel')}
                    >
                      Export as Excel
                    </button>
                  </div>
                </div>
              )}
            </div>
            
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
                ref={fileInputRef}
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
        
        {uploadError && (
          <div className="mt-2 p-2 bg-red-100 text-red-700 text-sm rounded">
            {uploadError}
          </div>
        )}
      </div>
    </nav>
  );
};

export default NavBar;