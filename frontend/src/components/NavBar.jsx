// src/components/NavBar.jsx
import React, { useState, useRef } from 'react';
import { Link } from 'react-router-dom'; // Import Link
import apiService from '../services/api';

const NavBar = ({
  availableDatasets,
  currentDataset,
  currentEngine = 'pandas',
  onDatasetUploaded,
  onEngineChange,
  onDatasetChange,
  onExport,
  onUploadText,
  onUploadDbFile,
  isLoading
  // Removed onRenameRequest, onDeleteRequest props
}) => {
  const [csvDatasetName, setCsvDatasetName] = useState('');
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [localUploadError, setLocalUploadError] = useState(null);
  const csvFileInputRef = useRef(null);
  const dbFileInputRef = useRef(null);

  const handleCsvFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    const name = csvDatasetName || file.name.replace(/\.[^/.]+$/, "");
    setLocalUploadError(null);

    try {
      const result = await apiService.uploadDataset(file, name);
      onDatasetUploaded(result);
      if (csvFileInputRef.current) csvFileInputRef.current.value = null;
      setCsvDatasetName('');
    } catch (error) {
      console.error('NavBar CSV Upload failed:', error);
      const message = error.response?.data?.detail || error.message || 'CSV Upload failed';
      setLocalUploadError(message);
    }
  };

  const handleDbFileUploadTrigger = (e) => {
    const file = e.target.files[0];
    if (file && onUploadDbFile) {
      onUploadDbFile(file);
      if (dbFileInputRef.current) dbFileInputRef.current.value = null;
    }
  };

  const handleEngineChange = (e) => onEngineChange(e.target.value);
  const handleDatasetSelect = (e) => onDatasetChange(e.target.value);
  const handleExportClick = (format) => { onExport(format); setShowExportMenu(false); };

  return (
    <nav className="bg-coffee bg-opacity-85 p-3 sticky top-0 z-30 shadow-md border-b border-coffee-light">
      <div className="max-w-full mx-auto px-2 sm:px-4">
        <div className="flex flex-wrap justify-between items-center gap-x-4 gap-y-2">
          {/* Left Side: Title & Logo */}
          <div className="flex items-center flex-shrink-0">
            <span className="text-maid-cream-light text-xl font-bold mr-2">
              DataMaid
            </span>
            <span className="text-maid-cream-light text-sm opacity-80">♡</span>
          </div>

          {/* Right Side: Controls */}
          <div className="flex flex-wrap items-center justify-end flex-grow gap-x-3 gap-y-2">
            {/* Dataset selector */}
            {availableDatasets && availableDatasets.length > 0 && (
              <select
                className="bg-white bg-opacity-95 text-maid-choco rounded-md px-3 py-1.5 max-w-xs text-sm focus:outline-none focus:ring-2 focus:ring-maid-cream-light border border-coffee-light"
                onChange={handleDatasetSelect}
                value={currentDataset || ""}
                disabled={isLoading}
                title={currentDataset || "Select Dataset"}
              >
                <option value="">Select Dataset</option>
                {availableDatasets.map((dataset) => (
                  <option key={dataset} value={dataset}>{dataset}</option>
                ))}
              </select>
            )}

            {/* Engine selector */}
            <select
              className="bg-white bg-opacity-95 text-maid-choco rounded-md px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-maid-cream-light border border-coffee-light"
              value={currentEngine}
              onChange={handleEngineChange}
              disabled={isLoading}
            >
              <option value="pandas">Pandas</option>
              <option value="polars">Polars</option>
              <option value="sql">SQL</option>
            </select>

            {/* Export dropdown */}
            <div className="relative">
              <button
                className="btn-nav btn-coffee flex items-center"
                onClick={() => setShowExportMenu(!showExportMenu)}
                disabled={!currentDataset || isLoading}
                title="Export current dataset state"
              >
                Export
                <svg className="w-3 h-3 ml-1 fill-current" viewBox="0 0 20 20"><path d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" /></svg>
              </button>
              {showExportMenu && (
                <div className="absolute right-0 mt-2 w-48 rounded-md shadow-lg bg-white ring-1 ring-coffee-light ring-opacity-5 z-40 py-1 border border-maid-gray-light">
                  <button className="block w-full text-left px-4 py-2 text-sm text-maid-choco hover:bg-maid-cream hover:text-maid-choco-dark" onClick={() => handleExportClick('csv')}>Export as CSV</button>
                  <button className="block w-full text-left px-4 py-2 text-sm text-maid-choco hover:bg-maid-cream hover:text-maid-choco-dark" onClick={() => handleExportClick('json')}>Export as JSON</button>
                  <button className="block w-full text-left px-4 py-2 text-sm text-maid-choco hover:bg-maid-cream hover:text-maid-choco-dark" onClick={() => handleExportClick('excel')}>Export as Excel</button>
                </div>
              )}
            </div>

            {/* Upload Area Group */}
            <div className="flex items-center space-x-2 border-l border-coffee-light border-opacity-50 pl-3 ml-1">
                {/* CSV Upload */}
                <div className="flex items-center space-x-1">
                    <input
                        type="text"
                        placeholder="CSV Name (opt.)"
                        className="bg-white bg-opacity-95 text-maid-choco rounded-md px-2 py-1.5 text-sm w-28 focus:outline-none focus:ring-2 focus:ring-maid-cream-light border border-coffee-light"
                        value={csvDatasetName}
                        onChange={(e) => setCsvDatasetName(e.target.value)}
                        disabled={isLoading}
                        title="Optional name for uploaded CSV"
                    />
                    <div className="relative inline-block">
                        <input
                           type="file" id="csv-file-upload" ref={csvFileInputRef} accept=".csv"
                           onChange={handleCsvFileUpload}
                           className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                           disabled={isLoading}
                        />
                        <label htmlFor="csv-file-upload" className={`btn-nav btn-blue whitespace-nowrap ${isLoading ? 'cursor-not-allowed' : 'cursor-pointer'}`}>
                           Upload CSV
                        </label>
                    </div>
                </div>

                {/* Text Paste Button */}
                <button
                    onClick={onUploadText}
                    className="btn-nav btn-green whitespace-nowrap"
                    disabled={isLoading}
                    title="Load data by pasting text (CSV or JSON)"
                >
                    Paste Text
                </button>

                {/* DB Upload Button */}
                <div className="relative inline-block">
                    <input
                        type="file" id="db-file-upload" ref={dbFileInputRef}
                        accept=".db,.sqlite,.sqlite3,.duckdb"
                        onChange={handleDbFileUploadTrigger}
                        className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
                        disabled={isLoading}
                    />
                    <label htmlFor="db-file-upload" className={`btn-nav btn-purple whitespace-nowrap ${isLoading ? 'cursor-not-allowed' : 'cursor-pointer'}`}>
                        Upload DB
                    </label>
                </div>
                <div className="border-l border-coffee-light border-opacity-50 pl-3 ml-1">
                    <Link
                       to="/manage-datasets"
                       className="btn-nav btn-outline whitespace-nowrap"
                       title="Rename or delete datasets"
                    >
                       Manage Datasets
                   </Link>
               </div>
            </div>
          </div>
        </div>

        {/* Local Upload Error Display */}
        {localUploadError && (
          <div className="mt-2 p-1.5 bg-red-100 text-red-700 text-xs rounded flex justify-between items-center">
            <span>Upload Error: {localUploadError}</span>
            <button onClick={() => setLocalUploadError(null)} className="ml-2 font-bold text-red-800 text-xs">✕</button>
          </div>
        )}
      </div>
    </nav>
  );
};

export default NavBar;