// src/components/NavBar.jsx
import React, { useState } from 'react';
import { Link } from 'react-router-dom'; // Import Link
import apiService from '../services/api';

// Simple File Input component (can be styled further)
const FileInput = ({ id, label, accept, onChange, disabled }) => (
  <div className="relative inline-block">
    <label
      htmlFor={id}
      className={`btn btn-outline px-3 py-1 text-sm cursor-pointer ${disabled ? 'opacity-50 cursor-not-allowed' : 'hover:bg-maid-cream'}`}
    >
      {label}
    </label>
    <input
      id={id}
      type="file"
      accept={accept}
      onChange={onChange}
      disabled={disabled}
      className="absolute inset-0 w-full h-full opacity-0 cursor-pointer" // Hide the default input
    />
  </div>
);

const NavBar = ({
  availableDatasets = [],
  currentDataset, // This is currentViewName from App.jsx
  currentEngine,
  onDatasetUploaded, // Expects result object
  onEngineChange,
  onDatasetChange, // Changes the current view
  onExport,
  onUploadText,
  onUploadDbFile,
  isLoading,
}) => {
  const [selectedFormat, setSelectedFormat] = useState('csv');

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      // Use file name as default dataset name if user doesn't provide one
      // For simplicity, we'll require the backend to handle naming or prompt later
      // For now, let's assume a naming convention or prompt happens elsewhere
      const defaultName = file.name.replace(/\.[^/.]+$/, ""); // Remove extension
      // Prompting here is an option, but complicates flow. Backend handles overwrite warning.
      const datasetName = prompt(`Enter a name for the uploaded dataset (default: ${defaultName}):`, defaultName);
      if (datasetName && datasetName.trim()) {
         // Call the handler passed from App.jsx which now expects the result object
         // The actual upload API call happens in App.jsx's handler
         // This component just triggers the process
         // We need to pass the file and chosen name up
         // Let's adjust the prop name for clarity
         // onUploadFileTrigger({ file: file, name: datasetName.trim() }); // OLD
         // NEW: Let App.jsx handle the prompt via onDatasetUploaded prop? No, that's for AFTER upload.
         // Let's keep the prompt here for now.
         apiService.uploadDataset(file, datasetName.trim())
            .then(result => onDatasetUploaded(result)) // Pass result up
            .catch(err => {
                // Handle error display locally or pass up? Pass up via handleError prop?
                console.error("Upload failed in NavBar:", err);
                alert(`Upload failed: ${err?.response?.data?.detail || err.message || 'Unknown error'}`);
            });

      } else {
          alert("Upload cancelled or invalid name provided.");
      }
      event.target.value = null; // Reset file input
    }
  };

   const handleDbFileChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      onUploadDbFile(file); // Pass file up to App.jsx handler
    }
    event.target.value = null; // Reset file input
  };


  const handleExportClick = () => {
    if (currentDataset) {
      onExport(selectedFormat);
    } else {
      alert("Please select a dataset to export.");
    }
  };

  return (
    <nav className="bg-white shadow-md px-4 py-2 flex flex-wrap justify-between items-center gap-y-2">
      {/* Left Side: Logo/Title and View Selector */}
      <div className="flex items-center space-x-4">
        <Link to="/" className="text-xl font-bold text-maid-choco hover:text-coffee">
          DataMaid <span role="img" aria-label="sparkles">✨</span>
        </Link>
        <div className="flex items-center space-x-2">
          <label htmlFor="dataset-select" className="text-sm font-medium text-maid-choco">View:</label>
          <select
            id="dataset-select"
            value={currentDataset || ''} // Use currentDataset (currentViewName)
            onChange={(e) => onDatasetChange(e.target.value)}
            disabled={isLoading || availableDatasets.length === 0}
            className="select-base text-sm p-1 border border-maid-gray rounded-md min-w-[150px]"
          >
            <option value="" disabled={!!currentDataset}>-- Select Dataset --</option>
            {availableDatasets.map(ds => (
              <option key={ds} value={ds}>{ds}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Right Side: Actions */}
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
         {/* Upload Buttons */}
         <FileInput
            id="csv-upload"
            label="Upload CSV"
            accept=".csv"
            onChange={handleFileChange} // Uses internal handler now
            disabled={isLoading}
          />
         <FileInput
            id="db-upload"
            label="Upload DB"
            accept=".db,.sqlite,.sqlite3,.duckdb"
            onChange={handleDbFileChange} // Uses internal handler now
            disabled={isLoading}
          />
         <button onClick={onUploadText} disabled={isLoading} className="btn btn-outline px-3 py-1 text-sm">
            Paste Text
         </button>

         {/* Engine Selector */}
        <div className="flex items-center space-x-1">
           <label htmlFor="engine-select" className="text-sm font-medium text-maid-choco">Engine:</label>
           <select
             id="engine-select"
             value={currentEngine}
             onChange={(e) => onEngineChange(e.target.value)}
             disabled={isLoading}
             className="select-base text-sm p-1 border border-maid-gray rounded-md"
           >
             <option value="pandas">Pandas</option>
             <option value="polars">Polars</option>
             <option value="sql">SQL</option>
           </select>
        </div>

        {/* Export Section */}
        <div className="flex items-center space-x-1 border-l pl-3 ml-1">
           <label htmlFor="format-select" className="text-sm font-medium text-maid-choco">Export:</label>
           <select
             id="format-select"
             value={selectedFormat}
             onChange={(e) => setSelectedFormat(e.target.value)}
             disabled={isLoading || !currentDataset}
             className="select-base text-sm p-1 border border-maid-gray rounded-md"
           >
             <option value="csv">CSV</option>
             <option value="json">JSON</option>
             <option value="excel">Excel</option>
           </select>
           <button
             onClick={handleExportClick}
             disabled={isLoading || !currentDataset}
             className="btn btn-outline px-3 py-1 text-sm"
           >
             Download
           </button>
        </div>
      </div>
    </nav>
  );
};

// Need to import apiService if handleFileChange calls it directly

export default NavBar;