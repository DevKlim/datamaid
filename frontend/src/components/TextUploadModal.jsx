import React, { useState, useEffect } from 'react';

const TextUploadModal = ({ isOpen, onClose, onSubmit, isLoading, currentDatasetName }) => {
    const [datasetName, setDatasetName] = useState('');
    const [dataText, setDataText] = useState('');
    const [dataFormat, setDataFormat] = useState('csv');

    useEffect(() => {
        // Pre-fill name when modal opens
        if (isOpen) {
            setDatasetName(currentDatasetName ? `${currentDatasetName}_pasted` : 'pasted_data');
        }
    }, [isOpen, currentDatasetName]);

    if (!isOpen) return null;

    const handleSubmit = (e) => {
        e.preventDefault();
        onSubmit({ datasetName, dataText, dataFormat });
    };

    const handleModalClick = (e) => {
        e.stopPropagation();
    };

    return (
        <div className="modal-overlay" onClick={onClose}>
            <div className="modal-content bg-white rounded-xl shadow-soft-lg" onClick={handleModalClick}>
                <form onSubmit={handleSubmit}>
                    <h3 className="modal-header text-maid-choco-dark">Paste Data</h3>
                    <div className="mb-4">
                        <label htmlFor="text_dataset_name_modal" className="input-label">Dataset Name:</label>
                        <input
                            type="text" id="text_dataset_name_modal" value={datasetName}
                            onChange={e => setDatasetName(e.target.value)} required
                            className="input-base"
                            disabled={isLoading}
                        />
                    </div>
                    <div className="mb-4">
                        <label htmlFor="text_format_modal" className="input-label">Data Format:</label>
                        <select
                            id="text_format_modal" value={dataFormat}
                            onChange={e => setDataFormat(e.target.value)}
                            className="select-base"
                            disabled={isLoading}
                        >
                            <option value="csv">CSV</option>
                            <option value="json">JSON (array of objects/records)</option>
                        </select>
                    </div>
                    <div className="mb-4">
                        <label htmlFor="data_text_modal" className="input-label">Paste Data Below:</label>
                        <textarea
                            id="data_text_modal" value={dataText}
                            onChange={e => setDataText(e.target.value)} required
                            className="input-base h-40 font-mono text-xs"
                            placeholder={dataFormat === 'csv' ? 'col1,col2\nval1,val2' : '[{"col1": "val1", "col2": "val2"}]'}
                            disabled={isLoading}
                        />
                    </div>
                    <div className="flex justify-end space-x-2">
                        <button type="button" onClick={onClose} disabled={isLoading} className="btn btn-gray">Cancel</button>
                        <button type="submit" disabled={isLoading || !datasetName.trim() || !dataText.trim()} className="btn btn-coffee">
                            {isLoading ? 'Loading...' : 'Load Data'}
                        </button>
                    </div>
                </form>
                <button onClick={onClose} className="modal-close-button" aria-label="Close">
                    &times;
                </button>
            </div>
        </div>
    );
};

export default TextUploadModal;