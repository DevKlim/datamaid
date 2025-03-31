import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});


api.interceptors.request.use(request => {
  console.log('Starting API Request:', request.method, request.url);
  return request;
}, error => {
  console.error('API Request Error:', error);
  return Promise.reject(error);
});

// Add response interceptor for debugging
api.interceptors.response.use(response => {
  console.log('API Response:', response.status, response.config.url);
  return response;
}, error => {
  console.error('API Response Error:', error.response?.status, error.config?.url, error.message);
  return Promise.reject(error);
});

export const testConnection = async () => {
  try {
    const response = await api.get('/test-connection');
    return response.data;
  } catch (error) {
    console.error('Connection test failed:', error);
    throw error;
  }
};

export const uploadDataset = async (file, datasetName) => {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('dataset_name', datasetName);
  try {
    const response = await api.post('/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 60000, // Increased timeout for larger files
    });
    return response.data; // Expects { message, dataset_name, preview, columns, row_count }
  } catch (error) {
    console.error('Upload failed:', error);
    throw error;
  }
};

export const uploadTextDataset = async (datasetName, dataText, dataFormat = 'csv') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('data_text', dataText);
  formData.append('data_format', dataFormat);
  try {
    const response = await api.post('/upload-text', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data; // Expects { message, dataset_name, preview, columns, row_count }
  } catch (error) {
    console.error('Text upload failed:', error);
    throw error;
  }
};

export const uploadDbFile = async (file) => {
  const formData = new FormData();
  formData.append('file', file);
  try {
    const response = await api.post('/upload-db', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 60000,
    });
    return response.data; // Expects { message, temp_db_id }
  } catch (error) {
    console.error('DB upload failed:', error);
    throw error;
  }
};

export const listDbTables = async (tempDbId) => {
  try {
    const response = await api.get(`/list-db-tables/${tempDbId}`);
    return response.data; // Expects { tables: [...] }
  } catch (error) {
    console.error('Listing DB tables failed:', error);
    throw error;
  }
};

export const importDbTable = async (tempDbId, tableName, newDatasetName) => {
  const formData = new FormData();
  formData.append('temp_db_id', tempDbId);
  formData.append('table_name', tableName);
  formData.append('new_dataset_name', newDatasetName);
  try {
    const response = await api.post('/import-db-table', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data; // Expects { message, dataset_name, preview, columns, row_count }
  } catch (error) {
    console.error('DB table import failed:', error);
    throw error;
  }
};

export const getDatasets = async () => {
  try {
    const response = await api.get('/datasets');
    return response.data.datasets; // Expects { datasets: [...] }
  } catch (error) {
    console.error('Error fetching datasets:', error);
    throw error;
  }
};

export const getDataset = async (datasetName, engine = 'pandas', limit = 100, offset = 0) => {
  if (!datasetName) return null; // Avoid request if no dataset selected
  try {
    const response = await api.get(`/dataset/${datasetName}?engine=${engine}&limit=${limit}&offset=${offset}`);
    return response.data; // Expects { data, columns, row_count, can_undo, can_reset, last_code }
  } catch (error) {
    console.error(`Error fetching dataset ${datasetName}:`, error.response?.data || error.message);
    throw error;
  }
};


export const loadMoreRows = async (datasetName, engine = 'pandas', offset = 0, limit = 100) => {
  if (!datasetName) return null;
  try {
    const response = await api.get(`/dataset/${datasetName}?engine=${engine}&offset=${offset}&limit=${limit}`);
    return { data: response.data.data, row_count: response.data.row_count }; // Return data and total count
  } catch (error) {
    console.error(`Error loading more rows for ${datasetName}:`, error.response?.data || error.message);
    throw error;
  }
};

export const performOperation = async (datasetName, operation, params, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('operation', operation);
  // Simple undefined/empty array check
  const sanitizedParams = Object.entries(params).reduce((acc, [key, value]) => {
    if (value !== undefined && !(Array.isArray(value) && value.length === 0)) {
      acc[key] = value;
    }
    return acc;
  }, {});
  formData.append('params', JSON.stringify(sanitizedParams));
  formData.append('engine', engine);

  try {
    const response = await api.post('/operation', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data; // Expects { data, columns, row_count, code, can_undo, can_reset }
  } catch (error) {
    console.error('Operation Error:', error.response?.data || error.message);
    throw error;
  }
};


export const executeCustomCode = async (datasetName, code, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('code', code);
  formData.append('engine', engine);

  try {
    const response = await api.post('/execute-code', formData, {
       headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { data, columns, row_count, code, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error executing custom code on ${datasetName}:`, error);
    throw error;
  }
};

export const getDatasetInfo = async (datasetName) => {
  if (!datasetName) return null;
  try {
    const response = await api.get(`/dataset-info/${datasetName}`);
    return response.data;
    // Expects { row_count, column_count, memory_usage, column_types, numeric_columns, ... }
  } catch (error) {
    console.error(`Error getting info for dataset ${datasetName}:`, error);
    throw error;
  }
};

export const getColumnStats = async (datasetName, columnName, engine = 'pandas') => {
  if (!datasetName || !columnName) return null;
  try {
    const response = await api.get(`/column-stats/${datasetName}/${columnName}?engine=${engine}`);
    return response.data;
    // Expects { column_name, dtype, missing_count, min, max, mean, ... or unique_count, top_values, ... }
  } catch (error) {
    console.error(`Error getting column stats for ${columnName} in ${datasetName}:`, error);
    throw error;
  }
};

export const saveTransformation = async (datasetName, newDatasetName, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('new_dataset_name', newDatasetName);
  formData.append('engine', engine); // Engine needed for preview generation

  try {
    const response = await api.post('/save-transformation', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, dataset_name, preview, columns, row_count }
    return response.data;
  } catch (error) {
    console.error(`Error saving transformation of ${datasetName} as ${newDatasetName}:`, error);
    throw error;
  }
};

export const exportDataset = async (datasetName, format = 'csv', engine = 'pandas') => {
  if (!datasetName) return false;
  try {
    const response = await api.get(`/export/${datasetName}?format=${format}&engine=${engine}`, {
      responseType: 'blob' // Crucial for file download
    });

    // Determine filename based on Content-Disposition header or fallback
    let filename = `${datasetName}.${format}`;
    const disposition = response.headers['content-disposition'];
    if (disposition && disposition.indexOf('attachment') !== -1) {
        const filenameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/;
        const matches = filenameRegex.exec(disposition);
        if (matches != null && matches[1]) {
          filename = matches[1].replace(/['"]/g, '');
        }
    }

    // Create a download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', filename); // Use determined filename
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url); // Clean up blob URL

    return true;
  } catch (error) {
    console.error(`Error exporting dataset ${datasetName} as ${format}:`, error);
    // Attempt to parse error response if it's JSON (e.g., backend error detail)
    if (error.response && error.response.data instanceof Blob && error.response.data.type === "application/json") {
        try {
            const errText = await error.response.data.text();
            const errJson = JSON.parse(errText);
            console.error("Backend export error detail:", errJson.detail);
            alert(`Export Error: ${errJson.detail || 'Server error'}`);
        } catch (parseError) {
            alert(`Export Error: An unknown server error occurred.`);
        }
    } else {
        alert(`Export Error: ${error.message || 'Could not connect to server'}`);
    }
    throw error; // Re-throw after handling
  }
};

export const mergeDatasets = async (leftDataset, rightDataset, params, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('left_dataset', leftDataset);
  formData.append('right_dataset', rightDataset);
  formData.append('params', JSON.stringify(params)); // Contains join_type, left_on, right_on
  formData.append('engine', engine);

  try {
    const response = await api.post('/merge-datasets', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, data, columns, row_count, code, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error merging datasets ${leftDataset} and ${rightDataset}:`, error);
    throw error;
  }
};


// New function for regex-based operations
export const performRegexOperation = async (datasetName, operation, params, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('operation', operation); // e.g., "filter_contains", "extract"
  formData.append('params', JSON.stringify(params)); // Contains column, regex, etc.
  formData.append('engine', engine);

  try {
    const response = await api.post('/regex-operation', formData, {
       headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, data, columns, row_count, code, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error performing regex operation ${operation} on ${datasetName}:`, error);
    throw error;
  }
};

export const performRelationalOperationPreview = async (
  operation,
  params,
  currentSqlState = null,
  baseDatasetName = null // Component should always provide this
) => {
const formData = new FormData();
formData.append('operation', operation);
formData.append('params', JSON.stringify(params));

// Append conditionally based on whether they are provided
if (currentSqlState) {
  formData.append('current_sql_state', currentSqlState);
}

// *** ALWAYS append base_dataset_name if it's provided to this function ***
// The backend requires it now. The component logic ensures it's passed.
if (baseDatasetName) {
  formData.append('base_dataset_name', baseDatasetName);
} else {
    // If the component somehow fails to send it, the backend will raise a 422
    // which should be handled by the component's error display.
    console.warn("performRelationalOperationPreview called without baseDatasetName!");
}

// Add step alias base - frontend can manage this if needed, or backend defaults
formData.append('step_alias_base', 'step');

try {
  // Use the new preview endpoint
  const response = await api.post('/relational-operation-preview', formData, {
     headers: { 'Content-Type': 'multipart/form-data' },
  });
  // Expects { message, data, columns, row_count, generated_sql_state }
  return response.data;
} catch (error) {
  console.error(`Error performing relational operation preview ${operation}:`, error);
  // Re-throw the error so the component can catch it
  throw error;
}
};


export const saveRaResult = async (finalSqlChain, newDatasetName, baseDatasetNames = []) => {
  const formData = new FormData();
  formData.append('final_sql_chain', finalSqlChain);
  formData.append('new_dataset_name', newDatasetName);
  formData.append('base_dataset_names_json', JSON.stringify(baseDatasetNames));

  try {
    const response = await api.post('/save-ra-result', formData, {
       headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, dataset_name, preview, columns, row_count, datasets }
    return response.data;
  } catch (error) {
    console.error(`Error saving RA result as ${newDatasetName}:`, error);
    throw error;
  }
};

export const undoTransformation = async (datasetName, currentEngine = 'pandas') => {
  if (!datasetName) return null;
  try {
    const response = await api.post(`/undo/${datasetName}?engine=${currentEngine}`);
    // Expects { message, data, columns, row_count, can_undo, can_reset, last_code }
    return response.data;
  } catch (error) {
    console.error(`Error undoing operation on ${datasetName}:`, error);
    throw error;
  }
};

export const resetTransformation = async (datasetName, currentEngine = 'pandas') => {
  if (!datasetName) return null;
  try {
    const response = await api.post(`/reset/${datasetName}?engine=${currentEngine}`);
    // Expects { message, data, columns, row_count, can_undo, can_reset, last_code }
    return response.data;
  } catch (error) {
    console.error(`Error resetting transformations for ${datasetName}:`, error);
    throw error;
  }
};

export const renameDataset = async (oldName, newName) => {
  if (!oldName || !newName) throw new Error("Old and new names are required for rename.");
  const formData = new FormData();
  formData.append('new_dataset_name', newName);
  try {
    const response = await api.post(`/rename-dataset/${oldName}`, formData, {
       headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, old_name, new_name, datasets }
    return response.data;
  } catch (error) {
    console.error(`Error renaming dataset ${oldName} to ${newName}:`, error);
    throw error;
  }
};

// --- (Optional) Add delete function ---
export const deleteDataset = async (datasetName) => {
  if (!datasetName) throw new Error("Dataset name is required for deletion.");
  try {
    const response = await api.delete(`/dataset/${datasetName}`);
    // Expects { message, deleted_name, datasets }
    return response.data;
  } catch (error) {
    console.error(`Error deleting dataset ${datasetName}:`, error);
    throw error;
  }
};

const apiService = {
  testConnection,
  uploadDataset,
  uploadTextDataset,
  uploadDbFile,
  listDbTables,
  importDbTable,
  getDatasets,
  getDataset,
  loadMoreRows,
  getDatasetInfo,
  getColumnStats,
  performOperation,
  performRegexOperation,
  mergeDatasets,
  performRelationalOperationPreview, // Use this for interactive steps
  saveRaResult, // Use this for saving the final chain
  renameDataset,
  deleteDataset,
  executeCustomCode,
  undoTransformation,
  resetTransformation,
  saveTransformation,
  exportDataset,
  renameDataset, // Added
  deleteDataset, // Added (Optional)
};

export default apiService;