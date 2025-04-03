import axios from 'axios';

// Use environment variable or default for local development
const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// --- Interceptors for Logging (Keep as is) ---
api.interceptors.request.use(request => {
  console.log('Starting API Request:', request.method?.toUpperCase(), request.url);
  // Log form data keys if it's FormData
  if (request.data instanceof FormData) {
    const formDataKeys = [];
    for (const key of request.data.keys()) {
      formDataKeys.push(key);
    }
    console.log('FormData Keys:', formDataKeys);
  } else if (request.data) {
    // Log JSON data if not too large
    // try {
    //   const dataStr = JSON.stringify(request.data);
    //   if (dataStr.length < 500) { // Avoid logging huge payloads
    //     console.log('Request Data (JSON):', dataStr);
    //   } else {
    //      console.log('Request Data (JSON): [Too Large to Log]');
    //   }
    // } catch (e) {
    //   console.log('Request Data: [Could not stringify]');
    // }
  }
  return request;
}, error => {
  console.error('API Request Error:', error);
  return Promise.reject(error);
});

api.interceptors.response.use(response => {
  console.log('API Response:', response.status, response.config.url);
  return response;
}, error => {
  console.error('API Response Error:', error.response?.status, error.config?.url, error.message, error.response?.data);
  // Construct a more informative error object
  const enhancedError = new Error(error.message);
  enhancedError.response = error.response;
  enhancedError.request = error.request;
  enhancedError.config = error.config;
  // Add detail from FastAPI response if available
  if (error.response?.data?.detail) {
    enhancedError.detail = error.response.data.detail;
  }
  return Promise.reject(enhancedError); // Reject with the enhanced error
});

// --- Core API Functions ---

export const testConnection = async () => {
  try {
    const response = await api.get('/test-connection');
    return response.data; // { status, message }
  } catch (error) {
    console.error('Connection test failed:', error);
    throw error;
  }
};

// --- Upload Functions (Updated return structure) ---
export const uploadDataset = async (file, datasetName) => {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('dataset_name', datasetName);
  try {
    const response = await api.post('/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 120000, // Increased timeout for larger uploads
    });
    // Expects { message, dataset_name, dataset_type, preview, columns, row_count, datasets }
    return response.data;
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
     // Expects { message, dataset_name, dataset_type, preview, columns, row_count, datasets }
    return response.data;
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
      timeout: 120000, // Increased timeout
    });
    // Expects { message, temp_db_id }
    return response.data;
  } catch (error) {
    console.error('DB upload failed:', error);
    throw error;
  }
};

export const listDbTables = async (tempDbId) => {
  try {
    const response = await api.get(`/list-db-tables/${tempDbId}`);
    // Expects { tables: [...] }
    return response.data;
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
    // Expects { message, dataset_name, dataset_type, preview, columns, row_count, datasets }
    return response.data;
  } catch (error) {
    console.error('DB table import failed:', error);
    throw error;
  }
};

// --- Dataset Listing and Retrieval (Updated) ---
export const getDatasets = async () => {
  try {
    const response = await api.get('/datasets');
    // Expects { datasets: [...] } - Holds ALL names
    return response.data.datasets || [];
  } catch (error) {
    console.error('Error fetching datasets:', error);
    throw error;
  }
};

export const getDatasetView = async (datasetName, limit = 100, offset = 0) => {
  if (!datasetName) {
      console.warn("getDatasetView called with no datasetName");
      return null; // Or throw error? Returning null might be safer for UI flow.
  }
  try {
    // Fetches view for any named dataset
    const response = await api.get(`/dataset/${encodeURIComponent(datasetName)}?limit=${limit}&offset=${offset}`);
    // Expects { dataset_name, dataset_type, data, columns, row_count, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error fetching view for ${datasetName}:`, error.response?.data || error.message);
    throw error; // Re-throw for App.jsx to handle (e.g., 404)
  }
};

// loadMoreRows might not be needed if DataTable pagination works on loaded `viewData`
// If server-side pagination is desired, this would fetch subsequent pages using the /dataset endpoint
export const loadMoreRows = async (datasetName, offset = 0, limit = 100) => {
   console.warn("loadMoreRows called - ensure server-side pagination is intended.");
   if (!datasetName) return null;
   try {
     const response = await api.get(`/dataset/${encodeURIComponent(datasetName)}?offset=${offset}&limit=${limit}`);
     // Return only the necessary parts for appending/displaying more rows
     return { data: response.data.data, row_count: response.data.row_count };
   } catch (error) {
     console.error(`Error loading more rows for ${datasetName}:`, error.response?.data || error.message);
     throw error;
   }
};


// --- Info/Stats Functions (Unchanged signatures, backend handles type) ---
export const getDatasetInfo = async (datasetName) => {
  if (!datasetName) return null;
  try {
    const response = await api.get(`/dataset-info/${encodeURIComponent(datasetName)}`);
    // Returns info structure, potentially adapted for Series vs DataFrame by backend
    return response.data;
  } catch (error) {
    console.error(`Error getting info for dataset ${datasetName}:`, error);
    throw error;
  }
};

export const getColumnStats = async (datasetName, columnName) => {
  if (!datasetName || !columnName) return null;
  try {
    // Backend handles Series vs DataFrame column stats
    const response = await api.get(`/column-stats/${encodeURIComponent(datasetName)}/${encodeURIComponent(columnName)}`);
    return response.data;
  } catch (error) {
    console.error(`Error getting column stats for ${columnName} in ${datasetName}:`, error);
    throw error;
  }
};


// --- Code Execution (Centralized Endpoint) ---
export const executeCustomCode = async (code, engine = 'pandas', currentViewName = null) => {
  const formData = new FormData();
  formData.append('code', code);
  formData.append('engine', engine);
  if (currentViewName) {
      formData.append('current_view_name', currentViewName);
  }

  try {
    const response = await api.post('/execute-code', formData, {
       headers: { 'Content-Type': 'multipart/form-data' },
       timeout: 180000, // Longer timeout for potentially complex code
    });
    // Expects { message, datasets, primary_result_name, primary_result_type, preview, columns, row_count, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error executing custom code:`, error);
    throw error;
  }
};


// --- Relational Algebra (Updated signatures for base datasets) ---
export const performRelationalOperationPreview = async (
  operation,
  params,
  baseDatasetNames = [], // Expect list of names
  currentSqlState = null
) => {
  if (!baseDatasetNames || baseDatasetNames.length === 0) {
      throw new Error("At least one base dataset name is required for RA preview.");
  }
  const formData = new FormData();
  formData.append('operation', operation);
  formData.append('params', JSON.stringify(params));
  formData.append('base_dataset_names_json', JSON.stringify(baseDatasetNames)); // Send list

  if (currentSqlState) {
    formData.append('current_sql_state', currentSqlState);
  }
  formData.append('step_alias_base', 'step'); // Keep default base

  try {
    const response = await api.post('/relational-operation-preview', formData, {
       headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, data, columns, row_count, generated_sql_state, current_step_sql_snippet }
    return response.data;
  } catch (error) {
    console.error(`Error performing relational operation preview ${operation}:`, error);
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
    // Expects { message, dataset_name, dataset_type, preview, columns, row_count, datasets }
    return response.data;
  } catch (error) {
    console.error(`Error saving RA result as ${newDatasetName}:`, error);
    throw error;
  }
};

// --- Undo/Reset (Operate on specific dataset) ---
export const undoTransformation = async (datasetName) => {
  if (!datasetName) return null;
  try {
    const response = await api.post(`/undo/${encodeURIComponent(datasetName)}`);
    // Expects { message, dataset_name, dataset_type, data, columns, row_count, can_undo, can_reset }
    return response.data;
  } catch (error)   {
    console.error(`Error undoing operation on ${datasetName}:`, error);
    throw error;
  }
};

export const resetTransformation = async (datasetName) => {
  if (!datasetName) return null;
  try {
    const response = await api.post(`/reset/${encodeURIComponent(datasetName)}`);
    // Expects { message, dataset_name, dataset_type, data, columns, row_count, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error resetting transformations for ${datasetName}:`, error);
    throw error;
  }
};

// --- Export (Operates on specific dataset) ---
export const exportDataset = async (datasetName, format = 'csv') => {
  if (!datasetName) return false;
  try {
    const response = await api.get(`/export/${encodeURIComponent(datasetName)}?format=${format}`, {
      responseType: 'blob' // Crucial for file download
    });

    // Determine filename based on Content-Disposition header or fallback
    let filename = `${datasetName}_export.${format}`; // Default export name
    const disposition = response.headers['content-disposition'];
    if (disposition && disposition.indexOf('attachment') !== -1) {
        const filenameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/;
        const matches = filenameRegex.exec(disposition);
        if (matches != null && matches[1]) {
          // Decode URI component and remove quotes
          filename = decodeURIComponent(matches[1].replace(/['"]/g, ''));
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
    let errorDetail = 'Could not connect to server or unknown error.';
    if (error.response && error.response.data instanceof Blob && error.response.data.type === "application/json") {
        try {
            const errText = await error.response.data.text();
            const errJson = JSON.parse(errText);
            errorDetail = errJson.detail || 'Server error during export.';
            console.error("Backend export error detail:", errorDetail);
        } catch (parseError) {
            console.error("Could not parse blob error response:", parseError);
            errorDetail = 'Export failed with an unknown server error response.';
        }
    } else if (error.detail) { // Use enhanced error detail if present
        errorDetail = typeof error.detail === 'string' ? error.detail : JSON.stringify(error.detail);
    } else if (error.message) {
        errorDetail = error.message;
    }
    // Use alert for now, replace with better notification system later
    alert(`Export Error: ${errorDetail}`);
    // Do not re-throw here if alert is sufficient user feedback
    // throw error; // Re-throw if calling code needs to handle it further
    return false; // Indicate failure
  }
};


// --- Rename / Delete (Operate on specific dataset) ---
export const renameDataset = async (oldName, newName) => {
  if (!oldName || !newName) throw new Error("Old and new names are required for rename.");
  const formData = new FormData();
  formData.append('new_dataset_name', newName);
  try {
    const response = await api.post(`/rename-dataset/${encodeURIComponent(oldName)}`, formData, {
       headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, old_name, new_name, datasets }
    return response.data;
  } catch (error) {
    console.error(`Error renaming dataset ${oldName} to ${newName}:`, error);
    throw error;
  }
};

export const deleteDataset = async (datasetName) => {
  if (!datasetName) throw new Error("Dataset name is required for deletion.");
  try {
    const response = await api.delete(`/dataset/${encodeURIComponent(datasetName)}`);
    // Expects { message, deleted_name, datasets }
    return response.data;
  } catch (error) {
    console.error(`Error deleting dataset ${datasetName}:`, error);
    throw error;
  }
};

// --- REMOVED FUNCTIONS (Now handled via executeCustomCode or RA) ---
// performOperation
// mergeDatasets
// performRegexOperation
// saveTransformation

export const performOperation = async (datasetName, operation, params) => {
  if (!datasetName) throw new Error("Dataset name is required to perform an operation.");

  const formData = new FormData();
  formData.append('operation', operation);
  formData.append('params_json', JSON.stringify(params)); // Send params as JSON string
  formData.append('engine', 'pandas'); // Hardcode pandas for this endpoint

  try {
    // Use the new endpoint structure: /operation/{dataset_name}
    const response = await api.post(`/operation/${encodeURIComponent(datasetName)}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    // Expects { message, dataset_name, data, columns, row_count, can_undo, can_reset, generated_code }
    return response.data;
  } catch (error) {
    console.error(`Error performing operation '${operation}' on '${datasetName}':`, error);
    throw error; // Let the calling component handle the error display
  }
};


// --- Consolidate Exported Functions ---
const apiService = {
  testConnection,
  // Uploads
  uploadDataset,
  uploadTextDataset,
  uploadDbFile,
  listDbTables,
  importDbTable,
  // Data Listing/Retrieval
  getDatasets,
  getDatasetView,
  loadMoreRows,
  // Info/Stats
  getDatasetInfo,
  getColumnStats,
  // Execution & RA & UI Ops
  executeCustomCode,
  performOperation, // <-- Added back
  performRelationalOperationPreview,
  saveRaResult,
  // State Management
  undoTransformation,
  resetTransformation,
  // File/Dataset Management
  exportDataset,
  renameDataset,
  deleteDataset,
};

export default apiService;