import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add request interceptor for debugging
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
  
  console.log('Uploading file:', file.name, 'size:', file.size, 'type:', file.type);
  console.log('Dataset name:', datasetName);
  
  try {
    const response = await api.post('/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      // Add timeout to prevent hanging uploads
      timeout: 30000,
    });
    
    console.log('Upload successful:', response.data);
    return response.data;
  } catch (error) {
    console.error('Upload failed:', error);
    
    // Enhanced error handling
    if (error.response) {
      // The server responded with a status code outside the 2xx range
      console.error('Server response:', error.response.status, error.response.data);
    } else if (error.request) {
      // The request was made but no response was received
      console.error('No response received:', error.request);
    } else {
      // Something happened in setting up the request
      console.error('Request setup error:', error.message);
    }
    
    throw error;
  }
};

export const getDatasets = async () => {
  try {
    const response = await api.get('/datasets');
    return response.data.datasets;
  } catch (error) {
    console.error('Error fetching datasets:', error);
    throw error;
  }
};

export const getDataset = async (datasetName, engine = 'pandas', limit = 100, offset = 0) => {
  try {
    // Use the renamed backend endpoint
    const response = await api.get(`/dataset/${datasetName}?engine=${engine}&limit=${limit}&offset=${offset}`);
    // Expecting { data, columns, row_count, can_undo, can_reset, last_code }
    return response.data;
  } catch (error) {
    console.error(`Error fetching dataset ${datasetName}:`, error.response?.data || error.message);
    throw error;
  }
};


export const loadMoreRows = async (datasetName, engine = 'pandas', offset = 0, limit = 50) => {
  try {
    // This endpoint might not need the flags, but uses get_dataset_preview logic now
    const response = await api.get(`/dataset/${datasetName}?engine=${engine}&offset=${offset}&limit=${limit}`);
    // Return only data for appending
    return { data: response.data.data };
  } catch (error) {
    console.error(`Error loading more rows for ${datasetName}:`, error.response?.data || error.message);
    throw error;
  }
};

export const performOperation = async (datasetName, operation, params, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('operation', operation);
  const sanitizedParams = {};
  Object.keys(params).forEach(key => {
    if (params[key] === undefined) return;
    if (Array.isArray(params[key]) && params[key].length === 0) return;
    sanitizedParams[key] = params[key];
  });
  formData.append('params', JSON.stringify(sanitizedParams));
  formData.append('engine', engine);

  console.log('Operation Request:', { dataset: datasetName, operation, params: sanitizedParams, engine });
  formData.forEach((value, key) => { console.log(`${key}:`, value); });

  try {
    const response = await api.post('/operation', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    console.log('Operation Response:', response.data);
    // Expecting { data, columns, row_count, code, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error('Operation Error:', error.response?.data || error.message);
    console.error('FastAPI Validation Detail:', error.response?.data?.detail);
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
       headers: { 'Content-Type': 'multipart/form-data' }, // Add header here too!
    });
     // Expecting { data, columns, row_count, code, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error executing custom code on ${datasetName}:`, error.response?.data || error.message);
     console.error('FastAPI Validation Detail:', error.response?.data?.detail);
    throw error;
  }
};

export const getDatasetInfo = async (datasetName) => {
  try {
    const response = await api.get(`/dataset-info/${datasetName}`);
    return response.data;
  } catch (error) {
    console.error(`Error getting info for dataset ${datasetName}:`, error);
    throw error;
  }
};

export const getColumnStats = async (datasetName, columnName, engine = 'pandas') => {
  try {
    const response = await api.get(`/column-stats/${datasetName}/${columnName}?engine=${engine}`);
    return response.data;
  } catch (error) {
    console.error(`Error getting column stats for ${columnName} in ${datasetName}:`, error);
    throw error;
  }
};

export const saveTransformation = async (datasetName, newDatasetName, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('new_dataset_name', newDatasetName);
  formData.append('engine', engine);

  try {
    const response = await api.post('/save-transformation', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }, // Add header
    });
    return response.data;
  } catch (error) {
    console.error(`Error saving transformation of ${datasetName} as ${newDatasetName}:`, error.response?.data || error.message);
     console.error('FastAPI Validation Detail:', error.response?.data?.detail);
    throw error;
  }
};

export const exportDataset = async (datasetName, format = 'csv', engine = 'pandas') => {
  try {
    const response = await api.get(`/export/${datasetName}?format=${format}&engine=${engine}`, {
      responseType: 'blob'
    });
    
    // Create a download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `${datasetName}.${format}`);
    document.body.appendChild(link);
    link.click();
    link.remove();
    
    return true;
  } catch (error) {
    console.error(`Error exporting dataset ${datasetName} as ${format}:`, error);
    throw error;
  }
};

export const mergeDatasets = async (leftDataset, rightDataset, params, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('left_dataset', leftDataset);
  formData.append('right_dataset', rightDataset);
  formData.append('params', JSON.stringify(params));
  formData.append('engine', engine);

  try {
    const response = await api.post('/merge-datasets', formData, {
        headers: { 'Content-Type': 'multipart/form-data' }, // Add header
    });
     // Expecting { message, data, columns, row_count, code, can_undo, can_reset }
    return response.data;
  } catch (error) {
    console.error(`Error merging datasets ${leftDataset} and ${rightDataset}:`, error.response?.data || error.message);
     console.error('FastAPI Validation Detail:', error.response?.data?.detail);
    throw error;
  }
};


// New function for regex-based operations
export const performRegexOperation = async (datasetName, operation, regex, options, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('operation', operation);
  formData.append('regex', regex);
  formData.append('options', JSON.stringify(options));
  formData.append('engine', engine);
  
  try {
    const response = await api.post('/regex-operation', formData);
    return response.data;
  } catch (error) {
    console.error(`Error performing regex operation ${operation} on ${datasetName}:`, error);
    throw error;
  }
};

export const undoTransformation = async (datasetName, currentEngine = 'pandas') => {
  try {
    // Send current engine preference for the preview response
    const response = await api.post(`/undo/${datasetName}?engine=${currentEngine}`);
     // Expecting { message, data, columns, row_count, can_undo, can_reset, last_code }
    return response.data;
  } catch (error) {
    console.error(`Error undoing operation on ${datasetName}:`, error.response?.data || error.message);
    throw error;
  }
};

export const resetTransformation = async (datasetName, currentEngine = 'pandas') => {
  try {
    const response = await api.post(`/reset/${datasetName}?engine=${currentEngine}`);
     // Expecting { message, data, columns, row_count, can_undo, can_reset, last_code }
    return response.data;
  } catch (error) {
    console.error(`Error resetting transformations for ${datasetName}:`, error.response?.data || error.message);
    throw error;
  }
};

const apiService = {
  testConnection,
  uploadDataset,
  getDatasets,
  getDataset,
  loadMoreRows,
  performOperation,
  executeCustomCode,
  getDatasetInfo,
  getColumnStats,
  saveTransformation,
  exportDataset,
  mergeDatasets,
  performRegexOperation,
  undoTransformation,
  resetTransformation,
};

export default apiService;