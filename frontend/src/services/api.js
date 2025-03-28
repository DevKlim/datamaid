// Replace your existing api.js with this improved version

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

export const getDataset = async (datasetName, engine = 'pandas', limit = 10) => {
  try {
    const response = await api.get(`/dataset/${datasetName}?engine=${engine}&limit=${limit}`);
    return response.data;
  } catch (error) {
    console.error(`Error fetching dataset ${datasetName}:`, error);
    throw error;
  }
};

export const loadMoreRows = async (datasetName, engine = 'pandas', offset = 0, limit = 50) => {
  try {
    const response = await api.get(`/dataset/${datasetName}?engine=${engine}&offset=${offset}&limit=${limit}`);
    return response.data;
  } catch (error) {
    console.error(`Error loading more rows for ${datasetName}:`, error);
    throw error;
  }
};

export const performOperation = async (datasetName, operation, params, engine = 'pandas') => {
  // Create form data for the request
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('operation', operation);
  
  // Ensure params is properly stringified and check for any empty values
  // This helps prevent malformed JSON
  const sanitizedParams = {};
  Object.keys(params).forEach(key => {
    // Skip empty arrays or undefined values
    if (params[key] === undefined) return;
    if (Array.isArray(params[key]) && params[key].length === 0) return;
    sanitizedParams[key] = params[key];
  });
  
  formData.append('params', JSON.stringify(sanitizedParams));
  formData.append('engine', engine);
  
  // Add some debugging logs
  console.log('Operation Request:', {
    dataset: datasetName,
    operation,
    params: sanitizedParams,
    engine
  });
  
  try {
    const response = await api.post('/operation', formData);
    console.log('Operation Response:', response.data);
    return response.data;
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
    const response = await api.post('/execute-code', formData);
    return response.data;
  } catch (error) {
    console.error(`Error executing custom code on ${datasetName}:`, error);
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
    const response = await api.post('/save-transformation', formData);
    return response.data;
  } catch (error) {
    console.error(`Error saving transformation of ${datasetName} as ${newDatasetName}:`, error);
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
    const response = await api.post('/merge-datasets', formData);
    return response.data;
  } catch (error) {
    console.error(`Error merging datasets ${leftDataset} and ${rightDataset}:`, error);
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

export default {
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
};