import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const uploadDataset = async (file, datasetName) => {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('dataset_name', datasetName);
  
  const response = await api.post('/upload', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  });
  
  return response.data;
};

export const getDatasets = async () => {
  const response = await api.get('/datasets');
  return response.data.datasets;
};

export const getDataset = async (datasetName, engine = 'pandas', limit = 100) => {
  const response = await api.get(`/dataset/${datasetName}?engine=${engine}&limit=${limit}`);
  return response.data;
};

export const performOperation = async (datasetName, operation, params, engine = 'pandas') => {
  const formData = new FormData();
  formData.append('dataset_name', datasetName);
  formData.append('operation', operation);
  formData.append('params', JSON.stringify(params));
  formData.append('engine', engine);
  
  const response = await api.post('/operation', formData);
  return response.data;
};

export default {
  uploadDataset,
  getDatasets,
  getDataset,
  performOperation,
};