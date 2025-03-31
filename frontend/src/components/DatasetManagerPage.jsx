// src/components/DatasetManagerPage.jsx
import React from 'react';
import { Link } from 'react-router-dom';

const DatasetManagerPage = ({ datasets = [], onRenameRequest, onDeleteRequest, isLoading }) => {
  return (
    <main className="flex-grow p-4 max-w-4xl mx-auto w-full">
      <div className="mb-6 flex justify-between items-center">
         <h1 className="text-2xl font-bold text-maid-choco">Manage Datasets</h1>
         <Link to="/" className="btn btn-outline">
            ← Back to Main App
         </Link>
      </div>

      {datasets.length === 0 ? (
        <div className="text-center text-maid-gray-dark p-6 bg-white rounded-lg shadow-soft border border-maid-gray-light">
          No datasets loaded yet. Upload a CSV/DB or paste text on the main page.
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow-soft border border-maid-gray-light overflow-hidden">
          <ul className="divide-y divide-maid-gray-light">
            {datasets.map((name) => (
              <li key={name} className="px-4 py-3 flex justify-between items-center hover:bg-maid-cream transition-colors duration-150">
                <span className="text-maid-choco font-medium">{name}</span>
                <div className="space-x-2 flex-shrink-0">
                  <button
                    onClick={() => onRenameRequest(name)}
                    disabled={isLoading}
                    className="btn btn-sm btn-yellow"
                    title={`Rename ${name}`}
                  >
                    Rename
                  </button>
                  <button
                    onClick={() => onDeleteRequest(name)}
                    disabled={isLoading}
                    className="btn btn-sm btn-red"
                    title={`Delete ${name}`}
                  >
                    Delete
                  </button>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
       {isLoading && <p className="text-center mt-4 text-maid-gray">Loading...</p>}
    </main>
  );
};

export default DatasetManagerPage;