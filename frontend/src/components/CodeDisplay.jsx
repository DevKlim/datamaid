import React from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { atomDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

const CodeDisplay = ({ code, engine }) => {
  if (!code) {
    return (
      <div className="p-4 bg-gray-800 rounded shadow-lg">
        <p className="text-gray-300">No code to display. Perform an operation to see the generated code.</p>
      </div>
    );
  }

  // Determine language based on engine
  let language = 'python';
  if (engine === 'sql') {
    language = 'sql';
  }

  const codeWithImports = engine === 'pandas' 
    ? `import pandas as pd\n\n# Assuming df is your DataFrame\n${code}`
    : engine === 'polars'
      ? `import polars as pl\n\n# Assuming df is your DataFrame\n${code}`
      : code;

  return (
    <div className="rounded shadow-lg overflow-hidden">
      <div className="flex justify-between items-center bg-gray-700 px-4 py-2">
        <h3 className="text-white font-semibold">Generated Code ({engine})</h3>
        <button 
          onClick={() => navigator.clipboard.writeText(codeWithImports)}
          className="px-3 py-1 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded"
        >
          Copy
        </button>
      </div>
      <SyntaxHighlighter 
        language={language} 
        style={atomDark}
        customStyle={{ margin: 0, borderRadius: '0 0 0.25rem 0.25rem' }}
        showLineNumbers={true}
      >
        {codeWithImports}
      </SyntaxHighlighter>
    </div>
  );
};

export default CodeDisplay;