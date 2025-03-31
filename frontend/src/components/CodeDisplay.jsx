import React from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { atomDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

const CodeDisplay = ({ code, engine }) => {
  if (!code) {
    return (
      <div className="card bg-white p-4 border border-maid-gray-light rounded-lg shadow-soft">
        <p className="text-maid-gray-dark text-center py-4 italic">No code to display. Perform an operation to see the generated code ♡</p>
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
    <div className="code-block rounded-lg shadow-soft overflow-hidden border border-coffee-light border-opacity-20">
      <div className="code-header flex justify-between items-center px-4 py-2">
        <h3 className="text-maid-cream-light font-semibold">Generated Code ({engine})</h3>
        <button 
          onClick={() => navigator.clipboard.writeText(codeWithImports)}
          className="code-copy-button"
        >
          Copy
        </button>
      </div>
      <SyntaxHighlighter 
        language={language} 
        style={atomDark}
        customStyle={{ margin: 0, borderRadius: '0 0 0.5rem 0.5rem' }}
        showLineNumbers={true}
        className="syntax-highlighter-container"
      >
        {codeWithImports}
      </SyntaxHighlighter>
    </div>
  );
};

export default CodeDisplay;