import React, { useState, useEffect, useRef } from 'react';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { atomDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

const CodeEditor = ({ currentDataset, engine, initialCode = '', onCodeExecuted }) => {
  const [code, setCode] = useState(initialCode);
  const [isExecuting, setIsExecuting] = useState(false);
  const [codeSnippets, setCodeSnippets] = useState([]);
  const [showSnippets, setShowSnippets] = useState(false);
  const textareaRef = useRef(null);

  // Update internal code state if initialCode prop changes (e.g., after undo/reset)
  useEffect(() => {
    if (initialCode !== code) {
      setCode(initialCode);
   }
  }, [initialCode, code]);

  // Initialize code snippets based on the selected engine
  useEffect(() => {
    loadSnippetsForEngine(engine);
  }, [engine]);

  const loadSnippetsForEngine = (engineType) => {
    // Define common operations with syntax for each engine
    const snippets = {
      pandas: [
        { name: 'Filter rows', description: 'Filter rows based on a condition', code: "df = df[df['column_name'] > value]" },
        { name: 'Select columns', description: 'Select specific columns', code: "df = df[['column1', 'column2']]" },
        { name: 'Sort values', description: 'Sort by one or more columns', code: "df = df.sort_values('column_name', ascending=False)" },
        { name: 'Group by', description: 'Group and aggregate', code: "df = df.groupby('group_col')['agg_col'].mean().reset_index()" },
        { name: 'Rename columns', description: 'Rename one or more columns', code: "df = df.rename(columns={'old_name': 'new_name'})" },
        { name: 'Drop columns', description: 'Remove columns', code: "df = df.drop(columns=['col_to_drop'])" },
        { name: 'Pivot table', description: 'Create a pivot table', code: "pivot_df = pd.pivot_table(df, index='idx_col', columns='col_col', values='val_col', aggfunc='mean')\ndf = pivot_df.reset_index() # Optional reset index" },
      ],
      polars: [
        { name: 'Filter rows', description: 'Filter rows based on a condition', code: "df = df.filter(pl.col('column_name') > value)" },
        { name: 'Select columns', description: 'Select specific columns', code: "df = df.select(['column1', 'column2'])" },
        { name: 'Sort values', description: 'Sort by one or more columns', code: "df = df.sort('column_name', descending=True)" },
        { name: 'Group by', description: 'Group and aggregate', code: "df = df.group_by('group_col').agg(pl.col('agg_col').mean())" },
        { name: 'Rename columns', description: 'Rename one or more columns', code: "df = df.rename({'old_name': 'new_name'})" },
        { name: 'Drop columns', description: 'Remove columns', code: "df = df.drop(['col_to_drop'])" },
        { name: 'With Column (apply)', description: 'Apply expression to create/modify column', code: "df = df.with_column( (pl.col('existing_col') * 2).alias('new_col') )" }
      ],
      sql: [
        { name: 'Filter rows (WHERE)', description: 'Filter rows using WHERE', code: "SELECT * FROM {{dataset_name}} WHERE column_name > value" },
        { name: 'Select columns', description: 'Select specific columns', code: "SELECT column1, column2 FROM {{dataset_name}}" },
        { name: 'Sort values (ORDER BY)', description: 'Sort results', code: "SELECT * FROM {{dataset_name}} ORDER BY column_name DESC" },
        { name: 'Group by', description: 'Group and aggregate', code: "SELECT group_col, AVG(agg_col) as avg_agg FROM {{dataset_name}} GROUP BY group_col" },
        { name: 'Join', description: 'Combine with another table (requires table_right)', code: "SELECT t1.*, t2.col\nFROM {{dataset_name}} t1\nINNER JOIN table_right t2 ON t1.key_col = t2.key_col" },
        { name: 'CASE statement', description: 'Conditional logic', code: "SELECT\n    column1,\n    CASE\n        WHEN condition1 THEN result1\n        WHEN condition2 THEN result2\n        ELSE default_result\n    END AS new_conditional_col\nFROM {{dataset_name}}" },
      ]
    };
    setCodeSnippets(snippets[engineType] || []);
  };

  const handleExecute = async () => {
    if (!currentDataset || !code.trim() || !onCodeExecuted) return;
    setIsExecuting(true);
    try {
        await onCodeExecuted(code);
    } catch (error) {
        console.error('Code execution trigger failed locally in CodeEditor:', error);
    } finally {
        setIsExecuting(false);
    }
  };

  const insertSnippet = (snippetCode) => {
      // Replace placeholder if SQL engine
      if (engine === 'sql') {
          snippetCode = snippetCode.replace(/\{\{dataset_name\}\}/g, `"${currentDataset || 'your_table_name'}"`);
      }

      const currentCode = textareaRef.current?.value || code;
      const cursorPosition = textareaRef.current?.selectionStart ?? currentCode.length;
      const prefix = currentCode.substring(0, cursorPosition);
      const suffix = currentCode.substring(cursorPosition);

      let codeToInsert = snippetCode;
      let finalCursorPosition = cursorPosition + codeToInsert.length;

      // Add newlines smartly based on context
      if (prefix.trim() && !/\n\s*$/.test(prefix)) {
          codeToInsert = '\n\n' + codeToInsert;
          finalCursorPosition += 2;
      } else if (prefix.trim() && !/\n\n\s*$/.test(prefix)) {
          codeToInsert = '\n' + codeToInsert;
          finalCursorPosition += 1;
      }

      if (suffix.trim() && !/^\s*\n/.test(suffix)) {
          codeToInsert = codeToInsert + '\n\n';
      } else if (suffix.trim() && !/^\s*\n\n/.test(suffix)) {
           codeToInsert = codeToInsert + '\n';
      }

       const newCode = prefix + codeToInsert + suffix;
       setCode(newCode);

       // Restore focus and set cursor after state update
       setTimeout(() => {
         if (textareaRef.current) {
           textareaRef.current.focus();
           textareaRef.current.setSelectionRange(finalCursorPosition, finalCursorPosition);
         }
       }, 0);
  };

  // Get language for syntax highlighting
  const getLanguage = () => {
    switch (engine) {
      case 'sql':
        return 'sql';
      default: // pandas, polars
        return 'python';
    }
  };

  // getPlaceholder function
  const getPlaceholder = () => {
    const safeDatasetName = currentDataset ? `"${currentDataset}"` : '"your_table_name"';
    switch (engine) {
        case 'pandas': return `# Write pandas code...\n# DataFrame is 'df'\ndf = df[df['col'] > 0]`;
        case 'polars': return `# Write polars code...\n# DataFrame is 'df'\ndf = df.filter(pl.col('col') > 0)`;
        case 'sql': return `-- Write SQL query...\n-- Use ${safeDatasetName} as table\nSELECT * FROM ${safeDatasetName} WHERE col > 0`;
        default: return '// Write code here...';
    }
  };

  return (
    <div className="card bg-white p-4 rounded-lg shadow-soft border border-maid-gray-light">
      <div className="code-editor-toolbar">
        <h2 className="code-editor-title text-maid-choco-dark">Code Editor ({engine})</h2>
        <div className="flex space-x-2">
          <button 
            onClick={() => setShowSnippets(!showSnippets)} 
            className={`btn ${showSnippets ? 'btn-coffee' : 'btn-outline'} px-3 py-1 text-sm`}
          >
            {showSnippets ? 'Hide Snippets' : 'Show Snippets'}
          </button>
          <button 
            onClick={handleExecute} 
            disabled={isExecuting || !currentDataset || !code.trim()} 
            className="btn btn-coffee px-3 py-1 text-sm"
          >
            {isExecuting ? 'Running...' : 'Run Code'}
          </button>
        </div>
      </div>

      {/* Snippets Panel */}
      {showSnippets && (
        <div className="code-snippets-panel">
          <h3 className="font-medium mb-2 text-maid-choco-dark">Common Operations ({engine}):</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {codeSnippets.length > 0 ? (
              codeSnippets.map((snippet, index) => (
                <div
                  key={index}
                  className="snippet-item"
                  onClick={() => insertSnippet(snippet.code)}
                  title={`Click to insert:\n${snippet.code}`}
                >
                  <div className="snippet-name">{snippet.name}</div>
                  <div className="text-xs text-maid-choco mt-1">{snippet.description}</div>
                </div>
              ))
            ) : (
              <p className="text-sm text-maid-gray-dark italic col-span-full">No snippets available for {engine}.</p>
            )}
          </div>
        </div>
      )}

      <div className="mb-4 border border-maid-gray rounded-md focus-within:ring-2 focus-within:ring-coffee-light">
        <textarea
          ref={textareaRef}
          value={code}
          onChange={(e) => setCode(e.target.value)}
          placeholder={getPlaceholder()}
          className="code-editor-textarea"
          spellCheck="false"
          disabled={!currentDataset || isExecuting}
        />
      </div>
      
      {/* Code Preview */}
      {code && (
        <div className="mt-4">
          <h3 className="font-medium mb-2 text-maid-choco-dark">Code Preview:</h3>
          <div className="rounded-md shadow-inner overflow-x-auto border border-maid-gray-light">
              <SyntaxHighlighter
                language={getLanguage()}
                style={atomDark}
                showLineNumbers={true}
                wrapLines={true}
                lineProps={{style: {wordBreak: 'break-all', whiteSpace: 'pre-wrap'}}}
                customStyle={{ margin: 0, maxHeight: '400px', overflowY: 'auto' }}
              >
                {code}
             </SyntaxHighlighter>
          </div>
        </div>
      )}

      {/* Message when no dataset is selected */}
      {!currentDataset && (
         <div className="mt-4 p-3 bg-yellow-100 text-yellow-800 rounded-md border border-yellow-200 text-center">
           Please select or upload a dataset to start editing code ♡
         </div>
       )}
    </div>
  );
};

export default CodeEditor;