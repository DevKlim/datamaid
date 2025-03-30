import React, { useState, useEffect, useRef } from 'react';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { atomDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
// executeCustomCode is now called from App.jsx handler

// Accept initialCode and onCodeExecuted function
const CodeEditor = ({ currentDataset, engine, initialCode = '', onCodeExecuted }) => {
  const [code, setCode] = useState(initialCode);
  const [isExecuting, setIsExecuting] = useState(false);
  const [codeSnippets, setCodeSnippets] = useState([]);
  const [showSnippets, setShowSnippets] = useState(false);
  const textareaRef = useRef(null); // Ref for textarea

  // Update internal code state if initialCode prop changes (e.g., after undo/reset)
  useEffect(() => {
      setCode(initialCode);
  }, [initialCode]);

  // Initialize code snippets based on the selected engine
  useEffect(() => {
    loadSnippetsForEngine(engine);
    // Consider clearing code on engine change, or keep it? User preference.
    // setCode('');
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
        { name: 'Melt (unpivot)', description: 'Transform wide to long', code: "melted_df = pd.melt(df, id_vars=['id_col'], value_vars=['val_col1', 'val_col2'], var_name='variable', value_name='value')\ndf = melted_df" },
        { name: 'Merge/Join', description: 'Combine two DataFrames (requires df_right)', code: "# Assume df_right exists\ndf = pd.merge(df, df_right, on='key_col', how='inner')" },
        { name: 'Apply function', description: 'Apply a function to a column', code: "def my_func(x):\n    # ... your logic ...\n    return x * 2\ndf['new_col'] = df['existing_col'].apply(my_func)" }
      ],
      polars: [
        { name: 'Filter rows', description: 'Filter rows based on a condition', code: "df = df.filter(pl.col('column_name') > value)" },
        { name: 'Select columns', description: 'Select specific columns', code: "df = df.select(['column1', 'column2'])" },
        { name: 'Sort values', description: 'Sort by one or more columns', code: "df = df.sort('column_name', descending=True)" },
        { name: 'Group by', description: 'Group and aggregate', code: "df = df.group_by('group_col').agg(pl.col('agg_col').mean())" },
        { name: 'Rename columns', description: 'Rename one or more columns', code: "df = df.rename({'old_name': 'new_name'})" },
        { name: 'Drop columns', description: 'Remove columns', code: "df = df.drop(['col_to_drop'])" },
        { name: 'Pivot', description: 'Create a pivot table', code: "pivot_df = df.pivot(index='idx_col', columns='col_col', values='val_col', aggregate_function='mean')\ndf = pivot_df" },
        { name: 'Melt (unpivot)', description: 'Transform wide to long', code: "melted_df = df.melt(id_vars=['id_col'], value_vars=['val_col1', 'val_col2'], variable_name='variable', value_name='value')\ndf = melted_df" },
        { name: 'Join', description: 'Combine two DataFrames (requires df_right)', code: "# Assume df_right exists\ndf = df.join(df_right, on='key_col', how='inner')" },
        { name: 'With Column (apply)', description: 'Apply expression to create/modify column', code: "df = df.with_column( (pl.col('existing_col') * 2).alias('new_col') )" }

      ],
       sql: [
        { name: 'Filter rows (WHERE)', description: 'Filter rows using WHERE', code: "SELECT * FROM {{dataset_name}} WHERE column_name > value" },
        { name: 'Select columns', description: 'Select specific columns', code: "SELECT column1, column2 FROM {{dataset_name}}" },
        { name: 'Sort values (ORDER BY)', description: 'Sort results', code: "SELECT * FROM {{dataset_name}} ORDER BY column_name DESC" },
        { name: 'Group by', description: 'Group and aggregate', code: "SELECT group_col, AVG(agg_col) as avg_agg FROM {{dataset_name}} GROUP BY group_col" },
        { name: 'Rename columns (AS)', description: 'Rename columns in select', code: "SELECT old_name AS new_name, other_col FROM {{dataset_name}}" },
        { name: 'Drop columns (by not selecting)', description: 'Select only columns to keep', code: "SELECT col_to_keep1, col_to_keep2 FROM {{dataset_name}}" },
        { name: 'Join', description: 'Combine with another table (requires table_right)', code: "SELECT t1.*, t2.col\nFROM {{dataset_name}} t1\nINNER JOIN table_right t2 ON t1.key_col = t2.key_col" },
        { name: 'CASE statement', description: 'Conditional logic', code: "SELECT\n    column1,\n    CASE\n        WHEN condition1 THEN result1\n        WHEN condition2 THEN result2\n        ELSE default_result\n    END AS new_conditional_col\nFROM {{dataset_name}}" },
        { name: 'Common Table Expression (CTE)', description: 'Organize complex queries', code: "WITH MyCTE AS (\n    SELECT col1, COUNT(*) as count\n    FROM {{dataset_name}}\n    WHERE col2 > 10\n    GROUP BY col1\n)\nSELECT * FROM MyCTE WHERE count > 5;" }
      ]
      // Add more snippets as needed
    };
    setCodeSnippets(snippets[engineType] || []);
  };


  const handleExecute = async () => {
    if (!currentDataset || !code.trim()) return;
    setIsExecuting(true);
    try {
      // Call the handler passed from App.jsx, passing the current code
      await onCodeExecuted(code);
    } catch (error) {
      // Error handling is now primarily in App.jsx's handler
      console.error('Code execution trigger failed locally:', error);
      // Optionally show a local alert here too if needed
      // alert(`Local trigger error: ${error.message}`);
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
      if (prefix.trim() && !/\n\s*$/.test(prefix)) { // If prefix not ending with newline(s)
          codeToInsert = '\n\n' + codeToInsert;
          finalCursorPosition += 2; // Account for added newlines
      } else if (prefix.trim() && !/\n\n\s*$/.test(prefix)) { // If prefix ending with single newline
          codeToInsert = '\n' + codeToInsert;
          finalCursorPosition += 1;
      }

      if (suffix.trim() && !/^\s*\n/.test(suffix)) { // If suffix not starting with newline(s)
          codeToInsert = codeToInsert + '\n\n';
      } else if (suffix.trim() && !/^\s*\n\n/.test(suffix)) { // If suffix starting with single newline
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
       }, 0); // Timeout ensures this runs after React's state update cycle

      // Optional: Close snippets panel after insertion
      // setShowSnippets(false);
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

  const getPlaceholder = () => {
    switch (engine) {
      case 'pandas':
        return '# Write pandas code here\n# The current DataFrame is available as `df`\n# Example: df = df[df["column"] > value]';
      case 'polars':
        return '# Write polars code here\n# The current DataFrame is available as `df`\n# Example: df = df.filter(pl.col("column") > value)';
      case 'sql':
        return `-- Write SQL query here\n-- The current dataset is available as "${currentDataset || 'your_table_name'}"\n-- Example: SELECT * FROM "${currentDataset || 'your_table_name'}" WHERE column > value`;
      default:
        return '// Write your code here';
    }
  };


  return (
    <div className="bg-white p-4 rounded shadow">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-semibold">Code Editor ({engine})</h2>
        <div className="flex space-x-2">
          <button
            type="button"
            className="px-3 py-1 text-sm bg-gray-200 hover:bg-gray-300 rounded"
            onClick={() => setShowSnippets(!showSnippets)}
          >
            {showSnippets ? 'Hide Snippets' : 'Code Snippets'}
          </button>
          <button
            type="button"
            className={`px-3 py-1 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded ${
              isExecuting || !currentDataset || !code.trim() ? 'opacity-50 cursor-not-allowed' : ''
            }`}
            onClick={handleExecute}
            disabled={isExecuting || !currentDataset || !code.trim()} // Disable if no code or not dataset
          >
            {isExecuting ? 'Running...' : 'Run Code'}
          </button>
        </div>
      </div>

      {/* --- Snippets Panel --- */}
      {showSnippets && (
        <div className="mb-4 p-3 border rounded bg-gray-50 max-h-60 overflow-y-auto">
          <h3 className="font-medium mb-2 text-gray-700">Common Operations ({engine}):</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {codeSnippets.length > 0 ? (
              codeSnippets.map((snippet, index) => (
                <div
                  key={index}
                  className="p-2 border rounded cursor-pointer hover:bg-blue-50 bg-white shadow-sm"
                  onClick={() => insertSnippet(snippet.code)}
                  title={`Click to insert:\n${snippet.code}`} // Tooltip shows the code
                >
                  <div className="font-medium text-sm text-blue-700">{snippet.name}</div>
                  <div className="text-xs text-gray-600 mt-1">{snippet.description}</div>
                </div>
              ))
            ) : (
              <p className="text-sm text-gray-500 italic col-span-full">No snippets available for {engine}.</p>
            )}
          </div>
        </div>
      )}
      {/* --- End Snippets Panel --- */}


      <div className="mb-4 border rounded focus-within:ring-2 focus-within:ring-blue-500">
        <textarea
          ref={textareaRef} // Assign ref
          value={code}
          onChange={(e) => setCode(e.target.value)}
          placeholder={getPlaceholder()}
          className="w-full h-60 p-3 font-mono text-sm focus:outline-none resize-y block" // Added block display
          spellCheck="false"
          disabled={!currentDataset || isExecuting} // Disable textarea if no dataset or executing
        />
      </div>

      {/* Code Preview */}
      {code && (
        <div className="mt-4">
          <h3 className="font-medium mb-2">Code Preview:</h3>
          {/* Added overflow-x-auto for long lines */}
          <div className="rounded shadow-inner overflow-x-auto">
              <SyntaxHighlighter
                language={getLanguage()}
                style={atomDark}
                showLineNumbers={true}
                wrapLines={true} // Enable line wrapping in preview
                lineProps={{style: {wordBreak: 'break-all', whiteSpace: 'pre-wrap'}}} // Ensure wrapping works
                customStyle={{ margin: 0, maxHeight: '400px', overflowY: 'auto' }} // Max height and scroll
              >
                {code}
             </SyntaxHighlighter>
          </div>
        </div>
      )}

      {/* Message when no dataset is selected */}
      {!currentDataset && (
         <div className="mt-4 p-3 bg-yellow-100 text-yellow-800 rounded border border-yellow-200">
           Please select or upload a dataset to start editing code.
         </div>
       )} {/* <-- Corrected: Added the div wrapper */}
    </div>
  );
};

export default CodeEditor;