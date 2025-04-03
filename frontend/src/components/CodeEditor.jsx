// src/components/CodeEditor.jsx
import React, { useState, useEffect, useRef } from 'react';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { atomDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

// Helper function (can be moved to a utils file)
const sanitizeVariableName = (name) => {
  if (!name) return 'dataframe';
  let s_name = name.replace(/[^\w_]/g, '_');
  if (s_name && /^\d/.test(s_name)) {
    s_name = '_' + s_name;
  }
  const keywords = new Set(["if", "else", "while", "for", "def", "class", "import", "from", "try", "except", "finally", "return", "yield", "lambda", "global", "nonlocal", "pass", "break", "continue", "with", "as", "assert", "del", "in", "is", "not", "or", "and"]);
  if (keywords.has(s_name)) {
    s_name += '_';
  }
  return s_name || 'dataframe';
};


const CodeEditor = ({
  currentDataset,
  engine,
  initialCode = '',
  onExecute,
  isLoading // Prop from App.jsx
}) => {
  const [code, setCode] = useState(initialCode);
  const [isExecuting, setIsExecuting] = useState(false); // Local execution state
  const [codeSnippets, setCodeSnippets] = useState([]);
  const [showSnippets, setShowSnippets] = useState(false);
  const textareaRef = useRef(null);
  const initialCodeRef = useRef(initialCode);

  // --- Effects ---
  useEffect(() => {
    // Update internal code ONLY if the incoming prop is different
    if (initialCode !== initialCodeRef.current) {
        console.log("CodeEditor EFFECT [initialCode]: Prop changed. Updating internal state.");
        setCode(initialCode);
        initialCodeRef.current = initialCode; // Track the new prop value
    } else {
        // console.log("CodeEditor EFFECT [initialCode]: Prop unchanged, skipping state update.");
    }
  }, [initialCode]);

  useEffect(() => {
    console.log(`CodeEditor EFFECT [engine, currentDataset]: Loading snippets for engine: ${engine}, dataset: ${currentDataset}`);
    loadSnippetsForEngine(engine, currentDataset);
  }, [engine, currentDataset]);

  // --- Handlers ---
  const handleCodeChange = (event) => {
    // *** LOGGING POINT 1: Check if handler is called ***
    // This is the MOST critical log for the typing issue.
    console.log("CodeEditor handleCodeChange: Typing detected! New value:", event.target.value);
    setCode(event.target.value);
  };

  const handleExecute = async () => {
    console.log("CodeEditor handleExecute: 'Run Code' clicked.");
    if (!currentDataset || !code.trim() || !onExecute || isExecuting) {
        console.warn("CodeEditor handleExecute: Aborted.", { currentDataset: !!currentDataset, codeTrimmed: !!code.trim(), onExecute: !!onExecute, isExecuting });
        return;
    }
    setIsExecuting(true);
    console.log("CodeEditor handleExecute: Calling onExecute prop...");
    try {
        await onExecute(code);
        console.log("CodeEditor handleExecute: onExecute finished.");
    } catch (error) {
        console.error('CodeEditor handleExecute: Error during onExecute call:', error);
    } finally {
        setIsExecuting(false); // Ensure reset
        console.log("CodeEditor handleExecute: Finished.");
    }
  };

  const insertSnippet = (snippetCode) => {
      if (!textareaRef.current) return;
      const textarea = textareaRef.current;
      const { value, selectionStart, selectionEnd } = textarea;
      const prefix = value.substring(0, selectionStart);
      const suffix = value.substring(selectionEnd);
      let codeToInsert = snippetCode;
      let finalCursorPosition = selectionStart + codeToInsert.length;

      // Add newlines smartly
      if (prefix.trim() && !/\n\s*$/.test(prefix)) { codeToInsert = '\n\n' + codeToInsert; finalCursorPosition += 2; }
      else if (prefix.trim() && !/\n\n\s*$/.test(prefix)) { codeToInsert = '\n' + codeToInsert; finalCursorPosition += 1; }
      if (suffix.trim() && !/^\s*\n/.test(suffix)) { codeToInsert = codeToInsert + '\n\n'; }
      else if (suffix.trim() && !/^\s*\n\n/.test(suffix)) { codeToInsert = codeToInsert + '\n'; }

      const newCode = prefix + codeToInsert + suffix;
      setCode(newCode); // Update the internal state

      // Defer focus/cursor update
      setTimeout(() => {
         if (textareaRef.current) {
           textareaRef.current.focus();
           textareaRef.current.setSelectionRange(finalCursorPosition, finalCursorPosition);
         }
       }, 0);
      setShowSnippets(false);
  };

  // --- Snippet Loading & Helpers ---
  const loadSnippetsForEngine = (engineType, datasetName) => {
    const safeVarName = sanitizeVariableName(datasetName);
    const safeTableName = datasetName ? `"${datasetName.replace(/"/g, '""')}"` : '"your_table_name"';
    const snippets = {
      pandas: [
        { name: 'Filter rows', description: 'Filter rows based on a condition', code: `${safeVarName} = ${safeVarName}[${safeVarName}['column_name'] > value]` },
        { name: 'Select columns', description: 'Select specific columns', code: `${safeVarName} = ${safeVarName}[['column1', 'column2']]` },
        { name: 'Sort values', description: 'Sort by one or more columns', code: `${safeVarName} = ${safeVarName}.sort_values('column_name', ascending=False)` },
        { name: 'Group by', description: 'Group and aggregate', code: `${safeVarName} = ${safeVarName}.groupby('group_col')['agg_col'].mean().reset_index()` },
        { name: 'Rename columns', description: 'Rename one or more columns', code: `${safeVarName} = ${safeVarName}.rename(columns={'old_name': 'new_name'})` },
        { name: 'Drop columns', description: 'Remove columns', code: `${safeVarName} = ${safeVarName}.drop(columns=['col_to_drop'])` },
      ],
      polars: [
        { name: 'Filter rows', description: 'Filter rows based on a condition', code: `${safeVarName} = ${safeVarName}.filter(pl.col('column_name') > value)` },
        { name: 'Select columns', description: 'Select specific columns', code: `${safeVarName} = ${safeVarName}.select(['column1', 'column2'])` },
        { name: 'Sort values', description: 'Sort by one or more columns', code: `${safeVarName} = ${safeVarName}.sort('column_name', descending=True)` },
        { name: 'Group by', description: 'Group and aggregate', code: `${safeVarName} = ${safeVarName}.group_by('group_col').agg(pl.col('agg_col').mean())` },
        { name: 'Rename columns', description: 'Rename one or more columns', code: `${safeVarName} = ${safeVarName}.rename({'old_name': 'new_name'})` },
        { name: 'Drop columns', description: 'Remove columns', code: `${safeVarName} = ${safeVarName}.drop(['col_to_drop'])` },
        { name: 'With Column (apply)', description: 'Apply expression to create/modify column', code: `${safeVarName} = ${safeVarName}.with_column( (pl.col('existing_col') * 2).alias('new_col') )` }
      ],
      sql: [
        { name: 'Filter rows (WHERE)', description: 'Filter rows using WHERE', code: `SELECT * FROM ${safeTableName} WHERE column_name > value;` },
        { name: 'Select columns', description: 'Select specific columns', code: `SELECT column1, column2 FROM ${safeTableName};` },
        { name: 'Sort values (ORDER BY)', description: 'Sort results', code: `SELECT * FROM ${safeTableName} ORDER BY column_name DESC;` },
        { name: 'Group by', description: 'Group and aggregate', code: `SELECT group_col, AVG(agg_col) as avg_agg FROM ${safeTableName} GROUP BY group_col;` },
        { name: 'Join', description: 'Combine with another table', code: `SELECT t1.*, t2.col\nFROM ${safeTableName} t1\nINNER JOIN other_table t2 ON t1.key_col = t2.key_col;` },
        { name: 'CASE statement', description: 'Conditional logic', code: `SELECT\n    column1,\n    CASE\n        WHEN condition1 THEN result1\n        WHEN condition2 THEN result2\n        ELSE default_result\n    END AS new_conditional_col\nFROM ${safeTableName};` },
        { name: 'Create Table', description: 'Create a new table from a query', code: `CREATE TABLE new_table_name AS\nSELECT * FROM ${safeTableName} WHERE condition;` },
      ]
    };
    setCodeSnippets(snippets[engineType] || []);
  };

  const getLanguage = () => {
    switch (engine) {
        case 'pandas': case 'polars': return 'python';
        case 'sql': return 'sql';
        default: return 'plaintext';
    }
  };

  const getPlaceholder = () => {
    const safeVarName = sanitizeVariableName(currentDataset);
    const safeTableName = currentDataset ? `"${currentDataset.replace(/"/g, '""')}"` : '"your_table_name"';
    switch (engine) {
        case 'pandas': return `# Write Pandas code...\n# DataFrame is available as: ${safeVarName}\n# Example: ${safeVarName} = ${safeVarName}[${safeVarName}['your_column'] > 0]`;
        case 'polars': return `# Write Polars code...\n# DataFrame is available as: ${safeVarName}\n# Example: ${safeVarName} = ${safeVarName}.filter(pl.col('your_column') > 0)`;
        case 'sql': return `-- Write SQL query...\n-- Query the table: ${safeTableName}\n-- Example: SELECT * FROM ${safeTableName} WHERE your_column > 0;`;
        default: return '// Write code here...';
    }
  };

  // --- Render Logic ---
  // Determine if the editor should be disabled
  const isDisabled = !currentDataset || isLoading || isExecuting;

  // *** LOGGING POINT 2: Check props and calculated disabled state on each render ***
  console.log(`%cCodeEditor RENDER: isDisabled=${isDisabled}`, 'color: blue; font-weight: bold;', {
      currentDataset: currentDataset,
      isLoading_Prop: isLoading, // Prop from App
      isExecuting_Local: isExecuting, // Local state
      codeStateLength: code.length, // Current internal code state
      initialCodePropLength: initialCode.length // Prop value
  });

  return (
    <div className="card bg-white p-4 rounded-lg shadow-soft border border-maid-gray-light">
      {/* Toolbar */}
      <div className="flex justify-between items-center mb-3">
        <h2 className="text-lg font-semibold text-maid-choco-dark">Code Editor ({engine})</h2>
        <div className="flex space-x-2">
          <button
            onClick={() => setShowSnippets(!showSnippets)}
            className={`btn ${showSnippets ? 'btn-coffee text-white' : 'btn-outline'} px-3 py-1 text-sm`}
            disabled={isDisabled}
            title={showSnippets ? "Hide code snippets" : "Show code snippets"}
          >
            {showSnippets ? 'Hide Snippets' : 'Show Snippets'}
          </button>
          <button
            onClick={handleExecute}
            disabled={isLoading || isExecuting || !currentDataset || !code.trim()}
            className="btn btn-coffee px-3 py-1 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
            title={!currentDataset ? "Select a dataset first" : !code.trim() ? "Write some code first" : "Execute the code"}
          >
            {isExecuting ? 'Running...' : 'Run Code'}
          </button>
        </div>
      </div>

      {/* Snippets Panel */}
      {showSnippets && (
        <div className="mb-4 p-3 bg-maid-cream rounded border border-maid-gray-light max-h-60 overflow-y-auto">
          <h3 className="font-medium mb-2 text-maid-choco-dark text-sm">Common Operations ({engine}):</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
            {codeSnippets.length > 0 ? (
              codeSnippets.map((snippet, index) => (
                <div
                  key={index}
                  className="p-2 border border-maid-gray rounded bg-white hover:bg-maid-cream-light cursor-pointer transition-colors duration-150"
                  onClick={() => insertSnippet(snippet.code)}
                  title={`Click to insert:\n${snippet.code}`}
                >
                  <div className="font-medium text-xs text-maid-choco-dark">{snippet.name}</div>
                  <div className="text-xs text-maid-choco mt-1">{snippet.description}</div>
                </div>
              ))
            ) : (
              <p className="text-sm text-maid-gray-dark italic col-span-full">No snippets available for {engine}.</p>
            )}
          </div>
        </div>
      )}

      {/* Textarea */}
      <div className="mb-4 border border-maid-gray rounded-md focus-within:ring-2 focus-within:ring-coffee-light transition-shadow duration-150">
        <textarea
          ref={textareaRef}
          value={code} // Controlled component
          onChange={handleCodeChange} // *** This should enable typing ***
          placeholder={getPlaceholder()}
          className="w-full p-2 rounded-md font-mono text-sm bg-white text-maid-choco-dark focus:outline-none resize-y min-h-[150px] disabled:bg-gray-100 disabled:text-gray-500"
          spellCheck="false"
          disabled={isDisabled} // *** Check this attribute in dev tools ***
          aria-label={`Code editor for ${engine}`}
          // *** DEBUGGING STEP: Temporarily remove disabled prop if needed ***
          // disabled={false} // <-- Uncomment this line ONLY for testing
        />
      </div>

      {/* Code Preview */}
      {code && (
        <div className="mt-4">
          <h3 className="font-medium mb-2 text-maid-choco-dark text-sm">Code Preview:</h3>
          <div className="rounded-md shadow-inner overflow-x-auto border border-maid-gray-light bg-[#2d2d2d]">
              <SyntaxHighlighter
                language={getLanguage()}
                style={atomDark}
                showLineNumbers={true}
                wrapLines={true}
                lineProps={{style: {wordBreak: 'break-all', whiteSpace: 'pre-wrap'}}}
                customStyle={{ margin: 0, padding: '1em', maxHeight: '400px', overflowY: 'auto', fontSize: '0.875rem' }}
                lineNumberStyle={{ color: '#6c757d', fontSize: '0.8em' }}
              >
                {code || ''} {/* Ensure children is always a string */}
             </SyntaxHighlighter>
          </div>
        </div>
      )}

      {/* No Dataset Message */}
      {!currentDataset && (
         <div className="mt-4 p-3 bg-yellow-100 text-yellow-800 rounded-md border border-yellow-200 text-center text-sm">
           Please select or upload a dataset to start editing code ♡
         </div>
       )}
    </div>
  );
};

export default CodeEditor;