import React, { useState, useEffect } from 'react';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { atomDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { executeCustomCode } from '../services/api';

const CodeEditor = ({ currentDataset, engine, onCodeExecuted }) => {
  const [code, setCode] = useState('');
  const [isExecuting, setIsExecuting] = useState(false);
  const [codeSnippets, setCodeSnippets] = useState([]);
  const [showSnippets, setShowSnippets] = useState(false);

  // Initialize code snippets based on the selected engine
  useEffect(() => {
    loadSnippetsForEngine(engine);
  }, [engine]);

  const loadSnippetsForEngine = (engineType) => {
    // Define common operations with syntax for each engine
    const snippets = {
      pandas: [
        {
          name: 'Filter rows',
          description: 'Filter rows based on a condition',
          code: "# Filter rows where column_name > value\ndf = df[df['column_name'] > value]"
        },
        {
          name: 'Select columns',
          description: 'Select specific columns',
          code: "# Select specific columns\ndf = df[['column1', 'column2', 'column3']]"
        },
        {
          name: 'Sort values',
          description: 'Sort by one or more columns',
          code: "# Sort by column in ascending order\ndf = df.sort_values('column_name')\n\n# Sort by column in descending order\ndf = df.sort_values('column_name', ascending=False)\n\n# Sort by multiple columns\ndf = df.sort_values(['column1', 'column2'], ascending=[True, False])"
        },
        {
          name: 'Group by',
          description: 'Group by one or more columns and apply aggregation',
          code: "# Group by one column and calculate mean\ndf = df.groupby('column_name')['value_column'].mean().reset_index()\n\n# Group by multiple columns\ndf = df.groupby(['column1', 'column2'])['value_column'].mean().reset_index()\n\n# Multiple aggregations\ndf = df.groupby('column_name').agg({\n    'numeric_col1': 'mean',\n    'numeric_col2': 'sum',\n    'string_col': 'count'\n}).reset_index()"
        },
        {
          name: 'Pivot table',
          description: 'Create a pivot table',
          code: "# Create pivot table\ndf = pd.pivot_table(\n    df,\n    index='row_column',\n    columns='column_header',\n    values='value_column',\n    aggfunc='mean'\n)"
        },
        {
          name: 'Melt (unpivot)',
          description: 'Transform wide data to long format',
          code: "# Melt wide format to long format\ndf = pd.melt(\n    df,\n    id_vars=['id_column1', 'id_column2'],\n    value_vars=['value_col1', 'value_col2', 'value_col3'],\n    var_name='variable',\n    value_name='value'\n)"
        },
        {
          name: 'Join / merge',
          description: 'Combine two DataFrames',
          code: "# Merge two dataframes (SQL-like join)\ndf = pd.merge(\n    df_left,\n    df_right,\n    how='inner',  # 'left', 'right', 'outer'\n    left_on='left_key_column',\n    right_on='right_key_column'\n)"
        },
        {
          name: 'Regex filtering',
          description: 'Filter using regular expressions',
          code: "# Filter rows where column matches regex pattern\ndf = df[df['column_name'].str.contains('pattern', case=False, regex=True)]\n\n# Extract information with regex\ndf['extracted'] = df['column_name'].str.extract('(\\d+)')"
        },
        {
          name: 'Index operations',
          description: 'Set and reset index',
          code: "# Set a column as index\ndf = df.set_index('column_name')\n\n# Reset index back to column\ndf = df.reset_index()"
        }
      ],
      polars: [
        {
          name: 'Filter rows',
          description: 'Filter rows based on a condition',
          code: "# Filter rows where column_name > value\ndf = df.filter(pl.col('column_name') > value)"
        },
        {
          name: 'Select columns',
          description: 'Select specific columns',
          code: "# Select specific columns\ndf = df.select(['column1', 'column2', 'column3'])"
        },
        {
          name: 'Sort values',
          description: 'Sort by one or more columns',
          code: "# Sort by column in ascending order\ndf = df.sort('column_name')\n\n# Sort by column in descending order\ndf = df.sort('column_name', descending=True)\n\n# Sort by multiple columns\ndf = df.sort(['column1', 'column2'], descending=[False, True])"
        },
        {
          name: 'Group by',
          description: 'Group by one or more columns and apply aggregation',
          code: "# Group by one column and calculate mean\ndf = df.group_by('column_name').agg(pl.col('value_column').mean())\n\n# Group by multiple columns\ndf = df.group_by(['column1', 'column2']).agg(pl.col('value_column').mean())\n\n# Multiple aggregations\ndf = df.group_by('column_name').agg([\n    pl.col('numeric_col1').mean(),\n    pl.col('numeric_col2').sum(),\n    pl.col('string_col').count()\n])"
        },
        {
          name: 'Pivot',
          description: 'Create a pivot table',
          code: "# Create pivot table\ndf = df.pivot(\n    index='row_column',\n    columns='column_header',\n    values='value_column',\n    aggregate_function='mean'\n)"
        },
        {
          name: 'Melt (unpivot)',
          description: 'Transform wide data to long format',
          code: "# Melt wide format to long format\ndf = df.melt(\n    id_vars=['id_column1', 'id_column2'],\n    value_vars=['value_col1', 'value_col2', 'value_col3'],\n    variable_name='variable',\n    value_name='value'\n)"
        },
        {
          name: 'Join',
          description: 'Combine two DataFrames',
          code: "# Join two dataframes\ndf = df_left.join(\n    df_right,\n    left_on='left_key_column',\n    right_on='right_key_column',\n    how='inner'  # 'left', 'right', 'outer'\n)"
        },
        {
          name: 'Regex filtering',
          description: 'Filter using regular expressions',
          code: "# Filter rows where column matches regex pattern\ndf = df.filter(pl.col('column_name').str.contains('pattern', literal=False))\n\n# Extract information with regex\ndf = df.with_column(pl.col('column_name').str.extract('(\\d+)', group_index=0).alias('extracted'))"
        },
        {
          name: 'Index operations',
          description: 'Work with DataFrame indices',
          code: "# Set a column as index (Polars does not have indexes in the same way as pandas)\n# Usually you would just sort or group by the column instead\ndf = df.sort('column_name')"
        }
      ],
      sql: [
        {
          name: 'Filter rows (WHERE)',
          description: 'Filter rows using WHERE clause',
          code: "-- Filter rows where column_name > value\nSELECT * FROM table_name WHERE column_name > value"
        },
        {
          name: 'Select columns',
          description: 'Select specific columns',
          code: "-- Select specific columns\nSELECT column1, column2, column3 FROM table_name"
        },
        {
          name: 'Sort values (ORDER BY)',
          description: 'Sort by one or more columns',
          code: "-- Sort by column in ascending order\nSELECT * FROM table_name ORDER BY column_name ASC\n\n-- Sort by column in descending order\nSELECT * FROM table_name ORDER BY column_name DESC\n\n-- Sort by multiple columns\nSELECT * FROM table_name ORDER BY column1 ASC, column2 DESC"
        },
        {
          name: 'Group by',
          description: 'Group by one or more columns and apply aggregation',
          code: "-- Group by one column and calculate average\nSELECT column_name, AVG(value_column) AS average\nFROM table_name\nGROUP BY column_name\n\n-- Group by multiple columns\nSELECT column1, column2, AVG(value_column) AS average\nFROM table_name\nGROUP BY column1, column2\n\n-- Multiple aggregations\nSELECT column_name,\n       AVG(numeric_col1) AS avg_value,\n       SUM(numeric_col2) AS total_value,\n       COUNT(string_col) AS count\nFROM table_name\nGROUP BY column_name"
        },
        {
          name: 'Pivot table (CROSSTAB)',
          description: 'Create a pivot table using CROSSTAB',
          code: "-- Pivot table (DuckDB syntax)\nSELECT * FROM PIVOT(\n  SELECT row_column, column_header, value_column\n  FROM table_name\n) AS p\nPIVOT_BY(column_header)\nFOR value_column"
        },
        {
          name: 'Joins',
          description: 'Join tables together',
          code: "-- Inner join\nSELECT *\nFROM left_table\nINNER JOIN right_table ON left_table.left_key_column = right_table.right_key_column\n\n-- Left join\nSELECT *\nFROM left_table\nLEFT JOIN right_table ON left_table.left_key_column = right_table.right_key_column\n\n-- Right join\nSELECT *\nFROM left_table\nRIGHT JOIN right_table ON left_table.left_key_column = right_table.right_key_column\n\n-- Full outer join\nSELECT *\nFROM left_table\nFULL OUTER JOIN right_table ON left_table.left_key_column = right_table.right_key_column"
        },
        {
          name: 'Regex filtering',
          description: 'Filter using regular expressions',
          code: "-- Filter rows where column matches regex pattern (DuckDB syntax)\nSELECT *\nFROM table_name\nWHERE REGEXP_MATCHES(column_name, 'pattern')\n\n-- Extract information with regex\nSELECT *,\n       REGEXP_EXTRACT(column_name, '(\\d+)') AS extracted\nFROM table_name"
        },
        {
          name: 'Common Table Expressions (CTE)',
          description: 'Use CTEs for complex queries',
          code: "-- Use a CTE for a multi-step query\nWITH filtered_data AS (\n  SELECT *\n  FROM table_name\n  WHERE value > 100\n),\nsummarized_data AS (\n  SELECT category, AVG(value) AS avg_value\n  FROM filtered_data\n  GROUP BY category\n)\nSELECT *\nFROM summarized_data\nORDER BY avg_value DESC"
        }
      ]
    };

    setCodeSnippets(snippets[engineType] || []);
  };

  const handleExecute = async () => {
    if (!currentDataset || !code.trim()) return;
    
    setIsExecuting(true);
    try {
      const result = await executeCustomCode(currentDataset, code, engine);
      onCodeExecuted(result);
    } catch (error) {
      console.error('Error executing code:', error);
      // You might want to display an error message to the user
    } finally {
      setIsExecuting(false);
    }
  };

  const insertSnippet = (snippetCode) => {
    setCode(prevCode => {
      // If there's already code, add a newline
      const prefix = prevCode.trim() ? prevCode.trim() + '\n\n' : '';
      return prefix + snippetCode;
    });
    setShowSnippets(false);
  };

  // Get language for syntax highlighting
  const getLanguage = () => {
    switch (engine) {
      case 'sql':
        return 'sql';
      default:
        return 'python';
    }
  };

  const getPlaceholder = () => {
    switch (engine) {
      case 'pandas':
        return '# Write pandas code here\n# Example: df = df[df["column"] > value]';
      case 'polars':
        return '# Write polars code here\n# Example: df = df.filter(pl.col("column") > value)';
      case 'sql':
        return '-- Write SQL query here\n-- Example: SELECT * FROM dataset WHERE column > value';
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
            {showSnippets ? 'Hide Snippets' : 'Show Snippets'}
          </button>
          <button
            type="button"
            className={`px-3 py-1 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded ${
              isExecuting || !currentDataset ? 'opacity-50 cursor-not-allowed' : ''
            }`}
            onClick={handleExecute}
            disabled={isExecuting || !currentDataset}
          >
            {isExecuting ? 'Running...' : 'Run Code'}
          </button>
        </div>
      </div>

      {showSnippets && (
        <div className="mb-4 overflow-auto max-h-60 p-2 border rounded">
          <h3 className="font-medium mb-2">Common Operations:</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            {codeSnippets.map((snippet, index) => (
              <div 
                key={index}
                className="p-2 border rounded cursor-pointer hover:bg-gray-50"
                onClick={() => insertSnippet(snippet.code)}
              >
                <div className="font-medium">{snippet.name}</div>
                <div className="text-sm text-gray-600">{snippet.description}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="mb-4 border rounded">
        <textarea
          value={code}
          onChange={(e) => setCode(e.target.value)}
          placeholder={getPlaceholder()}
          className="w-full h-60 p-3 font-mono text-sm focus:outline-none"
          spellCheck="false"
        />
      </div>

      {code && (
        <div className="mt-4">
          <h3 className="font-medium mb-2">Code Preview:</h3>
          <SyntaxHighlighter language={getLanguage()} style={atomDark} showLineNumbers={true}>
            {code}
          </SyntaxHighlighter>
        </div>
      )}

      {!currentDataset && (
        <div className="mt-4 p-3 bg-yellow-100 text-yellow-800 rounded">
          Please select or upload a dataset to start coding.
        </div>
      )}
    </div>
  );
};

export default CodeEditor;