import React, { useState } from 'react';

const Documentation = () => {
  const [activeTab, setActiveTab] = useState('general');

  const tabs = [
    { id: 'general', label: 'Getting Started' },
    { id: 'operations', label: 'Data Operations' },
    { id: 'pandas', label: 'Pandas Syntax' },
    { id: 'polars', label: 'Polars Syntax' },
    { id: 'sql', label: 'SQL Syntax' },
    { id: 'regex', label: 'Regex Guide' },
  ];

  return (
    <div className="max-w-7xl mx-auto px-4 py-8">
      <h1 className="text-3xl font-bold mb-6">Data Analysis GUI Documentation</h1>

      <div className="flex flex-wrap border-b">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`px-4 py-2 font-medium text-sm focus:outline-none ${
              activeTab === tab.id
                ? 'border-b-2 border-blue-500 text-blue-600'
                : 'text-gray-500 hover:text-gray-700'
            }`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="mt-8">
        {activeTab === 'general' && (
          <div>
            <h2 className="text-2xl font-bold mb-4">Getting Started</h2>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Introduction</h3>
              <p className="mb-4">
                Data Analysis GUI is a tool for data exploration and transformation that helps you work with
                data using Pandas, Polars, or SQL syntax. This application is designed to be a "Symbolab for data science" -
                allowing you to perform common operations through a user-friendly interface and see the equivalent code.
              </p>
              <p className="mb-4">
                Whether you're a student learning data science, a beginner looking to understand data manipulation,
                or a professional wanting a quick way to explore datasets, this tool can help you.
              </p>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Uploading Data</h3>
              <p className="mb-4">
                To get started, you need to upload a CSV file:
              </p>
              <ol className="list-decimal pl-6 mb-4 space-y-2">
                <li>Click the <code className="bg-gray-200 px-1 rounded">Upload CSV</code> button in the navigation bar.</li>
                <li>Select a CSV file from your computer.</li>
                <li>Optionally, you can provide a custom name for your dataset in the "Dataset name" field.</li>
                <li>After uploading, your data will be displayed in the main window, showing the first 10 rows.</li>
              </ol>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Switching Engines</h3>
              <p className="mb-4">
                You can choose between three different data processing engines:
              </p>
              <ul className="list-disc pl-6 mb-4 space-y-2">
                <li>
                  <strong>Pandas:</strong> The most popular Python data analysis library. Great for beginners and widely used in data science.
                </li>
                <li>
                  <strong>Polars:</strong> A high-performance DataFrame library written in Rust. Often 5-10× faster than Pandas for large datasets.
                </li>
                <li>
                  <strong>SQL:</strong> Uses SQL-like syntax for querying data, powered by DuckDB. Familiar to those with database experience.
                </li>
              </ul>
              <p>
                Select your preferred engine from the dropdown in the navigation bar. The code displayed for operations will change based on your selection.
              </p>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Operations vs Code Mode</h3>
              <p className="mb-4">
                The application offers two ways to work with your data:
              </p>
              <ul className="list-disc pl-6 mb-4 space-y-2">
                <li>
                  <strong>Operations Panel:</strong> Use the GUI to perform common operations like filtering, grouping, and sorting.
                  Each operation will generate code that you can see and learn from.
                </li>
                <li>
                  <strong>Code Editor:</strong> Write and execute code directly using Pandas, Polars, or SQL syntax.
                  This is great for more advanced operations or custom logic.
                </li>
              </ul>
              <p>
                Switch between these modes using the "Switch to..." button at the top of the screen.
              </p>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Saving and Exporting</h3>
              <p className="mb-4">
                After transforming your data, you can:
              </p>
              <ul className="list-disc pl-6 mb-4 space-y-2">
                <li>
                  <strong>Save As:</strong> Save the transformed dataset with a new name to continue working with it later.
                </li>
                <li>
                  <strong>Export:</strong> Download your data as CSV, JSON, or Excel file for use in other applications.
                </li>
              </ul>
            </section>
          </div>
        )}

        {activeTab === 'operations' && (
          <div>
            <h2 className="text-2xl font-bold mb-4">Data Operations Guide</h2>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Basic Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Filter Rows</h4>
                <p className="mb-2">
                  Filter data based on column values. You can use various operators like equals, greater than, contains, etc.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Find all rows where "Age" is greater than 30</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Age" as the column</li>
                    <li>Choose "&gt" as the operator</li>
                    <li>Enter "30" as the value</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Select Columns</h4>
                <p className="mb-2">
                  Keep only specific columns in your dataset.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Keep only "Name", "Age", and "Salary" columns</p>
                  <ul className="list-disc pl-6">
                    <li>Check the boxes for the columns you want to keep</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Sort Values</h4>
                <p className="mb-2">
                  Sort your data by one column in ascending or descending order.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Sort by "Salary" in descending order</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Salary" as the sort column</li>
                    <li>Choose "Descending" as the sort order</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Rename Columns</h4>
                <p className="mb-2">
                  Change column names for clarity or consistency.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Rename "fname" to "First Name"</p>
                  <ul className="list-disc pl-6">
                    <li>Add a column rename entry</li>
                    <li>Select "fname" as the old name</li>
                    <li>Enter "First Name" as the new name</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Drop Columns</h4>
                <p className="mb-2">
                  Remove unwanted columns from your dataset.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Remove "ID" and "Notes" columns</p>
                  <ul className="list-disc pl-6">
                    <li>Check the boxes for the columns you want to remove</li>
                  </ul>
                </div>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Aggregation Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Group By (Single Column)</h4>
                <p className="mb-2">
                  Group data by one column and calculate aggregate statistics.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Calculate average salary by department</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Department" as the group column</li>
                    <li>Select "Salary" as the aggregation column</li>
                    <li>Choose "Mean" as the aggregation function</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Group By (Multiple Columns)</h4>
                <p className="mb-2">
                  Group data by multiple columns for more detailed analysis.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Calculate average salary by department and job title</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Department" and "Job Title" as the group columns</li>
                    <li>Select "Salary" as the aggregation column</li>
                    <li>Choose "Mean" as the aggregation function</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Group By with Multiple Aggregations</h4>
                <p className="mb-2">
                  Calculate multiple aggregate statistics in one operation.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Calculate min, max, and average salary by department</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Department" as the group column</li>
                    <li>Add three aggregations for "Salary": Min, Max, and Mean</li>
                  </ul>
                </div>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Reshaping Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Pivot Table</h4>
                <p className="mb-2">
                  Create a spreadsheet-like pivot table to summarize data.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Create a pivot table showing average sales by product and region</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Product" as the index column (rows)</li>
                    <li>Select "Region" as the column</li>
                    <li>Select "Sales" as the values column</li>
                    <li>Choose "Mean" as the aggregation function</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Melt (Wide to Long)</h4>
                <p className="mb-2">
                  Transform data from wide format to long format (unpivot).
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Convert quarterly sales columns to a single "quarter" column and "sales" value</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Product" and "Year" as ID variables (columns to keep)</li>
                    <li>Select "Q1_Sales", "Q2_Sales", "Q3_Sales", "Q4_Sales" as value variables</li>
                    <li>Enter "Quarter" for variable name</li>
                    <li>Enter "Sales" for value name</li>
                  </ul>
                </div>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Index Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Set Index</h4>
                <p className="mb-2">
                  Set a column as the DataFrame index.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Set "Employee_ID" as the index</p>
                  <ul className="list-disc pl-6">
                    <li>Select "Employee_ID" as the index column</li>
                    <li>Choose whether to drop the column from the DataFrame</li>
                  </ul>
                </div>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Reset Index</h4>
                <p className="mb-2">
                  Reset the index back to default numeric index.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Convert the current index back to a regular column</p>
                  <ul className="list-disc pl-6">
                    <li>Choose whether to drop the index (don't keep as a column)</li>
                  </ul>
                </div>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Joining & Merging</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Merge Datasets</h4>
                <p className="mb-2">
                  Combine two datasets based on common columns.
                </p>
                <div className="bg-gray-100 p-3 rounded">
                  <p className="mb-1"><strong>Example:</strong> Merge employee data with department data</p>
                  <ul className="list-disc pl-6">
                    <li>Select the second dataset to merge with</li>
                    <li>Choose the join type (inner, left, right, outer)</li>
                    <li>Specify the key columns to join on</li>
                  </ul>
                </div>
              </div>
            </section>
          </div>
        )}

        {activeTab === 'pandas' && (
          <div>
            <h2 className="text-2xl font-bold mb-4">Pandas Syntax Guide</h2>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Basic Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Filter Rows</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Filter by one condition
df = df[df['column_name'] > value]

# Filter by multiple conditions (AND)
df = df[(df['column1'] > value1) & (df['column2'] == value2)]

# Filter by multiple conditions (OR)
df = df[(df['column1'] > value1) | (df['column2'] == value2)]

# String contains
df = df[df['column'].str.contains('pattern')]

# Regex pattern matching
df = df[df['column'].str.contains('pattern', regex=True)]`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Select Columns</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Select single column (returns Series)
df['column_name']

# Select multiple columns (returns DataFrame)
df[['column1', 'column2', 'column3']]`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Sort Values</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Sort by one column (ascending)
df = df.sort_values('column_name')

# Sort by one column (descending)
df = df.sort_values('column_name', ascending=False)

# Sort by multiple columns
df = df.sort_values(['column1', 'column2'], ascending=[True, False])`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Aggregation Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Group By</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Group by one column with one aggregation
df = df.groupby('group_col')['value_col'].mean().reset_index()

# Group by multiple columns
df = df.groupby(['group_col1', 'group_col2'])['value_col'].mean().reset_index()

# Multiple aggregations
df = df.groupby('group_col').agg({
    'num_col1': 'mean',
    'num_col2': 'sum',
    'str_col': 'count'
}).reset_index()

# Named aggregations (pandas >= 0.25)
df = df.groupby('group_col').agg(
    mean_val=('num_col', 'mean'),
    sum_val=('num_col', 'sum'),
    count_val=('str_col', 'count')
).reset_index()`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Reshaping Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Pivot Tables</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Basic pivot table
df_pivot = pd.pivot_table(
    df,
    index='row_col',
    columns='col_col',
    values='value_col',
    aggfunc='mean'
)

# Multiple value columns
df_pivot = pd.pivot_table(
    df,
    index=['row_col1', 'row_col2'],
    columns='col_col',
    values=['value_col1', 'value_col2'],
    aggfunc={'value_col1': 'mean', 'value_col2': 'sum'}
)`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Melting (Wide to Long)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Basic melt
df_melted = pd.melt(
    df,
    id_vars=['id_col1', 'id_col2'],  # columns to keep
    value_vars=['val_col1', 'val_col2', 'val_col3'],  # columns to unpivot
    var_name='variable',  # name for the column containing former column names
    value_name='value'  # name for the column containing values
)`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Joining & Merging</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Merging DataFrames</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Inner join
df_merged = pd.merge(
    left_df,
    right_df,
    how='inner',
    left_on='left_key_col',
    right_on='right_key_col'
)

# Left join
df_merged = pd.merge(
    left_df,
    right_df,
    how='left',
    left_on='left_key_col',
    right_on='right_key_col'
)

# Right join
df_merged = pd.merge(
    left_df,
    right_df,
    how='right',
    left_on='left_key_col',
    right_on='right_key_col'
)

# Outer join
df_merged = pd.merge(
    left_df,
    right_df,
    how='outer',
    left_on='left_key_col',
    right_on='right_key_col'
)

# Join on index
df_merged = pd.merge(
    left_df,
    right_df,
    left_index=True,
    right_index=True
)`}
                  </code>
                </pre>
              </div>
            </section>
          </div>
        )}

        {activeTab === 'polars' && (
          <div>
            <h2 className="text-2xl font-bold mb-4">Polars Syntax Guide</h2>

            <p className="mb-4">
              Polars is a high-performance DataFrame library similar to pandas, but with different syntax and often better performance.
            </p>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Basic Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Filter Rows</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Filter by one condition
df = df.filter(pl.col('column_name') > value)

# Filter by multiple conditions (AND)
df = df.filter((pl.col('column1') > value1) & (pl.col('column2') == value2))

# Filter by multiple conditions (OR)
df = df.filter((pl.col('column1') > value1) | (pl.col('column2') == value2))

# String contains
df = df.filter(pl.col('column').str.contains('pattern'))

# Regex pattern matching
df = df.filter(pl.col('column').str.contains('pattern', literal=False))`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Select Columns</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Select columns
df = df.select(['column1', 'column2', 'column3'])

# Select with expressions
df = df.select([
    pl.col('column1'),
    pl.col('column2'),
    (pl.col('column3') * 2).alias('column3_doubled')
])`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Sort Values</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Sort by one column (ascending)
df = df.sort('column_name')

# Sort by one column (descending)
df = df.sort('column_name', descending=True)

# Sort by multiple columns
df = df.sort(['column1', 'column2'], descending=[False, True])`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Aggregation Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Group By</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Group by one column with one aggregation
df = df.group_by('group_col').agg(pl.col('value_col').mean())

# Group by multiple columns
df = df.group_by(['group_col1', 'group_col2']).agg(pl.col('value_col').mean())

# Multiple aggregations
df = df.group_by('group_col').agg([
    pl.col('num_col1').mean(),
    pl.col('num_col2').sum(),
    pl.col('str_col').count()
])`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Reshaping Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Pivot</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Basic pivot
df_pivot = df.pivot(
    index='row_col',
    columns='col_col',
    values='value_col',
    aggregate_function='mean'
)`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Melt</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Basic melt
df_melted = df.melt(
    id_vars=['id_col1', 'id_col2'],
    value_vars=['val_col1', 'val_col2', 'val_col3'],
    variable_name='variable',
    value_name='value'
)`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Joining</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Joining DataFrames</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Inner join
df_joined = df_left.join(
    df_right,
    left_on='left_key_col',
    right_on='right_key_col',
    how='inner'
)

# Left join
df_joined = df_left.join(
    df_right,
    left_on='left_key_col',
    right_on='right_key_col',
    how='left'
)

# Right join (Note: Polars might not directly support 'right' in older versions, simulate with left join swapped)
# Or use newer syntax if available:
df_joined = df_left.join(
    df_right,
    left_on='left_key_col',
    right_on='right_key_col',
    how='right' # Check Polars documentation for current support
)
# Alternative for older versions: df_right.join(df_left, ...)

# Outer join
df_joined = df_left.join(
    df_right,
    left_on='left_key_col',
    right_on='right_key_col',
    how='outer'
)`}
                  </code>
                </pre>
              </div>
            </section>
          </div>
        )}

        {activeTab === 'sql' && (
          <div>
            <h2 className="text-2xl font-bold mb-4">SQL Syntax Guide (DuckDB)</h2>

            <p className="mb-4">
              The SQL mode uses DuckDB, which supports most standard SQL and has powerful analytical features. Use <code className="font-mono bg-gray-200 px-1 rounded">table_name</code> to refer to your current dataset.
            </p>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Basic Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Filtering (WHERE)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Filter by one condition
SELECT * FROM table_name
WHERE column_name > value

-- Filter by multiple conditions (AND)
SELECT * FROM table_name
WHERE column1 > value1 AND column2 = value2

-- Filter by multiple conditions (OR)
SELECT * FROM table_name
WHERE column1 > value1 OR column2 = value2

-- String contains (LIKE)
SELECT * FROM table_name
WHERE column_name LIKE '%pattern%'

-- Regex pattern matching (DuckDB specific)
SELECT * FROM table_name
WHERE REGEXP_MATCHES(column_name, 'pattern')`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Select Columns</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Select specific columns
SELECT column1, column2, column3 FROM table_name

-- Column expressions
SELECT
  column1,
  column2,
  column3 * 2 AS column3_doubled
FROM table_name`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Sort (ORDER BY)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Sort by one column (ascending)
SELECT * FROM table_name
ORDER BY column_name ASC

-- Sort by one column (descending)
SELECT * FROM table_name
ORDER BY column_name DESC

-- Sort by multiple columns
SELECT * FROM table_name
ORDER BY column1 ASC, column2 DESC`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Aggregation Operations</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Group By (GROUP BY)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Group by one column with one aggregation
SELECT group_col, AVG(value_col) AS average
FROM table_name
GROUP BY group_col

-- Group by multiple columns
SELECT group_col1, group_col2, AVG(value_col) AS average
FROM table_name
GROUP BY group_col1, group_col2

-- Multiple aggregations
SELECT
  group_col,
  AVG(num_col1) AS average,
  SUM(num_col2) AS total,
  COUNT(str_col) AS count
FROM table_name
GROUP BY group_col

-- Having clause (filter groups)
SELECT group_col, AVG(value_col) AS average
FROM table_name
GROUP BY group_col
HAVING AVG(value_col) > 100`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Joining Tables</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Joins</h4>
                <p className="mb-2">Assume you have another table named <code className="font-mono bg-gray-200 px-1 rounded">right_table</code> loaded.</p>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Inner join
SELECT *
FROM table_name -- This is your left_table
INNER JOIN right_table ON table_name.key_col = right_table.key_col

-- Left join
SELECT *
FROM table_name
LEFT JOIN right_table ON table_name.key_col = right_table.key_col

-- Right join
SELECT *
FROM table_name
RIGHT JOIN right_table ON table_name.key_col = right_table.key_col

-- Full outer join
SELECT *
FROM table_name
FULL OUTER JOIN right_table ON table_name.key_col = right_table.key_col`}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Advanced Features</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Pivot (DuckDB specific)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Pivot table example
SELECT * FROM PIVOT (
  SELECT row_col, pivot_col, value_col
  FROM table_name
)
ON pivot_col -- Column whose distinct values become new columns
USING AVG(value_col) -- Aggregation function for values
GROUP BY row_col -- Columns to keep as rows
`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Common Table Expressions (CTE)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Using a CTE
WITH filtered_data AS (
  SELECT *
  FROM table_name
  WHERE value > 100
),
summarized_data AS (
  SELECT
    category,
    AVG(value) AS avg_value
  FROM filtered_data
  GROUP BY category
)
SELECT *
FROM summarized_data
ORDER BY avg_value DESC`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Window Functions</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`-- Rank by value within groups
SELECT
  *,
  RANK() OVER (PARTITION BY category ORDER BY value DESC) AS rank_in_category
FROM table_name

-- Running total
SELECT
  *,
  SUM(value) OVER (PARTITION BY category ORDER BY date_column) AS running_total
FROM table_name

-- Moving average (3-period)
SELECT
  *,
  AVG(value) OVER (
    PARTITION BY category
    ORDER BY date_column
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS moving_avg_3_period
FROM table_name`}
                  </code>
                </pre>
              </div>
            </section>
          </div>
        )}

        {activeTab === 'regex' && (
          <div>
            <h2 className="text-2xl font-bold mb-4">Regular Expression Guide</h2>

            <p className="mb-4">
              Regular expressions (regex) are powerful patterns for matching text. They can be used in filtering, extraction, and replacement operations. Syntax might vary slightly between engines (Python/Pandas vs. DuckDB SQL).
            </p>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Regex Basics</h3>

              <div className="overflow-x-auto">
                <table className="min-w-full bg-white border">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border px-4 py-2 text-left">Pattern</th>
                      <th className="border px-4 py-2 text-left">Description</th>
                      <th className="border px-4 py-2 text-left">Example</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border px-4 py-2 font-mono">.</td>
                      <td className="border px-4 py-2">Any single character (except newline)</td>
                      <td className="border px-4 py-2"><code className="font-mono">a.c</code> matches "abc", "adc", "a3c"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">^</td>
                      <td className="border px-4 py-2">Start of string (or line in multiline mode)</td>
                      <td className="border px-4 py-2"><code className="font-mono">^abc</code> matches "abc" only at the start</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">$</td>
                      <td className="border px-4 py-2">End of string (or line in multiline mode)</td>
                      <td className="border px-4 py-2"><code className="font-mono">abc$</code> matches "abc" only at the end</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">*</td>
                      <td className="border px-4 py-2">0 or more occurrences of the preceding element</td>
                      <td className="border px-4 py-2"><code className="font-mono">ab*c</code> matches "ac", "abc", "abbc"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">+</td>
                      <td className="border px-4 py-2">1 or more occurrences of the preceding element</td>
                      <td className="border px-4 py-2"><code className="font-mono">ab+c</code> matches "abc", "abbc", but not "ac"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">?</td>
                      <td className="border px-4 py-2">0 or 1 occurrence of the preceding element</td>
                      <td className="border px-4 py-2"><code className="font-mono">ab?c</code> matches "ac", "abc", but not "abbc"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">\d</td>
                      <td className="border px-4 py-2">Any digit (0-9)</td>
                      <td className="border px-4 py-2"><code className="font-mono">\d{3}</code> matches "123", "987"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">\D</td>
                      <td className="border px-4 py-2">Any non-digit</td>
                      <td className="border px-4 py-2"><code className="font-mono">\D+</code> matches "abc", " "</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">\w</td>
                      <td className="border px-4 py-2">Any word character (alphanumeric + underscore)</td>
                      <td className="border px-4 py-2"><code className="font-mono">\w+</code> matches "word", "num1", "under_score"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">\W</td>
                      <td className="border px-4 py-2">Any non-word character</td>
                      <td className="border px-4 py-2"><code className="font-mono">\W</code> matches " ", "!", "@"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">\s</td>
                      <td className="border px-4 py-2">Any whitespace character (space, tab, newline)</td>
                      <td className="border px-4 py-2"><code className="font-mono">a\sb</code> matches "a b", "a\tb"</td>
                    </tr>
                     <tr>
                      <td className="border px-4 py-2 font-mono">\S</td>
                      <td className="border px-4 py-2">Any non-whitespace character</td>
                      <td className="border px-4 py-2"><code className="font-mono">\S+</code> matches "word", "123"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">[...]</td>
                      <td className="border px-4 py-2">Any single character specified in the set</td>
                      <td className="border px-4 py-2"><code className="font-mono">[aeiou]</code> matches any vowel</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">[^...]</td>
                      <td className="border px-4 py-2">Any single character NOT specified in the set</td>
                      <td className="border px-4 py-2"><code className="font-mono">[^aeiou]</code> matches any non-vowel</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">(...)</td>
                      <td className="border px-4 py-2">Group patterns and capture the matched text</td>
                      <td className="border px-4 py-2"><code className="font-mono">(abc)+</code> matches "abc", "abcabc"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">|</td>
                      <td className="border px-4 py-2">Alternative (OR) operator</td>
                      <td className="border px-4 py-2"><code className="font-mono">cat|dog</code> matches "cat" or "dog"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">{`{n}`}</td>
                      <td className="border px-4 py-2">Exactly n occurrences</td>
                      <td className="border px-4 py-2"><code className="font-mono">a{`{3}`}</code> matches "aaa"</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">{`{n,}`}</td>
                      <td className="border px-4 py-2">n or more occurrences</td>
                      <td className="border px-4 py-2"><code className="font-mono">a{`{2,}`}</code> matches "aa", "aaa", etc.</td>
                    </tr>
                    <tr>
                      <td className="border px-4 py-2 font-mono">{`{n,m}`}</td>
                      <td className="border px-4 py-2">Between n and m occurrences (inclusive)</td>
                      <td className="border px-4 py-2"><code className="font-mono">a{`{2,4}`}</code> matches "aa", "aaa", "aaaa"</td>
                    </tr>
                     <tr>
                      <td className="border px-4 py-2 font-mono">\</td>
                      <td className="border px-4 py-2">Escape special characters</td>
                      <td className="border px-4 py-2"><code className="font-mono">\.</code> matches a literal period "."</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Common Regex Patterns</h3>
              <p className="mb-2 text-sm text-gray-600">Note: Backslashes may need to be escaped (e.g., `\\d` instead of `\d`) when used inside string literals in code.</p>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Email Address</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}

# Matches common email formats:
# - user@example.com
# - first.last@company-name.co.uk`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Phone Number (North American style)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`\\+?1?[\\s.-]?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}

# Matches formats like:
# - 123-456-7890
# - (123) 456-7890
# - 123.456.7890
# - 123 456 7890
# - +1 (123) 456-7890`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Date (YYYY-MM-DD)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`(19|20)\\d{2}[-/](0[1-9]|1[0-2])[-/](0[1-9]|[12]\\d|3[01])

# Matches formats like:
# - 2023-01-31
# - 1999/12/01`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">URL</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`https?://(?:www\\.)?[\\w\\d\\-]+(?:\\.[\\w\\d\\-]+)+[\\w\\d\\-._~:/?#[\\]@!$&'()*+,;=]*

# Matches common URLs:
# - http://example.com
# - https://www.example.com/path?query=value#fragment`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">IP Address (IPv4)</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b

# Matches patterns like:
# - 192.168.1.1
# - 10.0.0.255
# (Note: Doesn't validate ranges like 0-255 per octet) `}
                  </code>
                </pre>
              </div>
            </section>

            <section className="mb-8">
              <h3 className="text-xl font-semibold mb-2">Regex in Data Analysis</h3>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Extracting Numbers</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Extract integers or decimals
\\d+(\\.\\d+)?

# Example Use (Pandas):
# df['numeric_part'] = df['text_column'].str.extract(r'(\\d+(\\.\\d+)?)')

# Example Use (SQL/DuckDB):
# SELECT regexp_extract(text_column, '\\d+(\\.\\d+)?', 1) FROM table_name`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Extracting Text Between Markers</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Extract text between square brackets []
(?<=\\[).+?(?=\\])

# Example Use (Pandas):
# df['extracted'] = df['log_entry'].str.extract(r'\\[(.*?)\\]') # Capture group ()

# Example Use (SQL/DuckDB):
# SELECT regexp_extract(log_entry, '\\[(.*?)\\]', 1) FROM table_name`}
                  </code>
                </pre>
              </div>

              <div className="mb-6">
                <h4 className="text-lg font-medium mb-1">Cleaning and Standardizing</h4>
                <pre className="bg-gray-100 p-3 rounded overflow-x-auto">
                  <code>
{`# Replace multiple spaces with a single space
\\s+

# Example Use (Pandas):
# df['clean_text'] = df['text'].str.replace(r'\\s+', ' ', regex=True)

# Example Use (SQL/DuckDB):
# SELECT regexp_replace(text_column, '\\s+', ' ', 'g') FROM table_name

# Remove non-alphanumeric characters (keep spaces)
[^a-zA-Z0-9\\s]

# Example Use (Pandas):
# df['alphanum_text'] = df['text'].str.replace(r'[^a-zA-Z0-9\\s]', '', regex=True)

# Example Use (SQL/DuckDB):
# SELECT regexp_replace(text_column, '[^a-zA-Z0-9\\s]', '', 'g') FROM table_name`}
                  </code>
                </pre>
              </div>
            </section>
          </div>
        )}
      </div>
    </div>
  );
};

export default Documentation;