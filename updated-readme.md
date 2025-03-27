# Data Analysis GUI

A graphical interface tool for data analysis operations using pandas, polars, and SQL queries.

## Overview

This project provides an intuitive GUI for data analysis operations, allowing users (especially students and beginners) to work with datasets without needing to write code. It generates equivalent pandas, polars, or SQL code for every operation, helping users learn data manipulation syntax through practical examples.

Think of it as a "Symbolab for pandas" – just as Symbolab provides step-by-step solutions for math problems, this application provides a visual approach to data analysis tasks with the code shown for each operation.

## Features

- **Upload and analyze CSV files** locally (no server-side storage)
- **Perform data operations** through a user-friendly interface:
  - Filter rows based on column values
  - Group by columns and apply aggregation functions
  - Rename columns for clarity
- **Multiple execution engines**:
  - Pandas: Standard Python data analysis library
  - Polars: High-performance DataFrame library written in Rust (5-10× faster than pandas)
  - SQL: SQL-like queries powered by DuckDB
- **Real-time code generation** showing the equivalent code for each operation
- **Interactive data preview** with pagination for larger datasets

## Getting Started

### Prerequisites

- Python 3.9+ (for the backend)
- Node.js 18+ (for the frontend)
- npm (Node.js package manager)

### Installation

#### Mac Installation (One-Click Setup)

For Mac users, we provide a simple installation script:

1. Download the repository
2. Open Terminal and navigate to the project directory
3. Run the following command to make the installer executable:
   ```bash
   chmod +x mac_install_frontend.sh
   ```
4. Execute the installer:
   ```bash
   ./mac_install_frontend.sh
   ```
5. The script will install all dependencies and start the frontend application

#### Manual Installation

##### Backend Setup:
```bash
# Create and activate virtual environment
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start the server
cd app
uvicorn main:app --reload
```

##### Frontend Setup:
```bash
# Install dependencies
cd frontend
npm install

# Start the development server
npm start
```

## Project Structure

```
data-analysis-gui/
├── backend/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── operations/
│   │   │   ├── __init__.py
│   │   │   ├── filter.py
│   │   │   ├── groupby.py
│   │   │   └── rename.py
│   │   └── services/
│   │       ├── __init__.py
│   │       ├── pandas_service.py
│   │       ├── polars_service.py
│   │       └── sql_service.py
│   └── requirements.txt
└── frontend/
    ├── public/
    │   └── index.html
    ├── src/
    │   ├── components/
    │   │   ├── DataTable.jsx
    │   │   ├── NavBar.jsx
    │   │   ├── OperationsPanel.jsx
    │   │   └── CodeDisplay.jsx
    │   ├── services/
    │   │   └── api.js
    │   ├── App.jsx
    │   └── index.jsx
    └── package.json
```

## Usage

1. **Upload a CSV file**: Click the "Upload CSV" button in the navbar and select your file
2. **View the data**: The uploaded data will be displayed in a table
3. **Select an operation**: Choose from Filter, Group By, or Rename Column
4. **Configure the operation**: Fill in the required parameters
5. **Apply the operation**: Click "Apply" to execute
6. **View the results**: See the updated data table and the generated code
7. **Switch engines**: Select Pandas, Polars, or SQL to see different code implementations

## Technologies Used

- **Backend**:
  - FastAPI: Modern Python web framework
  - Pandas: Data analysis library
  - Polars: High-performance DataFrame library
  - DuckDB: In-process SQL database engine

- **Frontend**:
  - React: UI library
  - Tailwind CSS: Utility-first CSS framework
  - Axios: HTTP client
  - Chart.js: Visualization library

## Troubleshooting

- **Backend Connection Issues**: Ensure the backend is running on port 8000 and that your browser allows connections to localhost
- **File Upload Errors**: Check that your CSV file is properly formatted
- **Operation Errors**: Verify the data types in your operation parameters match your data (e.g., numeric comparisons for numeric columns)

## Future Features

- Natural language query interpretation using LLMs
- Smart operation recommendations based on dataset characteristics
- Additional visualizations (scatter plots, box plots, etc.)
- Support for more file formats (Excel, JSON, Parquet)
- Additional operations (join/merge, pivot tables, sorting)

## Contributing

Contributions are welcome! Feel free to submit issues or pull requests if you have ideas for improvements or have found bugs.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
