# DataMaid ✨🧹 - Interactive GUI for Data Analysis & Transformation

## Overview

DataMaid is an intuitive **Graphical User Interface (GUI)** web application designed for interactive data cleaning, transformation, and analysis using popular Python libraries and SQL. It helps students and beginners work with datasets without needing extensive coding knowledge, while still providing the underlying code for learning purposes. Users can upload various data sources, apply common operations via a visual interface or custom code, choose execution engines (Pandas, Polars, SQL), and manage multiple datasets simultaneously. The primary goal is to make data wrangling accessible and educational, helping users learn **pandas/polars syntax and SQL** by example.

## Core Features

*   **Multi-Dataset Support:** Load, manage, and operate on multiple datasets simultaneously within the application state.
*   **Multiple Data Sources:**
    *   Upload CSV files.
    *   Paste raw text data (CSV or JSON records format).
    *   Upload SQLite or DuckDB database files and import specific tables as datasets.
*   **Multiple Execution Engines:** Choose the best engine for the task:
    *   **Pandas Mode:** Use the familiar and widely taught pandas library for operations.
    *   **Polars Mode:** Leverage the high-performance Polars library (Rust-based) for potentially significant speedups (often **5–10×+ faster** than pandas).
    *   **SQL Mode (DuckDB):** Execute SQL queries directly on datasets using the fast, in-process DuckDB engine. Structured operations in SQL mode build a chain of Common Table Expressions (CTEs) for lineage tracking.
*   **Interactive Data Table:**
    *   View paginated previews of datasets.
    *   Optionally view the full loaded data subset.
    *   Click headers for quick column statistics.
*   **Structured Operations Panel:** Apply common data transformations via a UI form without writing code:
    *   Includes Filter, Sort, Select/Drop Columns, Rename, Group By (with multiple aggregations), Fill/Drop NA, Type Casting, String/Date operations, **Sample**, **Shuffle**, Lambda Functions (Pandas/SQL - *with security considerations*), Merge/Join, and more.
    *   Choose between Pandas or SQL engines for executing these structured operations.
*   **Code Editor:**
    *   Execute custom Python code using Pandas or Polars syntax.
    *   Execute custom SQL code using DuckDB.
    *   Access all loaded datasets within the code execution context using sanitized variable names.
*   **Relational Algebra Builder:**
    *   Visually construct relational algebra operations (Select, Project, Rename, Join, Union, etc.).
    *   Preview results step-by-step.
    *   Save the final result as a new dataset in the application state.
*   **Code Generation and Display:** For structured operations, the tool displays the equivalent Pandas code or SQL CTE definition, aiding learning. Custom code is executed as written.
*   **Dataset Information & Statistics:**
    *   View overall dataset info (rows, columns, memory usage, data types).
    *   View detailed statistics for individual columns.
*   **State Management:**
    *   Basic undo functionality for the last operation applied to a dataset.
    *   Ability to clear the operation history for a dataset.
*   **Export:** Download the current state of any dataset as a CSV, JSON, or Excel file.
*   **Dataset Management:** Rename or delete datasets from the application state.
*   **Local-First:** Primarily designed for local execution. Data uploaded is processed in backend memory and not sent to external servers, ensuring privacy.

## Execution Modes in Detail

-   **Pandas Mode:** Utilizes the standard Python data analysis library. Great for compatibility and learning the most common syntax taught in courses.
-   **Polars Mode:** Employs the Polars library for high performance, especially on larger datasets or complex queries. Operations are translated to Polars expressions. Code generation shows Polars syntax.
-   **SQL Mode:** Leverages DuckDB.
    *   **Structured Operations:** UI actions are translated into SQL queries, typically building a chain of Common Table Expressions (CTEs) like `WITH step0 AS (...), step1 AS (...) SELECT * FROM step1`. This allows tracking lineage for UI-driven SQL transformations.
    *   **Custom Code:** Users can write and execute arbitrary SQL queries (SELECT, CREATE TABLE AS, etc.) that operate on the loaded datasets, which are registered as temporary tables in DuckDB.

## Lambda Operations Security

The "Apply Lambda Function" operation available for Pandas and SQL engines in the UI panel currently uses `eval()` (Pandas backend) or limited string translation (SQL backend) to execute user-provided lambda strings.

**⚠️ SECURITY WARNING:** Using `eval()` on arbitrary user input is inherently insecure in web applications exposed to untrusted users. It allows for potential remote code execution. This feature is included for demonstration and educational purposes in a controlled environment. **Do not deploy DataMaid with the current `eval()` implementation to an untrusted network or the public internet without replacing `eval()` with a secure sandboxing mechanism (e.g., `asteval` library, restricted execution environments).** The SQL engine's lambda translation is also basic and should be used with caution.

## Future Features (Planned)

Beyond the current feature set, the project has an exciting roadmap to incorporate AI assistance and smarter functionalities:

- **Natural Language Problem Solver:** Leverage Large Language Models (LLMs) to interpret descriptive problem statements (e.g., a homework question) and suggest or automatically generate the sequence of GUI operations or code (pandas/polars/SQL) needed to solve it.
- **AI-Assisted Intent Recognition (Agentic Protocol):** Integrate with local or free LLMs (e.g., Mistral 7B, DeepSeek) to create an agent that assists in multi-step analysis based on high-level user requests (e.g., "Clean this dataset and plot salaries by country").
- **Smart Function Recommendations:** Analyze dataset shape and user actions to suggest relevant next steps or operations (e.g., "Suggest Group By on categorical column X," "Suggest Merge based on common column Y").
- **Enhanced Visualization & Analysis:** Include more chart types (scatter plots, box plots), advanced data cleaning utilities (outlier detection), and potentially basic statistical tests or ML model integration via the GUI.
- **Collaboration and Cloud Features (Optional):** Explore options for user accounts, cloud storage (with privacy considerations), and sharing analysis workflows, while maintaining the core local-first approach.

## Tech Stack

-   **Frontend:** [React](https://reactjs.org/) (JavaScript) with [Tailwind CSS](https://tailwindcss.com/) for a responsive and modern UI. Components like Monaco Editor are used for the code editor.
-   **Backend API:** [FastAPI](https://fastapi.tiangolo.com/) (Python) providing high performance, asynchronous support, and easy API definition for data operations.
-   **Data Processing Engines:**
    *   **pandas:** Core Python library for data manipulation.
    *   **Polars:** High-performance Rust-based DataFrame library via Python bindings.
    *   **DuckDB:** Fast, in-process analytical data management system used for SQL execution (structured operations and custom code) and the Relational Algebra builder.
-   **State Management:** Backend uses Python dictionaries held in memory (`datasets_state`) to manage multiple datasets and their history. Dataset content is typically stored as CSV bytes. Temporary storage is used for uploaded DB files.
-   **LLM Integration (Future):** Designed to be LLM-agnostic, potentially connecting to local models (Mistral, DeepSeek) or APIs (OpenAI, Anthropic) via the backend. Emphasis on local/open models for accessibility.

## Design Philosophy

-   **Education-First Approach:** Show the corresponding code (Pandas, Polars, SQL CTEs) for GUI actions to help users learn the syntax.
-   **Intuitive GUI Interactions:** Favor point-and-click, forms, and visual builders (like the RA panel) over manual coding for common tasks.
-   **Instant Feedback and Visualization:** Provide immediate previews of data transformations and results (table updates, stats, RA preview).
-   **Performance and Responsiveness:** Utilize efficient backends (FastAPI, Polars, DuckDB) to keep the UI snappy, even with moderately sized data.
-   **Engine Flexibility:** Allow users to choose the right tool (Pandas' ease, Polars' speed, SQL's expressiveness) for their task.
-   **“Simplify, Don’t Dumb Down”:** Offer a user-friendly interface for common operations but provide the Code Editor for advanced/custom logic, allowing users to grow.
-   **Multi-Dataset Workflow:** Natively support loading and working with multiple related datasets.
-   **Inspired by Successful Tools:** Takes cues from database GUIs, interactive notebooks (Jupyter), BI tools (Tableau/Power BI - for UI ideas), and educational tools.

Ultimately, the philosophy is to **empower users**: remove initial barriers, provide a sandbox for experimentation, facilitate learning through code visibility, and support increasingly complex analyses.

## Getting Started

Follow these steps to set up DataMaid on your local machine:

1.  **Prerequisites:**
    *   Python 3.9+
    *   Node.js 18+ and npm (or yarn)
2.  **Clone Repository:** `git clone <repository_url>` and `cd <repository_directory>`
3.  **Backend Setup (FastAPI):**
    *   `cd backend`
    *   Create/activate a virtual environment:
        *   `python -m venv venv`
        *   Windows: `.\venv\Scripts\activate`
        *   macOS/Linux: `source venv/bin/activate`
    *   Install dependencies: `pip install -r requirements.txt`
    *   Run the server: `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`
4.  **Frontend Setup (React):**
    *   Navigate to the frontend directory: `cd ../frontend`
    *   Install dependencies: `npm install` (or `yarn install`)
    *   Start the development server: `npm start` (or `yarn start`)
5.  **Using the App:**
    *   Open your browser to `http://localhost:3000` (or the port specified by React).
    *   Upload data (CSV, Text, DB file) or paste text.
    *   Select a dataset from the NavBar dropdown.
    *   Choose an execution engine (Pandas, Polars, SQL) from the NavBar.
    *   Use the Operations Panel, Code Editor, or Relational Algebra panel to manipulate the data.
    *   Observe results in the Data Table and generated code/snippets where applicable.
6.  **Stopping the App:** Press `Ctrl+C` in the backend and frontend terminal windows. Deactivate the virtual environment (`deactivate`).

## Contributing

Contributions are welcome! Please refer to the issue tracker on GitHub. Some ways to help:

-   **Feedback & Bug Reports:** Use GitHub Issues.
-   **Feature Suggestions:** Propose new operations, UI improvements, etc.
-   **Code Contributions:** Fork the repo, create a branch, and submit a Pull Request (ensure code is documented and tested). Look for "help wanted" or "good first issue" labels.
-   **Documentation:** Improve this README, add tutorials, or clarify help text.

Please adhere to the project’s code of conduct (if available).

## Project Status and Future Outlook

DataMaid is under active development. The current version provides a robust platform for multi-dataset interaction, offering core data manipulation tasks across Pandas, Polars, and SQL engines via both a structured UI and custom code execution. The Relational Algebra builder offers a visual way to construct queries.

Near-term focus includes polishing the UI/UX, enhancing error handling, potentially adding more visualization options, and refining the engine parity for structured operations.

Long-term goals include integrating the AI-powered features (Natural Language Solver, Agentic Assistance, Smart Recommendations) outlined in the roadmap, further expanding analysis capabilities, and potentially exploring more robust state persistence options.

Stay tuned for updates! We hope DataMaid becomes a valuable tool for learning and performing data analysis tasks in an interactive and accessible way.

## struc

./
    .babelrc
    netlify.toml
    package-lock.json
    package.json
    package_backup.json
    postcss.config.js
    tailwind.config.js
    backup/
        package.json
        public/
            index.html
            mainfest.json
        src/
            App.jsx
            index.jsx
            components/
                CodeDisplay.jsx
                DataTable.jsx
                NavBar.jsx
                OperationsPanel.jsx
            services/
                api.js
            styles/
                index.css
    build/
        asset-manifest.json
        index.html
        mainfest.json
        static/
            css/
                main.40309ac5.css
                main.40309ac5.css.map
            js/
                main.045b4f5e.js
                main.045b4f5e.js.LICENSE.txt
                main.045b4f5e.js.map
    public/
        index.html
        mainfest.json
    src/
        App.jsx
        index.jsx
        components/
            CodeDisplay.jsx
            CodeEditor.jsx
            ColumnStatsPanel.jsx
            DatasetInfoPanel.jsx
            DatasetManagerPage.jsx
            DataTable.jsx
            Documentation.jsx
            NavBar.jsx
            OperationsPanel.jsx
            RelationalAlgebraPanel.jsx
            TextUploadModal.jsx
        services/
            api.js
        styles/
            index.css