# Data Analysis GUI for Pandas, Polars, and SQL-Like Operations

## Overview

This project is an intuitive **Graphical User Interface (GUI)** tool that helps students and beginners work with datasets without needing to write code. It is designed as a **"Symbolab for pandas"** – just as Symbolab provides step-by-step solutions for math problems ([Step-by-Step Calculator](https://www.symbolab.com/solver/step-by-step#:~:text=,by%20step%20calculator%20for%20physics)), this application provides a step-by-step, visual approach to data analysis tasks. Users (especially those in data science courses like UCSD’s DSC10 or DSC80) can upload a CSV file and perform common data manipulation and analysis operations through a friendly interface. The tool executes these operations using the popular Python libraries **pandas** and **polars**, or via **SQL-like** queries, and shows users the equivalent code for each action. The primary goal is to make data wrangling accessible to beginners while helping them learn **pandas/polars syntax and SQL** by example. 

## Core Features (MVP)

- **Import Data Easily:** Users can upload CSV files or paste raw tabular data directly into the app to start exploring. The data stays local (no servers or cloud upload), ensuring privacy and quick access.

- **GUI-Based Data Operations:** Perform data transformations through point-and-click interface components instead of writing code. Key operations supported in the Minimum Viable Product include:
  - **Merge/Join:** Combine two datasets on a common column (equivalent to `pandas.merge` or SQL joins).
  - **Group By:** Group data by one or more keys and compute aggregations (like summing or averaging within groups).
  - **Filter Rows:** Apply filter conditions to display a subset of the data (for example, column value comparisons).
  - **Aggregate Functions:** Compute summary statistics such as sum, average (mean), count, minimum, and maximum on columns or groups.
  - **Rename Columns:** Easily rename column labels for clarity or consistency.
  - **Basic Visualizations:** Generate simple charts like histograms and bar charts from the data to visualize distributions and comparisons.

- **Multiple Execution Modes:** The backend can execute operations in different modes:
  - **Pandas Mode:** Use the pandas library (the standard Python data analysis tool) for all operations. This is familiar to most students and widely used in courses.
  - **Polars Mode:** Use the Polars library as an alternative DataFrame engine. Polars is built in Rust for high performance – often **5–10× faster** than pandas for common operations ([Polars vs. pandas: What’s the Difference? | The PyCharm Blog](https://blog.jetbrains.com/pycharm/2024/07/polars-vs-pandas/#:~:text=In%20a%20word%3A%20performance,4%20times%20needed%20for%20Polars)) – allowing users to work with larger datasets smoothly.
  - **SQL Mode:** Use SQL-like queries to manipulate data. Under the hood, this could be powered by **DuckDB**, a fast in-process SQL engine. DuckDB can efficiently run SQL queries directly on dataframes or CSV files ([Efficient SQL on Pandas with DuckDB – DuckDB](https://duckdb.org/2021/05/14/sql-on-pandas.html#:~:text=Published%20on%202021)), so users familiar with SQL can query their data without writing Python code.

- **Code Generation and Display:** For every action performed through the GUI, the tool generates the equivalent code and displays it. If the user is in Pandas mode, it will show the pandas code (in Python) that accomplishes the action; in Polars mode, it shows the Polars code; in SQL mode, it shows the SQL query. This helps users **learn the syntax** for each library by observing how GUI actions translate to code.

- **Real-Time Results:** As users build operations, the resulting dataset (and any corresponding visualization) updates in real-time. Users can see the effect of each transformation immediately, making the tool interactive and feedback-rich. For example, applying a filter will instantly update the table view to show only filtered rows.

- **Local-Only Storage:** All data and computations occur locally in the user’s browser and/or machine. There is no account system or cloud upload in the MVP. This means:
  - No sign-up or login is required to use the app.
  - Data remains private and is not sent to a server or cloud service.
  - Students can use it offline (once the app is installed or running locally) without internet, which is useful for classroom or homework environments with sensitive data.

## Future Features (Planned)

Beyond the core feature set, the project has an exciting roadmap to incorporate AI assistance and smarter functionalities:

- **Natural Language Problem Solver:** Leverage Large Language Models (LLMs) to interpret descriptive problem statements. Users will be able to paste a homework question or a task description (e.g., *“Find the average income by education level from the survey dataset”*) into a text box. The system will parse this and automatically generate the sequence of GUI operations or code (pandas/polars) needed to solve the problem. This is inspired by how Symbolab can interpret word problems in math ([Step-by-Step Calculator](https://www.symbolab.com/solver/step-by-step#:~:text=,by%20step%20calculator%20for%20physics)). For instance, a student could input a question from DSC10/DSC80 coursework, and the app would output a solution approach using the data at hand (complete with code that the student can learn from).

- **AI-Assisted Intent Recognition (Agentic Protocol):** Integrate with local or free LLMs (such as **Mistral 7B** or **DeepSeek** models) to create an **agent** that can assist in multi-step analysis. This agent would observe the user’s actions or hear a user’s high-level request (e.g., *“Clean this dataset and plot the distribution of salaries by country”*) and then plan and execute a series of operations in the app. The term *agentic protocoling* refers to the app guiding itself through the steps with the LLM’s help, acting like a smart assistant. For users, this means they could get suggestions like “I will remove rows with null values in the Salary column, then group by Country and plot a bar chart of the average salary.” The integration with models like Mistral (considered a gold standard for open-source LLM performance ([DeepSeek vs. Mistral: Which LLM is Better? | Sapling](https://sapling.ai/llm/mistral-vs-deepseek#:~:text=Developed%20by%20some%20of%20the,and%20performant%20open%20source%20models)) in 2024) or DeepSeek ensures this feature can run locally or at low cost.

- **Smart Function Recommendations:** The app will analyze the dataset’s shape and the user’s current progress to **recommend next steps** or functions. For example, if the user’s data has many columns with repeated categories, the tool might suggest *“You could use a Group By on column X”* or if two tables have a common field, it might suggest a *merge/join*. These recommendations can help guide beginners who aren’t sure what analysis method to apply. The recommendation engine could be rule-based and enhanced with AI, learning from typical patterns in data science workflows.

- **Additional Visualization and Analysis Tools:** Future versions plan to include more chart types (scatter plots, box plots, etc.), data cleaning utilities (handling missing values, outlier detection), and possibly integration with statistical tests or machine learning model training on the dataset – all through the GUI.

- **Collaboration and Cloud Features:** Although the initial version is local-only, future enhancements might introduce user accounts, cloud storage of datasets, and the ability to easily share analysis results or workflows with others (for example, exporting a notebook or script that reproduces the GUI steps). This would be implemented carefully to maintain privacy and simplicity.

## Tech Stack

The project is built with a modern, open-source tech stack to ensure a smooth user experience and easy integration of advanced features:

- **Frontend:** [React](https://reactjs.org/) (JavaScript/TypeScript) for building a responsive single-page application. The UI components are styled with [Tailwind CSS](https://tailwindcss.com/) for a clean, modern look and efficient design workflow. React provides the interactive foundation, while Tailwind allows rapid UI customization via utility classes.

- **Backend API:** [FastAPI](https://fastapi.tiangolo.com/) (or Flask as an alternative) in Python for handling data operations and AI interactions. FastAPI is chosen for its high performance and intuitive syntax, which makes it straightforward to define API endpoints for each operation (e.g., an endpoint to perform a group-by and return results). It also simplifies integration with Python-based LLMs and data libraries. Flask could be used similarly, but FastAPI offers asynchronous support and automatic docs, beneficial for future expansion.

- **Data Processing Engines:** 
  - **pandas:** The tried-and-true Python data analysis library for DataFrame operations. Widely taught and used, it’s the default engine in the app.
  - **polars:** A Rust-based DataFrame library accessible via Python. Polars operates in a similar DataFrame paradigm as pandas but is optimized for speed and low memory usage; it can be **10-100× faster on certain queries compared to pandas ([Polars — Updated PDS-H benchmark results](https://pola.rs/posts/benchmarks/#:~:text=Takeaways))**, especially for larger data.
  - **DuckDB:** An in-process SQL database engine. It allows executing SQL queries on local data (even without a separate database server) and can directly query pandas DataFrames or CSV files using SQL syntax ([Efficient SQL on Pandas with DuckDB – DuckDB](https://duckdb.org/2021/05/14/sql-on-pandas.html#:~:text=Published%20on%202021)). In this app, DuckDB (or a similar SQL engine) powers the SQL execution mode, enabling users to write SELECT queries on their dataset or have the GUI translate their actions to SQL behind the scenes.

- **LLM Integration:** The system is designed to be LLM-agnostic for the AI features. It can connect to local or remote LLMs through an API. For example:
  - *Mistral 7B/8x7B:* open-source models known for strong performance and accessibility (available under Apache 2.0 license ([DeepSeek vs. Mistral: Which LLM is Better? | Sapling](https://sapling.ai/llm/mistral-vs-deepseek#:~:text=Developed%20by%20some%20of%20the,and%20performant%20open%20source%20models))). These could be run locally on high-end hardware or accessed via a community API for natural language to code translation.
  - *DeepSeek models:* another set of cutting-edge models (e.g., DeepSeek R1 or V3) that are highly efficient and were notable in 2024-2025 for their cost-effective training and strong capabilities in reasoning. These can be used to interpret complex instructions or provide guided steps.
  - *Alternatively,* the backend could integrate with OpenAI/Anthropic APIs if available, but the emphasis is on **local or open** models to keep the tool accessible in academic settings without requiring paid API keys.
  
- **Storage & State:** In the MVP, the app does not use a database for user data. Uploaded datasets are held in-memory (or in the browser’s memory for client-side operations). For any necessary local persistence (like caching a dataset or storing user preferences), lightweight options like browser localStorage or small JSON files can be used. Since no authentication is involved initially, there’s no need for user accounts or heavy database usage. 

- **Deployment:** The recommended deployment is to run the frontend as a static web app (which could be hosted on GitHub Pages, Vercel, etc.) and the backend as a separate service (which could run on a small cloud VM or locally on the user’s machine). During development or local use, a user can run the React dev server and the FastAPI server on their machine. For a more user-friendly distribution, packaging the app as a desktop application (using Electron or PyInstaller for instance) could be explored, but early versions will likely be run by users following setup instructions.

## Design Philosophy

The design of this tool is heavily influenced by the needs of students learning data science and the desire to make data manipulation as straightforward as possible:

- **Education-First Approach:** Every feature is built not just to get the result, but to teach. By showing the code for each GUI action, the app encourages users to learn how to perform tasks in pandas or polars themselves next time. It bridges the gap between drag-and-drop ease and coding power.

- **Intuitive GUI Interactions:** The interface favors drag-and-drop actions, form inputs, and dropdown selections over coding or configuration files. For example, to merge two datasets, a user might drag one table onto another or choose merge options from a dialog (selecting keys, join type, etc.). The goal is that a user with no programming experience can still navigate and use the tool to do meaningful data analysis.

- **Instant Feedback and Visualization:** Taking inspiration from spreadsheets and interactive notebooks, the tool provides immediate feedback. When you apply a filter or create a new calculated column, you see the table update instantly. When you group data and aggregate, you might see a summary table *and* could quickly plot a chart of the results. By coupling transformations with visual output, users can visually verify each step of their analysis.

- **Performance and Responsiveness:** Under the hood, using efficient libraries (Polars, DuckDB) ensures that even with moderately large datasets, the UI remains responsive. The app avoids full page reloads and heavy operations on the main thread. Expensive computations are offloaded to Web Workers or the backend server as needed. The philosophy is that waiting around kills learning momentum – so the app should feel snappy. Polars and DuckDB are included precisely because they can handle larger data or complex queries much faster than traditional pandas in many cases ([Polars — Updated PDS-H benchmark results](https://pola.rs/posts/benchmarks/#:~:text=Takeaways)).

- **“Simplify, Don’t Dumb Down”:** While the GUI simplifies the process of data manipulation, it does not dumb it down or put a ceiling on what users can do. Users can chain multiple operations to perform complex analyses. Advanced users could even modify the generated code or add custom code blocks (a possible future feature) for flexibility. The design avoids overwhelming new users with too many options at once, but as users grow, they discover more powerful features.

- **Inspired by Successful Tools:** In addition to Symbolab’s step-by-step learning model ([Step-by-Step Calculator](https://www.symbolab.com/solver/step-by-step#:~:text=,by%20step%20calculator%20for%20physics)), this project takes cues from:
  - *PandasGUI* and other DataFrame GUIs – for how to display data and basic plotting in a user-friendly way.
  - *Tableau/Power BI* – for the idea of drag-and-drop analytics (though our scope is smaller and focused on coding education, not enterprise BI).
  - *Jupyter Notebooks* – for the mix of seeing code and results together. Here the “notebook” is essentially the GUI + code panel side by side.
  - By studying these, the design tries to combine ease-of-use with the depth of a coding environment.

Ultimately, the philosophy is to **empower students**: remove the initial barriers to entry for data analysis, provide an environment to experiment freely, and gently introduce coding and advanced concepts as they become comfortable. The tool should feel like a personal tutor or sandbox for data science.

## Getting Started

Follow these steps to set up the project on your local machine for development or trial use:

1. **Prerequisites:** Make sure you have **Python 3.9+** and **Node.js 18+** installed on your system. You’ll also need `pip` for Python package installation and `npm` or `yarn` for Node package management.
2. **Clone the Repository:** Download the project source code from the repository. (If this README is on GitHub, you can use `git clone <repo_url>`.)
3. **Backend Setup (FastAPI/Flask):**  
   - Navigate to the backend directory (e.g., `cd backend`).
   - Create a virtual environment (optional but recommended): `python -m venv venv` and activate it.
   - Install the required Python packages: `pip install -r requirements.txt`. This will install FastAPI/Flask, pandas, polars, DuckDB, and any LLM-related libraries specified.
   - Start the backend server:
     - If using FastAPI: run `uvicorn main:app --reload` (assuming the FastAPI app instance is in `main.py`).
     - If using Flask: run `flask run` (after setting any necessary environment variables).
   - The backend should start on a local port (e.g., http://127.0.0.1:8000 for FastAPI by default).
4. **Frontend Setup (React):**  
   - Navigate to the frontend directory (e.g., `cd frontend`).
   - Install dependencies: `npm install` (this will install React, Tailwind, and other libraries).
   - Start the development server: `npm start` or `npm run dev` (depending on the setup). This will launch the React app on a local development server (e.g., http://localhost:3000).
   - The React app will likely proxy API calls to the backend (check configuration) so that requests to the API routes (for data ops or LLM queries) are forwarded to the FastAPI/Flask server.
5. **Using the App:**  
   - Open your web browser and go to the local frontend URL (e.g., http://localhost:3000).
   - You should see the application interface. Start by uploading a CSV file or choosing a sample dataset if provided.
   - Try out some operations: filter a column, do a group-by, or switch to SQL mode and run a query. You’ll see the resulting data table update, and you can view the generated code in the code panel.
   - Experiment with switching the execution engine (pandas/polars/SQL) in the settings or menu to see how the code and performance differ.
   - *(If anything is not working, check the terminal where the backend is running for error messages, and ensure the frontend is configured to talk to the correct backend URL.)*

6. **Stopping the App:** When done, you can stop the frontend and backend by pressing `Ctrl+C` in their terminal windows. If using a virtual environment for Python, you can deactivate it with `deactivate`.

*(Pre-built binaries or Docker images for easier setup may be provided in the future. For now, the above steps help developers and early adopters run the project.)*

## Contributing

Contributions are welcome to make this tool better for all users, especially students learning data science. If you have ideas for improvements or new features, feel free to open an issue or pull request. Some ways to contribute:

- **Feedback & Ideas:** Use the GitHub Issues to report bugs, suggest enhancements, or propose new features (e.g., new operation types, UI improvements, additional chart types).
- **Code Contributions:** If you’re a developer, check out the issue tracker for items labeled “help wanted” or “good first issue.” You can fork the repo, create a new branch, and submit a pull request with your changes. Ensure that new code is well-documented and tested.
- **Documentation:** Improving documentation is also a valuable contribution. If you find sections of this README or the in-app help unclear, you can help by rephrasing or expanding them. Likewise, tutorials or example notebooks using the tool for a real dataset analysis would be very helpful for new users.
- **Community Support:** If you’re an early user, you can help others by answering questions (for example, if someone is confused about an operation or installation) in the discussion boards or issues.

When contributing, please adhere to the project’s code of conduct (see `CODE_OF_CONDUCT.md` if available) to foster a friendly and cooperative community. All contributions are subject to review by the maintainers. We appreciate your interest in making this project a success!

## Project Status and Future Outlook

This project is in active development. The core functionality (MVP) is being built and tested with input from data science students. As of now, the focus is on creating a stable, usable base – the essential data operations and a smooth UI/UX. Once the MVP is solid, the attention will shift to the advanced, AI-powered features outlined in the roadmap.

Near-term milestones to look forward to:
- Polishing the GUI and ensuring a consistent experience across different browsers and datasets.
- Validating the code generation accuracy for pandas and polars (so that copying the shown code and running it independently yields the same result).
- Basic integration of a local LLM (possibly a small model) to prototype the natural language problem solver.
- Collecting user feedback from initial testers (e.g., students in a data science class) to prioritize the next enhancements.

In the long run, this project aspires to be a **learning platform for data science**. Just as one might use a calculator to verify math homework, students might use this tool to verify and understand their data analysis homework. The **"Symbolab for pandas"** nickname captures this vision: step-by-step guidance with the ability to peek under the hood at any time. 

Stay tuned for updates, and thank you for your interest in the project! With the support of the community, we hope to lower the barrier to entry in data science and make learning data analysis an engaging, interactive experience.

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
