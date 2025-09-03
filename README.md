# dbt-project-evaluator-streamlit

A Streamlit web application for visualizing the results of the [dbt Project Evaluator](https://github.com/dbt-labs/dbt-project-evaluator) package. This is a post-execution reporting tool that provides interactive dashboards and detailed insights into your dbt project's health, violations, and compliance with best practices.

---

## Deployment Options

### 1. Local Deployment
- Run Streamlit on your local machine
- Connect to Snowflake using `.streamlit/secrets.toml` or manual credentials

### 2. Snowflake Native App Deployment
- Deploy as a Streamlit app within Snowflake
- Uses Snowflake's built-in authentication and `st.connection("snowflake")`
- No need for `.streamlit/secrets.toml` or manual credentials

---

## Prerequisites

**Important**: This application is designed to visualize results **after** you have:
1. ✅ Installed and configured the [dbt Project Evaluator package](https://github.com/dbt-labs/dbt-project-evaluator) in your dbt project
2. ✅ Successfully executed `dbt run` to generate the evaluation results tables
3. ✅ Ensured the results are available in your data warehouse (Snowflake)

The app expects to find the dbt Project Evaluator results tables in your configured database and schema.
**For Local:**
- Python 3.8+
- Streamlit
- Snowflake Python Connector
- dbt Project Evaluator package installed and run in your dbt project

**For Snowflake:**
- Snowflake account with Streamlit enabled
- dbt Project Evaluator results tables available in your Snowflake database/schema

---

## Installation (Local)

1. **Clone the repository**:
   ```bash
   git clone https://github.com/tabaghak/dbt-project-evalator-streamlit.git
   cd dbt-project-evalator-streamlit
   ```
2. **Create and activate virtual environment**:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # On Windows
   # source .venv/bin/activate  # On macOS/Linux
   ```
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
4. **Generate rules data** (if not already done):
   ```bash
   python rule_extractor.py
   ```

---

## Configuration

### Local: Using `.streamlit/secrets.toml`
Create a `.streamlit/secrets.toml` file with your Snowflake connection details:
```toml
[connections.snowflake]
account = "your-account-name"
user = "your.email@company.com"
authenticator = "externalbrowser"  # or "password", "key-pair", etc.
role = "YOUR_ROLE"
warehouse = "YOUR_WAREHOUSE"
database = "DBT_SOURCE_PROJECT_EVAL"    # Where dbt_project_evaluator results are stored
schema = "RESULTS"                      # Schema containing the evaluation tables
```

Or use manual entry in the app sidebar.

### Snowflake Native App
No configuration needed. The app uses Snowflake's built-in authentication and connection.

---

## Usage

### Local
1. **Start the application**:
   ```bash
   streamlit run app.py
   ```
2. **Connect to Snowflake**:
   - Automatic: If `secrets.toml` is configured, click "Connect using secrets.toml"
   - Manual: Use the sidebar to enter credentials
3. **Open your browser** at `http://localhost:8502`

### Snowflake
1. **Deploy the app in Snowflake** (see Snowflake documentation)
2. **Open the app from the Snowflake UI**
3. **App will use your Snowflake session for authentication and data access**

---

## Data Structure

- dbt Project Evaluator results tables in Snowflake:
  - `FCT_DOCUMENTATION_COVERAGE`
  - `FCT_TEST_COVERAGE`
  - `INT_ALL_GRAPH_RESOURCES`
  - Individual rule violation tables (e.g., `FCT_DIRECT_JOIN_TO_SOURCE`)
- `dbt_project_evaluator_rules.json`: Contains rule metadata and explanations

---

## Troubleshooting

- Module not found: `pip install -r requirements.txt`
- JSON file missing: `python rule_extractor.py`
- Port in use: `streamlit run app.py --server.port 8503`
- Cache issues: Restart app, clear browser cache

---

## License
See LICENSE file for terms.

## Support
- Check troubleshooting section
- Review application logs
- Create an issue in the repository
- Consult Streamlit and Snowflake documentation for UI issues

---

**Built with ❤️ using Streamlit and Python**
This streamlit app visualizes the results of dbt_project_evaluator package plugged in a dbt project.
