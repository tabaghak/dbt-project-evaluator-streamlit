#!/usr/bin/env python3
"""
dbt Project Evaluator Rules Management App

A Streamlit application for managing dbt_project_evaluator rules.
Users can view rules across different evaluation categories.

Date: Sept. 3, 2025
"""

import streamlit as st
import json
import uuid
from typing import Dict, List, Any, Optional
from io import StringIO
import pandas as pd
import os
import re
from cryptography.hazmat.primitives import serialization
import plotly.graph_objects as go
import plotly.express as px

# Try to import Snowflake connector, but don't fail if not available (for Snowflake Native App)
try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import pd_writer
    SNOWFLAKE_CONNECTOR_AVAILABLE = True
except ImportError:
    SNOWFLAKE_CONNECTOR_AVAILABLE = False

# Environment detection
IS_SNOWFLAKE_NATIVE = False
try:
    # Multiple ways to detect Snowflake Native environment
    # Method 1: Check for Snowflake-specific environment variables
    if (os.environ.get("SNOWFLAKE_STREAMLIT") == "1" or 
        os.environ.get("SNOWFLAKE_HOST") or 
        os.environ.get("SNOWFLAKE_ACCOUNT") or
        os.environ.get("STREAMLIT_SERVER_PORT") == "8080"):  # Snowflake Native typically uses port 8080
        IS_SNOWFLAKE_NATIVE = True
    
    # Method 2: Check if we can create a snowflake connection without explicit config
    if not IS_SNOWFLAKE_NATIVE:
        try:
            import streamlit as st_test
            # Try to access the connection without errors
            test_conn = st_test.connection("snowflake")
            if test_conn:
                # Additional check: see if we can get connection info
                try:
                    conn_info = test_conn._instance
                    if conn_info:
                        IS_SNOWFLAKE_NATIVE = True
                except:
                    pass
        except Exception:
            pass
    
    # Method 3: Check if streamlit is running in a Snowflake context
    if not IS_SNOWFLAKE_NATIVE:
        try:
            # Look for Snowflake-specific modules or contexts
            import sys
            for module_name in sys.modules:
                if 'snowflake' in module_name.lower() and 'streamlit' in module_name.lower():
                    IS_SNOWFLAKE_NATIVE = True
                    break
        except Exception:
            pass
            
except Exception:
    pass

# Force Snowflake Native mode for testing (can be removed later)
# Uncomment the next line when testing in actual Snowflake environment
# IS_SNOWFLAKE_NATIVE = True

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #FF6B35;
        text-align: center;
        margin-bottom: 2rem;
    }
    .category-header {
        color: #4A90E2;
        border-bottom: 2px solid #4A90E2;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
    }
    .rule-panel {
        border: 1px solid #e0e0e0;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
        background-color: #f8f9fa;
    }
    .tab-content {
        min-height: 200px;
        padding: 1rem;
    }
    .button-container {
        display: flex;
        gap: 10px;
        margin-top: 1rem;
    }
    .success-message {
        color: #28a745;
        font-weight: bold;
    }
    .error-message {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# File path for the JSON data
JSON_FILE_PATH = "config/dbt_project_evaluator_rules.json"

@st.cache_data
def load_rules_data() -> Dict[str, Any]:
    """Load rules data from JSON file"""
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        dismissible_error(f"Rules file not found: {JSON_FILE_PATH}", key="rules_file_not_found")
        return {}
    except json.JSONDecodeError:
        dismissible_error("Invalid JSON format in rules file", key="invalid_json_format")
        return {}

def save_rules_data(data: Dict[str, Any]) -> bool:
    """Save rules data to JSON file"""
    try:
        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        st.cache_data.clear()  # Clear cache to reload data
        return True
    except Exception as e:
        dismissible_error(f"Error saving rules: {str(e)}", key="save_rules_error")
        return False

def generate_rule_id(name: str) -> str:
    """Generate a unique rule ID based on the name"""
    return f"fct_{name.lower().replace(' ', '_').replace('-', '_')}"

def create_empty_rule() -> Dict[str, str]:
    """Create an empty rule template"""
    return {
        "name": "",
        "description": "",
        "example": "",
        "exception": "",
        "reason_to_flag": "",
        "remediation": ""
    }

def dismissible_error(message: str, key: Optional[str] = None) -> None:
    """Display a dismissible error message - click to dismiss"""
    if key is None:
        key = f"error_{hash(message)}"
    
    if f"dismissed_{key}" not in st.session_state:
        st.session_state[f"dismissed_{key}"] = False
    
    if not st.session_state[f"dismissed_{key}"]:
        # Custom CSS for error button styling
        st.markdown(f"""
        <style>
        .stButton > button[data-testid="baseButton-secondary"] {{
            background-color: rgba(255, 75, 75, 0.1) !important;
            border: 1px solid rgba(255, 75, 75, 0.2) !important;
            color: rgb(120, 10, 10) !important;
            width: 100% !important;
            text-align: left !important;
            border-radius: 0.5rem !important;
            padding: 0.75rem 1rem !important;
            font-size: 1rem !important;
            line-height: 1.6 !important;
            transition: none !important;
        }}
        .stButton > button[data-testid="baseButton-secondary"]:hover,
        .stButton > button[data-testid="baseButton-secondary"]:active,
        .stButton > button[data-testid="baseButton-secondary"]:focus {{
            background-color: rgba(255, 75, 75, 0.1) !important;
            border: 1px solid rgba(255, 75, 75, 0.2) !important;
            color: rgb(120, 10, 10) !important;
            transform: none !important;
            box-shadow: none !important;
        }}
        </style>
        """, unsafe_allow_html=True)
        
        if st.button(f"🚨 {message}", key=f"dismiss_error_{key}", 
                    type="secondary", use_container_width=True):
            st.session_state[f"dismissed_{key}"] = True
            st.rerun()

def dismissible_warning(message: str, key: Optional[str] = None) -> None:
    """Display a dismissible warning message - click to dismiss"""
    if key is None:
        key = f"warning_{hash(message)}"
    
    if f"dismissed_{key}" not in st.session_state:
        st.session_state[f"dismissed_{key}"] = False
    
    if not st.session_state[f"dismissed_{key}"]:
        # Custom CSS for warning button styling
        st.markdown(f"""
        <style>
        .stButton > button[data-testid="baseButton-secondary"] {{
            background-color: rgba(255, 196, 0, 0.1) !important;
            border: 1px solid rgba(255, 196, 0, 0.2) !important;
            color: rgb(147, 112, 0) !important;
            width: 100% !important;
            text-align: left !important;
            border-radius: 0.5rem !important;
            padding: 0.75rem 1rem !important;
            font-size: 1rem !important;
            line-height: 1.6 !important;
            transition: none !important;
        }}
        .stButton > button[data-testid="baseButton-secondary"]:hover,
        .stButton > button[data-testid="baseButton-secondary"]:active,
        .stButton > button[data-testid="baseButton-secondary"]:focus {{
            background-color: rgba(255, 196, 0, 0.1) !important;
            border: 1px solid rgba(255, 196, 0, 0.2) !important;
            color: rgb(147, 112, 0) !important;
            transform: none !important;
            box-shadow: none !important;
        }}
        </style>
        """, unsafe_allow_html=True)
        
        if st.button(f"⚠️ {message}", key=f"dismiss_warning_{key}", 
                    type="secondary", use_container_width=True):
            st.session_state[f"dismissed_{key}"] = True
            st.rerun()

def dismissible_info(message: str, key: Optional[str] = None) -> None:
    """Display a dismissible info message - click to dismiss"""
    if key is None:
        key = f"info_{hash(message)}"
    
    if f"dismissed_{key}" not in st.session_state:
        st.session_state[f"dismissed_{key}"] = False
    
    if not st.session_state[f"dismissed_{key}"]:
        # Custom CSS for info button styling
        st.markdown(f"""
        <style>
        .stButton > button[data-testid="baseButton-secondary"] {{
            background-color: rgba(0, 178, 255, 0.1) !important;
            border: 1px solid rgba(0, 178, 255, 0.2) !important;
            color: rgb(0, 104, 181) !important;
            width: 100% !important;
            text-align: left !important;
            border-radius: 0.5rem !important;
            padding: 0.75rem 1rem !important;
            font-size: 1rem !important;
            line-height: 1.6 !important;
            transition: none !important;
        }}
        .stButton > button[data-testid="baseButton-secondary"]:hover,
        .stButton > button[data-testid="baseButton-secondary"]:active,
        .stButton > button[data-testid="baseButton-secondary"]:focus {{
            background-color: rgba(0, 178, 255, 0.1) !important;
            border: 1px solid rgba(0, 178, 255, 0.2) !important;
            color: rgb(0, 104, 181) !important;
            transform: none !important;
            box-shadow: none !important;
        }}
        </style>
        """, unsafe_allow_html=True)
        
        if st.button(f"ℹ️ {message}", key=f"dismiss_info_{key}", 
                    type="secondary", use_container_width=True):
            st.session_state[f"dismissed_{key}"] = True
            st.rerun()

def dismissible_success(message: str, key: Optional[str] = None) -> None:
    """Display a dismissible success message - click to dismiss"""
    if key is None:
        key = f"success_{hash(message)}"
    
    if f"dismissed_{key}" not in st.session_state:
        st.session_state[f"dismissed_{key}"] = False
    
    if not st.session_state[f"dismissed_{key}"]:
        # Custom CSS for success button styling
        st.markdown(f"""
        <style>
        .stButton > button[data-testid="baseButton-secondary"] {{
            background-color: rgba(33, 195, 84, 0.1) !important;
            border: 1px solid rgba(33, 195, 84, 0.2) !important;
            color: rgb(23, 114, 51) !important;
            width: 100% !important;
            text-align: left !important;
            border-radius: 0.5rem !important;
            padding: 0.75rem 1rem !important;
            font-size: 1rem !important;
            line-height: 1.6 !important;
            transition: none !important;
        }}
        .stButton > button[data-testid="baseButton-secondary"]:hover,
        .stButton > button[data-testid="baseButton-secondary"]:active,
        .stButton > button[data-testid="baseButton-secondary"]:focus {{
            background-color: rgba(33, 195, 84, 0.1) !important;
            border: 1px solid rgba(33, 195, 84, 0.2) !important;
            color: rgb(23, 114, 51) !important;
            transform: none !important;
            box-shadow: none !important;
        }}
        </style>
        """, unsafe_allow_html=True)
        
        if st.button(f"✅ {message}", key=f"dismiss_success_{key}", 
                    type="secondary", use_container_width=True):
            st.session_state[f"dismissed_{key}"] = True
            st.rerun()

@st.cache_resource
def init_snowflake_connection():
    """Initialize Snowflake connection using Streamlit secrets or session state"""
    if IS_SNOWFLAKE_NATIVE:
        # In Snowflake Native apps, we're always "connected"
        st.session_state.snowflake_connected = True
        st.session_state.snowflake_conn = None  # Not needed for native apps
        return True
    
    if "snowflake_connected" not in st.session_state:
        st.session_state.snowflake_connected = False
        st.session_state.snowflake_conn = None
        # Try to connect using secrets.toml if available
        try_secrets_connection()
    
    return st.session_state.get("snowflake_conn")

def try_secrets_connection():
    """Try to connect using Streamlit secrets configuration"""
    try:
        if "connections" in st.secrets and "snowflake" in st.secrets.connections:
            sf_config = st.secrets.connections.snowflake
            if connect_to_snowflake_with_config(sf_config):
                st.session_state.secrets_connected = True
                return True
        else:
            # Debug information for missing sections
            available_sections = list(st.secrets.keys()) if hasattr(st.secrets, 'keys') else []
            if "connections" not in st.secrets:
                st.session_state.secrets_error = f"No 'connections' section found in secrets.toml. Available sections: {available_sections}"
            elif "snowflake" not in st.secrets.connections:
                conn_sections = list(st.secrets.connections.keys()) if hasattr(st.secrets.connections, 'keys') else []
                st.session_state.secrets_error = f"No 'snowflake' section found in connections. Available connections: {conn_sections}"
    except FileNotFoundError:
        st.session_state.secrets_error = "secrets.toml file not found in .streamlit directory"
    except Exception as e:
        st.session_state.secrets_error = f"Error accessing secrets.toml: {str(e)}"
    return False

def connect_to_snowflake_with_config(config: Dict[str, str]) -> bool:
    """Connect to Snowflake using configuration dictionary"""
    try:
        # Base connection parameters
        conn_params = {
            "account": config.get("account"),
            "user": config.get("user"),
            "warehouse": config.get("warehouse"),
            "database": config.get("database"),
            "schema": config.get("schema"),
        }
        
        # Add optional parameters if present
        if "role" in config:
            conn_params["role"] = config["role"]
        if "client_session_keep_alive" in config:
            conn_params["client_session_keep_alive"] = config["client_session_keep_alive"]
        if "login_timeout" in config:
            conn_params["login_timeout"] = config["login_timeout"]
        
        # Handle different authentication methods
        authenticator = config.get("authenticator", "password")
        
        if authenticator == "password":
            conn_params["password"] = config.get("password")
        
        elif authenticator == "externalbrowser":
            conn_params["authenticator"] = "externalbrowser"
        
        elif authenticator == "key-pair":
            # Load private key for key-pair authentication
            private_key_path = config.get("private_key_path")
            private_key_passphrase = config.get("private_key_passphrase")
            
            if private_key_path and os.path.exists(private_key_path):
                with open(private_key_path, 'rb') as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=private_key_passphrase.encode() if private_key_passphrase else None
                    )
                    pkb = private_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                    conn_params["private_key"] = pkb
            else:
                raise ValueError("Private key file not found or not specified")
        
        elif authenticator == "oauth":
            conn_params["authenticator"] = "oauth"
            conn_params["token"] = config.get("token")
        
        else:
            conn_params["authenticator"] = authenticator
        
        # Attempt connection
        conn = snowflake.connector.connect(**conn_params)
        st.session_state.snowflake_conn = conn
        st.session_state.snowflake_connected = True
        return True
        
    except Exception as e:
        dismissible_error(f"Failed to connect to Snowflake: {str(e)}", key="snowflake_connection_config")
        return False

def connect_to_snowflake_manual(account: str, user: str, auth_method: str, **auth_params) -> bool:
    """Connect to Snowflake with manually provided credentials"""
    try:
        conn_params = {
            "account": account,
            "user": user,
            "warehouse": auth_params.get("warehouse", "COMPUTE_WH"),
            "database": auth_params.get("database", "DBT_SOURCE_PROJECT_EVAL"),
            "schema": auth_params.get("schema", "RESULTS"),
        }
        
        # Handle authentication based on method
        if auth_method == "password":
            conn_params["password"] = auth_params.get("password")
        
        elif auth_method == "externalbrowser":
            conn_params["authenticator"] = "externalbrowser"
        
        elif auth_method == "key-pair":
            private_key_content = auth_params.get("private_key_content")
            private_key_passphrase = auth_params.get("private_key_passphrase")
            
            if private_key_content:
                try:
                    private_key = serialization.load_pem_private_key(
                        private_key_content.encode(),
                        password=private_key_passphrase.encode() if private_key_passphrase else None
                    )
                    pkb = private_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                    conn_params["private_key"] = pkb
                except Exception as e:
                    st.error(f"Error processing private key: {str(e)}")
                    return False
            else:
                st.error("Private key content is required for key-pair authentication")
                return False
        
        elif auth_method == "oauth":
            conn_params["authenticator"] = "oauth"
            conn_params["token"] = auth_params.get("token")
        
        # Add role if specified
        if auth_params.get("role"):
            conn_params["role"] = auth_params["role"]
        
        conn = snowflake.connector.connect(**conn_params)
        st.session_state.snowflake_conn = conn
        st.session_state.snowflake_connected = True
        st.session_state.secrets_connected = False  # Mark as manually connected
        return True
        
    except Exception as e:
        dismissible_error(f"Failed to connect to Snowflake: {str(e)}", key="snowflake_connection_manual")
        return False

# Session database and schema (default values)
SESSION_DATABASE = "DBT_SOURCE_PROJECT_EVAL"
SESSION_SCHEMA = "RESULTS"

def get_current_session_info() -> tuple:
    """Get current session database and schema"""
    try:
        query = "SELECT CURRENT_DATABASE() as current_db, CURRENT_SCHEMA() as current_schema"
        df = execute_snowflake_query(query)
        if df is not None and not df.empty:
            return df.iloc[0]['CURRENT_DB'], df.iloc[0]['CURRENT_SCHEMA']
    except Exception as e:
        st.sidebar.error(f"Error getting session info: {str(e)}")
    return SESSION_DATABASE, SESSION_SCHEMA

def get_available_databases() -> list:
    """Get list of available databases"""
    try:
        query = "SELECT database_name FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES ORDER BY database_name"
        df = execute_snowflake_query(query)
        if df is not None and not df.empty:
            return df['DATABASE_NAME'].tolist()
    except Exception as e:
        st.sidebar.error(f"Error getting databases: {str(e)}")
    return []

def get_available_schemas(database: str) -> list:
    """Get list of available schemas for the selected database"""
    try:
        query = f"SELECT schema_name FROM {database}.INFORMATION_SCHEMA.SCHEMATA ORDER BY schema_name"
        df = execute_snowflake_query(query)
        if df is not None and not df.empty:
            return df['SCHEMA_NAME'].tolist()
    except Exception as e:
        st.sidebar.error(f"Error getting schemas for {database}: {str(e)}")
    return []

# Unified query execution for both environments

def execute_snowflake_query(query: str) -> Optional[pd.DataFrame]:
    """Execute a query on Snowflake and return results as DataFrame (supports both local and Snowflake Native)"""
    # Check if we're in forced Snowflake Native mode or actual Snowflake Native
    is_snowflake_mode = IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False)
    
    if is_snowflake_mode:
        try:
            conn = st.connection("snowflake")
            df = conn.query(query, ttl=600)
            return df
        except Exception as e:
            st.error(f"Query execution failed (Snowflake Native): {str(e)}")
            # Additional debugging for Snowflake Native
            st.error("Debug: Please ensure you have configured the Snowflake connection in your Streamlit app settings.")
            return None
    else:
        # Local/manual connection
        if not st.session_state.get("snowflake_connected"):
            dismissible_warning("Not connected to Snowflake", key="not_connected_warning")
            return None
        try:
            conn = st.session_state.snowflake_conn
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=columns)
            cursor.close()
            return df
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            return None

# Helper to build fully qualified table name

def fq_table(table: str) -> str:
    """Build fully qualified table name using session state database and schema"""
    current_db = st.session_state.get('selected_database', SESSION_DATABASE)
    current_schema = st.session_state.get('selected_schema', SESSION_SCHEMA)
    return f"{current_db}.{current_schema}.{table}"

def get_rule_status_emoji(rule_key: str, rule_name: str) -> str:
    """Get emoji and title based on violations count"""
    if not (st.session_state.get("snowflake_connected") or IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False)):
        return f"🔍 {rule_name}"
    
    # Check if violations data is already loaded
    violations_key = f"violations_{rule_key}"
    if violations_key in st.session_state:
        df = st.session_state[violations_key]
        if df.empty:
            return f"✅ {rule_name}"
        else:
            violation_count = len(df)
            return f"❌ {rule_name} ({violation_count})"
    else:
        # Try to load violations data quickly
        try:
            query = f"SELECT * FROM {fq_table(rule_key)}"
            df = execute_snowflake_query(query)
            if df is not None:
                st.session_state[violations_key] = df
                if df.empty:
                    return f"✅ {rule_name}"
                else:
                    violation_count = len(df)
                    return f"❌ {rule_name} ({violation_count})"
        except:
            pass
    
    # Default fallback
    return f"🔍 {rule_name}"

def preload_violations_for_rules(rules_data: Dict[str, Any]):
    """Preload violations data for all rules if connected to Snowflake"""
    if not (st.session_state.get("snowflake_connected") or IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False)):
        return
    
    # Only preload if not already done in this session
    if st.session_state.get("violations_preloaded"):
        return
    
    with st.spinner("Loading violations data for all rules..."):
        total_rules = 0
        loaded_rules = 0
        
        # Count total rules
        for category_data in rules_data.values():
            total_rules += len(category_data.get("rules", {}))
        
        # Create progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for category_name, category_data in rules_data.items():
            rules = category_data.get("rules", {})
            for rule_key, rule_data in rules.items():
                status_text.text(f"Loading violations for {rule_key}...")
                
                # Skip if already loaded
                if f"violations_{rule_key}" not in st.session_state:
                    query = f"SELECT * FROM {fq_table(rule_key)}"
                    df = execute_snowflake_query(query)
                    if df is not None:
                        st.session_state[f"violations_{rule_key}"] = df
                
                loaded_rules += 1
                progress_bar.progress(loaded_rules / total_rules)
        
        st.session_state["violations_preloaded"] = True
        status_text.empty()
        progress_bar.empty()
        dismissible_success(f"Loaded violations data for {total_rules} rules!")

def calculate_dashboard_metrics(rules_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate metrics for the dashboard overview"""
    metrics = {
        "total_rules": 0,
        "categories": len(rules_data),
        "rules_with_violations": 0,
        "total_violations": 0,
        "category_violations": {},
        "violation_details": {}
    }
    
    if not (st.session_state.get("snowflake_connected") or IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False)):
        return metrics
    
    # Calculate totals
    for category_name, category_data in rules_data.items():
        rules = category_data.get("rules", {})
        metrics["total_rules"] += len(rules)
        category_violations = 0
        
        for rule_key, rule_data in rules.items():
            violations_key = f"violations_{rule_key}"
            if violations_key in st.session_state:
                df = st.session_state[violations_key]
                violation_count = len(df) if not df.empty else 0
                
                if violation_count > 0:
                    metrics["rules_with_violations"] += 1
                    metrics["total_violations"] += violation_count
                    category_violations += violation_count
                    
                    # Store detailed violation info
                    metrics["violation_details"][rule_key] = {
                        "name": rule_data.get("name", rule_key),
                        "category": category_name,
                        "count": violation_count
                    }
        
        metrics["category_violations"][category_name] = category_violations
    
    return metrics

def display_dashboard_overview(rules_data: Dict[str, Any]) -> None:
    """Display the overview dashboard similar to the provided image"""
    st.markdown('<h1 class="main-header">📊 Project Health Overview</h1>', unsafe_allow_html=True)
    
    # Calculate metrics
    metrics = calculate_dashboard_metrics(rules_data)
    
    if not (st.session_state.get("snowflake_connected") or IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False)):
        st.warning("🔌 Connect to Snowflake to see live project health metrics")
        st.info("The dashboard will show rule violations and project statistics once connected.")
        return
    
    # Project Health Overview Cards
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Documentation Coverage - get real data from Snowflake and display as doughnut chart
        doc_coverage = 0
        doc_status = "No Data"
        try:
            doc_query = f"SELECT * FROM {fq_table('FCT_DOCUMENTATION_COVERAGE')} ORDER BY MEASURED_AT DESC LIMIT 1"
            doc_df = execute_snowflake_query(doc_query)
            if doc_df is not None and not doc_df.empty:
                # Assuming the coverage is in a column - adjust column name as needed
                coverage_columns = [col for col in doc_df.columns if 'COVERAGE' in col.upper() or 'PCT' in col.upper() or 'PERCENT' in col.upper()]
                if coverage_columns:
                    doc_coverage = float(doc_df[coverage_columns[0]].iloc[0]) * 100 if doc_df[coverage_columns[0]].iloc[0] <= 1 else float(doc_df[coverage_columns[0]].iloc[0])
                elif 'DOCUMENTATION_COVERAGE_PCT' in doc_df.columns:
                    doc_coverage = float(doc_df['DOCUMENTATION_COVERAGE_PCT'].iloc[0])
                else:
                    # Try to find any numeric column that might be coverage
                    numeric_cols = doc_df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        doc_coverage = float(doc_df[numeric_cols[0]].iloc[0]) * 100 if doc_df[numeric_cols[0]].iloc[0] <= 1 else float(doc_df[numeric_cols[0]].iloc[0])
                doc_status = "Good" if doc_coverage > 80 else "Needs Improvement"
        except Exception as e:
            doc_coverage = 0
            doc_status = f"Error: {str(e)}"
        
        # Create doughnut chart for documentation coverage
        doc_color = "#2e7d32" if doc_coverage > 80 else "#c62828"
        
        fig_doc = go.Figure(data=[go.Pie(
            labels=['Documented', 'Not Documented'], 
            values=[doc_coverage, 100-doc_coverage],
            hole=.6,
            marker_colors=[doc_color, '#f0f0f0'],
            textinfo='none',
            hovertemplate='%{label}: %{value:.1f}%<extra></extra>'
        )])
        
        fig_doc.update_layout(
            title={
                'text': f"<b>Documentation Coverage</b>",
                'x': 0.5,
                'xanchor': 'center'
            },
            showlegend=False,
            height=320,
            margin=dict(t=60, b=80, l=20, r=20),
            annotations=[
                # Center percentage text
                dict(
                    text=f"<b style='font-size:24px; color:{doc_color}'>{doc_coverage:.1f}%</b>",
                    x=0.5, y=0.5,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=24, color=doc_color)
                ),
                # Bottom status text - moved lower to avoid overlap
                dict(
                    text=f"<span style='font-size:12px; color:#666'>{doc_status}</span>",
                    x=0.5, y=-0.15,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=12, color="#666")
                )
            ]
        )
        
        st.plotly_chart(fig_doc, use_container_width=True)
    
    with col2:
        # Test Coverage - get real data from Snowflake and display as doughnut chart
        test_coverage = 0
        test_status = "No Data"
        try:
            test_query = f"SELECT * FROM {fq_table('FCT_TEST_COVERAGE')} ORDER BY MEASURED_AT DESC LIMIT 1"
            test_df = execute_snowflake_query(test_query)
            if test_df is not None and not test_df.empty:
                # Look for coverage columns
                coverage_columns = [col for col in test_df.columns if 'COVERAGE' in col.upper() or 'PCT' in col.upper() or 'PERCENT' in col.upper()]
                if coverage_columns:
                    test_coverage = float(test_df[coverage_columns[0]].iloc[0]) * 100 if test_df[coverage_columns[0]].iloc[0] <= 1 else float(test_df[coverage_columns[0]].iloc[0])
                elif 'TEST_COVERAGE_PCT' in test_df.columns:
                    test_coverage = float(test_df['TEST_COVERAGE_PCT'].iloc[0])
                else:
                    # Try to find any numeric column that might be coverage
                    numeric_cols = test_df.select_dtypes(include=['number']).columns
                    if len(numeric_cols) > 0:
                        test_coverage = float(test_df[numeric_cols[0]].iloc[0]) * 100 if test_df[numeric_cols[0]].iloc[0] <= 1 else float(test_df[numeric_cols[0]].iloc[0])
                test_status = "Good" if test_coverage > 80 else "Needs Improvement"
        except Exception as e:
            test_coverage = 0
            test_status = f"Error: {str(e)}"
        
        # Create doughnut chart for test coverage
        test_color = "#2e7d32" if test_coverage > 80 else "#c62828"
        
        fig_test = go.Figure(data=[go.Pie(
            labels=['Tested', 'Not Tested'], 
            values=[test_coverage, 100-test_coverage],
            hole=.6,
            marker_colors=[test_color, '#f0f0f0'],
            textinfo='none',
            hovertemplate='%{label}: %{value:.1f}%<extra></extra>'
        )])
        
        fig_test.update_layout(
            title={
                'text': f"<b>Test Coverage</b>",
                'x': 0.5,
                'xanchor': 'center'
            },
            showlegend=False,
            height=320,
            margin=dict(t=60, b=80, l=20, r=20),
            annotations=[
                # Center percentage text
                dict(
                    text=f"<b style='font-size:24px; color:{test_color}'>{test_coverage:.1f}%</b>",
                    x=0.5, y=0.5,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=24, color=test_color)
                ),
                # Bottom status text - moved lower to avoid overlap
                dict(
                    text=f"<span style='font-size:12px; color:#666'>{test_status}</span>",
                    x=0.5, y=-0.15,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=12, color="#666")
                )
            ]
        )
        
        st.plotly_chart(fig_test, use_container_width=True)
    
    with col3:
        # Total Models - get real data from Snowflake
        total_models = 0
        model_status = "No Data"
        try:
            models_query = f"SELECT COUNT(1) AS models FROM {fq_table('INT_ALL_GRAPH_RESOURCES')} WHERE resource_type = 'model'"
            models_df = execute_snowflake_query(models_query)
            if models_df is not None and models_df.empty == False:
                total_models = int(models_df['MODELS'].iloc[0])
                if total_models > 100:
                    model_status = "Large project"
                elif total_models > 50:
                    model_status = "Medium project"
                else:
                    model_status = "Small project"
        except Exception as e:
            total_models = metrics['total_rules']  # Fallback to rule count
            model_status = "Large project"
        
        st.markdown(f"""
        <div style="background-color: #e3f2fd; border: 1px solid #bbdefb; border-radius: 8px; padding: 16px; text-align: center; height: 200px; display: flex; flex-direction: column; justify-content: center;">
            <div style="color: #1565c0; font-size: 12px; font-weight: bold; margin-bottom: 8px;">Total Models</div>
            <div style="color: #1565c0; font-size: 24px; font-weight: bold;">{total_models}</div>
            <div style="color: #666; font-size: 12px;">{model_status}</div>
            <div style="color: #1565c0; font-size: 24px; margin-top: 8px;">📊</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Affected Models Summary
    st.subheader("Affected Models Summary")
    st.markdown("**Affected Models by Category (Blocked by Rule)**")
    
    # Create horizontal stacked bar chart data
    if metrics["violation_details"]:
        # Prepare data for stacked horizontal bar chart
        chart_data = []
        categories = set()
        
        # Collect all data points
        for rule_key, details in metrics["violation_details"].items():
            chart_data.append({
                "Category": details["category"],
                "Rule": details["name"],
                "Violations": details["count"]
            })
            categories.add(details["category"])
        
        if chart_data:
            # Create DataFrame
            df = pd.DataFrame(chart_data)
            
            # Create horizontal stacked bar chart using Plotly
            fig = px.bar(
                df, 
                x="Violations", 
                y="Category", 
                color="Rule",
                orientation='h',
                title="Violations by Category and Rule",
                labels={"Violations": "Number of Violations", "Category": "Category"},
                height=400
            )
            
            # Update layout for better appearance
            fig.update_layout(
                showlegend=True,
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=1,
                    xanchor="left",
                    x=1.02
                ),
                margin=dict(l=20, r=150, t=50, b=20),
                xaxis_title="Number of Violations",
                yaxis_title="Category"
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("🎉 No violations found across all categories!")
    else:
        st.success("🎉 No rule violations detected!")
    # Affected Models Details
    st.subheader("Affected Models Details")
    
    if metrics["violation_details"]:
        # Create detailed breakdown table
        detail_data = []
        for rule_key, details in metrics["violation_details"].items():
            detail_data.append({
                "Category": details["category"],
                "Rule": details["name"],
                "Violations": details["count"]
            })
        
        detail_df = pd.DataFrame(detail_data)
        
        # Group by category and display
        for category in detail_df["Category"].unique():
            cat_data = detail_df[detail_df["Category"] == category]
            with st.expander(f"{category} ({cat_data['Violations'].sum()} violations)"):
                st.dataframe(cat_data[["Rule", "Violations"]], hide_index=True, use_container_width=True)
    else:
        st.success("🎉 No rule violations detected!")
    
    # Project Structure Breakdown
    st.subheader("Project Structure Breakdown")
    st.markdown("**Distribution of Model Types**")

    # Query all model data from Snowflake
    models_query = f"""
    SELECT model_type, database, REPLACE(schema, 'RESULTS_') AS schema, materialized, directory_path, file_name, number_lines, sql_complexity
    FROM {fq_table('INT_ALL_GRAPH_RESOURCES')}
    WHERE resource_type = 'model'
    """
    models_df = None
    try:
        models_df = execute_snowflake_query(models_query)
    except Exception as e:
        st.error(f"Error loading model details: {str(e)}")

    # Pie chart: count per model_type
    selected_model_type = None
    if models_df is not None and not models_df.empty:
        # Normalize column names to lowercase
        models_df.columns = [col.lower() for col in models_df.columns]
        model_type_col = "model_type" if "model_type" in models_df.columns else models_df.columns[0]
        # Group by model_type and count unique models
        model_type_counts = models_df.groupby(model_type_col).size().reset_index(name="count")
        data_dict = dict(zip(model_type_counts['model_type'], model_type_counts['count']))
        
        # Create pie chart using Streamlit's built-in chart
        model_types_df = pd.DataFrame([
            {"Model Type": model_type, "Count": percentage} 
            for model_type, percentage in data_dict.items()
        ])

        # Use Streamlit's pie chart
        st.plotly_chart(
            {
                "data": [
                    {
                        "values": list(data_dict.values()),
                        "labels": list(data_dict.keys()),
                        "type": "pie",
                        "name": "Model Types"
                    }
                ],
                "layout": {
                    "title": "Model Type Distribution",
                    "showlegend": True
                }
            }, 
            use_container_width=True
        )

        st.dataframe(models_df, use_container_width=True, hide_index=True)
    else:
        st.info("No model type data available.")

def display_snowflake_connection_sidebar():
    """Display Snowflake connection form in sidebar"""
    st.sidebar.markdown("---")
    
    # First, check connection status to determine if we should show Database & Schema
    connection_established = False
    
    # Determine connection status
    if IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False):
        # Test the connection to make sure it works
        try:
            test_query = "SELECT CURRENT_USER() as current_user"
            test_result = execute_snowflake_query(test_query)
            if test_result is not None and not test_result.empty:
                connection_established = True
        except Exception:
            pass
    elif st.session_state.get("snowflake_connected"):
        connection_established = True

    # Database and Schema selection (show first if connected)
    if connection_established:
        
        # Initialize session state for database and schema if not exists
        if 'selected_database' not in st.session_state or 'selected_schema' not in st.session_state:
            # Get current session database and schema
            current_db, current_schema = get_current_session_info()
            st.session_state.selected_database = current_db
            st.session_state.selected_schema = current_schema
        
        # Get available databases
        with st.spinner("Loading databases..."):
            databases = get_available_databases()
        
        if databases:
            # Database selection
            current_db_index = 0
            if st.session_state.selected_database in databases:
                current_db_index = databases.index(st.session_state.selected_database)
            
            selected_database = st.sidebar.selectbox(
                "📊 Database",
                databases,
                index=current_db_index,
                key="database_selector"
            )
            
            # Update session state if database changed
            if selected_database != st.session_state.selected_database:
                st.session_state.selected_database = selected_database
                # Reset schema selection when database changes
                st.session_state.selected_schema = None
                st.rerun()
            
            # Get available schemas for selected database
            with st.spinner(f"Loading schemas for {selected_database}..."):
                schemas = get_available_schemas(selected_database) if selected_database else []
            
            if schemas:
                # Schema selection
                current_schema_index = 0
                if st.session_state.selected_schema and st.session_state.selected_schema in schemas:
                    current_schema_index = schemas.index(st.session_state.selected_schema)
                elif 'RESULTS' in schemas:
                    current_schema_index = schemas.index('RESULTS')
                    st.session_state.selected_schema = 'RESULTS'
                elif schemas:
                    st.session_state.selected_schema = schemas[0]
                
                selected_schema = st.sidebar.selectbox(
                    "📋 Schema (or selected iteration)",
                    schemas,
                    index=current_schema_index,
                    key="schema_selector"
                )
                
                # Update session state if schema changed
                if selected_schema != st.session_state.selected_schema:
                    st.session_state.selected_schema = selected_schema
                    # Clear violations cache when schema changes
                    keys_to_remove = [key for key in st.session_state.keys() if isinstance(key, str) and key.startswith('violations_')]
                    for key in keys_to_remove:
                        del st.session_state[key]
                    st.session_state["violations_preloaded"] = False
                    st.rerun()
                
            else:
                st.sidebar.warning(f"No schemas found in {selected_database}")
        else:
            st.sidebar.warning("No databases found")
        
        st.sidebar.markdown("---")

    # Snowflake Connection section (now appears second)
    st.sidebar.subheader("🏔️ Snowflake Connection")

    # Debug information (can be hidden in production)
    if st.sidebar.checkbox("Show Debug Info", value=False):
        st.sidebar.text(f"IS_SNOWFLAKE_NATIVE: {IS_SNOWFLAKE_NATIVE}")
        st.sidebar.text(f"SNOWFLAKE_STREAMLIT: {os.environ.get('SNOWFLAKE_STREAMLIT', 'Not set')}")
        st.sidebar.text(f"SNOWFLAKE_HOST: {os.environ.get('SNOWFLAKE_HOST', 'Not set')}")
        st.sidebar.text(f"SNOWFLAKE_ACCOUNT: {os.environ.get('SNOWFLAKE_ACCOUNT', 'Not set')}")
        st.sidebar.text(f"STREAMLIT_SERVER_PORT: {os.environ.get('STREAMLIT_SERVER_PORT', 'Not set')}")

    if IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False):
        if st.session_state.get('force_snowflake_native', False):
            st.sidebar.warning("🧪 Testing: Snowflake Native Mode (Forced)")
        else:
            st.sidebar.success("✅ Connected via Snowflake Native App session")
        st.sidebar.info("Manual connection is disabled in Snowflake Native Apps.")
        
        # Test the connection to make sure it works
        try:
            test_query = "SELECT CURRENT_USER() as current_user"
            test_result = execute_snowflake_query(test_query)
            if test_result is not None and not test_result.empty:
                current_user = test_result.iloc[0]['CURRENT_USER']
                st.sidebar.info(f"Connected as: {current_user}")
                connection_established = True
            else:
                st.sidebar.warning("⚠️ Connection test failed - please check your Snowflake connection configuration.")
                st.sidebar.info("Debug: Trying to connect with st.connection('snowflake')")
        except Exception as e:
            st.sidebar.error(f"Connection test error: {str(e)}")
            st.sidebar.error("Please ensure your Streamlit app has access to Snowflake and the required database/schema.")
            # Show more detailed debug info
            st.sidebar.info(f"Debug - IS_SNOWFLAKE_NATIVE: {IS_SNOWFLAKE_NATIVE}")
            st.sidebar.info("Debug: Make sure you have configured the Snowflake connection in your app.")

    elif st.session_state.get("snowflake_connected"):
        connection_type = "🔐 Secrets Configuration" if st.session_state.get("secrets_connected") else "🔧 Manual Configuration"
        st.sidebar.success(f"✅ Connected ({connection_type})")
        if st.sidebar.button("Disconnect"):
            if st.session_state.snowflake_conn:
                st.session_state.snowflake_conn.close()
            st.session_state.snowflake_connected = False
            st.session_state.snowflake_conn = None
            st.session_state.secrets_connected = False
            st.rerun()
        connection_established = True
    else:
        st.sidebar.info("Connect to view violations data")
        
        # Check if secrets.toml is configured
        secrets_available = False
        try:
            if "connections" in st.secrets and "snowflake" in st.secrets.connections:
                secrets_available = True
                st.sidebar.info("📋 Found secrets.toml configuration")
                if st.sidebar.button("🔐 Connect using secrets.toml"):
                    if try_secrets_connection():
                        dismissible_success("Connected using secrets.toml!", key="secrets_connection_success")
                        st.rerun()
        except:
            pass
        
        # Display secrets error if any
        if hasattr(st.session_state, 'secrets_error'):
            st.sidebar.error(f"🔍 Secrets issue: {st.session_state.secrets_error}")
            st.sidebar.markdown("---")
        
        # Manual connection form
        with st.sidebar.expander("🔧 Manual Connection", expanded=not secrets_available):
            # Authentication method selection
            auth_method = st.selectbox(
                "Authentication Method",
                ["password", "externalbrowser", "key-pair", "oauth"],
                help="Choose your preferred authentication method"
            )
            
            # Basic connection parameters
            account = st.text_input("Account", key="sf_account", help="your-account.snowflakecomputing.com")
            user = st.text_input("User", key="sf_user")
            warehouse = st.text_input("Warehouse", value="COMPUTE_WH", key="sf_warehouse")
            database = st.text_input("Database", value="DBT_SOURCE_PROJECT_EVAL", key="sf_database")
            schema = st.text_input("Schema", value="RESULTS", key="sf_schema")
            role = st.text_input("Role (optional)", key="sf_role", help="Leave empty to use default role")
            
            # Authentication-specific fields
            auth_params = {
                "warehouse": warehouse,
                "database": database,
                "schema": schema,
                "role": role if role else None
            }
            
            if auth_method == "password":
                password = st.text_input("Password", type="password", key="sf_password")
                auth_params["password"] = password
                connect_enabled = all([account, user, password, warehouse, database, schema])
                
            elif auth_method == "externalbrowser":
                st.info("🌐 External browser authentication (SSO)")
                st.caption("Your browser will open for authentication when connecting.")
                connect_enabled = all([account, user, warehouse, database, schema])
                
            elif auth_method == "key-pair":
                st.info("🔑 Key-pair authentication")
                private_key_content = st.text_area(
                    "Private Key Content", 
                    key="sf_private_key",
                    help="Paste your private key content (PEM format)",
                    placeholder="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
                )
                private_key_passphrase = st.text_input(
                    "Private Key Passphrase (optional)", 
                    type="password", 
                    key="sf_key_passphrase"
                )
                auth_params["private_key_content"] = private_key_content
                auth_params["private_key_passphrase"] = private_key_passphrase if private_key_passphrase else None
                connect_enabled = all([account, user, private_key_content, warehouse, database, schema])
                
            elif auth_method == "oauth":
                st.info("🎫 OAuth authentication")
                token = st.text_input("OAuth Token", type="password", key="sf_oauth_token")
                auth_params["token"] = token
                connect_enabled = all([account, user, token, warehouse, database, schema])
            
            # Connect button
            if st.button("Connect to Snowflake", disabled=not connect_enabled, use_container_width=True):
                if connect_to_snowflake_manual(account, user, auth_method, **auth_params):
                    dismissible_success("Connected successfully!", key="manual_connection_success")
                    st.rerun()
                else:
                    dismissible_error("Connection failed. Please check your credentials.", key="manual_connection_failed")
            
            if not connect_enabled:
                st.caption("⚠️ Please fill in all required fields to connect.")
        
        # Manual override toggle for testing Snowflake Native behavior
        if st.sidebar.checkbox("🧪 Force Snowflake Native Mode (Testing)", value=False, help="Enable this to test Snowflake Native behavior locally"):
            st.session_state['force_snowflake_native'] = True
            st.sidebar.info("⚠️ Snowflake Native mode forced for testing")
        else:
            st.session_state['force_snowflake_native'] = False

    return connection_established

def render_markdown_with_images(md_text):
    """Render markdown and display images using st.image for local images with { width=... }, preserving image position."""
    image_pattern = r'!\[([^\]]*)\]\((images/[^)]+)\)(\{[^}]*\})?'
    def image_replacer(match):
        alt_text = match.group(1)
        img_path = match.group(2)
        attr = match.group(3)
        width = None
        if attr:
            width_match = re.search(r'width\s*=\s*(\d+)', attr)
            if width_match:
                width = int(width_match.group(1))
        # Render image and return a unique placeholder
        placeholder = f"[[IMAGE_PLACEHOLDER_{hash(img_path)}]]"
        st.session_state[placeholder] = (img_path, alt_text, width)
        return placeholder
    # Replace images with placeholders
    md_text_with_placeholders = re.sub(image_pattern, image_replacer, md_text)
    # Split by placeholders and render in order
    parts = re.split(r'(\[\[IMAGE_PLACEHOLDER_\-?\d+\]\])', md_text_with_placeholders)
    for part in parts:
        img_match = re.match(r'\[\[IMAGE_PLACEHOLDER_\-?\d+\]\]', part)
        if img_match and part in st.session_state:
            img_path, alt_text, width = st.session_state[part]
            st.image(img_path, caption=alt_text, width=width)
        else:
            if part.strip():
                st.markdown(part, unsafe_allow_html=True)

def display_rule_viewer(rule_data: Dict[str, str], rule_key: str) -> None:
    """Display rule in read-only viewer mode"""
    # Create tabs for different content sections
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Description", "Example", "Exception", "Reason to Flag", "Remediation", "Violations"])
    
    with tab1:
        st.markdown("### Description")
        render_markdown_with_images(rule_data.get("description", "No description available"))
    
    with tab2:
        st.markdown("### Example")
        render_markdown_with_images(rule_data.get("example", "No example available"))
    
    with tab3:
        st.markdown("### Exception")
        render_markdown_with_images(rule_data.get("exception", "No exception specified"))
    
    with tab4:
        st.markdown("### Reason to Flag")
        render_markdown_with_images(rule_data.get("reason_to_flag", "No reason specified"))
    
    with tab5:
        st.markdown("### Remediation")
        render_markdown_with_images(rule_data.get("remediation", "No remediation steps available"))
    
    with tab6:
        
        if not (st.session_state.get("snowflake_connected") or IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False)):
            st.info("Connect to Snowflake in the sidebar to view violations data")
        else:
            # Create the query
            query = f"SELECT * FROM {fq_table(rule_key)}"
            
            # Auto-load violations if not already loaded
            if f"violations_{rule_key}" not in st.session_state:
                with st.spinner("Loading violations..."):
                    df = execute_snowflake_query(query)
                    if df is not None:
                        st.session_state[f"violations_{rule_key}"] = df
            
            # Show query and refresh button
            col1, col2 = st.columns([1, 4])
            with col1:
                if st.button(f"🔄 Refresh", key=f"refresh_{rule_key}", use_container_width=True):
                    with st.spinner("Refreshing violations data..."):
                        df = execute_snowflake_query(query)
                        if df is not None:
                            st.session_state[f"violations_{rule_key}"] = df
            
            with col2:
                st.code(query, language="sql")
            
            # Display results if available
            if f"violations_{rule_key}" in st.session_state:
                df = st.session_state[f"violations_{rule_key}"]
                
                if df.empty:
                    st.success("🎉 No violations found for this rule!")
                else:
                    st.warning(f"⚠️ Found {len(df)} violation(s)")
                                        
                    # Display the data table
                    st.dataframe(
                        df, 
                        use_container_width=True,
                        height=400
                    )
                                    
                    # Add download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"violations_{rule_key}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )

def main():
    """Main application function"""
    # Configure the page
    st.set_page_config(
        page_title="dbt Project Evaluator Rules Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Don't show main header for dashboard (it has its own)
    if st.session_state.get('nav_mode') != "Dashboard":
        st.markdown('<h1 class="main-header">📊 dbt Project Evaluator Results</h1>', unsafe_allow_html=True)
    
    # Initialize Snowflake connection early
    init_snowflake_connection()
    
    # Load data
    rules_data = load_rules_data()
    
    if not rules_data:
        st.error("No rules data available. Please ensure the JSON file exists.")
        return
    
    # Sidebar for main navigation
    st.sidebar.title("Navigation")
        
    # Initialize session state for navigation if not exists
    if 'nav_mode' not in st.session_state:
        st.session_state.nav_mode = "Dashboard"  # Default to dashboard
    
    # Handle mode override from Edit button (remove edit functionality)
    if 'mode_override' in st.session_state:
        # Only allow View Rules mode override
        if st.session_state['mode_override'] == "View Rules":
            st.session_state.nav_mode = st.session_state['mode_override']
        del st.session_state['mode_override']
    
    # Navigation - Category menu items
    categories = list(rules_data.keys())
    
    # Initialize selected_category from session state or default to first category
    if 'selected_category' not in st.session_state and categories:
        st.session_state.selected_category = categories[0]
    
    selected_category = None
    category_rules = {}
    
    if categories:
        # Dashboard button at the top
        if st.sidebar.button("📊 Overview Dashboard", use_container_width=True, 
                            type="primary" if st.session_state.nav_mode == "Dashboard" else "secondary"):
            st.session_state.nav_mode = "Dashboard"
            st.rerun()
        
        st.sidebar.markdown("### Categories")
        
        # Map categories to appropriate emojis
        category_emojis = {
            "Modeling": "🏗️",
            "Testing": "🧪", 
            "Structure": "🏛️",
            "Documentation": "📚",
            "Governance": "⚖️",
            "Performance": "⚡"
        }
        
        # Create buttons for each category
        for category in categories:
            emoji = category_emojis.get(category, "📁")  # Default to folder if not mapped
            button_type = "primary" if st.session_state.get('selected_category') == category else "secondary"
            
            # Custom CSS for left-aligned button text
            st.markdown("""
            <style>
            div[data-testid="stSidebar"] .stButton > button {
                text-align: left !important;
                justify-content: flex-start !important;
            }
            </style>
            """, unsafe_allow_html=True)
            
            if st.sidebar.button(f"{emoji} {category}", use_container_width=True, 
                                key=f"nav_{category}",
                                type=button_type):
                st.session_state.selected_category = category
                st.session_state.nav_mode = "View Rules"
                st.rerun()
        
        if st.sidebar.button("⚙️ Rule Settings", use_container_width=True):
            st.session_state.nav_mode = "Rule Settings"
            st.rerun()
        
        # Get current selection
        selected_category = st.session_state.get('selected_category')
        
        if not selected_category:
            st.error("No categories available")
            return
        
        # Get rules for selected category (only when not in Rule Settings)
        if st.session_state.nav_mode != "Rule Settings":
            category_rules = rules_data.get(selected_category, {}).get("rules", {})
    
    # Get current mode
    mode = st.session_state.nav_mode
    
    # Add Snowflake connection sidebar
    snowflake_connected = display_snowflake_connection_sidebar()
    
    # For Snowflake Native, always consider connected
    if IS_SNOWFLAKE_NATIVE or st.session_state.get('force_snowflake_native', False):
        snowflake_connected = True
    
    # Preload violations if connected to Snowflake (only when not in Rule Settings or Dashboard needs them)
    if snowflake_connected and st.session_state.nav_mode in ["View Rules", "Dashboard"]:
        preload_violations_for_rules(rules_data)
    
    # Display category information for View Rules mode (skip for Rule Settings and Dashboard)
    if st.session_state.nav_mode == "View Rules" and selected_category:
        st.markdown(f'<h2 class="category-header">{selected_category} Rules</h2>', unsafe_allow_html=True)
    
    # Mode-specific functionality
    if st.session_state.nav_mode == "Dashboard":
        display_dashboard_overview(rules_data)
        
    elif st.session_state.nav_mode == "View Rules":
        if not category_rules:
            st.info(f"No rules available in the {selected_category} category.")
            return
        
        # Display all rules in collapsible panels
        for rule_key, rule_data in category_rules.items():
            rule_title = get_rule_status_emoji(rule_key, rule_data.get('name', rule_key))
            with st.expander(rule_title, expanded=False):
                display_rule_viewer(rule_data, rule_key)
      
    elif st.session_state.nav_mode == "Rule Settings":
        st.subheader("Rule Settings - Export/Import")
        
        tab1, tab2 = st.tabs(["Export", "Import"])
        
        with tab1:
            st.markdown("### Export Rules")
            
            categories = list(rules_data.keys())
            export_options = st.multiselect(
                "Select categories to export",
                categories,
                default=categories
            )
            
            if export_options:
                # Create filtered data for export
                export_data = {cat: rules_data[cat] for cat in export_options if cat in rules_data}
                
                # Convert to JSON string
                json_string = json.dumps(export_data, indent=2, ensure_ascii=False)
                
                st.download_button(
                    label="Download JSON",
                    data=json_string,
                    file_name=f"dbt_rules_export_{'-'.join(export_options)}.json",
                    mime="application/json",
                    use_container_width=True
                )
                
                with st.expander("Preview Export Data"):
                    st.json(export_data)
        
        with tab2:
            st.markdown("### Import Rules")
            st.warning("⚠️ Importing will overwrite existing rules with the same keys!")
            
            uploaded_file = st.file_uploader("Choose JSON file", type="json")
            
            if uploaded_file is not None:
                try:
                    # Read the uploaded file
                    stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
                    import_data = json.load(stringio)
                    
                    st.success("File loaded successfully!")
                    
                    # Preview import data
                    with st.expander("Preview Import Data"):
                        st.json(import_data)
                    
                    # Import options
                    merge_option = st.radio(
                        "Import Mode",
                        ["Merge (add new rules, update existing)", "Replace (overwrite all data)"]
                    )
                    
                    if st.button("Import Rules", type="primary"):
                        if merge_option == "Replace (overwrite all data)":
                            rules_data.clear()
                            rules_data.update(import_data)
                        else:
                            # Merge mode
                            for category, category_data in import_data.items():
                                if category not in rules_data:
                                    rules_data[category] = {"rules": {}}
                                if "rules" not in rules_data[category]:
                                    rules_data[category]["rules"] = {}
                                
                                rules_data[category]["rules"].update(category_data.get("rules", {}))
                        
                        if save_rules_data(rules_data):
                            st.success("Rules imported successfully!")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("Failed to import rules.")
                
                except json.JSONDecodeError:
                    st.error("Invalid JSON file format!")
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")
        
        st.markdown("---")
        if st.button("Regenerate Rule Settings", key="regen_rule_settings", type="primary"):
            import subprocess
            result = subprocess.run(["python", "scripts/rule_extractor.py"], capture_output=True, text=True)
            if result.returncode == 0:
                st.success("Rule settings regenerated successfully!")
                st.code(result.stdout)
            else:
                st.error("Failed to regenerate rule settings.")
                st.code(result.stderr)
    
    # Footer
    st.markdown("---")
    st.markdown("""
<div style='text-align: center; color: #666;'>
  <p>DBT Project Evaluator Streamlit App | by Edgar Taboada<br/>
  Monitor your dbt project quality and maintain best practices compliance<br/>
  Learn more: <a href="https://dbt-labs.github.io/dbt-project-evaluator/1.0/" target="_blank" style="color: #4A90E2; text-decoration: underline;">dbt Project Evaluator Documentation</a>
  </p>
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()