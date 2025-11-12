# import os
# from dotenv import load_dotenv
# import streamlit as st
# import oracledb
# import pandas as pd
# import datetime
# import io 
# from typing import List, Dict, Any, Optional

# # ====================================================================
# # CONFIGURATION & QUERIES
# # ====================================================================
# load_dotenv()
# # --- Database Credentials ---
# DB_USER = os.getenv('DB_USER')
# DB_PASSWORD = os.getenv('DB_PASSWORD')
# DB_HOST = os.getenv('DB_HOST')
# DB_PORT = os.getenv('DB_PORT')
# DB_SERVICE_NAME = os.getenv('DB_SERVICE_NAME')

# # Status value to exclude from the results (e.g., 'P' for Pending/Processing)
# STATUS_TO_EXCLUDE = 'P'

# SCHEDULING_QUERIES = [
#     {
#         "name": "RAADW-SCHED",
#         "description": "Scheduled/Recurring Jobs",
#         "sql": f"""
#             SELECT
#                 'RAADW-SCHED' APPLICATION_SERVER,
#                 'ADW' APPLICAITON,
#                 XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
#                 TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
#                 TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
#                 XMLP.REPORT_URL,
#                 XMLP.ISSUER,
#                 XMLP.OWNER,
#                 XMLP.JOB_ID JOB_ID,
#                 XMLP.CREATED,
#                 XMLP.LAST_UPDATED,
#                 XMLP.START_DATE,
#                 XMLP.END_DATE,
#                 XMLP.JOB_GROUP,
#                 XMLP.SCHEDULE_DESCRIPTION,
#                 XMLP.USER_DESCRIPTION,
#                 XMLP.DELIVERY_DESCRIPTION
#             FROM SCHED_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
#             INNER JOIN SCHED_BIPLATFORM.XMLP_SCHED_JOB XMLP
#                 ON QRTZ.JOB_NAME = XMLP.JOB_ID
#             WHERE 1 = 1
#             AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
#             ORDER BY 1,2,3
#         """
#     },
#     {
#         "name": "RAADW-ADHOC",
#         "description": "Ad-Hoc Jobs",
#         "sql": f"""
#             SELECT
#                 'RAADW-ADHOC' APPLICATION_SERVER,
#                 'ADW' APPLICAITON,
#                 XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
#                 TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
#                 TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
#                 XMLP.REPORT_URL,
#                 XMLP.ISSUER,
#                 XMLP.OWNER,
#                 XMLP.JOB_ID JOB_ID,
#                 XMLP.CREATED,
#                 XMLP.LAST_UPDATED,
#                 XMLP.START_DATE,
#                 XMLP.END_DATE,
#                 XMLP.JOB_GROUP,
#                 XMLP.SCHEDULE_DESCRIPTION,
#                 XMLP.USER_DESCRIPTION,
#                 XMLP.DELIVERY_DESCRIPTION
#             FROM ADHOC_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
#             INNER JOIN ADHOC_BIPLATFORM.XMLP_SCHED_JOB XMLP
#                 ON QRTZ.JOB_NAME = XMLP.JOB_ID
#             WHERE 1 = 1
#             AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
#             ORDER BY 1,2,3
#         """
#     },
#     {
#         "name": "RAADW-APP",
#         "description": "Application & Production Jobs (UNION)",
#         "sql": f"""
#             SELECT
#                 'RAADW-APP' APPLICATION_SERVER,
#                 'ADW' APPLICAITON,
#                 XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
#                 TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
#                 TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
#                 XMLP.REPORT_URL,
#                 XMLP.ISSUER,
#                 XMLP.OWNER,
#                 XMLP.JOB_ID JOB_ID,
#                 XMLP.CREATED,
#                 XMLP.LAST_UPDATED,
#                 XMLP.START_DATE,
#                 XMLP.END_DATE,
#                 XMLP.JOB_GROUP,
#                 XMLP.SCHEDULE_DESCRIPTION,
#                 XMLP.USER_DESCRIPTION,
#                 XMLP.DELIVERY_DESCRIPTION
#             FROM RAAPP_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
#             INNER JOIN RAAPP_BIPLATFORM.XMLP_SCHED_JOB XMLP
#                 ON QRTZ.JOB_NAME = XMLP.JOB_ID
#             WHERE 1 = 1
#             AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
#             UNION ALL
#             SELECT
#                 'RAADW-APP' APPLICATION_SERVER,
#                 'ADW' APPLICAITON,
#                 XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
#                 TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
#                 TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
#                 XMLP.REPORT_URL,
#                 XMLP.ISSUER,
#                 XMLP.OWNER,
#                 XMLP.JOB_ID JOB_ID,
#                 XMLP.CREATED,
#                 XMLP.LAST_UPDATED,
#                 XMLP.START_DATE,
#                 XMLP.END_DATE,
#                 XMLP.JOB_GROUP,
#                 XMLP.SCHEDULE_DESCRIPTION,
#                 XMLP.USER_DESCRIPTION,
#                 XMLP.DELIVERY_DESCRIPTION
#             FROM PRD_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
#             INNER JOIN PRD_BIPLATFORM.XMLP_SCHED_JOB XMLP
#                 ON QRTZ.JOB_NAME = XMLP.JOB_ID
#             WHERE 1 = 1
#             AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
#         """
#     },
# ]

# # --- Initialize THICK MODE (if necessary) ---
# try:
#     oracledb.init_oracle_client()
# except oracledb.Error as e:
#     st.warning(f"Warning: Could not initialize oracledb thick mode: {e}. Running in THIN mode.")


# # ====================================================================
# # CACHED FUNCTIONS
# # ====================================================================

# @st.cache_resource(ttl=3600)
# def connect_to_oracle() -> Optional[oracledb.Connection]:
#     """Attempts to establish a connection to the Oracle database."""
#     try:
#         dsn = oracledb.makedsn(host=DB_HOST, port=DB_PORT, service_name=DB_SERVICE_NAME)
#         connection = oracledb.connect(
#             user=DB_USER,
#             password=DB_PASSWORD,
#             dsn=dsn
#         )
#         return connection
#     except oracledb.Error as error:
#         st.error(f"❌ Database Connection Error: {error}")
#         return None

# @st.cache_data(show_spinner="Executing Query...", ttl=600)
# def run_query(_connection: oracledb.Connection, sql: str) -> Optional[pd.DataFrame]:
#     """Executes a single SQL query and returns the result as a pandas DataFrame."""
#     if not _connection:
#         return None
#     try:
#         df = pd.read_sql(sql, con=_connection)
#         return df
#     except Exception as e:
#         st.error(f"❌ Error executing query: {e}")
#         return None

# def combine_all_queries(connection: oracledb.Connection) -> Optional[pd.DataFrame]:
#     """Executes all defined queries and concatenates the results into a single DataFrame."""
#     all_dataframes: List[pd.DataFrame] = []
    
#     with st.spinner("Running ALL queries and combining results..."):
#         for config in SCHEDULING_QUERIES:
#             name = config['name']
#             sql = config['sql']
            
#             st.info(f"Executing: **{name}**")
#             df = run_query(connection, sql) 
            
#             if df is not None and not df.empty:
#                 df['SOURCE_QUERY'] = name 
#                 all_dataframes.append(df)
#             elif df is None:
#                  st.warning(f"Skipped **{name}** due to execution error.")

#     if all_dataframes:
#         combined_df = pd.concat(all_dataframes, ignore_index=True)
#         return combined_df
#     return None

# @st.cache_data
# def convert_df_to_excel(df: pd.DataFrame) -> bytes:
#     """Converts a pandas DataFrame to an Excel (xlsx) file in memory."""
#     output = io.BytesIO()
#     df_export = df.astype(str)
#     with pd.ExcelWriter(output, engine='openpyxl') as writer:
#         df_export.to_excel(writer, sheet_name='Query_Results', index=False)
#     return output.getvalue()

# # ====================================================================
# # SESSION STATE & FILTERING HELPERS (CORRECTED)
# # ====================================================================

# # Initialize filter state
# def initialize_filter_state():
#     if 'run_query' not in st.session_state:
#         st.session_state['run_query'] = False
#     if 'filters' not in st.session_state:
#         st.session_state['filters'] = [] # List of {'column': '...', 'value': '...'}
#     if 'filtered_df' not in st.session_state:
#         st.session_state['filtered_df'] = None
#     if 'result_df' not in st.session_state:
#         st.session_state['result_df'] = None

# def add_filter():
#     """Adds a blank filter condition to the session state."""
#     # Ensure a default placeholder column is set for the new filter
#     if st.session_state.get('result_df') is not None and not st.session_state['result_df'].empty:
#         df_columns = st.session_state['result_df'].columns.tolist()
#         initial_column = df_columns[0] if df_columns else None
#     else:
#         initial_column = "-- Select Column --"
        
#     st.session_state['filters'].append({'column': initial_column, 'value': ''})
#     st.session_state['filtered_df'] = None 

# def remove_filter(index):
#     """Removes a filter condition by index."""
#     if index < len(st.session_state['filters']):
#         st.session_state['filters'].pop(index)
#         st.session_state['filtered_df'] = None

# def clear_filters():
#     """Clears all filter conditions."""
#     st.session_state['filters'] = []
#     st.session_state['filtered_df'] = None

# # --- NEW/FIXED CALLBACKS ---
# def update_filter_column(index):
#     """Updates the column name in the filter list based on the selectbox state."""
#     key = f"filter_col_{index}"
#     # Read the new value safely from session state (updated by the widget)
#     if key in st.session_state:
#         st.session_state['filters'][index]['column'] = st.session_state[key]
#         st.session_state['filtered_df'] = None 

# def update_filter_value(index):
#     """Updates the value in the filter list based on the text input state."""
#     key = f"filter_val_{index}"
#     # Read the new value safely from session state (updated by the widget)
#     if key in st.session_state:
#         st.session_state['filters'][index]['value'] = st.session_state[key]
#         st.session_state['filtered_df'] = None 


# def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
#     """Applies all stored filters to the DataFrame."""
#     filtered_df = df.copy()
    
#     for filter_cond in st.session_state.get('filters', []):
#         column = filter_cond.get('column')
#         value = filter_cond.get('value')
        
#         if column == "-- Select Column --":
#             continue
            
#         # Only apply filter if both column and value are valid
#         if column and value and column in filtered_df.columns:
#             try:
#                 # Filter condition is case-insensitive substring match (contains)
#                 filtered_df = filtered_df[
#                     filtered_df[column].astype(str).str.contains(value, case=False, na=False)
#                 ]
#             except Exception as e:
#                 st.warning(f"Could not apply filter on column '{column}' with value '{value}'. Error: {e}")
                
#     st.session_state['filtered_df'] = filtered_df
#     return filtered_df

# # ====================================================================
# # STREAMLIT UI LAYOUT AND LOGIC
# # ====================================================================

# def render_filter_builder(df_columns: List[str]):
#     """Renders the simple filter builder interface."""
    
#     st.subheader("🛠️ Filter Conditions")
    
#     # Add a dummy option for the initial filter state
#     available_columns = ["-- Select Column --"] + df_columns

#     # 3-column layout: Column Name | Value (Sub-string) | Remove
#     header_cols = st.columns([0.4, 0.5, 0.1])
#     header_cols[0].write("**Column Name**")
#     header_cols[1].write("**Value (Contains)**")

#     for i, filter_cond in enumerate(st.session_state['filters']):
#         cols = st.columns([0.4, 0.5, 0.1])
        
#         # Determine initial index for the selectbox
#         try:
#             # Use the stored column name, or default to the placeholder if not found
#             default_index = available_columns.index(filter_cond.get('column'))
#         except ValueError:
#             default_index = 0
#             # Initialize if not set
#             if filter_cond.get('column') is None:
#                 st.session_state['filters'][i]['column'] = available_columns[0]
        
#         # Column Name Dropdown
#         cols[0].selectbox(
#             "Column",
#             options=available_columns,
#             index=default_index,
#             label_visibility="collapsed",
#             key=f"filter_col_{i}",
#             # FIX: Use the dedicated callback and pass the index as an argument
#             on_change=update_filter_column, 
#             args=(i,)
#         )
        
#         # Value Input Text Box
#         cols[1].text_input(
#             "Value",
#             value=filter_cond['value'],
#             label_visibility="collapsed",
#             key=f"filter_val_{i}",
#             help="Enter a substring to filter by (case-insensitive contains match).",
#             # FIX: Use the dedicated callback and pass the index as an argument
#             on_change=update_filter_value, 
#             args=(i,)
#         )

#         # Remove Button
#         cols[2].button("🗑️", key=f"remove_filter_{i}", on_click=remove_filter, args=(i,))

#     # Add/Clear buttons
#     st.markdown("---")
#     col_add, col_clear = st.columns([0.2, 0.8])
#     col_add.button("➕ Add Condition", on_click=add_filter)
#     col_clear.button("❌ Clear All Filters", on_click=clear_filters)
    
#     st.markdown("---")


# def main_app():
#     st.set_page_config(layout="wide", page_title="Oracle Query App")
#     st.title("📊 Oracle Database Query & Visualization")
    
#     initialize_filter_state()
    
#     # 1. Database Connection Status
#     connection = connect_to_oracle()

#     if connection is None:
#         st.warning("Database connection failed. Please check credentials and configuration.")
#         return

#     st.success("Database Connected!")
#     st.markdown("---")

#     # 2. Query Selection & Input
#     query_options = [q['name'] for q in SCHEDULING_QUERIES]
#     query_options.insert(0, "ALL_QUERIES_COMBINED")
#     query_options.insert(1, "CUSTOM_SQL_QUERY") 

#     selected_query_name = st.selectbox(
#         "**1. Select a Query Source:**", 
#         options=query_options,
#         index=0,
#         key='query_source_select',
#         help="Select a pre-defined report, run all, or enter a custom query."
#     )

#     custom_sql = None
#     if selected_query_name == "CUSTOM_SQL_QUERY":
#         st.subheader("✍️ Enter Custom SQL")
#         custom_sql = st.text_area(
#             "Enter your Oracle SQL Query here (SELECT statements recommended):",
#             value="SELECT APPLICATION_SERVER, COUNT(*) FROM DUAL GROUP BY APPLICATION_SERVER",
#             height=200,
#             key='custom_sql_input'
#         )
        
#     st.markdown("---")

#     # 3. Execution Button
#     if st.button("🚀 Run Query", type="primary"):
#         st.session_state['run_query'] = True
#         st.session_state['filtered_df'] = None # Clear old filters
#         st.session_state['result_df'] = None  # Clear old results
#         st.session_state['filters'] = [] # Clear filter conditions for new query
        
#         if selected_query_name == "CUSTOM_SQL_QUERY":
#             st.session_state['current_sql'] = custom_sql
#         else:
#              st.session_state['current_sql'] = None

#     # --- MAIN EXECUTION BLOCK ---
#     if st.session_state.get('run_query'):
        
#         final_df = st.session_state.get('result_df') # Try to get cached result
        
#         if final_df is None:
#             # 4. Execute and Retrieve Results
#             sql_to_execute = None
#             if selected_query_name == "CUSTOM_SQL_QUERY":
#                 sql_to_execute = st.session_state.get('current_sql')
#                 st.header("Results for: **Custom SQL Query**")
#             elif selected_query_name == "ALL_QUERIES_COMBINED":
#                 final_df = combine_all_queries(connection) 
#                 st.header(f"Results for: **{selected_query_name}**")
#             else:
#                 query_config = next((q for q in SCHEDULING_QUERIES if q['name'] == selected_query_name), None)
#                 if query_config:
#                     sql_to_execute = query_config['sql']
#                     st.header(f"Results for: **{selected_query_name}**")

#             if sql_to_execute and not final_df:
#                 final_df = run_query(connection, sql_to_execute) 
            
#             st.session_state['result_df'] = final_df # Store original result

#         # -----------------------------------------------
#         # 5. Data Filtering
#         # -----------------------------------------------
#         if st.session_state.get('result_df') is not None and not st.session_state['result_df'].empty:
            
#             original_df = st.session_state['result_df']
#             df_columns = original_df.columns.tolist()
            
#             st.subheader(f"Data Table ({len(original_df)} total rows)")

#             # Render the filter builder
#             if df_columns:
#                 with st.expander("🔬 **Build Filters** (Click to define conditions)"):
#                     render_filter_builder(df_columns)
                    
#                     # Apply Filters Button
#                     # Using a simple button triggers a rerun and applies the stored filters
#                     if st.button("✅ Apply Filters", type="secondary"):
#                         # Forces filtering logic to run and updates filtered_df in session state
#                         st.session_state['filtered_df'] = apply_filters(original_df)
            
#             # Use filtered_df if filters have been applied and stored, otherwise use the original result
#             display_df = st.session_state.get('filtered_df')
#             if display_df is None:
#                 display_df = original_df
            
#             # 6. Display Results
#             if display_df.empty:
#                 st.warning("Filters applied, but no rows match the criteria.")
#             else:
#                 st.subheader(f"Displaying {len(display_df)} rows")
#                 st.dataframe(display_df, use_container_width=True)
                
#                 # 7. Excel Export
#                 st.subheader("⬇️ Export Data")
                
#                 excel_data = convert_df_to_excel(display_df)
                
#                 timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#                 name_for_file = selected_query_name.replace('-', '_').replace(' ', '_')
#                 filename = f"report-{name_for_file}-filtered-{timestamp_str}.xlsx"
                
#                 st.download_button(
#                     label=f"Download Displayed Data as Excel ({len(display_df)} rows)",
#                     data=excel_data,
#                     file_name=filename,
#                     mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#                     help="Downloads the currently displayed (filtered) data."
#                 )
                
#                 st.markdown("---")
                
#                 # 8. Data Visualization (Using display_df)
#                 st.subheader("📈 Data Visualization")
                
#                 for col in ['APPLICATION_SERVER', 'FREQUENCY', 'ISSUER']:
#                     if col in display_df.columns:
#                         try:
#                             chart_data = display_df[col].value_counts().nlargest(10).reset_index()
#                             chart_data.columns = [col, 'Count']
                            
#                             if len(chart_data) > 0:
#                                 st.markdown(f"**Count by {col} (Top {len(chart_data)})**")
#                                 st.bar_chart(chart_data.set_index(col))
#                         except Exception:
#                             pass 
                        
#         elif selected_query_name == "CUSTOM_SQL_QUERY" and (not st.session_state.get('current_sql') or not st.session_state.get('current_sql').strip()):
#              st.error("Cannot run query: Custom SQL field is empty. Please enter a statement.")
#         else:
#              st.warning("No data retrieved.")


# if __name__ == '__main__':
#     main_app()




import os
from dotenv import load_dotenv
import streamlit as st
import oracledb
import pandas as pd
import datetime
import io
from typing import List, Dict, Any, Optional

# ====================================================================
# CONFIGURATION & QUERIES
# ====================================================================
load_dotenv()
# --- Database Credentials ---
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_SERVICE_NAME = os.getenv('DB_SERVICE_NAME')

# --- FIX: Define the Instant Client Path ---
# This environment variable MUST be set in your Streamlit Secrets or .env
# to the absolute path of the Instant Client folder in the deployed environment.
INSTANT_CLIENT_PATH = "./instantclient_23_9"

# Status value to exclude from the results (e.g., 'P' for Pending/Processing)
STATUS_TO_EXCLUDE = 'P'

SCHEDULING_QUERIES = [
    {
        "name": "RAADW-SCHED",
        "description": "Scheduled/Recurring Jobs",
        "sql": f"""
            SELECT
                'RAADW-SCHED' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE,
                XMLP.JOB_GROUP,
                XMLP.SCHEDULE_DESCRIPTION,
                XMLP.USER_DESCRIPTION,
                XMLP.DELIVERY_DESCRIPTION
            FROM SCHED_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN SCHED_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
            ORDER BY 1,2,3
        """
    },
    {
        "name": "RAADW-ADHOC",
        "description": "Ad-Hoc Jobs",
        "sql": f"""
            SELECT
                'RAADW-ADHOC' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE,
                XMLP.JOB_GROUP,
                XMLP.SCHEDULE_DESCRIPTION,
                XMLP.USER_DESCRIPTION,
                XMLP.DELIVERY_DESCRIPTION
            FROM ADHOC_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN ADHOC_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
            ORDER BY 1,2,3
        """
    },
    {
        "name": "RAADW-APP",
        "description": "Application & Production Jobs (UNION)",
        "sql": f"""
            SELECT
                'RAADW-APP' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE,
                XMLP.JOB_GROUP,
                XMLP.SCHEDULE_DESCRIPTION,
                XMLP.USER_DESCRIPTION,
                XMLP.DELIVERY_DESCRIPTION
            FROM RAAPP_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN RAAPP_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
            UNION ALL
            SELECT
                'RAADW-APP' APPLICATION_SERVER,
                'ADW' APPLICAITON,
                XMLP.SCHEDULE_DESCRIPTION FREQUENCY,
                TRIM(XMLP.USER_JOB_NAME) JOB_NAME,
                TO_CHAR(START_DATE, 'HH24:MI:SS') START_TIME,
                XMLP.REPORT_URL,
                XMLP.ISSUER,
                XMLP.OWNER,
                XMLP.JOB_ID JOB_ID,
                XMLP.CREATED,
                XMLP.LAST_UPDATED,
                XMLP.START_DATE,
                XMLP.END_DATE,
                XMLP.JOB_GROUP,
                XMLP.SCHEDULE_DESCRIPTION,
                XMLP.USER_DESCRIPTION,
                XMLP.DELIVERY_DESCRIPTION
            FROM PRD_BIPLATFORM.QRTZ_JOB_DETAILS QRTZ
            INNER JOIN PRD_BIPLATFORM.XMLP_SCHED_JOB XMLP
                ON QRTZ.JOB_NAME = XMLP.JOB_ID
            WHERE 1 = 1
            AND XMLP.STATUS <> '{STATUS_TO_EXCLUDE}'
        """
    },
]

# --- Initialize THICK MODE (if necessary) ---
if INSTANT_CLIENT_PATH and os.path.exists(INSTANT_CLIENT_PATH):
    try:
        # Explicitly initialize thick mode using the defined path
        oracledb.init_oracle_client(lib_dir=INSTANT_CLIENT_PATH)
        st.success("Oracle Thick mode initialized successfully.")
    except oracledb.Error as e:
        # This will catch errors even if the path is set, e.g., missing Linux dependencies (libaio1)
        st.warning(f"Warning: Could not initialize oracledb thick mode: {e}. Running in THIN mode.")
elif INSTANT_CLIENT_PATH:
    # Path was defined but doesn't exist on the file system
    st.error(f"Instant Client path defined in env ({INSTANT_CLIENT_PATH}) but not found. Check deployment.")
    # Attempt fallback initialization (will likely fail with DPI-1047 if environment isn't set)
    try:
        oracledb.init_oracle_client()
    except oracledb.Error as e:
        st.warning(f"Warning: Could not initialize oracledb thick mode (fallback): {e}. Running in THIN mode.")
else:
    # Path was not defined at all. Attempt default initialization (likely DPI-1047)
    try:
        oracledb.init_oracle_client()
    except oracledb.Error as e:
        st.warning(f"Warning: Could not initialize oracledb thick mode: {e}. Running in THIN mode.")


# ====================================================================
# CACHED FUNCTIONS
# ====================================================================

@st.cache_resource(ttl=3600)
def connect_to_oracle() -> Optional[oracledb.Connection]:
    """Attempts to establish a connection to the Oracle database."""
    try:
        # oracledb.makedsn builds the connection string for the driver
        dsn = oracledb.makedsn(host=DB_HOST, port=DB_PORT, service_name=DB_SERVICE_NAME)
        connection = oracledb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=dsn
        )
        return connection
    except oracledb.Error as error:
        st.error(f"❌ Database Connection Error: {error}")
        return None

@st.cache_data(show_spinner="Executing Query...", ttl=600)
def run_query(_connection: oracledb.Connection, sql: str) -> Optional[pd.DataFrame]:
    """Executes a single SQL query and returns the result as a pandas DataFrame."""
    if not _connection:
        return None
    try:
        df = pd.read_sql(sql, con=_connection)
        return df
    except Exception as e:
        st.error(f"❌ Error executing query: {e}")
        return None

def combine_all_queries(connection: oracledb.Connection) -> Optional[pd.DataFrame]:
    """Executes all defined queries and concatenates the results into a single DataFrame."""
    all_dataframes: List[pd.DataFrame] = []
    
    with st.spinner("Running ALL queries and combining results..."):
        for config in SCHEDULING_QUERIES:
            name = config['name']
            sql = config['sql']
            
            st.info(f"Executing: **{name}**")
            df = run_query(connection, sql) 
            
            if df is not None and not df.empty:
                df['SOURCE_QUERY'] = name 
                all_dataframes.append(df)
            elif df is None:
                    st.warning(f"Skipped **{name}** due to execution error.")

    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        return combined_df
    return None

@st.cache_data
def convert_df_to_excel(df: pd.DataFrame) -> bytes:
    """Converts a pandas DataFrame to an Excel (xlsx) file in memory."""
    output = io.BytesIO()
    # Convert all columns to string to prevent mixed-type errors during export
    df_export = df.astype(str) 
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, sheet_name='Query_Results', index=False)
    return output.getvalue()

# ====================================================================
# SESSION STATE & FILTERING HELPERS (CORRECTED)
# ====================================================================

# Initialize filter state
def initialize_filter_state():
    if 'run_query' not in st.session_state:
        st.session_state['run_query'] = False
    if 'filters' not in st.session_state:
        st.session_state['filters'] = [] # List of {'column': '...', 'value': '...'}
    if 'filtered_df' not in st.session_state:
        st.session_state['filtered_df'] = None
    if 'result_df' not in st.session_state:
        st.session_state['result_df'] = None

def add_filter():
    """Adds a blank filter condition to the session state."""
    # Ensure a default placeholder column is set for the new filter
    if st.session_state.get('result_df') is not None and not st.session_state['result_df'].empty:
        df_columns = st.session_state['result_df'].columns.tolist()
        initial_column = df_columns[0] if df_columns else None
    else:
        initial_column = "-- Select Column --"
        
    st.session_state['filters'].append({'column': initial_column, 'value': ''})
    st.session_state['filtered_df'] = None 

def remove_filter(index):
    """Removes a filter condition by index."""
    if index < len(st.session_state['filters']):
        st.session_state['filters'].pop(index)
        st.session_state['filtered_df'] = None

def clear_filters():
    """Clears all filter conditions."""
    st.session_state['filters'] = []
    st.session_state['filtered_df'] = None

# --- NEW/FIXED CALLBACKS ---
def update_filter_column(index):
    """Updates the column name in the filter list based on the selectbox state."""
    key = f"filter_col_{index}"
    # Read the new value safely from session state (updated by the widget)
    if key in st.session_state:
        st.session_state['filters'][index]['column'] = st.session_state[key]
        st.session_state['filtered_df'] = None 

def update_filter_value(index):
    """Updates the value in the filter list based on the text input state."""
    key = f"filter_val_{index}"
    # Read the new value safely from session state (updated by the widget)
    if key in st.session_state:
        st.session_state['filters'][index]['value'] = st.session_state[key]
        st.session_state['filtered_df'] = None 


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Applies all stored filters to the DataFrame."""
    filtered_df = df.copy()
    
    for filter_cond in st.session_state.get('filters', []):
        column = filter_cond.get('column')
        value = filter_cond.get('value')
        
        if column == "-- Select Column --":
            continue
            
        # Only apply filter if both column and value are valid
        if column and value and column in filtered_df.columns:
            try:
                # Filter condition is case-insensitive substring match (contains)
                filtered_df = filtered_df[
                    filtered_df[column].astype(str).str.contains(value, case=False, na=False)
                ]
            except Exception as e:
                st.warning(f"Could not apply filter on column '{column}' with value '{value}'. Error: {e}")
                
    st.session_state['filtered_df'] = filtered_df
    return filtered_df

# ====================================================================
# STREAMLIT UI LAYOUT AND LOGIC
# ====================================================================

def render_filter_builder(df_columns: List[str]):
    """Renders the simple filter builder interface."""
    
    st.subheader("🛠️ Filter Conditions")
    
    # Add a dummy option for the initial filter state
    available_columns = ["-- Select Column --"] + df_columns

    # 3-column layout: Column Name | Value (Sub-string) | Remove
    header_cols = st.columns([0.4, 0.5, 0.1])
    header_cols[0].write("**Column Name**")
    header_cols[1].write("**Value (Contains)**")

    for i, filter_cond in enumerate(st.session_state['filters']):
        cols = st.columns([0.4, 0.5, 0.1])
        
        # Determine initial index for the selectbox
        try:
            # Use the stored column name, or default to the placeholder if not found
            default_index = available_columns.index(filter_cond.get('column'))
        except ValueError:
            default_index = 0
            # Initialize if not set
            if filter_cond.get('column') is None:
                st.session_state['filters'][i]['column'] = available_columns[0]
        
        # Column Name Dropdown
        cols[0].selectbox(
            "Column",
            options=available_columns,
            index=default_index,
            label_visibility="collapsed",
            key=f"filter_col_{i}",
            # FIX: Use the dedicated callback and pass the index as an argument
            on_change=update_filter_column, 
            args=(i,)
        )
        
        # Value Input Text Box
        cols[1].text_input(
            "Value",
            value=filter_cond['value'],
            label_visibility="collapsed",
            key=f"filter_val_{i}",
            help="Enter a substring to filter by (case-insensitive contains match).",
            # FIX: Use the dedicated callback and pass the index as an argument
            on_change=update_filter_value, 
            args=(i,)
        )

        # Remove Button
        cols[2].button("🗑️", key=f"remove_filter_{i}", on_click=remove_filter, args=(i,))

    # Add/Clear buttons
    st.markdown("---")
    col_add, col_clear = st.columns([0.2, 0.8])
    col_add.button("➕ Add Condition", on_click=add_filter)
    col_clear.button("❌ Clear All Filters", on_click=clear_filters)
    
    st.markdown("---")


def main_app():
    st.set_page_config(layout="wide", page_title="Oracle Query App")
    st.title("📊 Oracle Database Query & Visualization")
    
    initialize_filter_state()
    
    # 1. Database Connection Status
    connection = connect_to_oracle()

    if connection is None:
        st.warning("Database connection failed. Please check credentials and configuration.")
        return

    st.success("Database Connected!")
    st.markdown("---")

    # 2. Query Selection & Input
    query_options = [q['name'] for q in SCHEDULING_QUERIES]
    query_options.insert(0, "ALL_QUERIES_COMBINED")
    query_options.insert(1, "CUSTOM_SQL_QUERY") 

    selected_query_name = st.selectbox(
        "**1. Select a Query Source:**", 
        options=query_options,
        index=0,
        key='query_source_select',
        help="Select a pre-defined report, run all, or enter a custom query."
    )

    custom_sql = None
    if selected_query_name == "CUSTOM_SQL_QUERY":
        st.subheader("✍️ Enter Custom SQL")
        custom_sql = st.text_area(
            "Enter your Oracle SQL Query here (SELECT statements recommended):",
            value="SELECT APPLICATION_SERVER, COUNT(*) FROM DUAL GROUP BY APPLICATION_SERVER",
            height=200,
            key='custom_sql_input'
        )
        
    st.markdown("---")

    # 3. Execution Button
    if st.button("🚀 Run Query", type="primary"):
        st.session_state['run_query'] = True
        st.session_state['filtered_df'] = None # Clear old filters
        st.session_state['result_df'] = None  # Clear old results
        st.session_state['filters'] = [] # Clear filter conditions for new query
        
        if selected_query_name == "CUSTOM_SQL_QUERY":
            st.session_state['current_sql'] = custom_sql
        else:
              st.session_state['current_sql'] = None

    # --- MAIN EXECUTION BLOCK ---
    if st.session_state.get('run_query'):
        
        final_df = st.session_state.get('result_df') # Try to get cached result
        
        if final_df is None:
            # 4. Execute and Retrieve Results
            sql_to_execute = None
            if selected_query_name == "CUSTOM_SQL_QUERY":
                sql_to_execute = st.session_state.get('current_sql')
                st.header("Results for: **Custom SQL Query**")
            elif selected_query_name == "ALL_QUERIES_COMBINED":
                final_df = combine_all_queries(connection) 
                st.header(f"Results for: **{selected_query_name}**")
            else:
                query_config = next((q for q in SCHEDULING_QUERIES if q['name'] == selected_query_name), None)
                if query_config:
                    sql_to_execute = query_config['sql']
                    st.header(f"Results for: **{selected_query_name}**")

            if sql_to_execute and not final_df:
                final_df = run_query(connection, sql_to_execute) 
            
            st.session_state['result_df'] = final_df # Store original result

        # -----------------------------------------------
        # 5. Data Filtering
        # -----------------------------------------------
        if st.session_state.get('result_df') is not None and not st.session_state['result_df'].empty:
            
            original_df = st.session_state['result_df']
            df_columns = original_df.columns.tolist()
            
            st.subheader(f"Data Table ({len(original_df)} total rows)")

            # Render the filter builder
            if df_columns:
                with st.expander("🔬 **Build Filters** (Click to define conditions)"):
                    render_filter_builder(df_columns)
                    
                    # Apply Filters Button
                    # Using a simple button triggers a rerun and applies the stored filters
                    if st.button("✅ Apply Filters", type="secondary"):
                        # Forces filtering logic to run and updates filtered_df in session state
                        st.session_state['filtered_df'] = apply_filters(original_df)
            
            # Use filtered_df if filters have been applied and stored, otherwise use the original result
            display_df = st.session_state.get('filtered_df')
            if display_df is None:
                display_df = original_df
            
            # 6. Display Results
            if display_df.empty:
                st.warning("Filters applied, but no rows match the criteria.")
            else:
                st.subheader(f"Displaying {len(display_df)} rows")
                st.dataframe(display_df, use_container_width=True)
                
                # 7. Excel Export
                st.subheader("⬇️ Export Data")
                
                excel_data = convert_df_to_excel(display_df)
                
                timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                name_for_file = selected_query_name.replace('-', '_').replace(' ', '_')
                filename = f"report-{name_for_file}-filtered-{timestamp_str}.xlsx"
                
                st.download_button(
                    label=f"Download Displayed Data as Excel ({len(display_df)} rows)",
                    data=excel_data,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Downloads the currently displayed (filtered) data."
                )
                
                st.markdown("---")
                
                # 8. Data Visualization (Using display_df)
                st.subheader("📈 Data Visualization")
                
                for col in ['APPLICATION_SERVER', 'FREQUENCY', 'ISSUER']:
                    if col in display_df.columns:
                        try:
                            # Plot top 10 values for the categorical column
                            chart_data = display_df[col].value_counts().nlargest(10).reset_index()
                            chart_data.columns = [col, 'Count']
                            
                            if len(chart_data) > 0:
                                st.markdown(f"**Count by {col} (Top {len(chart_data)})**")
                                st.bar_chart(chart_data.set_index(col))
                        except Exception:
                            # Ignore if plotting fails for any reason
                            pass 
                        
        elif selected_query_name == "CUSTOM_SQL_QUERY" and (not st.session_state.get('current_sql') or not st.session_state.get('current_sql').strip()):
              st.error("Cannot run query: Custom SQL field is empty. Please enter a statement.")
        else:
              st.warning("No data retrieved.")


if __name__ == '__main__':
    main_app()