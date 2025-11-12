import oracledb
import os
import pandas as pd
import datetime 
from query import SCHEDULING_QUERIES
from typing import List, Dict, Any, Optional

# ====================================================================
# CONFIGURATION
# ====================================================================

# --- Database Credentials (Replace placeholders) ---
DB_USER = "APP_TEAM"
DB_PASSWORD = "App#22eam"
DB_HOST = "10.5.6.3"
DB_PORT = 1521
DB_SERVICE_NAME = "repoprd_pdb1.raprodprivatesu.raprodvcn.oraclevcn.com"

# --- Report Configuration ---
# Set to the 'name' key of a query (e.g., 'RAADW-B2B') OR 'ALL' to run all queries.
queryNameToUse = "ALL" # <--- CHANGE THIS TO YOUR DESIRED QUERY NAME OR 'ALL'
# ====================================================================

# --- TABLES TO INSPECT FOR COLUMNS ---
TABLES_TO_INSPECT = [
    ("SCHED_BIPLATFORM", "QRTZ_JOB_DETAILS"),
    ("SCHED_BIPLATFORM", "XMLP_SCHED_JOB")
]
# -------------------------------------


# --- Initialize THICK MODE for NNE (Native Network Encryption) support ---
try:
    oracledb.init_oracle_client() 
    print("oracledb initialized in THICK mode for NNE compatibility.")
except oracledb.Error as e:
    print(f"Warning: Could not initialize oracledb thick mode: {e}")
# -------------------------------------------------------------------------


def connect_to_oracle() -> Optional[oracledb.Connection]:
    """Attempts to establish a connection to the Oracle database."""
    connection = None
    try:
        dsn = oracledb.makedsn(host=DB_HOST, port=DB_PORT, service_name=DB_SERVICE_NAME)
        print(f"\nAttempting to connect to the database as user {DB_USER}...")
        connection = oracledb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=dsn
        )
        print("\nDatabase connected successfully!")
        return connection
    except oracledb.Error as error:
        print("\n--- ERROR DURING DATABASE CONNECTION ---")
        print(error)
        print("------------------------------------------")
        return None


def get_all_table_columns(connection: oracledb.Connection, tables: List[tuple]):
    """
    Queries ALL_TAB_COLUMNS view to list all columns for specified tables.
    """
    if not connection:
        return

    print("\n\n=======================================================")
    print("= Starting Table Schema Inspection =")
    print("=======================================================")

    results: Dict[str, List[str]] = {}

    for schema, table in tables:
        full_table_name = f"{schema}.{table}"
        print(f"\n--- Fetching columns for {full_table_name} ---")

        # SQL to retrieve column names, data types, and nullability
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema_name AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, schema_name=schema, table_name=table)
                rows = cursor.fetchall()

                if rows:
                    print(f"✅ Found {len(rows)} columns:")
                    for col_name, data_type, nullable in rows:
                        print(f"  - **{col_name}** ({data_type}, Nullable: {nullable})")
                        if full_table_name not in results:
                            results[full_table_name] = []
                        results[full_table_name].append(col_name)
                else:
                    print(f"❌ Table {full_table_name} not found or no columns retrieved.")

        except oracledb.Error as error:
            print(f"--- ERROR inspecting table {full_table_name}: ---")
            print(error)
            print("---------------------------------------")

    return results


def export_to_excel(dataframes: Dict[str, pd.DataFrame], filename: str):
    """
    Writes a dictionary of DataFrames to a single Excel file.
    Each key in the dictionary becomes a separate sheet.
    """
    if not dataframes:
        print("No data to export.")
        return

    print(f"\nExporting {len(dataframes)} sheet(s) to '{filename}'...")
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, df in dataframes.items():
                # Sheet names in Excel must be less than 31 characters
                safe_sheet_name = sheet_name[:31] 
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        
        print(f"✅ Success: Data exported to '{filename}'")
    except Exception as e:
        print(f"❌ Error during Excel export: {e}")


def run_queries_and_export(connection: oracledb.Connection, query_name_to_use: str, output_filename: str):
    """
    Executes specified queries and collects results as pandas DataFrames.
    Combines all results into a single DataFrame if 'ALL' is used.
    (This function remains the same as in your final code block)
    """
    if not connection:
        return

    is_all_mode = query_name_to_use.upper() == 'ALL'
    
    # 1. Filter the queries based on the configuration variable
    if is_all_mode:
        queries_to_run = SCHEDULING_QUERIES
        print("\nConfig: Running ALL defined scheduling queries.")
    else:
        queries_to_run = [
            q for q in SCHEDULING_QUERIES if q['name'] == query_name_to_use
        ]
        if not queries_to_run:
            print(f"\nError: Query name '{query_name_to_use}' not found in queries.py.")
            return
        print(f"\nConfig: Running single query: '{query_name_to_use}'.")

    # 2. Execute queries and store results
    individual_results: Dict[str, pd.DataFrame] = {}
    all_dataframes: List[pd.DataFrame] = [] 
    
    print("\n=======================================================")
    print("= Starting Query Execution =")
    print("=======================================================")

    with connection.cursor() as cursor:
        for config in queries_to_run:
            name = config['name']
            sql = config['sql']
            
            print(f"\n--- Executing {name} ---")

            try:
                cursor.execute(sql)
                
                column_names = [col[0] for col in cursor.description]
                rows = cursor.fetchall()

                if rows:
                    df = pd.DataFrame(rows, columns=column_names)
                    print(f" Result: Fetched {len(df)} rows.")
                    
                    if is_all_mode:
                        all_dataframes.append(df)
                    else:
                        individual_results[name] = df

                else:
                    print(" Result: No rows returned.")

            except oracledb.Error as error:
                print(f"--- ERROR executing query {name}: ---")
                print(error)
                print("---------------------------------------")

    # 3. Prepare data for export based on mode
    if is_all_mode:
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            print(f"\nConcatenation complete. Total rows: {len(combined_df)}")
            results_for_export = {"ALL_QUERIES_COMBINED": combined_df}
        else:
            results_for_export = {}
    else:
        results_for_export = individual_results

    # 4. Export all collected results to Excel
    export_to_excel(results_for_export, output_filename)


if __name__ == "__main__":
    db_connection = None
    
    # Generate dynamic filename 
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if queryNameToUse.upper() == 'ALL':
        name_for_file = 'ALL_QUERIES_COMBINED'
    else:
        name_for_file = queryNameToUse.replace('-', '_')
        
    OUTPUT_FILENAME = f"report-{name_for_file}-{timestamp_str}.xlsx"
    print(f"Output file name set to: {OUTPUT_FILENAME}")

    try:
        # 1. Establish Connection
        db_connection = connect_to_oracle()

        if db_connection:
            # New Step: Inspect tables for all possible columns
            print("\n--- Listing All Possible Columns in Tables ---")
            get_all_table_columns(db_connection, TABLES_TO_INSPECT)
            print("--- Column Listing Complete ---")
            
            # 2. Run Queries and Export
            run_queries_and_export(db_connection, queryNameToUse, OUTPUT_FILENAME)

    finally:
        # 3. Ensure the connection is closed
        if db_connection:
            db_connection.close()
            print("\nDatabase connection successfully closed.")