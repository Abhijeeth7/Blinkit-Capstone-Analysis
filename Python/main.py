import pandas as pd
from sqlalchemy import create_engine, text
import mysql.connector  # Using the official MySQL Connector

# --- 1. Configuration ---
# Update these details for your setup

# Excel File Details
EXCEL_FILE_PATH = r"D:\Blinkit_Cap_Project\Excel\BlinkIT Grocery Data.xlsx" # IMPORTANT: Change this to your actual file path
EXCEL_SHEET_NAME = 'BlinkIT Grocery Data'  # Change if your data is on a different sheet, or None for the first sheet

# MySQL Database Details
DB_HOST = '127.0.0.1'  # Your MySQL server host (often 'localhost' or '127.0.0.1')
DB_USER = 'root'  # Your MySQL username (e.g., 'root')
DB_PASSWORD = 'password'  # Your MySQL password
DB_NAME = 'blinkit_capstone_db'  # The database we just created
TABLE_NAME = 'blinkit_sales_data'  # The new table name in your database

# --- 2. Read Excel into Pandas DataFrame ---
print(f"Attempting to read Excel file from: {EXCEL_FILE_PATH} (Sheet: {EXCEL_SHEET_NAME})...")
try:
    df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=EXCEL_SHEET_NAME)
    print(f"Successfully loaded {len(df)} rows from Excel.")
    print("First 5 rows of DataFrame:")
    print(df.head())
    print("\nDataFrame Info (before cleaning):")
    df.info()
except FileNotFoundError:
    print(f"Error: Excel file not found at '{EXCEL_FILE_PATH}'")
    print("Please check the path and filename.")
    exit()
except Exception as e:
    print(f"An error occurred while reading the Excel file: {e}")
    print("Ensure 'openpyxl' is installed (pip install openpyxl) and the sheet name is correct.")
    exit()

# --- 3. Data Cleaning and Preprocessing for SQL Import ---
# This step makes your column names SQL-friendly and handles basic issues.

# Standardize column names (remove spaces, special chars, convert to lowercase)
# Based on your image:
# 'Outlet Establishment Date', 'Outlet Location Type', 'Outlet Type', 'Item MRP',
# 'Item Outlet Sales', 'Item Visibility', 'Item Type', 'Item Weight', 'Sales', 'Rating'
# You might have other columns cut off in the image, add them here
original_columns = df.columns.tolist()
new_columns = []
for col in original_columns:
    # Basic cleaning: replace spaces with underscores, remove non-alphanumeric (except underscore)
    # and convert to lowercase for SQL compatibility.
    cleaned_col = col.strip().replace(' ', '_').replace('.', '').replace('/', '_').lower()
    # Remove any characters that are not alphanumeric or underscore
    cleaned_col = ''.join(c for c in cleaned_col if c.isalnum() or c == '_')
    new_columns.append(cleaned_col)

df.columns = new_columns
print("\nCleaned Column Names:")
print(df.columns.tolist())

# --- Additional Data Cleaning (customize as needed based on your data) ---

# Example: Convert 'Outlet_Establishment_Date' to a proper datetime object if not already
# The pd.read_excel often does this automatically, but it's good to ensure.
if 'outlet_establishment_date' in df.columns:
    df['outlet_establishment_date'] = pd.to_datetime(df['outlet_establishment_date'], errors='coerce')

# Handle duplicate 'Sales' column if it's redundant
# If 'sales' and 'item_outlet_sales' mean the same, drop one.
# For example, if 'sales' is a duplicate of 'item_outlet_sales':
if 'sales' in df.columns and 'item_outlet_sales' in df.columns and (df['sales'].equals(df['item_outlet_sales'])):
    print("\nDropping redundant 'sales' column (appears identical to 'item_outlet_sales').")
    df = df.drop(columns=['sales'])
elif 'sales' in df.columns and 'item_outlet_sales' in df.columns:
    print("\nWarning: Both 'sales' and 'item_outlet_sales' columns exist and are not identical.")
    print("Review your data to understand their meaning and decide which to keep or if both are needed.")
    # For now, we'll keep both, but you might want to drop one or rename them distinctively.

# Handle missing values - Example: fill numeric with 0, or mean/median
# df['item_weight'] = df['item_weight'].fillna(df['item_weight'].mean())
# df['rating'] = df['rating'].fillna(df['rating'].median())
# For simplicity, we'll let pandas' to_sql handle NULLs, but you can preprocess here.

print("\nDataFrame Info (after cleaning):")
df.info()

# --- 4. Establish MySQL Connection ---
print(f"\nAttempting to connect to MySQL database '{DB_NAME}'...")
try:
    engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

    # Test the connection (optional, but good for debugging)
    with engine.connect() as connection:
        # CORRECTED LINE: Use 'text' from sqlalchemy, not pandas
        result = connection.execute(text("SELECT 1")).scalar()
        if result == 1:
            print("Successfully connected to MySQL.")
        else:
            raise Exception("Failed to establish basic connection to MySQL.")

except mysql.connector.Error as err:
    if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
        print("Error: Access denied for your MySQL user or password. Please check credentials.")
    elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
        print(f"Error: Database '{DB_NAME}' does not exist. Please create it first.")
    else:
        print(f"An unexpected MySQL connection error occurred: {err}")
    exit()
except Exception as e:
    print(f"An error occurred during MySQL connection: {e}")
    exit()

# --- 5. Load DataFrame to MySQL Table ---
print(f"\nAttempting to load data into MySQL table '{TABLE_NAME}'...")
try:
    # `if_exists='replace'` will drop the table if it exists and recreate it.
    # `if_exists='append'` will add new rows to an existing table.
    # `index=False` prevents Pandas from writing the DataFrame index as a column in MySQL.

    # You might want to use 'append' after the first successful 'replace' run,
    # or implement checks if the table already exists.
    df.to_sql(name=TABLE_NAME, con=engine, if_exists='replace', index=False, chunksize=1000)
    print(f"\nSuccessfully loaded data into table '{TABLE_NAME}' in database '{DB_NAME}'.")
    print("You can now verify the data in MySQL Workbench or your preferred client.")

except Exception as e:
    print(f"An error occurred while loading data to MySQL: {e}")
finally:
    if 'engine' in locals() and engine:
        engine.dispose()  # Close the SQLAlchemy engine connection
        print("MySQL connection closed.")

