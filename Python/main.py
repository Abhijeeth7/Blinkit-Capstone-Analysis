import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError # <-- Correct import for SQLAlchemyError
import mysql.connector
# --- 1. Configuration (same as before) ---
EXCEL_FILE_PATH = r"D:\Blinkit_Cap_Project\Excel\BlinkIT Grocery Data.xlsx"
EXCEL_SHEET_NAME = "BlinkIT Grocery Data"
DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASSWORD = 'password'
DB_NAME = 'blinkit_capstone_db'
TABLE_NAME = 'blinkit_capstone_table'

# Define the path for the cleaned CSV output
CLEANED_CSV_FILE_PATH = r'D:\Blinkit_Cap_Project\Excel\BlinkIT_Grocery_Data_CLEANED.csv'


# --- 2. Read Excel into Pandas DataFrame (same as before) ---
print(f"Attempting to read Excel file from: {EXCEL_FILE_PATH} (Sheet: {EXCEL_SHEET_NAME})...")
try:
    df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=EXCEL_SHEET_NAME)
    print(f"Successfully loaded {len(df)} rows from Excel.")
    na_values = ['#N/A', 'N/A', 'NA', '-', '', ' ']
    # ... (rest of your print statements for df.head(), df.info()) ...
except Exception as e:
    print(f"An error occurred while reading the Excel file: {e}")
    exit()

# --- 3. Data Cleaning and Preprocessing for SQL Import ---

# Standardize column names (same as before)
original_columns = df.columns.tolist()
new_columns = []
for col in original_columns:
    cleaned_col = col.strip().replace(' ', '_').replace('.', '').replace('/', '_').lower()
    cleaned_col = ''.join(c for c in cleaned_col if c.isalnum() or c == '_')
    new_columns.append(cleaned_col)
df.columns = new_columns
print("\nCleaned Column Names:")
print(original_columns)


# --- 3. Initial Data Cleaning (Column Renaming) ---
print("\n--- Standardizing Column Names ---")
original_columns = df.columns.tolist()
cleaned_column_map = {}
for col in original_columns:
    cleaned_col = col.strip().replace(' ', '_').replace('.', '').replace('/', '_').lower()
    cleaned_col = ''.join(c for c in cleaned_col if c.isalnum() or c == '_')
    cleaned_column_map[col] = cleaned_col
df.rename(columns=cleaned_column_map, inplace=True)
print("Cleaned Column Names:")
print(df.columns.tolist())

# --- DIAGNOSTIC STEP: Check item_fat_content before standardization/final null handling ---
print("\n--- Diagnostic Check for item_fat_content before Standardization ---")
if 'item_fat_content' in df.columns:
    print(f"Initial unique values in 'item_fat_content' (post read_excel with na_values): {df['item_fat_content'].unique()}")
    print(f"Nulls in 'item_fat_content' after read_excel (but before standardization): {df['item_fat_content'].isnull().sum()}")

    # Display rows where item_fat_content is NaN right after read_excel
    nan_rows_initial = df[df['item_fat_content'].isnull()]
    if not nan_rows_initial.empty:
        print("\nFirst 5 rows with NaN in 'item_fat_content' (post read_excel):")
        print(nan_rows_initial[['item_identifier', 'item_fat_content']].head())
    else:
        print("No NaN values found in 'item_fat_content' immediately after read_excel.")
else:
    print("Warning: 'item_fat_content' column not found for diagnostic check.")


# --- Standardizing 'item_fat_content' Column ---
print("\n--- Standardizing 'item_fat_content' Column ---")
if 'item_fat_content' in df.columns:
    # Ensure all values are treated as strings for .str.upper() before mapping
    # This also converts existing NaNs to string 'nan' if not caught by na_values in read_excel
    df['item_fat_content'] = df['item_fat_content'].astype(str)

    # Convert all to uppercase first to handle case variations easily
    df['item_fat_content'] = df['item_fat_content'].str.upper()

    # Define the mapping for standardization
    fat_content_mapping = {
        'LOW FAT': 'Low Fat',
        'LF': 'Low Fat',
        'REGULAR': 'Regular',
        'REG': 'Regular',
        'reg': 'Regular',
        'NAN': 'Unknown' # Map the string 'NAN' to 'Unknown', if it exists after astype(str)
    }

    # Apply the mapping
    df['item_fat_content'] = df['item_fat_content'].replace(fat_content_mapping)

    # --- Handle remaining NaN values in 'item_fat_content' after standardization ---
    # This catch-all is important in case some new form of NaN appears
    initial_nulls_fat_content = df['item_fat_content'].isnull().sum()
    if initial_nulls_fat_content > 0:
        print(f"  - Found {initial_nulls_fat_content} NaN values in 'item_fat_content' AFTER standardization (before final fill).")
        # Strategy: Impute with 'Unknown' for any remaining NaNs
        df['item_fat_content'].fillna('Unknown', inplace=True)
        print(f"  - Imputed 'item_fat_content' NaN values with 'Unknown'.")
    else:
        print("  - No NaN values found in 'item_fat_content' after standardization.")

    print(f"  - Unique 'item_fat_content' values after ALL standardization and NaN handling: {df['item_fat_content'].unique()}")

else:
    print("  - Warning: 'item_fat_content' column not found for standardization.")


# --- Comprehensive Null Value Check (Before Specific Imputation) ---
# This is a good general overview of ALL nulls.
print("\n--- Initial Comprehensive Null Value Check ---")
initial_null_counts = df.isnull().sum()
initial_null_percentages = (df.isnull().sum() / len(df)) * 100

columns_with_initial_nulls = initial_null_counts[initial_null_counts > 0]
if not columns_with_initial_nulls.empty:
    print("\nColumns with Null Values (Count & Percentage) before specific imputation:")
    print(pd.DataFrame({
        'Null Count': columns_with_initial_nulls,
        'Null Percentage': initial_null_percentages[columns_with_initial_nulls.index].round(2).astype(str) + '%'
    }))
else:
    print("\nNo null values found in any column (initial check).")


# --- CENTRALIZED NULL VALUE HANDLING LOGIC ---
print("\n--- Applying Specific Null Value Handling Strategies ---")

# ... (your existing code for item_weight, rating, outlet_size imputation) ...
# Ensure these are the ones you previously set up, e.g.:
if 'item_weight' in df.columns:
 if df['item_weight'].isnull().sum() > 0:
     median_item_weight = df['item_weight'].median()
     df['item_weight'] = df['item_weight'].fillna(median_item_weight)
     print(f"  - Imputed 'item_weight' nulls with median value: {median_item_weight:.2f}")
 else:
     print("  - 'item_weight' has no nulls to impute.")
else:
     print("  - Warning: 'item_weight' column not found.")

if 'rating' in df.columns:
    if df['rating'].isnull().sum() > 0:
        median_rating = df['rating'].median()
        df['rating'] = df['rating'].fillna(median_rating)
        print(f"  - Imputed 'rating' nulls with median value: {median_rating:.2f}")
    else:
        print("  - 'rating' has no nulls to impute.")
else:
    print("  - Warning: 'rating' column not found.")
if 'outlet_size' in df.columns:
    if df['outlet_size'].isnull().sum() > 0:
        mode_outlet_size = df['outlet_size'].mode()[0]
        df['outlet_size'] = df['outlet_size'].fillna(mode_outlet_size)
        print(f"  - Imputed 'outlet_size' nulls with mode value: '{mode_outlet_size}'")
    else:
        print("  - 'outlet_size' has no nulls to impute.")
else:
    print("  - Warning: 'outlet_size' column not found.")

# --- ADD THIS SECTION FOR ITEM_VISIBILITY ROUNDING ---
print("\n--- Rounding 'item_visibility' for Precision ---")
if 'item_visibility' in df.columns:
    # Round to a sensible number of decimal places, e.g., 6 or 7
    # 6 decimal places should be sufficient for 0.100014
    df['item_visibility'] = df['item_visibility'].round(6)
    print("  - 'item_visibility' rounded to 6 decimal places.")
else:
    print("  - Warning: 'item_visibility' column not found for rounding.")



# --- ADD THIS SECTION TO DROP ROWS WITH REMAINING NULLS ---
print("\n--- Dropping Rows with Remaining Critical Nulls ---")

# List of columns that should *not* have nulls
critical_columns_to_check = [
    'item_identifier',
    'item_type',
    'outlet_establishment_year',
    'outlet_identifier',
    'outlet_location_type',
    'outlet_type',
    #'item_visibility'
]

# Get initial row count
initial_row_count = len(df)

# Drop rows where any of the critical columns have nulls
# .dropna(subset=[...]) only drops if null in specified columns
df.dropna(subset=critical_columns_to_check, inplace=True)

rows_dropped = initial_row_count - len(df)
if rows_dropped > 0:
    print(f"Successfully dropped {rows_dropped} rows with nulls in critical columns.")
else:
    print("No additional rows with critical nulls found or dropped.")


# --- Final Null Value Check (After all specific handling and row dropping) ---
print("\n--- Final Null Value Check (After All Handling Strategies) ---")
final_null_counts = df.isnull().sum()
columns_with_final_nulls = final_null_counts[final_null_counts > 0]
if not columns_with_final_nulls.empty:
    print("\nWARNING: Some columns still have nulls after all handling steps:")
    print(pd.DataFrame({
        'Null Count': columns_with_final_nulls,
        'Null Percentage': (columns_with_final_nulls / len(df) * 100).round(2).astype(str) + '%'
    }))
    print("\nAction Required: Review these remaining nulls, as they were not expected or handled.")
else:
    print("\nAll null values successfully handled. No remaining nulls found in any column.")


print("\nDataFrame Info (after all cleaning and imputation):")
df.info()

# --- NEW STEP: SAVE THE CLEANED DATAFRAME TO A CSV FILE ---
print(f"\nSaving cleaned data to CSV: {CLEANED_CSV_FILE_PATH}...")
try:
    df.to_csv(CLEANED_CSV_FILE_PATH, index=False, encoding='utf-8')
    print("Cleaned data successfully saved to CSV.")
except Exception as e:
    print(f"Error saving cleaned data to CSV: {e}")
    # Decide if you want to exit here or proceed with MySQL import of potentially outdated data
    exit()

# --- 4. Establish MySQL Connection (Ensure this block is present and NOT commented out) ---
print("\n--- Establishing MySQL Connection ---")
try:
    # Create the SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

    # Test connection
    with engine.connect() as connection:
        connection.execute(text("SELECT 1")) # A simple query to test the connection
    print("Successfully connected to MySQL.")

    # --- 5. Load DataFrame to MySQL Table ---
    print(f"\nAttempting to load data into table '{TABLE_NAME}' in database '{DB_NAME}'...")
    try:
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=1000  # Use chunksize for large datasets
        )
        print(f"Successfully loaded {len(df)} rows into table '{TABLE_NAME}' in database '{DB_NAME}'.")

        # --- ADD THIS NEW VERIFICATION BLOCK ---
        print("\nVerifying row count from Python after load...")
        with engine.connect() as connection_verify:  # Use a new connection object for clarity
            # Fetch the scalar result (single value) from the COUNT(*) query
            result = connection_verify.execute(text(f"SELECT COUNT(*) FROM {DB_NAME}.{TABLE_NAME};")).scalar()
            print(f"Row count in '{TABLE_NAME}' as reported by Python: {result}")
            if result == len(df):
                print("Python verification: Row count matches DataFrame size. Data loaded successfully.")
            else:
                print(f"Python verification: WARNING! Row count mismatch. Expected {len(df)}, got {result}.")
        # --- END NEW VERIFICATION BLOCK ---

    except SQLAlchemyError as e:  # Correctly indented except for inner try-block
        print(f"SQLAlchemy Error during data load: {e}")
        print(f"Underlying DBAPI error: {e.orig}")
        print("This often indicates a problem with table schema, data types, constraints, or database session.")
        exit()
    except Exception as e:  # General catch for inner try-block
        print(f"General Error loading data into MySQL: {e}")
        exit()

except SQLAlchemyError as e:  # Correctly indented except for outer try-block (connection)
    print(f"SQLAlchemy Connection Error: {e}")
    print(f"Underlying DBAPI error: {e.orig}")
    print("Please check DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, and MySQL server status.")
    exit()
except Exception as e:  # General catch for outer try-block
    print(f"General Connection Error: {e}")
    exit()
finally:
    if 'engine' in locals() and engine:
        engine.dispose()
        print("MySQL connection closed.")


