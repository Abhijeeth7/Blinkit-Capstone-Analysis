import pandas as pd
from sqlalchemy import create_engine, text # Make sure 'text' is imported
import mysql.connector

# --- 1. Configuration (same as before) ---
EXCEL_FILE_PATH = r"D:\Blinkit_Cap_Project\Excel\BlinkIT Grocery Data.xlsx"
EXCEL_SHEET_NAME = "BlinkIT Grocery Data"
DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASSWORD = 'password'
DB_NAME = 'blinkit_capstone_db'
TABLE_NAME = 'blinkit_sales_data'

# Define the path for the cleaned CSV output
CLEANED_CSV_FILE_PATH = r'D:\Blinkit_Cap_Project\Excel\BlinkIT_Grocery_Data_CLEANED.csv'


# --- 2. Read Excel into Pandas DataFrame (same as before) ---
print(f"Attempting to read Excel file from: {EXCEL_FILE_PATH} (Sheet: {EXCEL_SHEET_NAME})...")
try:
    df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=EXCEL_SHEET_NAME)
    print(f"Successfully loaded {len(df)} rows from Excel.")
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

