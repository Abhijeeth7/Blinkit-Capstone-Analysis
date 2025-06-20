CREATE DATABASE blinkit_capstone_db;
USE blinkit_capstone_db;
CREATE TABLE blinkit_sales_data (
    -- Primary Key: Do you have a unique identifier for each row/transaction?
    -- If not, you might add an auto-incrementing ID.
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Outlet Information
    outlet_establishment_date DATE,
    outlet_location_type VARCHAR(50),
    outlet_type VARCHAR(50),

    -- Item Information
    item_mrp DECIMAL(10, 2), -- Maximum Retail Price
    item_type VARCHAR(100),
    item_weight DECIMAL(10, 2),
    item_visibility DECIMAL(10, 4), -- Often a percentage or ratio

    -- Sales and Performance
    item_outlet_sales DECIMAL(10, 2), -- Specific item sales at an outlet
    -- If 'Sales' is distinct, you'd add it here too:
    -- total_order_sales DECIMAL(10, 2),
    rating DECIMAL(3, 1), -- Assuming rating is like 1.0 to 5.0
    
    -- Add any other columns you might uncover from the left side of the image
    -- e.g., product_id, item_id, transaction_id, etc.
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT COUNT(*) from blinkit_sales_data;

