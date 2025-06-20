SELECT COUNT(*) FROM blinkit_capstone_db.blinkit_capstone_table;

SELECT
    item_fat_content,
    SUM(sales) AS Total_Sales,
    AVG(sales) AS Average_Sales,
    COUNT(DISTINCT item_identifier) AS Number_of_Unique_Items,
    COUNT(item_identifier) AS Total_Items_Sold, -- This counts all instances of items sold (e.g., if Item A sold 5 times, it counts as 5)
    AVG(rating) AS Average_Rating
FROM
    blinkit_capstone_db.blinkit_capstone_table
GROUP BY
    item_fat_content
ORDER BY
    Total_Sales DESC;
    
SELECT COUNT(outlet_identifier) as OU_ID from blinkit_capstone_db.blinkit_capstone_table
WHERE outlet_establishment_year = 2011;