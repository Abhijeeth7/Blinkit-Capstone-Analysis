Blinkit Sales Data Analysis - Project Requirements
1. Project Overview

This project aims to analyze Blinkit grocery sales data to derive actionable insights, optimize sales strategies, and inform business decisions for product placement, pricing, and outlet management. By transforming raw data into meaningful metrics and visualizations, we will identify performance trends, evaluate key drivers of sales, and provide data-driven recommendations.

2. Data Source

Source: Blinkit Grocery Sales Data (Excel file)
Original Path: D:\Blinkit_Cap_Project\Excel\BlinkIT Grocery Data.xlsx
Database Destination: MySQL blinkit_capstone_db, table blinkit_capstone_table

3. Tools & Technologies

Data Extraction & Transformation (ETL): Python (Pandas, SQLAlchemy, MySQL Connector)
Database Management: MySQL, MySQL Workbench
Data Visualization & Reporting: Power BI
Version Control: Git / GitHub

4. Business Requirements (Why and What - High Level)

The primary goal is to understand the overall sales performance and key drivers within the Blinkit grocery ecosystem.

Key Performance Indicators (KPIs):
Total Sales: Overall revenue generated.
Average Sales: Average sales per transaction/item.
Number of Items: Total count of unique or transacted items.
Average Rating: Average customer satisfaction rating for products/outlets.

5. Granular Requirements (How - Detailed Level)

These requirements specify the detailed breakdowns and calculations needed to support the high-level business objectives and KPIs. For "additional KPI metrics," it is assumed that Total Sales, Average Sales, Number of Items, and Average Rating should be calculated for each segment in addition to the primary objective.

1. Total Sales by Fat Content:

Objective: Analyze the impact of fat content (item_fat_content) on overall sales performance.
Metrics: Total sales (item_outlet_sales), Average sales (item_outlet_sales), Number of items (count of item_identifier), Average rating (rating) — all segmented by item_fat_content.

2. Total Sales by Item Type:

Objective: Identify the performance of different item types (item_type) in terms of total sales.
Metrics: Total sales (item_outlet_sales), Average sales (item_outlet_sales), Number of items (count of item_identifier), Average rating (rating) — all segmented by item_type.

3. Fat Content by Outlet for Total Sales:

Objective: Compare total sales across different outlets (outlet_identifier) segmented by item_fat_content. This will reveal which fat content categories perform best at which outlets.
Metrics: Total sales (item_outlet_sales), Average sales (item_outlet_sales), Number of items (count of item_identifier), Average rating (rating) — all segmented by outlet_identifier AND item_fat_content.

4. Total Sales by Outlet Establishment:

Objective: Evaluate how the age or type of outlet establishment (outlet_establishment_year, outlet_type) influences total sales.
Metrics: Total sales (item_outlet_sales), Average sales (item_outlet_sales), Number of items (count of item_identifier), Average rating (rating) — all segmented by outlet_establishment_year and outlet_type.

6. Chart Requirements (How to Visualize - Specific Visualization Level)

These requirements define the specific visualizations to be created in Power BI to present the insights derived from the granular analysis.

1. Percentage of Sales by Outlet Size:

Objective: Analyze the correlation between outlet_size and total sales.
Visualization: Donut Chart or Pie Chart showing the percentage contribution of each outlet_size to overall item_outlet_sales. (A bar chart showing total sales by outlet size could also be effective for direct comparison).

2. Sales by Outlet Location:

Objective: Assess the geographic distribution of sales across different outlet_location_types.
Visualization: Bar Chart showing total item_outlet_sales for each outlet_location_type.

3. All Metrics by Outlet Type:

Objective: Provide a comprehensive view of all specified KPIs (Total Sales, Average Sales, Number of Items, Average Rating) broken down by different outlet_types.
Visualization: Multiple charts or a single dashboard section:
Bar Chart for Total item_outlet_sales by outlet_type.
Bar Chart for Average item_outlet_sales by outlet_type.
Bar Chart for Count of item_identifier by outlet_type.
Bar Chart or Card for Average rating by outlet_type.
(Consider using a multi-KPI card or a table for a consolidated view if suitable in Power BI).
