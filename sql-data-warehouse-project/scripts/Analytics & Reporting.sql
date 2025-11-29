/*
===============================================================================
EDA Script: Create Explatory Views
===============================================================================
Script Purpose:To perform Exploratory Data Analysis (EDA) on the Gold Layer tables.
    - Inspect the structure (columns) of the main dimension tables.
    - Check date ranges and data timelines.
    - Calculate high-level Key Performance Indicators (KPIs).
    - Identify Top and Bottom performing products.

Usage:
    - Run the entire script to generate multiple result sets for a full overview.
    - Highlight specific sections to inspect individual metrics.
   
=============================================================================== */

-- Explore Database Objects & Schema
-- ============================================

Select * from INFORMATION_SCHEMA.Columns

Select * from INFORMATION_SCHEMA.Columns
Where TABLE_NAME = 'dim_customers'

Select * from INFORMATION_SCHEMA.Columns
Where TABLE_NAME = 'dim_products'


-- Dimension Exploration (Categories)
-- ============================================

Select Distinct 
     category, 
     subcategory, 
     product_name 
From gold.dim_products
     Order by 1,2,3;


-- Date & Timeline Exploration
-- ============================================

Select 
	Min(order_date) as first_order_date,
	Max(order_date) as last_order_date,
	Datediff(MONTH, Min(order_date),Max(order_date)) as order_range_months
From gold.fact_sales

Select 
	Min(birth_date) as oldest_birth_date,
	Datediff(year, MIN(birth_date), Getdate()) as oldest_age,
	Max(birth_date) as youngest_birth_date,
	Datediff(year, MAX(birth_date), Getdate()) as youngest_age
 From gold.dim_customers



-- Key Metric Exploration (KPIs)
-- ============================================

Select 'Total Sales' as measure_name, Sum(quantity*sales_amount) as measure_value From gold.fact_sales
    Union All
Select 'Total Quantity', Sum(Quantity) From gold.fact_sales
    Union All
Select 'Average Price', AVG(Price) From gold.fact_sales
    Union All
Select 'Total Nr. Orders', Count(Distinct order_number) From gold.fact_sales
    Union All
Select 'Total Nr. Products', Count(product_name) From gold.dim_products
    Union All
Select 'Total Nr. Customers', Count(customer_key) From gold.dim_customers




-- Select * from gold.fact_sales
-- Select 
--     category, 
--     avg(cost)
-- From gold.dim_products
-- group by category


-- Ranking Analysis (Top & Bottom Performers)
-- ============================================

-- Top 5 Products by Total Revenue

SELECT Top(5)
    p.product_name,
    SUM(f.sales_amount) as total_revenue,
    RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) as revenue_rank
FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
    GROUP BY p.product_name;


-- Bottom 5 Products by Total Revenue

SELECT * From (
    Select    
        p.product_name,
        SUM(f.sales_amount) as total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) Asc) as revenue_rank
    FROM gold.fact_sales f
        LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
        GROUP BY p.product_name) t
        Where revenue_rank <=5

-- select * from gold.dim_products



/*
===============================================================================
Advanced Analytics Script: Create Explatory Views
===============================================================================
Script Purpose: 
    This script performs advanced data analysis on the 'Gold' layer of the 
    Data Warehouse. It includes:
    1. Exploratory Data Analysis (EDA): Time-series, cumulative, and performance metrics.
    2. Data Segmentation: Categorizing products and customers.
    3. View Creation: Establishing persistent views for BI tools (Power BI/Tableau).
   
=============================================================================== */

-- Change Over Time Analysis
-- =================================================

-- Total Sales by Order Date
Select
	 Order_date,
	 Sum(Sales_amount) as total_sales
From gold.fact_sales
     Where order_date Is Not Null
     Group by order_date
     Order by order_date



-- Total Sales by Month (Seasonality Analysis
Select
	 Month(Order_date) as order_month,
	 Sum(Sales_amount) as total_sales,
	 Count(Distinct customer_key) as total_customers,
	 Sum(quantity) as total_quantity
From gold.fact_sales
     Where order_date Is Not Null
     Group by Month(Order_date)
     Order by Month(Order_date)


-- Total Sales by Year (High-Level Trends)
Select
	Year(Order_date) as order_year,
	Sum(Sales_amount) as total_sales,
	Count(Distinct customer_key) as total_customers,
	Sum(quantity) as total_quantity
From gold.fact_sales
    Where order_date Is Not Null
    Group by Year(Order_date)
    Order by Year(Order_date)


-- Sales by Year and Month (Drill-Down)
Select
	Year(Order_date) as order_year,
	Month(Order_date) as order_month,
	Sum(Sales_amount) as total_sales,
	Count(Distinct customer_key) as total_customers,
	Sum(quantity) as total_quantity
From gold.fact_sales
    Where order_date Is Not Null
    Group by Year(Order_date),Month(Order_date)
    Order by Year(Order_date),Month(Order_date)


Select
	Datetrunc(month,Order_date) as order_year_month,
	Sum(Sales_amount) as total_sales,
	Count(Distinct customer_key) as total_customers,
	Sum(quantity) as total_quantity
From gold.fact_sales
    Where order_date Is Not Null
    Group by Datetrunc(month,Order_date)
    Order by Datetrunc(month,Order_date)


Select
	Format(Order_date, 'yyyy-MMM') as order_year_month,
	Sum(Sales_amount) as total_sales,
	Count(Distinct customer_key) as total_customers,
	Sum(quantity) as total_quantity
From gold.fact_sales    
    Where order_date Is Not Null
    Group by Format(Order_date, 'yyyy-MMM')
    Order by Format(Order_date, 'yyyy-MMM')



-- Cumilative Analysis
-- =================================================

-- Running Total of Sales by Month
Select
	order_date,
	Sum(sales_amount) as total_sales, 
	Sum(Sum(sales_amount)) Over (Order by order_date) as running_total_sales
From gold.fact_sales
    Group by Order_date

Select 
	order_month,
	total_sales,
	Sum(total_sales) Over (Order by order_month) as running_total_sales
From(
	Select	
		DATETRUNC(month, order_date) as order_month,
		Sum(Sales_amount) as total_sales
	From gold.fact_sales
	    Where order_date Is not Null
	    Group by DATETRUNC(month, order_date)) t
	    ORDER BY order_month;


-- Performance Analysis
-- =================================================

With yearly_product_sales as (
	Select	
		year(f.order_date) as order_year,
		p.product_name,
		Sum(f.sales_amount) as current_sales
	From gold.fact_sales f
	    Left Join gold.dim_products p
	    On f.product_key = p.product_key
	    Where  order_date Is Not Null
	    Group by YEAR(f.order_date), p.product_name)

	Select 
		order_year,
		product_name,
		current_sales,
		Avg(current_sales) Over (Partition by product_name) as avg_sales,
		current_sales - Avg(current_sales) Over (Partition by product_name) as diff_avg,
		Case When current_sales - Avg(current_sales) Over (Partition by product_name) >0 Then 'Above Avg'
			 When current_sales - Avg(current_sales) Over (Partition by product_name) <0 Then 'Below Avg'
			 Else 'Avg'
		End as avg_chnage,
		Lag(current_sales) Over (Partition by product_name Order By order_year) as py_sales,
		current_sales - Lag(current_sales) Over (Partition by product_name Order By order_year) as diff_py,
		Case When current_sales - Lag(current_sales) Over (Partition by product_name Order By order_year) >0 Then 'Increase'
			 When current_sales - Lag(current_sales) Over (Partition by product_name Order By order_year) <0 Then 'Decrease'
			 Else 'No Change'
		End as py_chnage
	From yearly_product_sales
	    Order by product_name,order_year




-- Part to Whole Analysis
-- =================================================

Select 
	p.category,
	Sum(sales_amount) as total_sales,
	Sum(Sum(sales_amount)) Over () as overall_sales,
	CONCAT(Round((Cast(Sum(sales_amount) as Float)/Sum(Sum(sales_amount)) Over ())*100,2),'%') as percentage_of_total
From gold.fact_sales f
    Left Join gold.dim_products p
    On p.product_key  = f.product_key
    Group by p.category
    Order by Sum(sales_amount) Desc;


-- Data Segmentation
-- =================================================

With product_segments as (
	Select
		product_key,
		product_name,
		cost,
		Case When cost <100 Then 'Below 100'
			 When cost Between 100 and 500 Then '100-500'
			 When cost Between 500 and 1000 Then '500-1000'
			 Else 'Above 1000'
		End as cost_range
	From gold.dim_products)

	Select
		cost_range,
		Count(product_key) as total_products
	From product_segments
	    Group by cost_range
	    Order by total_products Desc






	/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================

If OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    Drop View gold.report_customers;
Go

Create View gold.report_customers AS

    WITH base_query AS(
    /*---------------------------------------------------------------------------
    1) Base Query: Retrieves core columns from tables
    ---------------------------------------------------------------------------*/
    Select
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        DATEDIFF(year, c.birth_date, GETDATE()) age
    FROM gold.fact_sales f
        LEFT JOIN gold.dim_customers c
        On c.customer_key = f.customer_key
        Where order_date IS NOT NULL)

    , customer_aggregation AS (
    /*---------------------------------------------------------------------------
    2) Customer Aggregations: Summarizes key metrics at the customer level
    ---------------------------------------------------------------------------*/
    Select 
	    customer_key,
	    customer_number,
	    customer_name,
	    age,
	    COUNT(DISTINCT order_number) AS total_orders,
	    SUM(sales_amount) AS total_sales,
	    SUM(quantity) AS total_quantity,
	    COUNT(DISTINCT product_key) AS total_products,
	    MAX(order_date) AS last_order_date,
	    DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    From base_query
        Group By
	    customer_key,
	    customer_number,
	    customer_name,
	    age
    )
    Select
        customer_key,
        customer_number,
        customer_name,
        age,
        CASE 
	         When age < 20 Then 'Under 20'
	         When age between 20 and 29 Then '20-29'
	         When age between 30 and 39 Then '30-39'
	         When age between 40 and 49 Then '40-49'
	         Else '50 and above'
        End AS age_group,
        CASE 
            When lifespan >= 12 AND total_sales > 5000 Then 'VIP'
            When lifespan >= 12 AND total_sales <= 5000 Then 'Regular'
            Else 'New'
        End AS customer_segment,
        last_order_date,
        DATEDIFF(month, last_order_date, GETDATE()) AS recency,
        total_orders,
        total_sales,
        total_quantity,
        total_products
        lifespan,
        -- Compuate average order value (AVO)
        CASE When total_sales = 0 Then 0
	         Else total_sales / total_orders
        End AS avg_order_value,
        -- Compuate average monthly spend
        CASE When lifespan = 0 Then total_sales
             Else total_sales / lifespan
        End AS avg_monthly_spend
    From customer_aggregation;
Go


	/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================
If OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    Drop View gold.report_products;
Go

Create View gold.report_products AS

    WITH base_query AS (
    /*---------------------------------------------------------------------------
    1) Base Query: Retrieves core columns from fact_sales and dim_products
    ---------------------------------------------------------------------------*/
        Select
	        f.order_number,
            f.order_date,
		    f.customer_key,
            f.sales_amount,
            f.quantity,
            p.product_key,
            p.product_name,
            p.category,
            p.subcategory,
            p.cost
        From gold.fact_sales f
            LEFT JOIN gold.dim_products p
            On f.product_key = p.product_key
            Where order_date IS NOT NULL  -- only consider valid sales dates
    ),

    product_aggregations AS (
    /*---------------------------------------------------------------------------
    2) Product Aggregations: Summarizes key metrics at the product level
    ---------------------------------------------------------------------------*/
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders,
	    COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
	    ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
    From base_query

    Group By
        product_key,
        product_name,
        category,
        subcategory,
        cost
    )

    /*---------------------------------------------------------------------------
      3) Final Query: Combines all product results into one output
    ---------------------------------------------------------------------------*/
    SELECT 
	    product_key,
	    product_name,
	    category,
	    subcategory,
	    cost,
	    last_sale_date,
	    DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	    CASE
		    When total_sales > 50000 THEN 'High-Performer'
		    When total_sales >= 10000 THEN 'Mid-Range'
		    Else 'Low-Performer'
	    End AS product_segment,
	    lifespan,
	    total_orders,
	    total_sales,
	    total_quantity,
	    total_customers,
	    avg_selling_price,
	    -- Average Order Revenue (AOR)
	    CASE 
		    When total_orders = 0 Then 0
		    Else total_sales / total_orders
	    End AS avg_order_revenue,

	    -- Average Monthly Revenue
	    CASE
		    When lifespan = 0 Then total_sales
		    Else total_sales / lifespan
	    End AS avg_monthly_revenue

    From product_aggregations
Go

Select * from gold.report_products;

    With customer_spending as (
	    Select
		    c.customer_key,
		    Sum(f.sales_amount) as total_spending,
		    Min(f.order_date) as first_order,
		    Max(f.order_date) as last_order,
		    Datediff(month, Min(f.order_date),Max(f.order_date)) as lifespan
	    From gold.fact_sales f
	    Left Join gold.dim_customers c
	    On f.customer_key = c.customer_key
	    Group by c.customer_key)

    Select
	    customer_segment,
	    Count(customer_key) as total_customers
    From(
	    Select 
		    customer_key,
		    total_spending,
		    lifespan,
		    Case When lifespan >= 12 And total_spending > 5000 Then 'VIP'
			     When lifespan >= 12 And total_spending <= 5000 Then 'Regular'
			     Else 'New'
		    End customer_segment
	    From customer_spending) t
	    Group by customer_segment
	    Order by total_customers Desc