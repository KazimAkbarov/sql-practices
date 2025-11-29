/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.
===============================================================================
*/

Use DataWarehouse;
Go

-- Finding Duplicated Rows

Select
    cst_id, 
    Count(*) 
From
    (Select
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    From silver.crm_cust_info ci
        Left Join Silver.erp_cust_az12 ca
        On ci.cst_key = ca.cid
        Left Join silver.erp_loc_a101 la
        On ci.cst_key = la.cid) t
        Group by cst_id
        Having COUNT(*) >1


-- Finding Issued Rows

Select Distinct
    ci.cst_marital_status,
    ci.cst_gndr,
    ca.gen
From silver.crm_cust_info ci
    Left Join Silver.erp_cust_az12 ca
    On ci.cst_key = ca.cid
    Left Join silver.erp_loc_a101 la
    On ci.cst_key = la.cid


Select Distinct
    ci.cst_gndr,
    ca.gen,
    Case When ci.cst_gndr != 'n/a' Then ci.cst_gndr -- CRM os the Master for Gender Info
         Else Coalesce(ca.gen, 'n/a')
    End as new_gen
From silver.crm_cust_info ci
    Left Join Silver.erp_cust_az12 ca
    On ci.cst_key = ca.cid
    Left Join silver.erp_loc_a101 la
    On ci.cst_key = la.cid


-- Loading Data to Gold layer Customer Dimension
/* -=================================================================*/


If OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    Drop View Gold.dim_customers;
Go

Create View gold.dim_customers as
    Select * from (
        Select  
        ROW_NUMBER() Over(Order By cst_id) as customer_key,
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        la.cntry as country,
        ci.cst_marital_status as marital_status,
        Case When ci.cst_gndr != 'n/a' Then ci.cst_gndr -- CRM os the Master for Gender Info
             Else Coalesce(ca.gen, 'n/a')
        End as gender,
        ca.bdate as birth_date,
        ci.cst_create_date as create_date   
    From silver.crm_cust_info ci
        Left Join Silver.erp_cust_az12 ca
        On ci.cst_key = ca.cid
        Left Join silver.erp_loc_a101 la
        On ci.cst_key = la.cid) t
        where customer_id is not null;
Go


-- select * from gold.dim_customers


-- Finding Duplicated Values

Select 
    prd_key, 
    Count(*)
From 
    (Select
        pn.prd_id,
        pn.prd_key,
        pn.prd_nm,
        pn.cat_id,
        pc.cat,
        pc.subcat,
        pc.maintenance,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt
    From silver.crm_prd_info pn
        Left Join silver.erp_px_cat_g1v2 pc
        On pn.cat_id = pc.id
        Where prd_end_dt is Null) t
        Group by prd_key
        Having count(*) >1;



-- Loading Data to Gold layer Product Dimension
/* -=================================================================*/

If OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    Drop View Gold.dim_products;
Go

Create View gold.dim_products as
    Select
        ROW_NUMBER() Over (Order By pn.prd_start_dt, pn.prd_key) as product_key,
        pn.prd_id as product_id,
        pn.prd_key as product_number,
        pn.prd_nm as product_name,
        pn.cat_id as category_id,
        pc.cat as category,
        pc.subcat as subcategory,
        pc.maintenance,
        pn.prd_cost as cost,
        pn.prd_line as product_line,
        pn.prd_start_dt as start_date
    From silver.crm_prd_info pn
        Left Join silver.erp_px_cat_g1v2 pc
        On pn.cat_id = pc.id
        Where prd_end_dt is Null -- Filter out all historical data;
Go

-- select * from Gold.dim_products



-- Loading Data to Gold layer Sales Dimension
/* -=================================================================*/

If OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    Drop View Gold.fact_sales;
Go

Create View gold.fact_sales as
    Select
        sd.sls_ord_num as order_number,
        pr.product_key,
        cu.customer_key,
        sd.sls_order_dt as order_date,
        sd.sls_ship_dt as shipping_date,
        sd.sls_due_dt as due_date,
        sd.sls_sales as sales_amount,
        sd.sls_quantity as quantity,
        sd.sls_price as price
    From silver.crm_sales_details sd
        Left Join gold.dim_products pr
        On sd.sls_prd_key = pr.product_number
        Left Join gold.dim_customers cu
        On sd.sls_cust_id = cu.customer_id;
Go

-- select * from gold.fact_sales

-- Checking Table Connection
Select *
From gold.fact_sales  f
   Left Join gold.dim_customers c
   On c.customer_key = f.customer_key
   Left Join gold.dim_products p
   On p.product_key = f.product_key
   Where p.product_key Is Null


--Select * from INFORMATION_SCHEMA.TABLES