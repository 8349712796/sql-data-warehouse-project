/*
DDL script: Create Gold Views
Script purpose:
The script create views for the gold layer in the data warehouse.
The gold layer represents the final dimension and fact tables(Star schema)

Each view perform transformation and combines data from the silver layer
to produce a clean, enriched and business ready dataset.

Usage:
Thsese views can be used directly for reporting and analytics.

*/


Create or Alter View gold.dim_customers As
Select
Row_number() Over(Order by cst_id) customer_key,
ci.cst_id customer_id,
ci.cst_key customer_number,
ci.cst_firstname first_name,
ci.cst_lastname last_name,
la.CNTRY country,
ci.cst_material_status marital_status,
Case
	When cst_gndr != 'N/A' Then ci.cst_gndr
	Else Coalesce(ca.GEN,'N/A')
End Gender,
ca.BDATE brithdate,
ci.cst_create_date create_date
From Silver.crm_cust_info ci
Left Join Silver.erp_CUST_AZ12 ca
On ci.cst_key = ca.CID
Left Join Silver.erp_LOC_A101 la
On ci.cst_key = la.CID

Select
*
From gold_dim_customers

Create View Gold.dim_products As
Select
Row_Number() Over(Order by prd_start_dt,prd_key) product_key,
pd.prd_id product_id,
pd.prd_key product_number,
pd.cat_id category_id,
pc.CAT category,
pc.SUBCAT sub_category,
pc.MAINTENANCE,
pd.prd_cost product_cost,
pd.prd_line product_line,
pd.prd_start_dt start_date
From Silver.crm_prd_info pd
Left Join Silver.erp_PX_CAT_G1V2 pc
On pd.cat_id = pc.ID

Select
*
From Gold.dim_products

Create View Gold.dim_fact As
Select
sls_ord_num order_numer,
dp.product_key,
dc.customer_key,
sls_order_dt order_date,
sls_ship_dt ship_date,
sls_due_dt due_date,
sls_sales sales,
sls_quantity quantity,
sls_price price
From Silver.crm_sale_details cs
Left Join Gold.dim_customers dc
On cs.sls_cust_id = dc.customer_id
Left Join Gold.dim_products dp
On cs.sls_prd_key = dp.product_number

Select
* 
From Gold.dim_customers
