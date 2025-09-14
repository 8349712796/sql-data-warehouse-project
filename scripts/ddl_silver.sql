/*
=====================================================
DDL Script: Create Silver Table

Purpose:
This script create table in 'silver' schema, droping existing table
if they already exist.
Run ths script to redefine the structure of 'bronze' tables.

*/

If OBJECT_ID ('Silver.crm_cust_info','U') Is Not Null
	Drop Table  Silver.crm_cust_info
Create Table Silver.crm_cust_info(
cst_id int,
cst_key Nvarchar(50),
cst_firstname Nvarchar(50),
cst_lastname Nvarchar(50),
cst_material_status Nvarchar(40),
cst_gndr  Nvarchar(50),
cst_create_date Date,
dwh_createdtm datetime2 DEFAULT GETDATE()
);
Go

If OBJECT_ID ('Silver.crm_prd_info','U') Is Not Null
	Drop Table  Silver.crm_prd_info
Create Table Silver.crm_prd_info(
prd_id int,
prd_key Nvarchar(50),
cat_id Nvarchar(50),
prd_nm Nvarchar(50),
prd_cost Nvarchar(50),
prd_line Nvarchar(40),
prd_start_dt Date,
prd_end_dt Date,
dwh_createdtm datetime2 DEFAULT GETDATE()
);
Go

If OBJECT_ID ('Silver.crm_sale_details','U') Is Not Null
	Drop Table  Silver.crm_sale_details
Create Table Silver.crm_sale_details(
sls_ord_num Nvarchar(50),
sls_prd_key Nvarchar(50),
sls_cust_id Nvarchar(50),
sls_order_dt Date,
sls_ship_dt Date,
sls_due_dt  Date,
sls_sales int,
sls_quantity Int,
sls_price Int,
dwh_createdtm datetime2 DEFAULT GETDATE()
);
Go

If OBJECT_ID ('Silver.erp_CUST_AZ12','U') Is Not Null
	Drop Table  Silver.erp_CUST_AZ12
Create Table Silver.erp_CUST_AZ12(
CID Nvarchar(50),
BDATE Date,
GEN Nvarchar(20),
dwh_createdtm datetime2 DEFAULT GETDATE()
);
Go

If OBJECT_ID ('Silver.erp_LOC_A101','U') Is Not Null
	Drop Table  Silver.erp_LOC_A101
Create Table Silver.erp_LOC_A101(
CID Nvarchar(20),
CNTRY Nvarchar(50),
dwh_createdtm datetime2 DEFAULT GETDATE()
);
Go

If OBJECT_ID ('Silver.erp_PX_CAT_G1V2','U') Is Not Null
	Drop Table  Silver.erp_PX_CAT_G1V2
Create Table Silver.erp_PX_CAT_G1V2(
ID Nvarchar(20),
CAT Nvarchar(50),
SUBCAT Nvarchar(50),
MAINTENANCE Nvarchar(50),
dwh_createdtm datetime2 DEFAULT GETDATE()
);
Go
