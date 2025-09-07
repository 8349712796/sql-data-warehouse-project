/*
DDL Scripts: Create Bronze Table
========================================================
Script purpose:
This script creates table in bronze schema, droping existing tables
if they already exists
Run this script to redefine the structure of 'Bronze' tables
========================================================
*/

If OBJECT_ID ('Bronze.crm_cust_info','U') Is Not Null
	Drop Table  Bronze.crm_cust_info
Create Table Bronze.crm_cust_info(
cst_id int,
cst_key Nvarchar(50),
cst_firstname Nvarchar(50),
cst_lastname Nvarchar(50),
cst_material_status Nvarchar(40),
cst_gndr  Nvarchar(50),
cst_create_date Date
);
Go

If OBJECT_ID ('Bronze.crm_prd_info','U') Is Not Null
	Drop Table  Bronze.crm_prd_info
Create Table Bronze.crm_prd_info(
prd_id int,
prd_key Nvarchar(50),
prd_nm Nvarchar(50),
prd_cost Nvarchar(50),
prd_line Nvarchar(40),
prd_start_dt Date,
prd_end_dt Date
);
Go

If OBJECT_ID ('Bronze.crm_sale_details','U') Is Not Null
	Drop Table  Bronze.crm_sale_details
Create Table Bronze.crm_sale_details(
sls_ord_num Nvarchar(50),
sls_prd_key Nvarchar(50),
sls_cust_id Nvarchar(50),
sls_order_dt Nvarchar(50),
sls_ship_dt Nvarchar(40),
sls_due_dt  Nvarchar(50),
sls_sales Varchar(50),
sls_quantity Int,
sls_price Int
);
Go

If OBJECT_ID ('Bronze.erp_CUST_AZ12','U') Is Not Null
	Drop Table  Bronze.erp_CUST_AZ12
Create Table Bronze.erp_CUST_AZ12(
CID Nvarchar(50),
BDATE Date,
GEN Nvarchar(20)
);
Go

If OBJECT_ID ('Bronze.erp_LOC_A101','U') Is Not Null
	Drop Table  Bronze.erp_LOC_A101
Create Table Bronze.erp_LOC_A101(
CID Nvarchar(20),
CNTRY Nvarchar(50)
);
Go

If OBJECT_ID ('Bronze.erp_PX_CAT_G1V2','U') Is Not Null
	Drop Table  Bronze.erp_PX_CAT_G1V2
Create Table Bronze.erp_PX_CAT_G1V2(
ID Nvarchar(20),
CAT Nvarchar(50),
SUBCAT Nvarchar(50),
MAINTENANCE Nvarchar(50)
);
Go


