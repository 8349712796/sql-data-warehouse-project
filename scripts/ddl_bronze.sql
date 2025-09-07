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

Exec Bronze.Bronze_load

Create or Alter Procedure Bronze.Bronze_load As
Begin
	Declare @start_time Datetime, @end_time Datetime,@batch_start_time Datetime, @batch_end_time Datetime;
	Begin Try
		Set @batch_start_time = GETDATE();
		Print '========================================================';
		Print 'Loading Bronze Layer';
		Print '========================================================';

		Print '--------------------------------------------------------';
		Print 'Loading CRM Tables';
		Print '--------------------------------------------------------';

		Set @start_time = GETDATE();
		Print 'Truncating the table: Bronze.crm_cust_info';
		Truncate Table Bronze.crm_cust_info
		Print 'Inserting the Data into: Bronze.crm_cust_info';
		Bulk Insert Bronze.crm_cust_info
		From 'C:\Users\acer\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With(
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
		);
		Set @end_time = GETDATE()
		Print '>> Total execution time: ' +Cast(DateDiff(Second,@start_time,@end_time) As Nvarchar) +' seconds';
		Print '>> -----------------'

		Set @start_time = GETDATE()
		Print 'Truncating the table: Bronze.crm_prd_info';
		Truncate Table Bronze.crm_prd_info
		Print 'Inserting the Data into: Bronze.crm_prd_info';
		Bulk Insert Bronze.crm_prd_info
		From 'C:\Users\acer\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With(
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
		);
		Set @end_time = GETDATE()
		Print '>> Total execution time: ' +Cast(DateDiff(Second,@start_time,@end_time) As Nvarchar) +' seconds';
		Print '>> -----------------'

		Set @start_time = GETDATE()
		Print 'Truncating the table: Bronze.crm_sale_details';
		Truncate Table Bronze.crm_sale_details
		Print 'Inserting the Data into: Bronze.crm_sale_details';
		Bulk Insert Bronze.crm_sale_details
		From 'C:\Users\acer\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With(
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
		);
		Set @end_time = GETDATE()
		Print '>> Total execution time: ' +Cast(DateDiff(Second,@start_time,@end_time) As Nvarchar) +' seconds';
		Print '>> -----------------'

		Print '---------------------------------------------------------';
		Print 'Loading ERP Tables';
		Print '---------------------------------------------------------';
		Set @start_time = GETDATE()
		Print 'Truncating the table: Bronze.erp_CUST_AZ12';
		Truncate Table Bronze.erp_CUST_AZ12
		Print 'Inserting the Data into: Bronze.erp_CUST_AZ12';
		Bulk Insert Bronze.erp_CUST_AZ12
		From 'C:\Users\acer\Documents\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		With(
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
		);
		Set @end_time = GETDATE()
		Print '>> Total execution time: ' +Cast(DateDiff(Second,@start_time,@end_time) As Nvarchar) +' seconds';
		Print '>> -----------------'

		Set @start_time = GETDATE()
		Print 'Truncating the table: Bronze.erp_LOC_A101';
		Truncate Table Bronze.erp_LOC_A101
		Print 'Inserting the Data into: Bronze.erp_LOC_A101';
		Bulk Insert Bronze.erp_LOC_A101
		From 'C:\Users\acer\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		With(
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
		);

		Set @end_time = GETDATE()
		Print '>> Total execution time: ' +Cast(DateDiff(Second,@start_time,@end_time) As Nvarchar) +' seconds';
		Print '>> -----------------'

		Set @start_time = GETDATE()
		Print 'Truncating the table: Bronze.erp_PX_CAT_G1V2';
		Truncate Table Bronze.erp_PX_CAT_G1V2
		Print 'Inserting the Data into: Bronze.erp_PX_CAT_G1V2';
		Bulk Insert Bronze.erp_PX_CAT_G1V2
		From 'C:\Users\acer\Documents\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		With(
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
		);
		Set @end_time = GETDATE()
		Print '>> Total execution time: ' +Cast(DateDiff(Second,@start_time,@end_time) As Nvarchar) +' seconds';
		Print '>> -----------------'

		Set @batch_end_time = GETDATE()
		Print '>> Bronze layer loading completed.........';
		Print '>> Total complete duration is: ' +Cast(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) As Nvarchar) +' seconds';
		Print '>> ---------------------------------------------------------------------------------------------------------------';
	End Try
	Begin Catch
		Print '=============================================================';
		Print '>> Error ocurred during Loading';
		Print '>> Error message ' +Error_Message();
		Print '>> Error Number ' +Cast(Error_Number() As Varchar);
		Print '>> Error Status ' +Cast(Error_State() As Varchar);
		Print '=============================================================';
	End Catch
End

