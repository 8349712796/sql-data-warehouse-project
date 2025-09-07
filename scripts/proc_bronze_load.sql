/*
Stored Procedure: Load Bronze layer(Source -> Bronze)
==========================================================
Script purpose: 
This script load the CSV file data into Bronze schema from external path in form of bulk insert.
It performs the following actions:
1.Truncate the bronze table before loading data.
2.Use bulk insert command to load the data from CSV files to bronze tables.

Parameters:
None
This stored procedure does not accept any parameter or return any values.

Usage Example:
Exec Bronze.Bronze_load
============================================================
*/
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
