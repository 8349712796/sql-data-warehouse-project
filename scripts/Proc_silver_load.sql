/*
===========================================================
Stored Procedure load silver layer (Bronze -> Silver)

Script purpose:
This stored procedure scripts perform the ETL(extract, transform, and load) process to 
populate the silver schema from bronze schema.

Actions performed:
Truncated silver tables;
Insert transformed and clean data from bronze to silver layer.

Parameters:
This stored procedure does not accept any parameter.

Usage Example:
Exec Silver.Load_Silver

*/


Create OR Alter Procedure Silver.Load_Silver As
Begin
	Declare @start_time datetime,@end_time datetime,@bch_start_time datetime,@bch_end_time datetime
Begin Try
		Set @bch_start_time = Getdate()

		Print '-----------------------------------------------------';
		Print 'Loading Silver Layer';
		Print '-----------------------------------------------------';
		
		Print '-----------------------------------------------------';
		Print 'Loading CRM Table';
		Print '-----------------------------------------------------';

		Set @start_time = Getdate()
		Print '>> Truncating the Table: Silver.crm_cust_info'
		Truncate Table Silver.crm_cust_info
		Print '>> Loading the data in table Silver.crm_cust_info'
		Insert Into Silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date)
		Select
		cst_id,
		cst_key,
		trim(cst_firstname) cst_firstname,
		trim(cst_lastname) cst_lastname,
		Case
			When UPPER(TRIM(cst_material_status)) = 'M' Then 'Married'
			When UPPER(TRIM(cst_material_status)) = 'S' Then 'Single'
			Else 'N/A'
		End AS cst_material_status,
		Case
			When UPPER(TRIM(cst_gndr)) = 'M' Then 'Male'
			When UPPER(TRIM(cst_gndr)) = 'F' Then 'Female'
			Else 'N/A'
		End As cst_gndr,
		cst_create_date
		From
		(SELECT
		*,
		ROW_NUMBER() over(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
		FROM
		Bronze.crm_cust_info
		Where cst_id is not null)t
		WHERE flag_last = 1

		Set @end_time = GETDATE()

		Print 'Total Duration is: ' +Cast(DateDiff(second,@start_time,@end_time) As Varchar) +' Seconds';

		Set @start_time = GETDATE()
		Print '>> Truncating the Table: Silver.crm_prd_info'
		Truncate Table Silver.crm_prd_info
		Print '>> Loading the data in table Silver.crm_prd_info'
		Insert Into Silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		SELECT
		prd_id,
		Replace(SUBSTRING(prd_key,1,5) ,'-','_')AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		coalesce(prd_cost,0) prd_cost,
		Case 
			When upper(trim(prd_line)) = 'M' Then 'Mountain'
			When upper(trim(prd_line)) = 'R' Then 'Road'
			When upper(trim(prd_line)) = 'S' Then 'Other Sales'
			When upper(trim(prd_line)) = 'T' Then 'Touring'
			Else 'N/A'
		End prd_line,
		prd_start_dt,
		  DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM
		Bronze.crm_prd_info 

		Set @end_time = GETDATE()

		Print 'Total Duration is: ' +Cast(DateDiff(second,@start_time,@end_time) As Varchar) +' Seconds';

		Set @start_time = Getdate()
		Print '>> Truncating the Table: Silver.crm_sale_details'
		Truncate Table Silver.crm_sale_details
		Print '>> Loading the data in table Silver.crm_sale_details'
		Insert Into Silver.crm_sale_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
		Select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		Case 
			When sls_order_dt <= 0 Or Len(sls_order_dt) != 8 Then Null
			Else cast(sls_order_dt as date)
		End sls_order_dt,
		Case 
			When sls_ship_dt <= 0 Or Len(sls_ship_dt) != 8 Then Null
			Else cast(sls_ship_dt as date)
		End sls_ship_dt,
		Case 
			When sls_due_dt <= 0 Or Len(sls_due_dt) != 8 Then Null
			Else cast(sls_due_dt as date)
		End sls_due_dt,
		Case
			When sls_sales Is Null Or sls_sales <= 0 Or sls_sales != sls_quantity * ABS(sls_price)
			Then  sls_quantity * ABS(sls_price)
			Else sls_sales
		End sls_sales,
		sls_quantity,
		Case
			When sls_price Is Null Or sls_price <= 0
			Then sls_sales/ NULLIF(sls_quantity,0)
			Else sls_price
		End sls_price
		From Bronze.crm_sale_details

		Set @end_time = GETDATE()

		Print 'Total Duration is: ' +Cast(DateDiff(second,@start_time,@end_time) As Varchar) +' Seconds';
		
		Print '-------------------------------------------------------';
		Print '>> Loading ERP Tables';
		Print '-------------------------------------------------------';
		Set @start_time = GETDATE()
		Print '>> Truncating the Table: Silver.erp_CUST_AZ12'
		Truncate Table Silver.erp_CUST_AZ12
		Print '>> Loading the data in table Silver.erp_CUST_AZ12'
		Insert Into Silver.erp_CUST_AZ12 (CID,BDATE,GEN)
		Select
		Case 
			When Len(CID) > 10 Then SUBSTRING(CID,4,len(CID)) 
			Else CID
		End CID,
		Case
			When BDATE >= GETDATE() Then Null
			Else BDATE
		End BDATE,
		Case
			When Upper(Trim(GEN)) = 'M' Then 'Male'
			When Upper(Trim(GEN)) = 'F' Then 'Female'
			When GEN Is Null Or Gen = '' Then 'N/A'
			Else GEN
		End GEN
		From Bronze.erp_CUST_AZ12
		Set @end_time = GETDATE()

		Print 'Total Duration is: ' +Cast(DateDiff(second,@start_time,@end_time) As Varchar) +' Seconds';

		Set @start_time = GETDATE()
		Print '>> Truncating the Table: Silver.erp_LOC_A101'
		Truncate Table Silver.erp_LOC_A101
		Print '>> Loading the data in table Silver.erp_LOC_A101'
		Insert into Silver.erp_LOC_A101(CID,CNTRY)
		Select
		REPLACE(CID,'-','') CID,
		Case
			When TRIM(CNTRY) in ('US','USA') Then 'United States'
			When TRIM(CNTRY) = 'DE' Then 'Delhi'
			When TRIM(CNTRY) Is NUll or CNTRY = '' Then 'N/A'
			Else TRIM(CNTRY)
		End CNTRY
		From Bronze.erp_LOC_A101
		Set @end_time = GETDATE()

		Print 'Total Duration is: ' +Cast(DateDiff(second,@start_time,@end_time) As Varchar) +' Seconds';

		Set @start_time = GETDATE()
		Print '>> Truncating the Table: Silver.erp_PX_CAT_G1V2'
		Truncate Table Silver.erp_PX_CAT_G1V2
		Print '>> Loading the data in table Silver.erp_PX_CAT_G1V2'
		Insert Into Silver.erp_PX_CAT_G1V2(ID,
		CAT,
		SUBCAT,
		MAINTENANCE)
		Select
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		From Bronze.erp_PX_CAT_G1V2
		Set @end_time = GETDATE()

		Print 'Total Duration is: ' +Cast(DateDiff(second,@start_time,@end_time) As Varchar) +' Seconds';

		Set @bch_end_time = GETDATE()

		Print 'Total Time taken for loading data into tables: ' +Cast(DateDiff(Second,@bch_start_time,@bch_end_time) As Varchar) + ' Seconds'
	End Try
	Begin Catch
	Print '>> ==========================================';
	Print '>> Error Occurred While Inserting';
	Print '>> Error Message:' +Error_Message();
	Print '>> Error line Number: ' +Cast(Error_Number() As Varchar);
	Print '=============================================';
	End Catch
End
