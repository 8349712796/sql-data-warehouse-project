/*
===================================================
Quality Checks
===================================================
Script Purpose:
This scripts perform various quality checks for data consistency, accuracy
and standardization accross the 'silver' schema. It includes check for:
- Null Or duplicate primary key.
- unwanted space in string fields.
- Data standardization & consistency.
- Invalid date range & orders.
- Data consistency between related fields.

*/
SELECT
cst_id,
Count(*)
FROM Silver.crm_cust_info
Group by cst_id
Having Count(*) > 1 OR cst_id IS NULL

Select
*
From
(SELECT
*,
ROW_NUMBER() over(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
FROM
Bronze.crm_cust_info)t
WHERE flag_last = 1

--check for unwanted space
--Expectation: No result
SELECT
cst_lastname
FROM Silver.crm_cust_info
Where cst_lastname != TRIM(cst_lastname)

Select
cst_id,
cst_key,
trim(cst_firstname) cst_firstname,
trim(cst_lastname) cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
From
(SELECT
*,
ROW_NUMBER() over(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
FROM
Bronze.crm_cust_info
Where cst_id is not null)t
WHERE flag_last = 1

--Data Standardization & Consistency
SELECT
distinct cst_gndr,cst_material_status
FROM
Silver.crm_cust_info

SELECT
*,
Case
	When cst_gndr = 'M' Then 'Male'
	When cst_gndr = 'F' Then 'Female'
	Else 'N/A'
End As cst_gndr,
Case
	When cst_material_status = 'M' Then 'Married'
	When cst_material_status = 'S' Then 'Single'
	Else 'N/A'
End AS cst_material_status
FROM Bronze.crm_cust_info

Select
cst_id,
cst_key,
trim(cst_firstname) cst_firstname,
trim(cst_lastname) cst_lastname,
Case
	When cst_material_status = 'M' Then 'Married'
	When cst_material_status = 'S' Then 'Single'
	Else 'N/A'
End AS cst_material_status,
Case
	When cst_gndr = 'M' Then 'Male'
	When cst_gndr = 'F' Then 'Female'
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

--Check for nulls or Duplicates in primary key
--Expectation: No results
SELECT
prd_id,
Count(*)
FROM 
Bronze.crm_prd_info
Group By prd_id
Having Count(*) >1 or prd_id is null

SELECT
prd_id,
prd_key,
Replace(SUBSTRING(prd_key,1,5) ,'-','_')AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key
FROM
Bronze.crm_prd_info 
Where SUBSTRING(prd_key,7,LEN(prd_key)) IN(
select
sls_prd_key
from
Bronze.crm_sale_details)

Select
prd_id,
prd_key,
prd_nm,
prd_cost,
coalesce(prd_cost,0)
From
Bronze.crm_prd_info
Where prd_cost < 0 OR prd_cost IS NULL

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


SELECT
  prd_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM
  Bronze.crm_prd_info;


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

Insert Into Silver.erp_CUST_AZ12 (CID,BDATE,GEN) Values('','','')
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


Select
Distinct
GEN,Case
	When Upper(Trim(GEN)) = 'M' Then 'Male'
	When Upper(Trim(GEN)) = 'F' Then 'Female'
	When GEN Is Null Or Gen = '' Then 'N/A'
	Else GEN
End GEN
From Bronze.erp_CUST_AZ12
Where BDATE < '1920-01-01' OR BDATE >= GETDATE()

Select
REPLACE(CID,'-','') CID,
Case
	When TRIM(CNTRY) in ('US','USA') Then 'United States'
	When TRIM(CNTRY) = 'DE' Then 'Delhi'
	When TRIM(CNTRY) Is NUll or CNTRY = '' Then 'N/A'
	Else TRIM(CNTRY)
End CNTRY
From Bronze.erp_LOC_A101

Select
Distinct CNTRY As Old,
Case
	When TRIM(CNTRY) in ('US','USA') Then 'United States'
	When TRIM(CNTRY) = 'DE' Then 'Delhi'
	When TRIM(CNTRY) Is NUll or CNTRY = '' Then 'N/A'
	Else TRIM(CNTRY)
End CNTRY
From Bronze.erp_LOC_A101
Order By CNTRY

Select
ID,
CAT,
SUBCAT,
MAINTENANCE
From Bronze.erp_PX_CAT_G1V2
Where ID != TRIM(ID) Or CAT ! = TRIM(CAT) Or SUBCAT != TRIM(SUBCAT) Or MAINTENANCE != TRIM(MAINTENANCE)

Select
Distinct MAINTENANCE
From Bronze.erp_PX_CAT_G1V2
