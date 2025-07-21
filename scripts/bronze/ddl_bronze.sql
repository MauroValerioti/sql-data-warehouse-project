/*
========================================================================
DDL Script: Create Bronze Tables
========================================================================
Script Putrpose:
  This scripts creates tables in the 'bronze' schemas, dropping existing 
  tables if they already exists.
  Run this script to re-define the DDL structure of 'bronze' tables.
========================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info', 'U') is not null
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
)

IF OBJECT_ID ('bronze.crm_prd_info', 'U') is not null
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
)

IF OBJECT_ID ('bronze.crm_sales_details', 'U') is not null
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
)

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2', 'U') is not null
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_CUST_AZ12', 'U') is not null
	DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12 (
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50)
)

IF OBJECT_ID ('bronze.erp_LOC_A101', 'U') is not null
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50)
)

--SELECT * FROM bronze.crm_cust_info;
