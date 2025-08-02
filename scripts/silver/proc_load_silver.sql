/*
=====================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=====================================================================
Script Purpose:
	This stored procedure performs the ETL(Extract, Transform and Load) process to
	populate the 'silver' schema tables from the 'bronze' schema.
  Actions Performed:
	- Truncates Silver tables.
	- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC silver.load_silver;
=====================================================================
*/

create or alter procedure silver.load_silver as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '===================================';
			PRINT 'Loading Silver Layer';
			PRINT '===================================';

			PRINT '-----------------------------------';
			PRINT 'Loading CRM Tables';
			PRINT '-----------------------------------';

			SET @start_time = GETDATE();

	print 'truncating table: silver.crm_cust_info';
	truncate table silver.crm_cust_info;
	print 'inserting data into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
		   cst_id
		  ,cst_key
		  ,cst_firstname
		  ,cst_lastname
		  ,cst_marital_status
		  ,cst_gndr
		  ,cst_create_date
		  )

	SELECT 
		   [cst_id]
		  ,[cst_key]
		  ,TRIM([cst_firstname]) as [cst_firstname]
		  ,TRIM([cst_lastname]) as [cst_lastname]
		  ,CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status
		  ,CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr
		  ,cst_create_date
	FROM (
		select
		*,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null) as t
		where flag_last = 1

		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @start_time = GETDATE();

	print 'truncating table: silver.crm_prd_info';
	truncate TABLE silver.crm_prd_info;
	print 'inserting data into table: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		   prd_id      
		  ,cat_id
		  ,prd_key
		  ,prd_nm
		  ,prd_cost
		  ,prd_line
		  ,prd_start_dt
		  ,prd_end_dt
	)

	SELECT [prd_id]
		  ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id --Extract Category id
		  ,SUBSTRING(prd_key, 7, len(prd_key)) as prd_key --Extract product key
		  ,[prd_nm]
		  ,ISNULL([prd_cost], 0) AS prd_cost
		  ,CASE WHEN UPPER(TRIM([prd_line])) = 'M' THEN 'Mountain'
			   WHEN UPPER(TRIM([prd_line])) = 'R' THEN 'Road'
			   WHEN UPPER(TRIM([prd_line])) = 'S' THEN 'Other Sales'
			   WHEN UPPER(TRIM([prd_line])) = 'T' THEN 'Touring'
			   ELSE 'n/a'
		   END AS prd_line -- Map product line codes to descriptive values
		  ,CAST([prd_start_dt] AS DATE) AS prd_start_dt
		  ,DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) -- Calculate end date as one day before the next start date
	FROM [bronze].[crm_prd_info]

		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @start_time = GETDATE();

	print 'truncating table: silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	print 'inserting data into table: silver.crm_sales_details';
	insert into silver.crm_sales_details(
		   [sls_ord_num]
		  ,[sls_prd_key]
		  ,[sls_cust_id]
		  ,[sls_order_dt]
		  ,[sls_ship_dt]
		  ,[sls_due_dt]
		  ,[sls_sales]
		  ,[sls_quantity]
		  ,[sls_price]
	)

	SELECT [sls_ord_num]
		  ,[sls_prd_key]
		  ,[sls_cust_id]
		  ,CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS varchar)AS DATE)
		   END AS sls_order_dt
		  ,CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS varchar)AS DATE)
		   END AS sls_due_dt
		  ,CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS varchar)AS DATE)
		   END AS sls_ship_dt
		  ,CASE WHEN sls_sales is null or sls_sales <=0 THEN ABS(sls_price) * nullif(sls_quantity,0)
			ELSE sls_sales
		   END AS sls_sales
		  ,CASE WHEN sls_quantity is null or sls_quantity<=0 THEN sls_sales / sls_price
			ELSE sls_quantity
		   END AS sls_quantity
		  ,CASE WHEN sls_price is null or sls_sales / sls_quantity != sls_price or sls_price<=0 THEN sls_sales / NULLIF(ABS(sls_quantity),0)
			ELSE sls_price
		   END AS sls_price
	FROM [DataWarehouse].[bronze].[crm_sales_details]

		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

	
		PRINT '-----------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------';


		SET @start_time = GETDATE();

	print 'truncating table: silver.erp_CUST_AZ12';
	truncate TABLE silver.erp_CUST_AZ12;
	print 'inserting data into table: silver.erp_CUST_AZ12';
	insert into silver.erp_cust_az12 (
		cid, 
		bdate, 
		gen
	)

	SELECT 
		   case when cid like 'NAS%' then substring(cid,4,len(cid))
				else CID
			end as cid
		  ,case when BDATE > getdate() then null
				else BDATE
		   end as Bdate
		  ,case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
				when upper(trim(gen)) in ('M', 'MALE') then 'Male'
				else 'n/a'
			end as gen
	FROM [DataWarehouse].bronze.[erp_CUST_AZ12]

		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @start_time = GETDATE();

	print 'truncating table: silver.erp_LOC_A101';
	truncate TABLE silver.erp_LOC_A101;
	print 'inserting data into table: silver.erp_LOC_A101';
	insert into silver.erp_LOC_A101(CID, CNTRY)

	SELECT replace([CID], '-', ''),
		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US', 'USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
			 else trim(cntry)
		end as cntry
	FROM [DataWarehouse].bronze.[erp_LOC_A101]

		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @start_time = GETDATE();


	print 'truncating table: silver.erp_PX_CAT_G1V2';
	truncate TABLE silver.erp_PX_CAT_G1V2;

	print 'inserting data into table: silver.erp_PX_CAT_G1V2';
	insert into silver.erp_PX_CAT_G1V2(
		id,
		cat,
		subcat,
		MAINTENANCE
	)

	SELECT [ID]
		  ,[CAT]
		  ,[SUBCAT]
		  ,[MAINTENANCE]
	FROM bronze.[erp_PX_CAT_G1V2]

		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @batch_end_time = GETDATE();
		PRINT '====================================================';
		PRINT '>> Loading Silver Layer is Completed';
		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '====================================================';

	END TRY
	BEGIN CATCH
		PRINT '====================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '====================================================';
	END CATCH
END
