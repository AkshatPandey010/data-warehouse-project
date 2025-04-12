/*
***********STORED PROCEDURE: Load Silver Layer (Bronze -> Silver)******************
|=================================================================================|
|Script Purpose:                                                                  |
|      This stored procedure performs the ETL(Extract, Transform, Load) procss to |
|      populate the 'silver' schema tables from the 'bronze' schema.              |
|    Actions Perfomed:                                                            |
|      - Truncates Silver tables.                                                 |
|      - Inserts transformed and cleansed data from Bronze into Silver tables.    |
|                                                                                 |
|  Parameters:                                                                    |
|      None.                                                                      |
|      This stored procedure does not accept any parameters or return any values. |
|  Usage Example:                                                                 |
|      EXEC Silver.load_silver;                                                   |
|=================================================================================|
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME;
	SET @START_TIME = GETDATE()
	BEGIN TRY
	--============================================================--
	/*CRM CUST INFO*/
	PRINT '>> TRUNCATING TABLE : silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> INSERTING DATA INTO : silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
	FROM (
		SELECT * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc)
		as flag_last
		FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL) 
	T WHERE flag_last = 1

	--============================================================-----
	/* CRM PROD INFO*/
	PRINT '>> TRUNCATING TABLE : silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> INSERTING DATA INTO : silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category id
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- extract product key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
			LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 
			AS DATE
		) AS prd_end_dt
	from bronze.crm_prd_info


	--======================================================================---
	/* CRM SALES DETAILS*/
	PRINT '>> TRUNCATING TABLE : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> INSERTING DATA INTO : silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 then NULL -- Handling invalid data
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 then NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- datatype casting
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 then NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales --- Handling missing data ansd invalid data
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			  THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details


	--===================================================================--
	/* ERP CUST AZ12 */
	PRINT '>> TRUNCATING TABLE : silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> INSERTING DATA INTO : silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12( 
		cid,
		bdate,
		gen
	)
	SELECT
	CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid)) --Remove 'NAS' prefix if present
		 ELSE cid
	END cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS bdate, -- Set future birthdates to NULL
	CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
		 ELSE 'n/a' 
	END AS gen -- Normalize gender values and handle unknown cases
	FROM bronze.erp_cust_az12

	--====================================================================--
	/* ERP LOC A101 */
	PRINT '>> TRUNCATING TABLE : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> INSERTING DATA INTO : silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	SELECT 
	REPLACE(cid, '-', '') cid, --handled inavlid values
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry -- Normalize and handle missing or blank country codes
	FROM bronze.erp_loc_a101

	--====================================================================--
	/* ERP PX CAT G1V2*/
	PRINT '>> TRUNCATING TABLE : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> INSERTING DATA INTO : silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2
	END TRY
	BEGIN CATCH
	PRINT '========================================='
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR)
	PRINT '========================================='
	END CATCH
	SET @END_TIME = GETDATE()
	PRINT '>> TOTAL TIME TO LOAD SILVER LAYER DATA: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECOND'
END

