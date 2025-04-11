/*
==================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================================
Script Purpose:
  This Stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
   - Truncates the bronze tables before loading data.
   - Uses the 'BULK INSERT' command to load data from CSV Files to bronze tables.
Parameters:
    None.
  This stored procedure does not accept any parameters or return any values.
Usage Exammple:
  EXEC bronze.load_bronze;
=======================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @TIME_START DATETIME, @TIME_END DATETIME;
	BEGIN TRY
    SET @TIME_START = GETDATE();
		PRINT '======================================'
		PRINT 'LOADING BRONZE LAYER DATA'
		PRINT '======================================'
		PRINT '--------------------------------------'
		PRINT 'Loading crm Tables'
		PRINT '--------------------------------------'
		SET @start_time = GETDATE();
	-- FOR FULL LOAD WE HAVE TO DELETE AND THEN INSERT THE RECORDS IN THE TABLE --
		PRINT '>> Deleting existing data from bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting data into bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		from 'E:\SQL\Data Warehouse\Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds'
		PRINT '||||||||||||||||||||||||||||||||||||||||'
		SET @start_time = GETDATE();
		PRINT '>> Deleting existing data from bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>>Innserting data into bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		from 'E:\SQL\Data Warehouse\Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds'
		PRINT '||||||||||||||||||||||||||||||||||||||||'
		SET @start_time = GETDATE();
		PRINT '>> Deleting existing data from bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting data into bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		from 'E:\SQL\Data Warehouse\Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds'
		PRINT '||||||||||||||||||||||||||||||||||||||||'
		PRINT '--------------------------------------'
		PRINT 'Loading erp source data'
		PRINT '--------------------------------------'
		SET @start_time = GETDATE();
		PRINT '>> Deleting existing data from bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting data into bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'E:\SQL\Data Warehouse\Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds'
		PRINT '||||||||||||||||||||||||||||||||||||||||'
		SET @start_time = GETDATE();
		PRINT '>> Deleting existing data from bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting data into bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'E:\SQL\Data Warehouse\Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds'
		PRINT '||||||||||||||||||||||||||||||||||||||||'
		SET @start_time = GETDATE();
		PRINT '>> Deleting existing data from bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting data into bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'E:\SQL\Data Warehouse\Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+' seconds'
		PRINT '||||||||||||||||||||||||||||||||||||||||'
  SET @TIME_END = GETDATE()
	PRINT '>>> TOTAL TIME TO LOAD BRONZE LAYER: '+ CAST(DATEDIFF(SECOND, @TIME_START, @TIME_END) AS NVARCHAR) + ' SECOND'
	END TRY
  BEGIN CATCH
		PRINT '========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR)
		PRINT '========================================='
	END CATCH
END


