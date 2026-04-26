/*
1. Check and Load the data into silver tables from bronze table.
2. Ensure Quality of data
3. Ensure data consistancy
4. Ensure data completeness
5. Ensure data granularity
*/


/*
================================================================================
Insert cleaned data from bronze_crm_cust_info table to silver_crm_cust_info table
================================================================================
*/
CALL load_silver_data();
DROP PROCEDURE load_silver_data;
DELIMITER //
CREATE PROCEDURE load_silver_data()
BEGIN
	TRUNCATE TABLE silver_crm_cust_info;
	INSERT INTO silver_crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	SELECT cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a' END cst_marital_status,
	CASE WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
		 WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
		 ELSE 'n/a' END cst_gndr,
	cst_create_date FROM (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	from bronze_crm_cust_info)t
	WHERE flag_last = 1 AND cst_id != 0;

	-- SELECT * FROM silver_crm_cust_info;
	/*
	================================================================================
	Insert cleaned data from bronze_crm_prd_info table to silver_crm_prd_info table
	================================================================================
	*/
	TRUNCATE TABLE silver_crm_prd_info;
	INSERT INTO silver_crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTR(prd_key, 1, 5),'-','_') AS cat_id,
	SUBSTR(prd_key, 7, length(prd_key)) AS prd_key,
	prd_nm,
	IF(trim(prd_cost) = " " OR prd_cost IS NULL, 0, prd_cost) AS prd_cost,
	CASE UPPER(trim(prd_line)) 
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'other Sales'
		 WHEN 'T' THEN 'Touring'
		 END prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) prd_end_dt
	FROM bronze_crm_prd_info;

	-- select * from silver_crm_prd_info;

	/*
	================================================================================
	Insert cleaned data from bronze_crm_sales_detail table to silver_crm_sales_detail table
	================================================================================
	*/
	TRUNCATE TABLE silver_crm_sales_detail;
	INSERT INTO silver_crm_sales_detail (
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
	SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0  THEN NULL
		 ELSE sls_order_dt 
		 END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0  THEN NULL
		 ELSE sls_ship_dt 
		 END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0  THEN NULL
		 ELSE sls_due_dt 
		 END AS sls_due_dt,
	CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_price * abs(sls_quantity) 
		 THEN round(abs(if(abs(sls_price) != 0, sls_price, abs(sls_sales)/abs(sls_quantity)) * ABS(sls_quantity)), 0)
		 ELSE sls_sales 
		 END AS sls_sales,

	sls_quantity,
	CASE WHEN sls_price IS NULL OR abs(sls_price) <= 0 OR sls_price != sls_sales / sls_quantity
		 THEN round(sls_sales / sls_quantity, 0)
		 ELSE sls_price
		 END AS sls_price
	FROM bronze_crm_sales_detail;

	-- SELECT * FROM silver_crm_sales_detail;

	/*
	================================================================================
	Insert cleaned data from bronze_erp_cust_az12 table to silver_erp_cust_az12 table
	================================================================================
	*/ 
	TRUNCATE TABLE silver_erp_cust_az12;
	INSERT INTO silver_erp_cust_az12 (
	CID,
	BDATE,
	GEN
	)
	SELECT 
	CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, length(cid))
		 ELSE CID END cid,
	CASE WHEN BDATE > current_date() THEN NUll
		 ELSE BDATE END BDATE,
	CASE WHEN upper(trim(GEN)) LIKE 'F%' THEN 'Female'
		 WHEN upper(trim(GEN)) LIKE 'M%' THEN 'Male'
		 ELSE 'n/a' END as gen
	FROM bronze_erp_cust_az12;
	-- SELECT * FROM silver_erp_cust_az12;


	/*
	================================================================================
	Insert cleaned data from bronze_erp_loc_a101 table to silver_erp_loc_a101 table
	================================================================================
	*/

	TRUNCATE TABLE silver_erp_loc_a101;
	INSERT INTO silver_erp_loc_a101(
	cid,
	cntry)
	SELECT 
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(CNTRY) LIKE 'DE%' THEN 'Germany'
		 WHEN TRIM(CNTRY) LIKE 'US%' THEN 'United States'
		 WHEN TRIM(CNTRY) LIKE 'USA%' THEN 'United States'
		 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
		 ELSE TRIM(CNTRY) END cntry
	FROM bronze_erp_loc_a101;

	-- select cst_key from silver_crm_cust_info;

	/*
	================================================================================
	Insert cleaned data from bronze_erp_px_cat_g1v1 table to silver_erp_px_cat_g1v1 table
	================================================================================
	*/
	TRUNCATE TABLE silver_erp_px_cat_g1v2;
	INSERT INTO silver_erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT id,
	cat,
	subcat,
	maintenance FROM bronze_erp_px_cat_g1v2;
END //
DELIMITER 
-- select * from silver_erp_px_cat_g1v2 where MAINTENANCE like 'Yes%';
