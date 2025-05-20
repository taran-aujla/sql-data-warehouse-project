/* ============================================================================
   Stored Procedure: Load Silver Layer (From Bronze to Silver)
   ============================================================================

   Purpose:
   This stored procedure handles the ETL (Extract, Transform, Load) process
   to populate the tables under the 'silver' schema using data from the 'bronze' schema.

   Key Operations:
   - Clears existing data by truncating all silver tables.
   - Loads cleaned and transformed records from bronze tables into silver tables.

   Input Parameters:
   None.
   This procedure does not take any parameters or return any result set.

   How to Execute:
   EXEC Silver.load_silver;

============================================================================ */
Create or alter procedure silver.load_silver as 
begin
    declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
	set @batch_start_time=getdate();
	print'=========================================='
	print'----------loading into SILVER layer-------'
	print'=========================================='

	print'------------------------------------------'
	print'--------data loading into silver crm------'
	print'------------------------------------------'

	print'>> truncating table silver.crm_cust_info';
	truncate table silver.crm_cust_info;
	print'>>inserting into silver.crm_cust_info';
	set @start_time=getdate();
	insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select cst_id,
	cst_key ,
	trim(cst_firstname) ,
	trim(cst_lastname) ,
	case when Upper(cst_marital_status)='S' then 'Single'
	when Upper(cst_marital_status)='M' then 'Married'
	else 'n/a' 
	end cst_marital_status ,
	case when Upper(cst_gndr)='F' then 'Female'
	when Upper(cst_gndr)='M' then 'Male'
	else 'n/a' 
	end cst_gndr,
	cst_create_date from (
	select *,
	ROW_NUMBER() over(partition by cst_id order by cst_create_date) as flag_no
	from bronze.crm_cust_info
	)as t where flag_no=1;
	set @end_time=getdate();
	print '--time taken for insertion:'+cast(datediff(second,@start_time,@end_time) as nvarchar);

	print'>> truncating table silver.crm_prd_info';
	truncate table silver.crm_prd_info;
	print'>>inserting into silver.crm_prd_info';
	set @start_time=getdate();
	insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	select prd_id ,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	replace(SUBSTRING(prd_key,7,len(prd_key)),'-','_') as prd_key,
	prd_nm ,
	isnull(prd_cost,0) as prd_cost,
	case when upper(trim(prd_line))='R' THEN 'Road'
	when upper(trim(prd_line))='M' THEN 'Mountain' 
	when upper(trim(prd_line))='S' THEN 'Other Sales' 
	when upper(trim(prd_line))='T' THEN 'Touring'
	else'n/a' end as prd_line,
	cast(prd_start_dt as date) as prd_start_dt ,
	cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date)as prd_end_dt
	from bronze.crm_prd_info;
	set @end_time=getdate();
	print '--time taken for insertion:'+cast(datediff(second,@start_time,@end_time) as nvarchar);

	print'>> truncating table silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	print'>>inserting into silver.crm_sales_details';
	set @start_time=getdate();
	insert into silver.crm_sales_details (
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
	select 
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	case when sls_order_dt=0 or len(sls_order_dt)!=8 then null
	else cast(cast(sls_order_dt as varchar)as date) end as sls_order_dt ,
	case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then null
	else cast(cast(sls_ship_dt as varchar)as date) end as sls_ship_dt ,
	case when sls_due_dt=0 or len(sls_due_dt)!=8 then null
	else cast(cast(sls_due_dt as varchar)as date) end as sls_due_dt  ,
	case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*abs(sls_price) 
	then sls_quantity*abs(sls_price) else sls_sales end as sls_sales,
	sls_quantity ,
	case when sls_price is null or sls_price <=0 
	then sls_sales/nullif(sls_quantity,0) else sls_price end as sls_price	
	from bronze.crm_sales_details;
	set @end_time=getdate();
	print '--time taken for insertion:'+cast(datediff(second,@start_time,@end_time) as nvarchar);

	print'------------------------------------------'
	print'--------data loading into silver erp------'
	print'------------------------------------------'

	print'>> truncating table silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;
	print'>>inserting into silver.erp_cust_az12';
	set @start_time=getdate();
	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select 
	case when cid like 'NAS%' then substring(cid,4,len(cid))
	else cid end cid,
	case when bdate>getdate() then null
	else bdate
	end as bdate,
	case when trim(upper(gen)) in ('M','Male' ) then 'Male'
	when trim(upper(gen))  in ('F','Female') then 'Female'
	Else 'n/a'
	end as gen
	from bronze.erp_cust_az12;	
	set @end_time=getdate();
	print '--time taken for insertion:'+cast(datediff(second,@start_time,@end_time) as nvarchar);

	print'>> truncating table erp_loc_a101';
	truncate table silver.erp_loc_a101;
	print'>>inserting into silver.erp_loc_a101';
	set @start_time=getdate();
	insert into silver.erp_loc_a101(
	cid,
	cntry
	)
	select
	trim(replace(cid,'-',''))as cid,
	case when cntry in ('US','USA') then 'United States '
	when trim(cntry)='DE' then 'Germany'
	when trim(cntry) is null then  'n/a'
	when trim(cntry)='' then 'n/a'
	else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101;
	set @end_time=getdate();
	print '--time taken for insertion:'+cast(datediff(second,@start_time,@end_time) as nvarchar);

	print'>> truncating table erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2;
	print'>>inserting into silver.erp_px_cat_g1v2';
	set @start_time=getdate();
	insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintainance
	)
	select 
	id,
	cat,
	subcat,
	maintainance
	from bronze.erp_px_cat_g1v2;
	set @end_time=getdate();
	print '--time taken for insertion:'+cast(datediff(second,@start_time,@end_time) as nvarchar);
	set @batch_end_time=getdate();
	print'-----------------------------------------'
	print'--total time taken by silver layer in seconds:'+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar);
	print'-----------------------------------------'
	end try
	BEGIN CATCH
    PRINT '========================================'
    PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER'
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT '========================================'
END CATCH
end
