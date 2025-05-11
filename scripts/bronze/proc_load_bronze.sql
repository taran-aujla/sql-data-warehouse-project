/*
===================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===================================================================
*/

go
create or alter procedure bronze.load_bronze as
begin
    Declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
    Begin try
	set @batch_start_time=getdate();
    Print '===================='
	print'loading Bronze layer'
	Print '===================='
	Print 'Loading from CRM --->'

	set @start_time = GETDATE();
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	Print 'load duration'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds'

	Print'insearting data into--> crm_prd';
	set @start_time = GETDATE();
	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info
	from 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
	firstrow=2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	Print 'load duration'+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds'

	
	Print'insearting data into--> crm_sales_details';
	set @start_time=GETDATE();
	truncate table bronze.crm_sales_details;
	bulk insert bronze.crm_sales_details
	from 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
	firstrow=2,
	fieldterminator = ',',
	tablock
	);
	set @end_time=GETDATE();
	Print 'loading duration'+cast(datediff(second,@start_time,@end_time) as nvarchar)+'seconds'

	Print 'Loading from ERP --->'
	Print 'inserting data into--> erp_cust_az12'
	set @start_time=GETDATE();
	truncate table bronze.erp_cust_az12;
	bulk insert bronze.erp_cust_az12
	from 'D:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	)
	set @end_time = GETDATE();
	Print 'loading duration' + cast(datediff(second,@start_time,@end_time) as nvarchar)+'secconds'

	print 'inserting data into--> erp_loc_a101'
	set @start_time=GETDATE();
	truncate table bronze.erp_loc_a101;
	bulk insert bronze.erp_loc_a101
	from 'D:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	)
	set @end_time = GETDATE();
	Print 'loading duration' + cast(datediff(second,@start_time,@end_time) as nvarchar)+'secconds'

	Print 'inserting date into--> erp_px_cat_g1v2' 
	set @start_time = GETDATE();
	truncate table bronze.erp_px_cat_g1v2;
	bulk insert bronze.erp_px_cat_g1v2
	from 'D:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	Print 'loading duration' + cast(datediff(second,@start_time,@end_time) as nvarchar)+'secconds'
	set @batch_end_time=getdate();
	print '=============================='
	Print 'bronze layer loading is  done '
	Print 'total time duration taken by bronze layer-->'+ cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+' seconds'
	end try
	begin catch
	    Print '============================================'
	    Print 'error occured during loading of bronze layer'
		Print 'error message'+error_message();
		Print 'error message'+cast(error_number() as nvarchar)
		Print 'error message'+cast(error_state() as nvarchar)
	end catch
End

exec bronze.load_bronze
