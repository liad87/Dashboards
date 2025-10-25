
--Q4 --What is the effect on robot allocation duration of having an error during the allocation?
-- check within the start with no end date how many have errors in +/- 100 seconds interval
select cc.* , o.* 
from [dbo].[check_start_with_no_end] cc
	outer apply ( select * 
				  from errors_distinct ed
				  where cc.robot_id = ed.component_id
					and ed.date_time between cc.date_time and dateadd(second , +300 ,  cc.date_time )
					) o

-- check for each error to which assignment it belongs

--allocations with end time
--drop table #allocations_start_end

;WITH CTE --create end date , and filter only start with existsing end date
AS
	(
		select * ,end_time = LEAD( created ,1 ,null) over (partition by job_id , robot_id,tote_id  order by created )
		from [dbo].allocations_distinct
	)
select *  ,duration_seconds = end_time - created
into #allocations_start_end
from cte
where allocation_status = 'Start' and end_time is not null

drop table if exists #errors_related_assignment
----#errors_related_assignment
select component_id	,component_type	,error_name	,error_created = ed.created ,has_related_job_id = IIF(o.created is not null , 1, 0)
		, error_create_date =   DATEADD(s, ed.created, '1970-01-01') , o.* 
into #errors_related_assignment
from errors_distinct ed 
	outer apply ( select * 
				  from #allocations_start_end ase
				  where ed.component_id = ase.robot_id 
					and ed.created between ase.created and ase.end_time
				) o

 select cc.* ,err.* ,time_diff_start_to_error = (err.error_created - cc.created)
 from #allocations_start_end cc
	outer apply ( select top 1 first_error_after_start = error_name	,error_created = created
				  from errors_distinct ed
				  where cc.robot_id = ed.component_id
					and ed.created between cc.created and end_time
					order by ed.created asc
					) err 
where err.error_created is not null


--duplicate errors - differmt error name in the same second
select error_created,error_create_date, component_id , freq = count(*) ,distinct_error_name = count(distinct error_name)
from #errors_related_assignment
group by error_created,error_create_date ,component_id
having count(*) > 1

--duplicate errors - differmt error name same jobid

--error_created	error_create_date			component_id	freq	distinct_error_name
--1682814078		2023-04-30 00:21:18.000		GR003283		4		   4
drop table if exists Fact_Errors

select component_id	,component_type,	error_name,	error_created	,has_related_job_id,	error_date_time = error_create_date ,error_date = cast(error_create_date as date)
		--,error_rounded_hour = datepart(hour, DATEADD(minute,+30,error_create_date ) )
		,job_created = created	
		,job_id	,tote_id , robot_type	,allocation_date_time = date_time , allocation_end_time = end_time	,duration_seconds
into Fact_Errors
from #errors_related_assignment