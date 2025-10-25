--Q1
drop table if exists Robots_Allocation_Distribution_to_Jobs

;with cte
as
(
	select job_id  --,ground_allocation= sum(iif( robot_type ='ground',1,0) ) , lift_allocation= sum(iif( robot_type ='lift',1,0) )
		,distinct_ground_robots_count = count(distinct iif( robot_type ='ground',robot_id,null)  )
		,distinct_lift_robots_count = count(distinct iif( robot_type ='lift',robot_id,null)  )
	from[dbo].[Fact_Allocations]
	group by job_id 
)
select distinct_ground_robots_count,distinct_lift_robots_count ,Jobs_count  =count(*)
into Robots_Allocation_Distribution_to_Jobs
from cte
group by distinct_ground_robots_count,distinct_lift_robots_count 

--Q2

--allocations with end time

drop table if exists Fact_Allocations
;WITH CTE
AS
	(
		select * ,end_time = LEAD( created ,1 ,null) over (partition by job_id , robot_id,tote_id  order by created )
		from [dbo].allocations_distinct
	)
select c.* ,allocation_date = cast(c.date_time as date) ,err.errors_count ,has_an_error = IIF(errors_count > 0 ,1,0), duration_sec = c.end_time - c.created
		,duration_Group = case when  (c.end_time - c.created) <= 30 then 'A ( 0 - 30 S)'
							   when (c.end_time - c.created)  <= 60 then 'B ( 30 - 60 S)'
							   when (c.end_time - c.created)  <= 90 then 'C ( 60 - 90 S)'
							   when (c.end_time - c.created)  <= 120 then'D ( 90 - 120 S)'
							   when (c.end_time - c.created)  <= 200 then 'E ( 120 - 200 S)'
							   when (c.end_time - c.created)  <= 400 then 'F ( 200 - 400 S)'
							   when (c.end_time - c.created)  <= 600 then 'G ( 400 - 600 S)'
							   when (c.end_time - c.created)  <= 800 then 'H ( 600 - 800 S)'
							   when (c.end_time - c.created)  <= 1000 then 'I ( 800 - 1000 S)'
							   when (c.end_time - c.created)  <= 1500 then 'J ( 1000 - 1500 S)'
							   when (c.end_time - c.created)  > 1500 then 'K ( > 1500 S)'
							   else 'Error' end

into dbo.Fact_Allocations
from cte c
	outer apply(
				select errors_count = count(*) 
				from [dbo].[errors_distinct] ed
				where ed.component_id = c.robot_id 
					and ed.created between c.created and c.end_time
				) err
where allocation_status = 'Start' and end_time is not null


---check_start_with_no_end time---
drop table if exists dbo.check_start_with_no_end
;WITH CTE
AS
	(
		select * ,end_time = LEAD( created ,1 ,null) over (partition by job_id , robot_id,tote_id  order by created )
		from [dbo].allocations_distinct
	)
select *
into dbo.check_start_with_no_end
from cte
where allocation_status = 'Start' and end_time is null -- #is there records with start and no end
--and DATEADD(s, created, '1970-01-01') < '2023-05-02' -- 2023-05-02 is the last day seen in the database , it might be records the started and yet loged the end time

----Q3 --YES -- Are there cases in which the same GR is used twice (or more) in the same job?
SELECT job_id,robot_id, count_ = count(*)
FROM dbo.Fact_Allocations --[dbo].[allocations]
where allocation_status = 'Start'
group by job_id,robot_id
having count(*) > 1
order by left(robot_id,2), count(*) desc


;with cte
as
(
	select job_id  --,ground_allocation= sum(iif( robot_type ='ground',1,0) ) , lift_allocation= sum(iif( robot_type ='lift',1,0) )
		,distinct_ground_robots_count = count(distinct iif( robot_type ='ground',robot_id,null)  )
		,distinct_lift_robots_count = count(distinct iif( robot_type ='lift',robot_id,null)  )
	from[dbo].[Fact_Allocations]
	group by job_id 
	)
	select sum(distinct_lift_robots_count)*1.0 /count(*)
	from cte


	--My Questions :
	-- #is there an overlamp timestamp between differnt jobs? overall \ same robot_id \ same Tote ?
	
	-- 1. aggragate job
	;with cte
	as
	(
		select job_id,robot_id ,tote_id , allocation_start = min(created) , allocation_end = max(end_time)
		from Fact_Allocations fa
		group by job_id,robot_id ,tote_id
	)
	select top 100 c.* ,o.* 
	from cte c
		cross apply ( select * 
					  from cte co
					  where c.job_id <> co.job_id and c.robot_id = co.robot_id and c.tote_id = co.tote_id
						and c.allocation_start between co.allocation_start and co.allocation_end
					 ) o


--is there a specif componnet\robot_id the work more than others?
select robot_id , allocations = count(*) 
from Fact_Allocations fa
group by robot_id
order by COUNT(*) desc

--LR wotking in more allocations for shorter time

--examplees

select * 
from  Fact_Allocations fa
where job_id in('290edcdc-ee9d-4983-a5ec-892e65b0dafa','8730815a-7c9b-494a-b8b4-c666b4291382')
	and robot_id = 'GR003207'
order by created

--		1682972904	1682973253		GR003207

select job_id , job_start = min(created) , job_end = max(end_time)
from Fact_Allocations fa
where job_id in('2b46923e-d289-4802-9dff-67f612959e36','80969fcf-7bea-4725-b2c4-6ced251581ae')
group by job_id
--1682962714
--1682962687
--does 1682962714 < 1682962687

declare @p bigint =  1682962714 ,
	@a bigint =  1682963035;

select 1
where @p < @a
