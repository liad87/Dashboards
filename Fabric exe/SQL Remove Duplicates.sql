drop table if exists dbo.errors_distinct
drop table if exists dbo.allocations_distinct

select component_id	,component_type	,error_name	,created , count(*)
from errors e
group by component_id	,component_type	,error_name	,created
having count(*) > 1
order by count(*) desc


select   created 	,job_id	,robot_id	,tote_id	,robot_type	,allocation_status ,date_time = DATEADD(s, created, '1970-01-01') , count_ = count(*)
from dbo.allocations
--where job_id = 'a11ca2af-896a-4582-add1-f144235b0989'
group by created 	,job_id	,robot_id	,tote_id	,robot_type	,allocation_status ,DATEADD(s, created, '1970-01-01') 
having count(*) > 1
order by count(*) desc , created


select   created 	,job_id	,robot_id	,tote_id	,robot_type	,allocation_status ,date_time = DATEADD(s, created, '1970-01-01') 
from dbo.allocations
where job_id = 'a11ca2af-896a-4582-add1-f144235b0989'
order by created 

select * 
from errors e
where component_id = 'LR00002254'
order by created

select distinct component_id	,component_type	,error_name	,created ,date_time =  DATEADD(s, created, '1970-01-01') 
into dbo.errors_distinct
from errors

select distinct  created 	,job_id	,robot_id	,tote_id	,robot_type	,allocation_status ,date_time = DATEADD(s, created, '1970-01-01') 
into dbo.allocations_distinct
from dbo.allocations
--where job_id = '345dc821-080c-475f-8656-9862acdc15eb' and robot_id = 'LR00002093'
--order by created
