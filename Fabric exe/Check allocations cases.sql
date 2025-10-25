---look for end with no start .... ?

;WITH CTE
AS
	(
		select * ,end_time = LEAD( created ,1 ,null) over (partition by job_id , robot_id,tote_id  order by created )
				, Next_status= LEAD( allocation_status,1 ,null) over (partition by job_id , robot_id,tote_id  order by created )
				,previous_status = lag( allocation_status,1 ,null) over (partition by job_id , robot_id,tote_id  order by created )
				, Next_status_robot_partition= LEAD( allocation_status,1 ,null) over (partition by robot_id,tote_id  order by created )

		from [dbo].allocations
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

--into dbo.Fact_Allocations
from cte c
	outer apply(
				select errors_count = count(*) 
				from [dbo].[errors_distinct] ed
				where ed.component_id = c.robot_id 
					and ed.created between c.created and c.end_time
				) err
where allocation_status = 'start' and Next_status_robot_partition = 'start'--previous_status is null-- and end_time is not null
	
order by created

--created		job_id									robot_id	tote_id	 robot_type	allocation_status	date_time		end_time	Next_status	previous_status	Next_status_robot_partition
--1682886401	f90910ef-c022-4d13-8e82-4960695a7a6d	GR003031	10058813	ground	start	2023-04-30 20:26:42.0000000		NULL	NULL		NULL				start
--1682886092	ff687fb3-0789-4a08-938e-d60a82ecd543	GR003053	10057984	ground	start	2023-04-30 20:21:32.0000000		NULL	NULL		NULL				start
--1682844624	1340e28f-b140-4f58-abcc-f55685c39d47	GR003058	10057249	ground	start	2023-04-30 08:50:25.0000000		NULL	NULL		NULL				start
--1682886099	f90910ef-c022-4d13-8e82-4960695a7a6d	GR003059	20022956	ground	start	2023-04-30 20:21:39.0000000		NULL	NULL		NULL				start
--1682843854	6b15cb1c-c042-4b67-b35f-24cb364d5ccd	GR003067	10054663	ground	start	2023-04-30 08:37:34.0000000		NULL	NULL		NULL				start
--1682886420	f32da720-15b0-432d-bce4-c3ad9aad7376	GR003067	20021898	ground	start	2023-04-30 20:27:01.0000000		NULL	NULL		NULL				start


select * 
from allocations
where robot_id = 'GR003031'
order by created

select * 
from check_start_with_no_end
where robot_id = 'GR003031'
order by created

select * from allocations
where cast( date_time as date) = '2023-04-30'
	and datepart(hour,date_time) = 6
order by created


---indication for center restart :
--if there is zero orders(job start) for at least 5 minutes and the first minute that I oberved start of a job after that , the number of jobs started needs to be >= jobs had the last minute obsrevation.
--examples: 
	--1. 30/04 6:20 -06:27
	--2. 30/04 20:31 -20:41 
	--3.

--is it possiable to commit an order  when the center is down ? what is the user notification when it is happening ?

select * 
from check_start_with_no_end
where  cast( date_time as date) = '2023-04-30'
	--and datepart(hour,date_time) = 6
--where robot_id = 'GR003067'
order by created

--created		job_id									robot_id	tote_id	  robot_type	allocation_status	date_time	
--1682843854	6b15cb1c-c042-4b67-b35f-24cb364d5ccd	GR003067	10054663	ground				start		2023-04-30 08:37:34.0000000	
--1682844208	8a8c6ea6-fdef-4258-8a00-a9200203f9f9	GR003179	10001323	ground				start		2023-04-30 08:43:29.0000000	
--1682844342	1915101e-3d8e-4981-b073-315239149a94	GR003284	10056346	ground				start		2023-04-30 08:45:43.0000000	
--1682844544	ece9828c-8d02-44e8-8fa0-855017accc52	GR003120	10048449	ground				start		2023-04-30 08:49:04.0000000	
--1682844544	ece9828c-8d02-44e8-8fa0-855017accc52	GR003285	10050892	ground				start		2023-04-30 08:49:04.0000000	
--1682844544	ece9828c-8d02-44e8-8fa0-855017accc52	GR003098	10060419	ground				start		2023-04-30 08:49:04.0000000	
--1682844548	ece9828c-8d02-44e8-8fa0-855017accc52	GR003221	10051805	ground				start		2023-04-30 08:49:08.0000000	
--1682844554	6a6bf2d2-754f-4437-ba39-c209fb49a9d4	GR003240	10050864	ground				start		2023-04-30 08:49:14.0000000	
--1682844564	ece9828c-8d02-44e8-8fa0-855017accc52	GR003205	10052987	ground				start		2023-04-30 08:49:25.0000000	
--1682844573	92b0ad73-50c6-4dab-89a2-a38e8f74a078	GR003144	10055324	ground				start		2023-04-30 08:49:34.0000000	
--1682844624	ece9828c-8d02-44e8-8fa0-855017accc52	GR003257	10065321	ground				start		2023-04-30 08:50:25.0000000	
--1682844624	1340e28f-b140-4f58-abcc-f55685c39d47	GR003058	10057249	ground				start		2023-04-30 08:50:25.0000000	
--1682844666	22c124aa-c49b-4a94-b496-5dea3366c5e0	GR003124	10051066	ground				start		2023-04-30 08:51:07.0000000	
--1682844670	1340e28f-b140-4f58-abcc-f55685c39d47	GR003283	10061945	ground				start		2023-04-30 08:51:10.0000000	
--1682844685	22c124aa-c49b-4a94-b496-5dea3366c5e0	GR003282	10055518	ground				start		2023-04-30 08:51:25.0000000	
--1682844721	9bbff08a-b0e5-4212-a324-ccf0e97c467f	GR003251	10058608	ground				start		2023-04-30 08:52:02.0000000	
--1682844752	e0cfe733-0978-4aaa-b4bc-96376cbb0184	GR003153	10057352	ground				start		2023-04-30 08:52:32.0000000	
--1682844752	1340e28f-b140-4f58-abcc-f55685c39d47	GR003121	10055482	ground				start		2023-04-30 08:52:32.0000000	
--1682844759	d0079eef-5fde-49e8-8590-574827baf749	GR003182	10058283	ground				start		2023-04-30 08:52:40.0000000	
--1682844763	9bbff08a-b0e5-4212-a324-ccf0e97c467f	GR003256	10065309	ground				start		2023-04-30 08:52:43.0000000	
--1682844765	e0cfe733-0978-4aaa-b4bc-96376cbb0184	GR003194	20022804	ground				start		2023-04-30 08:52:46.0000000	
--1682844776	1340e28f-b140-4f58-abcc-f55685c39d47	GR003117	10057908	ground				start		2023-04-30 08:52:57.0000000	
--1682844795	e0cfe733-0978-4aaa-b4bc-96376cbb0184	GR003175	10053175	ground				start		2023-04-30 08:53:15.0000000	
--1682844861	214aae95-a6e7-4f2e-b1db-e3f3096c8230	GR003277	10060179	ground				start		2023-04-30 08:54:21.0000000	
--1682844890	d0079eef-5fde-49e8-8590-574827baf749	GR003298	10065352	ground				start		2023-04-30 08:54:50.0000000	
--1682844926	1340e28f-b140-4f58-abcc-f55685c39d47	GR003243	10006331	ground				start		2023-04-30 08:55:26.0000000	
--1682844936	214aae95-a6e7-4f2e-b1db-e3f3096c8230	GR003183	10057830	ground				start		2023-04-30 08:55:37.0000000	
--1682845003	1340e28f-b140-4f58-abcc-f55685c39d47	GR003160	10056295	ground				start		2023-04-30 08:56:43.0000000	
--1682845038	1340e28f-b140-4f58-abcc-f55685c39d47	GR003161	10056421	ground				start		2023-04-30 08:57:19.0000000	
--1682845051	214aae95-a6e7-4f2e-b1db-e3f3096c8230	GR003258	10068103	ground				start		2023-04-30 08:57:32.0000000	
--1682845135	673bfb6b-17fa-410b-9c14-237954d8811e	GR003176	10056969	ground				start		2023-04-30 08:58:55.0000000	
--1682845171	fe82c188-fb88-4eff-8d94-9eac0cbbb8ee	LR00002284	10065928	lift				start		2023-04-30 08:59:32.0000000	


select * 
from allocations
where job_id = 'ece9828c-8d02-44e8-8fa0-855017accc52'
order by robot_id , created


--created	job_id	robot_id	tote_id	robot_type	allocation_status	date_time
--1682844544	ece9828c-8d02-44e8-8fa0-855017accc52	GR003195	10055074	ground	start	2023-04-30 08:49:04.000
--1682844544	ece9828c-8d02-44e8-8fa0-855017accc52	GR003098	10060419	ground	start	2023-04-30 08:49:04.000
--1682844544	ece9828c-8d02-44e8-8fa0-855017accc52	GR003285	10050892	ground	start	2023-04-30 08:49:04.000
--1682844544	ece9828c-8d02-44e8-8fa0-855017accc52	GR003120	10048449	ground	start	2023-04-30 08:49:04.000
--1682844548	ece9828c-8d02-44e8-8fa0-855017accc52	GR003221	10051805	ground	start	2023-04-30 08:49:08.000

select * 
from allocations
where --tote_id = 10065321 and 
	date_time between '2023-04-30 18:00:25.0000000' and '2023-04-30 19:00:25.0000000'
order by created

select * from check_start_with_no_end
order by created

select * 
from check_start_with_no_end
where date_time between '2023-04-30 03:40:25' and '2023-04-30 04:10:25'


select * 
from Fact_Allocations
where date_time between '2023-04-30 18:00:25' and '2023-04-30 19:00:25'
		--and job_id = '3afda02d-ac0a-4f11-a747-d7da93047b4d'
order by created

--same allocation date for two different jobs
--1. allocationsselect 

--A. allocations_in_the_same_second_precent
	
	;with cte
	as
	(
		select job_id ,allocations_in_the_same_second = count(distinct created) , allocations = count(*) ,allocations_in_the_same_second_precent = count(distinct created) *1.0 /  count(*)
		from Fact_Allocations fa
		group by  job_id
		--order by   count(distinct created) desc, count(distinct created) *1.0 /  count(*) desc
	)
	select allocations_in_the_same_second_precent, jobs = count(*) ,avg_allocations = avg(allocations *1.0)
	from cte
	group by allocations_in_the_same_second_precent
	order by allocations_in_the_same_second_precent desc

--B
--2 dim jobs :
--A.
	select Job_Created ,Job_Created_timestamp = Format(DATEADD(SECOND, Job_Created, '1970-01-01') , 'HH:mm:ss'),Jobs = count(*)  
	from Dim_Jobs
	group by Job_Created ,Format(DATEADD(SECOND, Job_Created, '1970-01-01') , 'HH:mm:ss')
	order by count(*) desc
--B
	WITH CTE
	AS
	(
		select Job_Created ,Job_Created_timestamp = Format(DATEADD(SECOND, Job_Created, '1970-01-01') , 'HH:mm:ss'),Jobs = count(*)  
		from Dim_Jobs
		group by Job_Created ,Format(DATEADD(SECOND, Job_Created, '1970-01-01') , 'HH:mm:ss')
		--order by count(*) desc
	)
	select hour_ = DATEPART(hour , Job_Created_timestamp),cases = count(*) ,total_jobs = sum(Jobs)
	from cte
	group by DATEPART(hour , Job_Created_timestamp)
	order by cases desc