/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [job_id]
		,Job_created_Date = min(allocation_date)
		,Job_Created = MIN(created) , Job_Ended = MAX(end_time)
		,Job_Duration = MAX(end_time) - MIN(created) 
  into dbo.Dim_Jobs
  FROM [Fabric_Assignment].[dbo].[Fact_Allocations]
  group by [job_id]

