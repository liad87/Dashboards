;with cte
as
(
	select cc.conditions_combination, ev.Event_Name ,Events_Amount = count(*)
	from events ev 
		left join Devices dev on dev.DeviceId = ev.DeviceId
		left join Users_conditions uc on uc.UserId = dev.UserId
		left join Conditions_combo cc on uc.Conditions_combination_Id = cc.cc_id
		group by cc.conditions_combination , ev.Event_Name 
		 
), rnk
as
(
	select * ,rn = Rank() over (partition by c.conditions_combination order by Events_Amount desc)
	from cte c
)
select conditions_combination, Event_Name ,Events_Amount 
from rnk
where rn <= 3

select Device , Events = Count(*) ,Users = count(distinct dev.UserId) , Days_used = count(distinct Event_Date) ,mini_program_impressions = sum(IIF(Event_Name = 'mini_program_impression' ,1 ,0 ))
from events ev 
		left join Devices dev on dev.DeviceId = ev.DeviceId
group by Device
	


;with cte
as
(
	select  Device ,Events = Count(*) ,Users = count(distinct dev.UserId) 
		, Users_Days = count(distinct concat(UserId,'_',Event_Date) ) 
		,mini_program_impressions = sum(IIF(Event_Name = 'mini_program_impression' ,1 ,0 )) 
		,Users_Used_mini_program_impression = count(distinct IIF(Event_Name = 'mini_program_impression' ,UserId ,null ) )
	from events ev 
			join Devices dev on dev.DeviceId = ev.DeviceId
	group by Device
)
select  *,AVG_events_per_user = Events *1.0 /Users
		,AVG_Days_used_per_user = Users_Days*1.0/Users 
		,AVG_mini_program_impressions_per_user = mini_program_impressions*1.0 /Users
		,precent_of_users_used_mini_program_impressions = Users_Used_mini_program_impression *1.0 / Users
from cte