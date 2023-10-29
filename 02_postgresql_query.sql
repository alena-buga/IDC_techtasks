-- convert from varchar to date after import data file
alter table stat alter COLUMN "day" type DATE USING "day"::date;

-- Show a number of clicks by User and calendar month
select name, sum(clicks) , extract(month from "day") as MM
from stat 
group by name, MM;

-- Show a share of clicks of each user by month
select  s.name, extract(month from "day") as MM ,
sum(clicks)/sum(q.ts)*100 as share_per_MM
from stat s
inner join (select name, sum(clicks) as ts from stat group by name) q on q.name = s.name
group by s.name, MM;