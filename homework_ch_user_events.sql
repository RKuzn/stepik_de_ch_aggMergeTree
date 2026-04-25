--2026-04-23
--движки, агрегаты, mat.view, метрики

--1. У Вас есть события пользователя (user_events), которые записываются в Clickhouse.  Данные в этой таблице должны хранится 30 дней.
drop table if exists user_events;
CREATE TABLE user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL '30 day' DELETE
;



--2. Построить агрегированную таблицу. Храним агрегаты 180 дней, чтобы делать трендовый анализ:
--drop table user_events_agg;
--truncate table user_events_agg;
CREATE TABLE user_events_agg (
	event_date date,
	event_type String,
	users_state AggregateFunction(uniq, UInt32),
	points_state AggregateFunction(sum, UInt32),
	actions_state AggregateFunction(count, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + interval '180 day' DELETE;


--3. Сделать Materialized View
--drop view user_events_mv;
create materialized view user_events_mv 
to user_events_agg
as
select 
	toDate(event_time) as event_date,
	event_type,
	uniqState(user_id) as users_state,
	sumState(points_spent) as points_state,
	countState() as actions_state
from user_events
group by event_date, event_type;


--Запрос для вставки тестовых данных 
--truncate table user_events;
INSERT INTO user_events VALUES

(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),


(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),


(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),


(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),


(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),


(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());


--select * from user_events;


--4. Создать запрос, показывающий:
--Retention: сколько пользователей вернулись в течение следующих 7 дней. Как считается Retention? Гуглим. 
--Формат результата - total_users_day_0|returned_in_7_days|retention_7d_percent|

SET join_use_nulls = 1;		--чтобы не возвращать 0 вместо null, когда left join не нашел совпадения

with
--Определение первого визита: 
--Сначала необходимо определить дату первого визита для каждого пользователя, чтобы использовать её как точку отсчёта
users_day0 as (
	select user_id, 
		min(event_time) as min_event_time 
	from user_events group by user_id
),
--Расчет количества пользователей, вернувшихся в разные дни: 
--Затем нужно подсчитать количество пользователей, которые были активны в каждый последующий день после их первого визита.
users_day7 as (
	select --*, toStartOfDay(min_event_time) + interval '8 day'
		count(distinct u0.user_id) as total_users_day_0,
		count(distinct ue.user_id) as returned_in_7_days
		--,count(distinct if (ue.user_id = 0, null, ue.user_id)) as returned_in_7_days2
		--,count(distinct nullif(ue.user_id, 0)) as returned_in_7_days3
		--,countDistinctIf(ue.user_id, ue.user_id <> 0) as returned_in_7_days4
	from users_day0 as u0
	  left join user_events as ue
	    on ue.user_id = u0.user_id
	    and ue.event_time > u0.min_event_time
	    and ue.event_time <= toDate(min_event_time) + interval '7 day'
)
--Расчет Retention: 
--Наконец, разделите количество вернувшихся пользователей на общее количество новых пользователей и умножьте на 100, чтобы получить процент.
select 
	total_users_day_0,
	returned_in_7_days,
	round(returned_in_7_days / total_users_day_0 * 100, 2) as retention_7d_percent
  from users_day7;


--5. Создать запрос с группировками по быстрой аналитике по дням, формат ниже.
select event_date, 
		event_type, 
		uniqMerge(users_state) as unique_users, 
		sumMerge(points_state) as total_spent,
		countMerge(actions_state) as total_actions
from user_events_agg
group by event_date, event_type
order by event_date, event_type;



