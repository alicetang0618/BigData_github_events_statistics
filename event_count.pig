A = LOAD '/inputs/xiaoruit/github/part*' USING PigStorage(',') AS (actor: int, type: chararray, day_of_week: int, hour_of_day: chararray, date: chararray);
B = GROUP A ALL;
C = FOREACH B {
	unique_dates = DISTINCT $1.date;
	GENERATE FLATTEN($1.(actor, type, day_of_week, hour_of_day, date)), COUNT(unique_dates) AS date_cnt;
}
D = GROUP C BY (type, (chararray) day_of_week, hour_of_day, date);
E = FOREACH D {
  	unique_actors = DISTINCT $1.actor;
  	GENERATE CONCAT(group.type, group.day_of_week, group.hour_of_day) AS key,
  		COUNT(unique_actors) AS unique_user_cnt,
  		COUNT(C) AS event_cnt,
  		MAX(C.date_cnt) AS date_cnt;
}
F = GROUP E BY key;
G = FOREACH F GENERATE group AS key, SUM($1.unique_user_cnt) AS user_count, SUM($1.event_cnt) AS event_count, MAX($1.date_cnt) AS date_count;

STORE G INTO 'hbase://github_events_xiaoruit'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'event:user_count, event:event_count, event:date_count');