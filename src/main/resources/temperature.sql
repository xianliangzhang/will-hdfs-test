create table t_hadoop_weather_station(
  id bigint not null primary key auto_increment comment '气象站ID',
  station_id varchar(255) not null comment '气象站ID',
  station_name varchar(255) not null comment '站点名'
);

create table t_hadoop_weather_resord(
  id bigint not null primary key auto_increment comment '气象记录ID',
  station_id varchar(255) not null comment '气象站ID',
  record_time datetime comment '记录时间',
  temperature int
);

insert into t_hadoop_weather_station(station_id, station_name) values('011990-999999', 'XianStation');
insert into t_hadoop_weather_station(station_id, station_name) values('012650-999999', 'BeijingStation');

insert into t_hadoop_weather_resord(station_id, record_time, temperature) values('011990-999999', '1949-03-24 12:00:00', 111);
insert into t_hadoop_weather_resord(station_id, record_time, temperature) values('011990-999999', '1949-03-24 19:00:00', 78);
insert into t_hadoop_weather_resord(station_id, record_time, temperature) values('011990-999999', '1949-03-24 20:00:00', 21);
insert into t_hadoop_weather_resord(station_id, record_time, temperature) values('011990-999999', '1949-03-24 21:00:00', -11);

insert into t_hadoop_weather_resord(station_id, record_time, temperature) values('012650-999999', '1949-03-24 16:00:00', 22);
insert into t_hadoop_weather_resord(station_id, record_time, temperature) values('012650-999999', '1949-03-24 17:00:00', -21);