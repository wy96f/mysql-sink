#Flume mysql sink

A custom sink which streams events containing delimited text directly into a mysql table. 

## Features

* auto choosing table

* auto partitioning table 

## Configuration

Each event will be wrote to the corresponding table following a custom rule and fields from incoming events will be mapped to corresponding columns in the table.


	mysql1.sinks = ms2

	mysql1.sinks.ms2.type = cn.v5.flume.sink.MysqlSink
	mysql1.sinks.ms2.channel = fileChannel2
	mysql1.sinks.ms2.dburl = jdbc:mysql://exmaple.com.cn/test?useUnicode=true&autoReconnect=true&rewriteBatchedStatements=true&connectTimeout=12000&socketTimeout=12000
	mysql1.sinks.ms2.dbuser = test
	mysql1.sinks.ms2.dbpass = test
	mysql1.sinks.ms2.batch_size = 100
	mysql1.sinks.ms2.dbmapper = user_channel_status

	mysql1.sinks.ms2.dbmapper.user_channel_status.pattern = 3==tcp&6=~^(login|logout)$ 
	mysql1.sinks.ms2.dbmapper.user_channel_status.partition = 5:yyyy_MM_dd
	mysql1.sinks.ms2.dbmapper.user_channel_status.fields = 4,5,6,7,9,10,11,12,13 
	mysql1.sinks.ms2.dbmapper.user_channel_status.prepare = insert into user_channel_status (req_time, req_time_long, action, user_id, session, ip, channel_uuid, channel_mode, country_code) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
	
Configuration above means that events in which 3th field equaling "tcp" and 6th field starting with "login" or "logout" will be wrote to user_channel_status table and partitioned by day identified by 5th field.