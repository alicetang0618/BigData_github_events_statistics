MPCS 53013 Project
Xiaorui Tang, xiaoruit, 449972

1. Purpose

Github Archive logs the activities performed by open source developers all over the world. It provides interesting data on the general working patterns of Github developers regarding the type, frequency and time of their activities on Github.

This project aims at presenting the overall working patterns of Github developers by summarizing the raw event data provided by Github Archive. Through the web interface (http://104.197.20.219/xiaoruit/github-event/github-event.html), the user can select a day of week, an hour of day and an event type, to view the average number of events and distinct actors per date for the given day of week, hour of day and event type. If more than one type is selected, it returns the average number of events and distinct type-actor pairs per date for the given day of week, hour of day and event types.


2. Data

Github Archive data from 2015-10-20 to 2015-11-30.
Source: https://www.githubarchive.org/
Sample record (json format):
	{"id":"2489651062","type":"PushEvent","actor":{"id":485033,"login":"winterbe","gravatar_id":"","url":"https://api.github.com/users/winterbe","avatar_url":"https://avatars.githubusercontent.com/u/485033?"},"repo":{"id":28593843,"name":"winterbe/streamjs","url":"https://api.github.com/repos/winterbe/streamjs"},"payload":{"push_id":536863975,"size":1,"distinct_size":1,"ref":"refs/heads/master","head":"15b303203be31bd295bc831075da8f74b99b3981","before":"0fef99f604154ccfe1d2fcd0aadeffb5c58e43ff","commits":[{"sha":"15b303203be31bd295bc831075da8f74b99b3981","author":{"email":"winterbe@googlemail.com","name":"Benjamin Winterberg"},"message":"Add comparator support for min, max operations","distinct":true,"url":"https://api.github.com/repos/winterbe/streamjs/commits/15b303203be31bd295bc831075da8f74b99b3981"}]},"public":true,"created_at":"2015-01-01T15:00:03Z"}
Fields of interests: "id", "type", "created_at"

1) Batch layer
- Use data from 2015-10-20 to 2015-11-30.
- Total size of compressed files: 9.8 GB
Note: Github archive data is available from 2011-02-12 to present. Due to the 10GB size limit of raw data, only use a small part of the dataset to compute the batch views.

2) Speed layer
- New data will be ingested every hour. Every time new data is ingested, the "githubKafka" will download the data file containing all the github events in the previous hour (UTC).


3. Steps

1) Batch and Serving layer

a. Download data from 2015-10-20 to 2015-11-30 into hadoop-m:/mnt/scratch/xiaoruit/data using the following commands:
	wget http://data.githubarchive.org/2015-10-{20..31}-{0..23}.json.gz
	wget http://data.githubarchive.org/2015-11-{01..30}-{0..23}.json.gz

b. Use the "github" program to ingest data into thrift:
	hadoop jar uber-github-0.0.1-SNAPSHOT.jar edu.uchicago.mpcs53013.github.SerializeDataSummary /mnt/scratch/xiaoruit/data
	Note: See the "github" Maven project and the output files in the hdfs file system of hadoop-m: "/inputs/xiaoruit/thriftData/".

c. Use pig scripts to process thrift data and store the outputs in HBase:
	Please see read_thrift.pig and event_count.pig, and the "github_events_xiaoruit" table in hadoop-m HBase.

d. Set up a web interface for querying data:
	Please see the submitted directory "website", or "/var/www/cgi-bin/xiaoruit/github-event/" and "/var/www/html/xiaoruit/github-event/" on webserver.
	The website can be accessed from: http://104.197.20.219/xiaoruit/github-event/github-event.html.

2) Speed layer

a. Create a Kafka topic "xiaoruit-github-events" on hadoop-m:
	kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic xiaoruit-github-events

b. Use the "githubKafka" java program to ingest the new hourly data into the Kafka queue:
	nohup java -cp uber-githubKafka-0.0.1-SNAPSHOT.jar edu.uchicago.mpcs53013.githubKafka.KafkaGithubFeed &
	Note: See the "githubKafka" program. Use the above command to download and ingest data every hour.

c. Use storm to process new data in the Kafka queue and update the records in the HBase table "github_events_xiaoruit":
	storm jar uber-githubTopology-0.0.1-SNAPSHOT.jar edu.uchicago.mpcs53013.githubTopology.GithubTopology
	Note: See the "githubTopology" program.