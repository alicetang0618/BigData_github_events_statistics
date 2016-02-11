package edu.uchicago.mpcs53013.githubTopology;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class GithubTopology {

	static class ExtractInfoBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String report = tuple.getString(0);
			try {
				JSONObject object = (JSONObject) new JSONTokener(report).nextValue();
				String datetime = object.getString("created_at");
				String date_string = datetime.substring(0, 10);
				DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
				Date date = format.parse(date_string);
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				collector.emit(new Values(
						object.getString("type")+Integer.toString(cal.get(Calendar.DAY_OF_WEEK))+datetime.substring(11, 13),
						object.getJSONObject("actor").getString("id"),
						date_string));
			} catch (JSONException | ParseException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("type_day_hour", "user", "date"));
		}

	}

	static class GetStatsBolt extends BaseBasicBolt {
		//fieldsGrouping ensures that unique values for each type_day_hour are kept track of.
		Set<String> user_distincter = Collections.synchronizedSet(new HashSet<String>());
		Set<String> date_distincter = Collections.synchronizedSet(new HashSet<String>());
		String datetime = "";
		
		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String group = input.getStringByField("type_day_hour");
			String user = input.getStringByField("user");
			String date = input.getStringByField("date");
			String hour = group.substring(group.length()-2);
			int user_count = 0;
			int date_count = 0;
			
			//Since the incoming data is sorted by time, clear the distincters when the bolt sees a new time.
			if ((date+hour) != datetime){
				datetime = date+hour;
				user_distincter.clear();
				date_distincter.clear();
			}
			
			if (!user_distincter.contains(group+user)){
				user_count = 1;
				user_distincter.add(group+user);
			}
			if (!date_distincter.contains(group)){
				date_count = 1;
				date_distincter.add(group);
			}
			
			collector.emit(new Values
					(group, 1, user_count, date_count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("type_day_hour", "event_cnt", "user_cnt", "date_cnt"));
		}

	}

	static class UpdateDataBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private HConnection hConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
				conf.set("zookeeper.znode.parent", "/hbase-unsecure");
				hConnection = HConnectionManager.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			HTableInterface table = null;
			try {
				String group = input.getStringByField("type_day_hour");
				int user_cnt = input.getIntegerByField("user_cnt");
				int event_cnt = input.getIntegerByField("event_cnt");
				int date_cnt = input.getIntegerByField("date_cnt");
				
				table = hConnection.getTable("github_events_xiaoruit");
				try {
					Increment increment = new Increment(Bytes.toBytes(group));
					increment.addColumn(Bytes.toBytes("event"), Bytes.toBytes("user_count"), (long)user_cnt);
					increment.addColumn(Bytes.toBytes("event"), Bytes.toBytes("event_count"), (long)event_cnt);
					increment.addColumn(Bytes.toBytes("event"), Bytes.toBytes("date_count"), (long)date_cnt);
					table.increment(increment);
				} catch (DoNotRetryIOException e){
					Put put = new Put(Bytes.toBytes(group));
					put.add(Bytes.toBytes("event"), Bytes.toBytes("user_count"), Bytes.toBytes(user_cnt));
					put.add(Bytes.toBytes("event"), Bytes.toBytes("event_count"),Bytes.toBytes(event_cnt));
					put.add(Bytes.toBytes("event"), Bytes.toBytes("date_count"), Bytes.toBytes(date_cnt));
					table.put(put);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if(table != null)
					try {
						table.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkIp = "localhost";

		String nimbusHost = "sandbox.hortonworks.com";

		String zookeeperHost = zkIp +":2181";

		ZkHosts zkHosts = new ZkHosts(zookeeperHost);
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(zkIp);
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "xiaoruit-github-events", "/xiaoruit-github-events","test_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		kafkaConfig.zkServers = zkServers;
		kafkaConfig.zkRoot = "/xiaoruit-github-events";
		kafkaConfig.zkPort = 2181;
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("raw-github-events", kafkaSpout, 1);
		builder.setBolt("extract-info", new ExtractInfoBolt(), 1).shuffleGrouping("raw-github-events");
		builder.setBolt("get-stats", new GetStatsBolt(), 1).fieldsGrouping("extract-info", new Fields("type_day_hour"));
		builder.setBolt("update-data", new UpdateDataBolt(), 1).fieldsGrouping("get-stats", new Fields("type_day_hour"));

		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("github-topology", conf, builder.createTopology());
		}
	}
}
