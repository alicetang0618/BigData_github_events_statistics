package edu.uchicago.mpcs53013.githubKafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaGithubFeed {
	static class Task extends TimerTask {
		Task() throws MalformedURLException {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd-H");
			format.setTimeZone(TimeZone.getTimeZone("UTC"));
			Calendar cal = Calendar.getInstance();
			//Download the hourly data from the previous hour.
			cal.add(Calendar.HOUR_OF_DAY, -1);
			String url = "http://data.githubarchive.org/" + format.format(cal.getTime()) + ".json.gz";
			githubURL = new URL(url);
		}
		@Override
		public void run() {
			try {
				// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		        Properties props = new Properties();
//		        props.put("metadata.broker.list", "sandbox.hortonworks.com:6667");
//		        props.put("zk.connect", "localhost:2181");
		        props.put("metadata.broker.list", "hadoop-m.c.mpcs53013-2015.internal:6667");
		        props.put("zk.connect", "hadoop-w-1.c.mpcs53013-2015.internal:2181,hadoop-w-0.c.mpcs53013-2015.internal:2181,hadoop-m.c.mpcs53013-2015.internal:2181");
		        props.put("serializer.class", "kafka.serializer.StringEncoder");
		        props.put("request.required.acks", "1");

		        String TOPIC = "xiaoruit-github-events";
		        ProducerConfig config = new ProducerConfig(props);

		        Producer<String, String> producer = new Producer<String, String>(config);
				URLConnection conn = githubURL.openConnection();
				GZIPInputStream gzis = new GZIPInputStream(conn.getInputStream());
				BufferedReader br = new BufferedReader(new InputStreamReader(gzis));
				String line;
				while((line = br.readLine()) != null) {				
	                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, line);
	                producer.send(data);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		URL githubURL;
	}
	public static void main(String[] args) {
		try {
			Timer timer = new Timer();
			timer.scheduleAtFixedRate(new Task(), 0, 60*60*1000);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
