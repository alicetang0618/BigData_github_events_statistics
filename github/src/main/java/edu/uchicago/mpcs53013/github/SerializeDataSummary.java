package edu.uchicago.mpcs53013.github;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.json.JSONException;

import edu.uchicago.mpcs53013.dataSummary.DataSummary;

public class SerializeDataSummary {
	static TProtocol protocol;
	public static void main(String[] args) throws NumberFormatException, JSONException, ParseException {
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			final Configuration finalConf = new Configuration(conf);
			final FileSystem fs = FileSystem.get(conf);
			final TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
			DataSummaryProcessor processor = new DataSummaryProcessor() {
				Map<String, SequenceFile.Writer> yearMonthMap = new HashMap<String, SequenceFile.Writer>();
				Pattern yearMonthPattern = Pattern.compile("^(\\d+-\\d+)-\\d+-\\d+");
				
				Writer getWriter(File file) throws IOException {
					Matcher yearMatcher = yearMonthPattern.matcher(file.getName());
					if(!yearMatcher.find())
						throw new IllegalArgumentException("Bad file name. Can't find year: " + file.getName());
					String yearMonth = yearMatcher.group(1);
					if(!yearMonthMap.containsKey(yearMonth)) {
						yearMonthMap.put(yearMonth, 
								SequenceFile.createWriter(finalConf,
										SequenceFile.Writer.file(
												new Path("/inputs/xiaoruit/thriftData/data-" + yearMonth)),
										SequenceFile.Writer.keyClass(IntWritable.class),
										SequenceFile.Writer.valueClass(BytesWritable.class),
										SequenceFile.Writer.compression(CompressionType.NONE)));
					}
					return yearMonthMap.get(yearMonth);
				}
				
				@Override
				public void processDataSummary(DataSummary summary, File file) throws IOException {
					try {
						getWriter(file).append(new IntWritable(1), new BytesWritable(ser.serialize(summary)));;
					} catch (TException e) {
						throw new IOException(e);
					}
				}
			};
			processor.processDirectory(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
