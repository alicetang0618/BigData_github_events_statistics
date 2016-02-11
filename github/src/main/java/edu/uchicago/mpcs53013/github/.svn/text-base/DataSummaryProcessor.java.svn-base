package edu.uchicago.mpcs53013.github;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.json.JSONTokener;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uchicago.mpcs53013.dataSummary.DataSummary;

public abstract class DataSummaryProcessor {
	static class MissingDataException extends Exception {

	    public MissingDataException(String message) {
	        super(message);
	    }

	    public MissingDataException(String message, Throwable throwable) {
	        super(message, throwable);
	    }

	}
	
	static double tryToReadMeasurement(String name, String s, String missing) throws MissingDataException {
		if(s.equals(missing))
			throw new MissingDataException(name + ": " + s);
		return Double.parseDouble(s.trim());
	}

	void processLine(String line, File file) throws IOException, NumberFormatException, JSONException, ParseException {
		try {
			DataSummary temp = dataFromLine(line);
			if (temp != null){
			processDataSummary(temp, file);
			}
		} catch(MissingDataException e) {
			// Just ignore lines with missing data
		}
	}
	
	abstract void processDataSummary(DataSummary summary, File file) throws IOException;
	BufferedReader getFileReader(File file) throws FileNotFoundException, IOException {
		if(file.getName().endsWith(".gz"))
			return new BufferedReader
					     (new InputStreamReader
					    		 (new GZIPInputStream
					    				 (new FileInputStream(file))));
		return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	}
	
	void processFile(File file) throws IOException, NumberFormatException, JSONException, ParseException {		
		BufferedReader br = getFileReader(file);
		br.readLine(); // Discard header
		String line;
		while((line = br.readLine()) != null) {
			processLine(line, file);
		}
	}

	void processDirectory(String directoryName) throws IOException, NumberFormatException, JSONException, ParseException {
		File directory = new File(directoryName);
		File[] directoryListing = directory.listFiles();
		for(File file : directoryListing)
			processFile(file);
	}
	
	DataSummary dataFromLine(String line) throws NumberFormatException, MissingDataException, ParseException {
		JSONObject object;
		try {
			object = (JSONObject) new JSONTokener(line).nextValue();
			String datetime = object.getString("created_at");
			String date_string = datetime.substring(0, 10);
			DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
			Date date = format.parse(date_string);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			
			DataSummary summary 
				= new DataSummary(Integer.parseInt(object.getJSONObject("actor").getString("id")),
						object.getString("type"),
						(short) cal.get(Calendar.DAY_OF_WEEK),
						datetime.substring(11, 13),
						date_string);
			return summary;
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
