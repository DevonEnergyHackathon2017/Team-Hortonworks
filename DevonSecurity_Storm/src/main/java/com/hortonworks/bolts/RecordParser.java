package com.hortonworks.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

public class RecordParser implements IRichBolt{
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(RecordParser.class);
	private final List<String> inputFields;
	private OutputCollector collector;
	private int parseField = -1;

	public RecordParser(Properties props, final List<String> inputFields) {
		this.inputFields = inputFields;
		
		for(int i=0;i<inputFields.size();i++) {
			//hardcoding the name of the Kafka field here
			if(inputFields.get(i).equals("value"));
			parseField = i;
		}
		if (parseField==-1) {
			logger.error("The parse field was not found in the list of input fields");
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String val = input.getString(parseField);
		DocumentContext doc = JsonPath.parse(val);
		
		String id = null;
		Long ts = null;
		Float lat = null, lon = null;
		
		try {
		id = doc.read("$.id", String.class);
		ts = doc.read("$.timestamp", Long.class);
		lat = doc.read("$.latitude", Float.class);
		lon = doc.read("$.longitude", Float.class);
		} catch(PathNotFoundException e) {
			//nothing
		}
		
		ArrayList<Object> vals = new ArrayList<Object>(4);
		vals.add(id);
		vals.add(ts);
		vals.add(lat);
		vals.add(lon);
		
		//only push the record if we got all the values
		if(id!=null && ts!=null && lat!=null && lon!=null) {
			collector.emit(input, vals);
			collector.ack(input);
		}
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		final ArrayList<String> outputFields = new ArrayList<String>();
		outputFields.add("ID");
		outputFields.add("TS");
		outputFields.add("LAT");
		outputFields.add("LON");
		declarer.declare(new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
