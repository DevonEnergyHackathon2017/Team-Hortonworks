package com.hortonworks.bolts;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rtempleton.poncho.StormUtils;

public class SiteLookup implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger Log = LoggerFactory.getLogger(SiteLookup.class);
	
	private final String table;
	private final String jdbcurl;
	private final String KRB5LOCATION, PRINCIPAL, KEYTAB;
	private final ArrayList<Site> siteCache = new ArrayList<Site>();
	
	private int counter = 0;
	private OutputCollector collector;
	
	private final List<String> inputFields;
	
	
	public SiteLookup(Properties props, String boltName, List<String> inputFields) {
		this.inputFields = inputFields;
		jdbcurl = StormUtils.getRequiredProperty(props, boltName + ".jdbcUrl");
		table = StormUtils.getRequiredProperty(props, boltName + ".table");
		
		KRB5LOCATION = (props.getProperty("security.krb5location")!=null)?props.getProperty("security.krb5location"):"/etc/krb5.conf";
		PRINCIPAL = (props.getProperty("security.principal")!=null)?props.getProperty("security.principal"):null;
		KEYTAB = (props.getProperty("security.keytab")!=null)?props.getProperty("security.keytab"):null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		populateCache();
	}

	@Override
	public void execute(Tuple input) {
		//implmenet a method to periodically refresh the cache
		counter++;
		if(counter%30000==0) {
			populateCache();
			counter=0;
		}
		
		Float lat = input.getFloatByField("LAT");
		Float lon = input.getFloatByField("LON");
		int siteId = 0;
		
		for(Site site : siteCache) {
			if(lat>site.P1_LATITUDE && lat<site.P2_LATITUDE && lon>site.P1_LONGITUDE && lon<site.P2_LONGITUDE) {
				siteId = site.id;
				break;
			}
		}
		
		List<Object> vals = input.getValues();
		vals.add(siteId);
		
		collector.emit(input, vals);
		collector.ack(input);

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//append the field names that will be added by this bolt
		final ArrayList<String> outputFields = new ArrayList<String>(this.inputFields);
		outputFields.add("SITEID");
		declarer.declare(new Fields(outputFields));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void populateCache() {
		try{
			authenticate();
			Connection con = DriverManager.getConnection(jdbcurl);
			
			String query = "select * from " + this.table;
			PreparedStatement stmt = con.prepareStatement(query);
			ResultSet rset = stmt.executeQuery();
			siteCache.clear();
			while (rset.next()){
				Site site = new Site(rset.getInt(1), rset.getDouble(2), rset.getDouble(3), rset.getDouble(4), rset.getDouble(5));
				this.siteCache.add(site);
			}
			stmt.close();
			
			con.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	private void authenticate() throws IOException {
		if(KEYTAB!=null && PRINCIPAL!=null) {
            System.setProperty("java.security.krb5.conf", KRB5LOCATION);
            UserGroupInformation.loginUserFromKeytab(PRINCIPAL, KEYTAB);
		}
	}
	
	public class Site{
		
		final int id;
		final double P1_LATITUDE, P1_LONGITUDE, P2_LATITUDE, P2_LONGITUDE;
		
		protected Site(int id, double p1lat, double p1lon, double p2lat, double p2lon) {
			this.id=id;
			this.P1_LATITUDE=p1lat;
			this.P1_LONGITUDE=p1lon;
			this.P2_LATITUDE=p2lat;
			this.P2_LONGITUDE=p2lon;
		}
	}

}
