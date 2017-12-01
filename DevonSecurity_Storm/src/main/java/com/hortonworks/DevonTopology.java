package com.hortonworks;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;

import com.github.rtempleton.poncho.StormUtils;
import com.github.rtempleton.poncho.io.ConsoleLoggerBolt;
import com.github.rtempleton.poncho.io.PhoenixUpsertBolt;
import com.hortonworks.bolts.RecordParser;
import com.hortonworks.bolts.SiteLookup;

public class DevonTopology {
	
	private static final Logger Log = Logger.getLogger(DevonTopology.class);
	private static final String thisName = DevonTopology.class.getSimpleName();
	private final Properties topologyConfig;

	public DevonTopology(String properties) {
		topologyConfig = StormUtils.readProperties(properties);
	}
	
	public static void main(String[] args) throws Exception{
		DevonTopology topo = new DevonTopology(args[0]);
		topo.submit(topo.compose());
	}

	protected TopologyBuilder compose() throws Exception{
		
		TopologyBuilder builder = new TopologyBuilder();
		
		String spoutParallelism = topologyConfig.getProperty("spout.parallelism", "1");
		int parallelismHint = Integer.parseInt(spoutParallelism);
		
		final String bootStrapServers = StormUtils.getRequiredProperty(topologyConfig, "kafka.bootStrapServers");
		final String topic = StormUtils.getRequiredProperty(topologyConfig, "kafka.topic");
		final String consumerGroupId = StormUtils.getRequiredProperty(topologyConfig, "kafka.consumerGroupId");
		
		KafkaSpoutConfig<String, String> spoutConf =  KafkaSpoutConfig.builder(bootStrapServers, topic)
		        .setGroupId(consumerGroupId)
		        .setOffsetCommitPeriodMs(10_000)
		        .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
		        .setMaxUncommittedOffsets(1000000)
		        .setRetry(new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
		                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10)))
		        .build();
		
		// 1. spout reads data
		KafkaSpout<String, String> spout = new KafkaSpout<String, String>(spoutConf);
		builder.setSpout("spout", spout, parallelismHint);
		
		// 2. parse the JSON message
		RecordParser parser = new RecordParser(topologyConfig, StormUtils.getOutputfields(spout));
		builder.setBolt("parser", parser).localOrShuffleGrouping("spout");
		
		// 3. enrich data with site lookup
		SiteLookup site = new SiteLookup(topologyConfig, "SITE", StormUtils.getOutputfields(parser));
		builder.setBolt("site", site).localOrShuffleGrouping("parser");
		
		// 4. upsert the record into Phoenix
		PhoenixUpsertBolt MOVEMENT = new PhoenixUpsertBolt(topologyConfig, "MOVEMENT", StormUtils.getOutputfields(site));
		builder.setBolt("MOVEMENT", MOVEMENT).localOrShuffleGrouping("site");
		
		
		IRichBolt logger = new ConsoleLoggerBolt(topologyConfig, StormUtils.getOutputfields(site));
		builder.setBolt("consoleLogger", logger).localOrShuffleGrouping("site");
		
		return builder;
	}
	
	protected void submit(TopologyBuilder builder){

		try {
			StormSubmitter.submitTopology(thisName, topologyConfig, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (AuthorizationException e) {
			e.printStackTrace();
			System.exit(-1);	
		}

	}
}
