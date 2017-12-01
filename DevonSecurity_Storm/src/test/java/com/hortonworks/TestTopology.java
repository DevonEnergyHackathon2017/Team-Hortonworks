package com.hortonworks;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Test;

import com.github.rtempleton.poncho.test.AbstractTestCase;

public class TestTopology extends AbstractTestCase {
	
	private final String TOPOLOGY_CONFIG = getResourcePath("config.properties");

	@Test
	public void testTopology() throws Exception{
		DevonTopology topo = new DevonTopology(TOPOLOGY_CONFIG);
		TopologyBuilder builder = topo.compose();
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		
		cluster.submitTopology("DevonTopology", conf, builder.createTopology());
		
		Thread.sleep(10*60*1000);
		
		cluster.shutdown();
		cluster.killTopology("DevonTopology");
		System.out.println("Finsihed!!!");
		
	}

}
