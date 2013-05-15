package org.bdigital;

import org.apache.log4j.PropertyConfigurator;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Main00_1s1Rb1Bb {

    public static void main(String[] args) {
	PropertyConfigurator.configure(Main00_1s1Rb1Bb.class.getClassLoader().getResourceAsStream(
		"log4j.properties"));

	Config conf = new Config();
	conf.setMessageTimeoutSecs(5000);

	TopologyBuilder builder = new TopologyBuilder();
	builder.setSpout("tweetSpout", // ID
		new FakeTweetSpout(), // Tipus
		1 // # d'instancies - cardinalitat
	);
	
	builder.setBolt("upperBolt", // ID
		new UpperBolt(), // Tipus
		1) // Cardinalitat
		.allGrouping("tweetSpout"); // origen de les tuples
	
	builder.setBolt("echoBolt", // ID
		new EchoConsoleBolt(), // Tipus
		1) // Cardinalitat
		.allGrouping("upperBolt"); // origen de les tuples

	LocalCluster cluster = new LocalCluster(); // Cluster de development
	cluster.submitTopology("tweetteater", conf, builder.createTopology());

    }

}
