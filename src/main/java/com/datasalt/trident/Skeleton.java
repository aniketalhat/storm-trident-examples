package com.datasalt.trident;

import java.io.IOException;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * Use this skeleton for starting your own topology that uses the Fake tweets generator as data source.
 * 
 * @author pere
 */
public class Skeleton {

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(100);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
				.partitionBy(new Fields("location"))
				//.each(new Fields("actor", "text"), new Utils.ActorFilter("dave"));
				//.each(new Fields("text", "actor"), new Utils.UpperCaseFuntion(), new Fields("uppercase"))
				//.each(new Fields("actor", "text"), new Utils.PrintFilter());
				.partitionAggregate(new Fields("location"), new Utils.LocationAggregator(), new Fields("location-count"))
				.each(new Fields("location-count"), new Utils.LocationCount());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, buildTopology(drpc));

	}
}
