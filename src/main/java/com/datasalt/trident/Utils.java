package com.datasalt.trident;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.tuple.Values;
import org.apache.commons.collections.MapUtils;
import storm.trident.operation.*;
import storm.trident.tuple.TridentTuple;

/**
 * Misc. util classes that can be used for implementing some stream processing examples.
 *  
 * @author pere
 */
public class Utils {

	/**
	 * A filter that filters nothing but prints the tuples it sees. Useful to test and debug things.
	 */
	@SuppressWarnings({ "serial", "rawtypes" })
	public static class PrintFilter implements Filter {

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
		}
		@Override
		public void cleanup() {
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println(tuple);
			return true;
		}
	}

	//Filter for actors
	public static class ActorFilter implements Filter {
		String actor;
		int partitionIndex;

		public ActorFilter(String actor) {
			this.actor = actor;
		}

		@Override
		public boolean isKeep(TridentTuple tridentTuple) {
			boolean filter = tridentTuple.getString(0).equals(actor);
			if(filter) {
				System.err.println("I am partition " + partitionIndex + " actor "+ actor);
			}
			return filter;
		}

		@Override
		public void prepare(Map map, TridentOperationContext tridentOperationContext) {
			partitionIndex = tridentOperationContext.getPartitionIndex();
		}

		@Override
		public void cleanup() {

		}
	}


	//Filter for location
	public static class LocationCount implements Filter{

		@Override
		public boolean isKeep(TridentTuple tridentTuple) {
			System.err.println(tridentTuple.getString(0));
			return true;
		}

		@Override
		public void prepare(Map map, TridentOperationContext tridentOperationContext) {

		}

		@Override
		public void cleanup() {

		}
	}

	//Function for uppercase
	public static class UpperCaseFuntion extends BaseFunction{

		@Override
		public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
			tridentCollector.emit(new Values(tridentTuple.getString(0).toUpperCase()));
		}
	}


	//Aggregator
	public static class LocationAggregator extends BaseAggregator<Map <String, Integer>>{

		@Override
		public Map<String, Integer> init(Object o, TridentCollector tridentCollector) {
			return new HashMap<String, Integer>();
		}

		@Override
		public void aggregate(Map<String, Integer> stringIntegerMap, TridentTuple tridentTuple, TridentCollector tridentCollector) {
			String location = tridentTuple.getString(0);
			stringIntegerMap.put(location, MapUtils.getInteger(stringIntegerMap, location, 0) + 1);
		}

		@Override
		public void complete(Map<String, Integer> stringIntegerMap, TridentCollector tridentCollector) {
			tridentCollector.emit(new Values(stringIntegerMap));
		}
	}



	/**
	 * Given a hashmap with string keys and integer counts, returns the "top" map of it. "n" specifies the size of
	 * the top to return.
	 */
	public final static Map<String, Integer> getTopNOfMap(Map<String, Integer> map, int n) {
		List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(map.size());
		entryList.addAll(map.entrySet());
		Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
				return arg1.getValue().compareTo(arg0.getValue());
			}
		});
		Map<String, Integer> toReturn = new HashMap<String, Integer>();
		for(Map.Entry<String, Integer> entry: entryList.subList(0, Math.min(entryList.size(), n))) {
			toReturn.put(entry.getKey(), entry.getValue());
		}
		return toReturn;
	}
}
