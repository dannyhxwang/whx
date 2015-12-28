package com.dsk.storm.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import org.apache.commons.io.FileUtils;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class LocalTridentWordCount {
	
	public static class DataSpout implements IBatchSpout{
		
		HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
		@Override
		public void open(Map conf, TopologyContext context) {
			
		}
		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			List<List<Object>> batch = this.batches.get(batchId);
	        if(batch == null){
	            batch = new ArrayList<List<Object>>();
	            Collection<File> listFiles = FileUtils.listFiles(new File("10.1.3.55:/home/hadoop/data"), new String[]{"txt"}, true);
	            for (File file : listFiles) {
					try {
						List<String> lines = FileUtils.readLines(file);
						for (String line : lines) {
							batch.add(new Values(line));
						}
						FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
	            this.batches.put(batchId, batch);
	        }
	        for(List<Object> list : batch){
	            collector.emit(list);
	        }
			
			
		}

		@Override
		public void ack(long batchId) {
			 this.batches.remove(batchId);
		}

		@Override
		public void close() {
			
		}

		@Override
		public Map getComponentConfiguration() {
			Config conf = new Config();
	        conf.setMaxTaskParallelism(1);
	        return conf;
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("line");
		}
		
	}
	public static class SplitBolt extends BaseFunction{

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String line = tuple.getString(0);
			String[] words = line.split("\t");
			for (String word : words) {
				collector.emit(new Values(word));
			}
		}
	}
	
	public static class WordCount extends BaseFunction{

		private int partitionIndex;
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

		public void prepare(Map conf, TridentOperationContext context) {
			this.partitionIndex = context.getPartitionIndex();
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String word = tuple.getString(0);
			Integer count = hashMap.get(word);
			if(count==null){
				count=0;
			}
			count++;
			hashMap.put(word, count);
			Utils.sleep(1000);
			System.err.println("I am partition [" + partitionIndex + "]");
			System.out.println("==================" + Thread.currentThread().getName() + "=====================");
			for (Entry<String, Integer> entry : hashMap.entrySet()) {
				System.out.println(entry);
			}
		}
	}

	public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("spout_id", new DataSpout())
		.each(new Fields("line"), new SplitBolt(), new Fields("word"))
		.each(new Fields("word"), new WordCount(), new Fields("")).parallelismHint(2);

		StormSubmitter.submitTopology("sumTopology", new Config(), tridentTopology.build());
//		LocalCluster localCluster = new LocalCluster();
//		localCluster.submitTopology("sumTopology", new Config(), tridentTopology.build());
	}
	
}
