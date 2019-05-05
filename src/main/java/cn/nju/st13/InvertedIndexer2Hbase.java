package cn.nju.st13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.*;


public class InvertedIndexer2Hbase {
	public static class InvertedIndexMapper
			extends Mapper<Object,Text,Text,IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			//去除文件后缀，仅保留文件名
			String[] tokens = fileName.split("[.]");
			fileName = "";
			for(int i = 0;i < tokens.length-2;i++) {
				fileName += tokens[i];
			}
			Text word = new Text();
			IntWritable frequence = new IntWritable();
			//统计一行中单词的个数
			HashMap<String, Integer> hashMap = new HashMap<>();
			Text fileName_Offset = new Text(fileName + "#" + key.toString());
			StringTokenizer itr = new StringTokenizer(value.toString());
			//过滤网址和广告
			if(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if(word.toString().equals("TXT") || word.toString().equals("www.txthj.com")) {
					return;
				}
				else {
					hashMap.put(word.toString(),1);
				}
			}
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if(hashMap.get(word.toString()) != null) {
					hashMap.put(word.toString(), hashMap.get(word.toString()) + 1);
				}
				else {
					hashMap.put(word.toString(), 1);
				}
			}

			for(String str : hashMap.keySet()) {
				Text wordAndFile = new Text(str + ","+fileName);
				Integer temp = hashMap.get(str);
				IntWritable frequency = new IntWritable(temp);
				context.write(wordAndFile, frequency);
			}
		}
	}

	//Combiner进行局部处理,合并键相同的值
	public static class InvertedIndexCombiner
			extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable(0);
		//Combiner本质就是Reducer
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	//定义Partitioner,将单词从key中拆分出来
	//目的：让相同的单词对应的记录发往同一个reducer
	public static class InvertedIndexPartitioner
			extends Partitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String[] tokens = key.toString().split(",");
			//(word, doc), value => word,(doc,value)
			String dummyKey = tokens[0];
			return (dummyKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	//Reducer
	public static class Reducer2HbasePhase1 extends Reducer<Text,IntWritable,Text,Text> {
		// word, <filename, value>
		private static String currentWord = "";
		private static HashMap<String, Integer> currentValues = new HashMap<>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			String 	word = key.toString().split(",")[0];
			String  fileName = key.toString().split(",")[1];
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			if(word.equals(currentWord)) {
				currentValues.put(fileName, sum);
			}
			else {
				if(!currentWord.equals("")) {
					//把前一个单词对应的记录写入Contex
					double frequency = 0;
					for(String string : currentValues.keySet()) {
						frequency += currentValues.get(string);
					}
					StringBuilder out = new StringBuilder();
					for(String string : currentValues.keySet()) {
						out.append(string+":"+currentValues.get(string)+";");
					}
					out.deleteCharAt(out.lastIndexOf(";"));
					context.write(new Text(currentWord), new Text(out.toString()));

				}
				currentWord = word;
				currentValues = new HashMap<>();
				currentValues.put(fileName, sum);
			}
		}

		//最后一个单词
		public void cleanup(Context context) throws IOException,InterruptedException{
			double frequency = 0;
			for(String string : currentValues.keySet()) {
				frequency += currentValues.get(string);
			}
			StringBuilder out = new StringBuilder();
			for(String string : currentValues.keySet()) {
				out.append(string+":"+currentValues.get(string)+";");
			}
			out.deleteCharAt(out.lastIndexOf(";"));
			context.write(new Text(currentWord), new Text(out.toString()));
		}
	}

	public static class Reducer2HbasePhase2 extends TableReducer<Text, IntWritable, NullWritable> {
		// word, <filename, value>
		private static String currentWord = "";
		private static HashMap<String, Integer> currentValues = new HashMap<>();
		private static NullWritable outputKey = NullWritable.get();
		private Put outputValue;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			String 	word = key.toString().split(",")[0];
			String  fileName = key.toString().split(",")[1];
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			if(word.equals(currentWord)) {
				currentValues.put(fileName, sum);
			}
			else {
				if(!currentWord.equals("")) {
					//把前一个单词对应的记录写入Contex
					double frequency = 0;
					for(String string : currentValues.keySet()) {
						frequency += currentValues.get(string);
					}
					double avgFrequency = frequency / currentValues.size();
					outputValue = new Put(Bytes.toBytes(currentWord));
					outputValue.addColumn(Bytes.toBytes("content"),Bytes.toBytes("avgFrequency"), Bytes.toBytes(String.format("%.2f",avgFrequency)));
					context.write(outputKey, outputValue);
				}
				currentWord = word;
				currentValues = new HashMap<>();
				currentValues.put(fileName, sum);
			}
		}

		//最后一个单词
		public void cleanup(Context context) throws IOException,InterruptedException{
			double frequency = 0;
			for(String string : currentValues.keySet()) {
				frequency += currentValues.get(string);
			}
			double avgFrequency = frequency / currentValues.size();
			outputValue = new Put(Bytes.toBytes(currentWord));
			outputValue.addColumn(Bytes.toBytes("content"),Bytes.toBytes("avgFrequency"), Bytes.toBytes(String.format("%.2f",avgFrequency)));
			context.write(outputKey, outputValue);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf =new Configuration();
		Job job1 = Job.getInstance(conf,"2019St13 Inverted Index Job to Hbase 1");
		job1.setJarByClass(InvertedIndexer2Hbase.class);
		job1.setMapperClass(InvertedIndexMapper.class);
		job1.setReducerClass(Reducer2HbasePhase1.class);
		job1.setCombinerClass(InvertedIndexCombiner.class);
		job1.setPartitionerClass(InvertedIndexPartitioner.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		if(job1.waitForCompletion(true)) {
			Configuration conf2 = HBaseConfiguration.create();
			Job job2 = Job.getInstance(conf2, "2019St13 Inverted Index Job to Hbase 2");
			job2.setJarByClass(InvertedIndexer2Hbase.class);
			job2.setMapperClass(InvertedIndexMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			TableMapReduceUtil.initTableReducerJob("WuXia", Reducer2HbasePhase2.class, job2);
			FileInputFormat.addInputPath(job2, new Path(args[0]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
		else {
			System.exit(1);
		}
	}
}
