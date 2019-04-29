package cn.nju.st13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	public static class InvertedIndexReducer extends Reducer<Text,IntWritable,Text,Text> {
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
					double avgFrequency = frequency / currentValues.size();
					StringBuilder out = new StringBuilder();
					for(String string : currentValues.keySet()) {
						out.append(string+":"+currentValues.get(string)+";");
					}
					out.deleteCharAt(out.lastIndexOf(";"));
					context.write(new Text(currentWord), new Text(out.toString()));

					//TODO: 将avgFrequency 和 word存入hbase
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
			StringBuilder out = new StringBuilder();
			for(String string : currentValues.keySet()) {
				out.append(string+":"+currentValues.get(string)+";");
			}
			out.deleteCharAt(out.lastIndexOf(";"));

			//TODO: 将avgFrequency 和 word存入hbase

			context.write(new Text(currentWord), new Text(out.toString()));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration,"2019St13 Inverted Index Job");
		job.setJarByClass(InvertedIndexer.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setPartitionerClass(InvertedIndexPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(5);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
