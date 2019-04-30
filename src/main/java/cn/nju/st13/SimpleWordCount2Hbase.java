package cn.nju.st13;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

/*这是一个示例程序*/
public class SimpleWordCount2Hbase {
	public static class MyMapper2 extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}

	}

	public static class MyReducer2 extends TableReducer<Text, IntWritable, NullWritable> {
		private IntWritable result = new IntWritable();
		private static NullWritable outputKey = NullWritable.get();
		private Put outputValue;
		private int sum;
		/*
		 * Text-IntWritable 来自map的输入key-value键值对的类型
		 * Text-IntWritable 输出key-value 单词-词频键值对
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			outputValue = new Put(Bytes.toBytes(key.toString()));
			outputValue.addColumn(Bytes.toBytes("word"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
			context.write(outputKey, outputValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "my word count to hbase");
		job.setJarByClass(SimpleWordCount2Hbase.class);
		job.setMapperClass(MyMapper2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		TableMapReduceUtil.initTableReducerJob("WordCount", MyReducer2.class, job);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
