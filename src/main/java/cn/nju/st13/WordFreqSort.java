package cn.nju.st13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
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

/*
正常情况下先使用文档倒排程序对文件进行处理，再对词频进行排序
如果命令行中在文件路径前加入-direct参数则会直接排序
*/
public class WordFreqSort {    
    //Job2 Mapper
    public static class WordFreqSortMapper extends Mapper<Object, Text, Text, FloatWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            String[] lines = input.split("\n");

            for (String str : lines) {
                String[] lineCompent = str.split("\t");
                String[] content = lineCompent[1].split(",");

                context.write(new Text(lineCompent[0]), new FloatWritable(new Float(content[0])));
            }
        }
    }

    //Job2 Mapper
    public static class WordFreqSortReducer extends Reducer<Text, FloatWritable, Text, Text> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            for (FloatWritable val : values) {
                context.write(key, new Text(":" + val.toString() + "\n"));
            }
        }
    }

	public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        
        if (args[0] != "-direct") {
            Job job1 = Job.getInstance(configuration, "2019St13 Word Frequency Sort Job1");
            job1.setJarByClass(WordFreqSort.class);
            job1.setMapperClass(InvertedIndexer.InvertedIndexMapper.class);
            job1.setReducerClass(InvertedIndexer.InvertedIndexReducer.class);
            job1.setCombinerClass(InvertedIndexer.InvertedIndexCombiner.class);
            job1.setPartitionerClass(InvertedIndexer.InvertedIndexPartitioner.class);
            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            job1.setNumReduceTasks(5);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + ".tempout"));

            if (job1.waitForCompletion(true)){
                Job job2 = Job.getInstance(configuration, "2019St13 Word Frequency Sort Job2");
                job2.setJarByClass(WordFreqSort.class);
                job2.setMapperClass(WordFreqSortMapper.class);
                job2.setReducerClass(WordFreqSortReducer.class);
                job2.setPartitionerClass(TotalOrderPartitioner.class);
                job2.setInputFormatClass(TextInputFormat.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                job2.setNumReduceTasks(5);
                FileInputFormat.addInputPath(job2, new Path(args[1] + ".tempout"));
                FileOutputFormat.setOutputPath(job2, new Path(args[1]));

                FileSystem fs =new Path(args[1] + ".tempout").getFileSystem(configuration);
                if(fs.exists(new Path(args[1] + ".tempout"))){
                      fs.delete(new Path(args[1] + ".tempout"), true); 
                }
            }
            else
                System.exit(1);
        }
        else {
            Job job2 = Job.getInstance(configuration, "2019St13 Word Frequency Sort Direct Job");
            job2.setJarByClass(WordFreqSort.class);
            job2.setMapperClass(WordFreqSortMapper.class);
            job2.setReducerClass(WordFreqSortReducer.class);
            job2.setPartitionerClass(TotalOrderPartitioner.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            job2.setNumReduceTasks(5);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        }
	}
}
