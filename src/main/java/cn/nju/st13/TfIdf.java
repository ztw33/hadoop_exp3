package cn.nju.st13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TfIdf {

    public static class TfIdfMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName(); //得到文件名

            Matcher m = p_author.matcher(filename);
            String author = "";
            if (m.find())
                author = m.group();
            else
                System.err.println("提取作者名字时出错");


            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // 过滤掉前两行
                if(word.toString().equals("TXT") || word.toString().equals("www.txthj.com")) {
                    return;
                } else {
                    word.set(author + "," + itr.nextToken() + "#" + filename);
                    context.write(word, one);
                }
            }

        }
    }

    public static class TfIdfCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce (Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TfIdfPartitioner
            extends HashPartitioner<Text, IntWritable> {

        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // 按照<作者名，单词>进行划分
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term),value,numReduceTasks);

        }
    }

    public static class TfIdfReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
        private static String currentTerm = " ";
        private static int currentSum = 0;
        private static int currentCount = 0;


        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String authorAndWord = key.toString().split("#")[0];
            if(authorAndWord.equals(currentTerm)) {
                currentCount++;
                currentSum+=sum;
            } else {
                // write上一次结果
                if(!currentTerm.equals(" ")) {
                    Configuration conf = context.getConfiguration();
                    String author = currentTerm.split(",")[0];
                    int countFile = Integer.parseInt(conf.get(author));
                    double tfidf = currentSum*Math.log((double)countFile/(double)(currentCount+1));
                    result.set(tfidf);
                    context.write(new Text(currentTerm), result);
                }
                currentTerm = authorAndWord;
                currentCount = 1;
                currentSum = sum;
            }

        }

        //最后一个单词
        public void cleanup(Context context)
                throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String author = currentTerm.split(",")[0];
            int countFile = Integer.parseInt(conf.get(author));
            double tfidf = currentSum*Math.log((double)countFile/(double)(currentCount+1));
            result.set(tfidf);
            context.write(new Text(currentTerm), result);
        }

    }


    private static Pattern p_author = Pattern.compile("^[\\u4e00-\\u9fa5]*"); //从文件名中提取作者

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2){
            System.err.println("必须输入读取文件路径和输出路径");
            System.exit(2);
        }

        // 获取作家与作品总数的对应关系
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        HashMap<String, Integer> authorFileCount = new HashMap<>();
        for (FileStatus file : status) {
            String filename = file.getPath().getName();
            Matcher m = p_author.matcher(filename);
            String author = "";
            if(m.find())
                author = m.group();
            if (authorFileCount.containsKey(author)) {
                int num = authorFileCount.get(author) + 1;
                authorFileCount.put(author, num);
            } else {
                authorFileCount.put(author, 1);
            }
        }
        // 通过设置Job的Configuration来共享变量
        for (Map.Entry<String, Integer> entry : authorFileCount.entrySet()) {
            conf.set(entry.getKey(), entry.getValue().toString());
        }

        Job job = Job.getInstance(conf, "tfidf job");
        job.setJarByClass(TfIdf.class);
        job.setMapperClass(TfIdfMapper.class);
        job.setCombinerClass(TfIdfCombiner.class);
        job.setPartitionerClass(TfIdfPartitioner.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}