/*
ENTER YOUR NAME HERE
NAME: Nicholas Tan Kian Boon
MATRICULATION NUMBER: A0223939W
*/

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TopkCommonWords {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String separator = new String();
        private String stopwords = new String();

        public void setup(Configuration conf) {
            stopwords = conf.get("Separator.stopwords");
            separator = conf.get("Separator.common");
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntCountAll
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path interDirPath = new Path("/home/course/cs4225/cs4225_assign/temp/assign1_inter/A0223939W"); // REPLACE THIS WITH YOUR OWN ID!

        java.nio.file.Path stopPath = java.nio.file.Path.of(args[2]);
        String data = new String();
        try (Stream<String> lines = Files.lines(stopPath))
        {
            lines.forEach(s -> data.concat(s+" "));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        conf.set("Separator.stopwords", data);
        conf.set("Separator.common", "\\s+");

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntCountAll.class);
        job.setReducerClass(IntCountAll.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}