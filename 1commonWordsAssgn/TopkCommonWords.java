/*
ENTER YOUR NAME HERE
NAME: Nicholas Tan Kian Boon
MATRICULATION NUMBER: A0223939W
*/
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String separator = new String();
        private String stopwords = new String();

        public void setup(Configuration conf) {
            /*InputStream is = FileSystem.get(conf).open(new Path(conf.get("stopwords.path")));

            System.out.println(is);
            System.out.println(is.getClass());
            */

            stopwords = conf.get("Separator.stopwords");
            separator = conf.get("Separator.common");
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //splits value which is input to individual tokens
            StringTokenizer itr = new StringTokenizer(value.toString());

            //iterates through each token to add the word and its count to context (which is a dict?)
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }

            //test
            String[] values = value.toString().split(separator);
            System.out.println(values);
        }
    }

    public static class IntSumReducer
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

        Path stopPath = new Path(args[2]);
        InputStream is = new FileInputStream(args[2]);
        byte[] array = new byte[100];
        is.read(array);
        String data = new String(array);

        conf.set("Separator.stopwords", data);
        conf.set("Separator.common", "//s");

        //\s\t\n\r\f
        Job job = Job.getInstance(conf, "Top k Common Words");
        job.setJarByClass(TopkCommonWords.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, TokenizerMapper.class);
        //MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
