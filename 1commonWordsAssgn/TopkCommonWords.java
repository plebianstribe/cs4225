/*
ENTER YOUR NAME HERE
NAME: Nicholas Tan Kian Boon
MATRICULATION NUMBER: A0223939W
*/
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
            /*
            //splits value which is input to individual tokens
            StringTokenizer itr = new StringTokenizer(value.toString());

            //iterates through each token to add the word and its count to context (which is a dict?)
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
            */

            //Makes an array of individual words split by separators give
            //Runs through array and writes output for each entry IF it does not appear in stopwords AND longer than 4 characters
            String[] values = value.toString().split(separator);
            String[] stopArray = stopwords.split("\s+");
            List<String> stopList = new ArrayList<>(Arrays.asList(stopArray));
            for (String str : itr) {
                if (str.length() > 4) {
                    if (!stopList.contains(str)) {
                        word.set(str);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class IntCountAll
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = -1;

            //input values should be Iterable Int thing, idk what is iterable but it should be "Hello":{5, 2}

            //For each item in the input, check if have {a, b}.
            //Case 1: have {a, b}. Then have to sum to {sum(a), sum(b), and write 1 value which is max(sum(a), sum(b))
            //Case 2: have only one value. then just sum that value and return
            for (IntWritable val : values) {
                int valI = val.get();
                if(sum == -1){
                    sum = valI;
                }
                else if(valI<sum){
                    sum = valI;
                }

            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortMap
            extends Mapper<Object, Text, IntWritable, Text>{
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
            /*
            //splits value which is input to individual tokens
            StringTokenizer itr = new StringTokenizer(value.toString());

            //iterates through each token to add the word and its count to context (which is a dict?)
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
            */

            //Makes an array of individual words split by separators give
            //Runs through array and writes output for each entry IF it does not appear in stopwords AND longer than 4 characters
            String[] values = value.toString().split(separator);
            String[] stopArray = stopwords.split("\s+");
            List<String> stopList = new ArrayList<>(Arrays.asList(stopArray));
            for (String str : itr) {
                if (str.length() > 4) {
                    if (!stopList.contains(str)) {
                        word.set(str);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class SortReduce
            extends Reducer<IntWritable,Text,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Iterable<IntWritable> key, Text values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = -1;

            //input values should be Iterable Int thing, idk what is iterable but it should be "Hello":{5, 2}

            //For each item in the input, check if have {a, b}.
            //Case 1: have {a, b}. Then have to sum to {sum(a), sum(b), and write 1 value which is max(sum(a), sum(b))
            //Case 2: have only one value. then just sum that value and return
            for (IntWritable val : values) {
                int valI = val.get();
                if(sum == -1){
                    sum = valI;
                }
                else if(valI<sum){
                    sum = valI;
                }

            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /*
        FileSystem fs = FileSystem.get(conf);
        Path interDirPath = new Path("/home/course/cs4225/cs4225_assign/temp/assign1_inter/A0223939W"); // REPLACE THIS WITH YOUR OWN ID!
        */
        Path stopPath = new Path(args[2]);
        InputStream is = new FileInputStream(args[2]);
        byte[] array = new byte[100];
        is.read(array);
        String data = new String(array);

        conf.set("Separator.stopwords", data);
        conf.set("Separator.common", "\s+");

        //\s\t\n\r\f
        Job job = Job.getInstance(conf, "Top k Common Words");
        job.setJarByClass(TopkCommonWords.class);
        job.setCombinerClass(IntCountAll.class);
        job.setReducerClass(IntCountAll.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, TokenizerMapper.class);
        //MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
        //FileOutputFormat.setOutputPath(job, interDirPath);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

    /*
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.setInt("k", new Int(args[4]));
        Job job2 = Job.getInstance(conf2, "Sorting");
        job2.setJarByClass(TopkCommonWords.class);
        job2.setMapperClass(SortMap.class)
        job2.setReducerClass(SortReduce.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, interDirPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        boolean hasCompleted = job2.waitForCompletion(true);
        fs.delete(interDirPath, true); // ONLY call this after your last job has completed to delete your intermediate directory
        System.exit(hasCompleted ? 0 : 1); // there should be NO MORE code below this line
     */
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
