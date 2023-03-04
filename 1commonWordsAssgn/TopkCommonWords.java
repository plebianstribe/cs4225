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
import org.apache.hadoop.fs.FileSystem;
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
            String[] stopArray = stopwords.split("\\s+");
            List<String> stopList = new ArrayList<>(Arrays.asList(stopArray));
            for (String str : values) {
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
            System.out.println(key + result.toString());
            context.write(key, result);
        }
    }

    public static class SortMap
            extends Mapper<Object, Text, Text, IntWritable>{
        private IntWritable count = new IntWritable();
        private Text word = new Text();
        private TreeMap<Integer, ArrayList<String>> tmap;
        private Integer kMap = 0;

        public void setup(Configuration conf) {
            tmap = new TreeMap<Integer, ArrayList<String>>(Collections.reverseOrder());
            kMap = Integer.parseInt(conf.get("k"));
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\R");
            //String valueOut = values[0];
            //Integer keyOut = Integer.parseInt(values[1]);

            for (String str : values) {
                String[] smol = str.split("\\t");
                count.set(Integer.parseInt(smol[1]));
                //word.set(smol[0]);
                //context.write(count, word);
                ArrayList<String> as = new ArrayList<String>();
                if (tmap.containsKey(count.get())){
                    as = tmap.get(count.get());
                }
                else {
                    as = new ArrayList<String>();
                }
                as.add(value.toString());
                tmap.put(count.get(), as);
                if (tmap.size() > kMap) {
                    tmap.remove(tmap.lastKey());
                }
            }
        }
        public void submit(Context context)
                throws IOException, InterruptedException
        {
            Integer countdown = kMap;
            for (Map.Entry<Integer, ArrayList<String>> entry :
                    tmap.entrySet()) {
                count.set(entry.getKey());
                ArrayList<String> asSort = entry.getValue();
                Collections.sort(asSort);
                for(String str: asSort){
                    if(countdown>0) {
                        word.set(str);
                        context.write(word, count);
                        countdown -= 1;
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem  hdfs = FileSystem.get(URI.create("hdfs://<namenode-hostname>:<port>"), conf);

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
        FileOutputFormat.setOutputPath(job, interDirPath);
        //FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        conf2.setInt("k", Integer.parseInt(args[4]));
        Job job2 = Job.getInstance(conf2, "Sorting");
        job2.setJarByClass(TopkCommonWords.class);
        job2.setMapperClass(SortMap.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, interDirPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        boolean hasCompleted = job2.waitForCompletion(true);
        fs.delete(interDirPath, true); // ONLY call this after your last job has completed to delete your intermediate directory
        System.exit(hasCompleted ? 0 : 1); // there should be NO MORE code below this line

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
