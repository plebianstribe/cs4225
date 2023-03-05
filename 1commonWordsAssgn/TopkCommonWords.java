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
    public static class TokenizerMapper1
            extends Mapper<Object, Text, Text, Text>{
        private Text count = new Text();
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

            //Makes an array of individual words split by separators give
            //Runs through array and writes output for each entry IF it does not appear in stopwords AND longer than 4 characters
            String[] values = value.toString().split(separator);
            String[] stopArray = stopwords.split("\\s+");
            List<String> stopList = new ArrayList<>(Arrays.asList(stopArray));
            for (String str : values) {
                if (str.length() > 4) {
                    if (!stopList.contains(str)) {
                        count.set("File1");
                        word.set(str);
                        context.write(word, count);
                    }
                }
            }
        }
    }

    public static class TokenizerMapper2
            extends Mapper<Object, Text, Text, Text>{
        private Text count = new Text();
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

            //Makes an array of individual words split by separators give
            //Runs through array and writes output for each entry IF it does not appear in stopwords AND longer than 4 characters
            String[] values = value.toString().split(separator);
            String[] stopArray = stopwords.split("\\s+");
            List<String> stopList = new ArrayList<>(Arrays.asList(stopArray));
            for (String str : values) {
                if (str.length() > 4) {
                    if (!stopList.contains(str)) {
                        word.set(str);
                        count.set("File2");
                        context.write(word, count);
                    }
                }
            }
        }
    }

    public static class IntCountAll
            extends Reducer<Text,Text,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Text values,
                           Context context
        ) throws IOException, InterruptedException {
            int sumA = 0;
            int sumB = 0;

            String[] vals = values.toString().split();

            for(String eachVal: vals) {
                if (eachVal == "File1") {
                    sumA += 1;
                } else {
                    sumB += 1;
                }
            }
            if(sumA > sumB){
                result.set(sumB);
            }
            else{
                result.set(sumA);
            }
            context.write(key, result);
        }
    }

    public static class SortMap
            extends Mapper<Object, Text, Text, IntWritable>{
        private IntWritable count = new IntWritable();
        private Text word = new Text();
        private TreeMap<Integer, ArrayList<String>> tmap
                = new TreeMap<>(Collections.reverseOrder());
        private Integer kMap = 1;

        public void setup(Configuration conf) {
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
                ArrayList<String> as;
                int valGet = count.get();
                System.out.println(valGet);
                System.out.println(tmap.isEmpty());
                System.out.println(tmap.containsKey(valGet));
                System.out.println(tmap.get(valGet));
                System.out.println(kMap);
                if (tmap.isEmpty()){
                    as = new ArrayList<String>();
                }else if (tmap.containsKey(valGet)){
                    as = tmap.get(valGet);
                }
                else {
                    as = new ArrayList<String>();
                }
                as.add(smol[0]);
                tmap.put(valGet, as);
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
                String res = String.join(",", asSort);
                for(String omg: asSort){
                    if(countdown>0) {
                        word.set(res);
                        context.write(word, count);
                        countdown -= 1;
                    }
                }
            }
        }
    }

    public static class SortReduce
            extends Reducer<Text,IntWritable,IntWritable,Text> {
        private IntWritable result = new IntWritable();
        private Text word = new Text();
        private TreeMap<Integer, ArrayList<String>> tmap;

        public void reduce(Text key, IntWritable values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] smol = key.toString().split(",");
            for (String str : smol) {
                word.set(str);
                context.write(values, word);
            }
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

        //\s\t\n\r\f
        Job job = Job.getInstance(conf, "Top k Common Words");
        job.setJarByClass(TopkCommonWords.class);
        job.setCombinerClass(IntCountAll.class);
        job.setReducerClass(IntCountAll.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
        conf2.setInt("k", Integer.parseInt(args[4]));
        Job job2 = Job.getInstance(conf2, "Sorting");
        job2.setJarByClass(TopkCommonWords.class);
        job2.setMapperClass(SortMap.class);
        job2.setReducerClass(SortReduce.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, interDirPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        boolean hasCompleted = job2.waitForCompletion(true);
        fs.delete(interDirPath, true); // ONLY call this after your last job has completed to delete your intermediate directory
        System.exit(hasCompleted ? 0 : 1); // there should be NO MORE code below this line
*/
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
