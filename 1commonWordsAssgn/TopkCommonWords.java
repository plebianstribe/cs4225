/*
ENTER YOUR NAME HERE
NAME: Nicholas Tan Kian Boon
MATRICULATION NUMBER: A0223939W
*/
import java.io.*;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.*;

public class TopkCommonWords {

    public static class MapperOne
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("OneNotWorking");
        //private List<String> stopList = new ArrayList<String>();
        private String stopwords = new String("");

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            stopwords = conf.get("stopwords");

            /*
            Path[] patternsFiles = new Path[0];
            try {
                patternsFiles = DistributedCache.getLocalCacheFiles(conf);
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
            }
            for (Path patternsFile : patternsFiles) {
                try {
                    BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
                    String pattern = null;
                    while ((pattern = fis.readLine()) != null) {
                        stopList.add(pattern);
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
                }
            }
            */
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Makes an array of individual words split by separators give
            //Runs through array and writes output for each entry IF it does not appear in stopwords AND longer than 4 characters
            String[] values = value.toString().split("\\s+");
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

    public static class MapperTwo
            extends Mapper<Object, Text, Text, IntWritable>{

        private IntWritable two = new IntWritable(2);
        private Text word = new Text("mapperNotWorking");
        //private List<String> stopList = new ArrayList<String>();
        private String stopwords = new String("");

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            stopwords = conf.get("stopwords");

            /*
            Path[] patternsFiles = new Path[0];
            try {
                patternsFiles = DistributedCache.getLocalCacheFiles(conf);
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
            }
            for (Path patternsFile : patternsFiles) {
                try {
                    BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
                    String pattern = null;
                    while ((pattern = fis.readLine()) != null) {
                        stopList.add(pattern);
                    }
                } catch (IOException ioe) {
                    System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
                }
            }
            */
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Makes an array of individual words split by separators give
            //Runs through array and writes output for each entry IF it does not appear in stopwords AND longer than 4 characters
            String[] values = value.toString().split("\\s+");
            String[] stopArray = stopwords.split("\\s+");
            List<String> stopList = new ArrayList<>(Arrays.asList(stopArray));
            //System.err.println(Arrays.toString(stopList.toArray()));
            for (String str : values) {
                if (str.length() > 4) {
                    if (!stopList.contains(str)) {
                        word.set(str);
                        context.write(word, two);
                    }else{
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
            int sumA = 0;
            int sumB = 0;
            for (IntWritable val : values) {
                int eachVal = val.get();
                if (eachVal == 1) {
                    sumA += 1;
                } else {
                    sumB += 1;
                }
            }

            if(sumA > sumB && sumB != 0){
                result.set(sumB);
                context.write(key, result);
            }
            else if(sumA != 0){
                result.set(sumA);
                context.write(key, result);
            }else if (sumB != 0){
                result.set(sumB);
                context.write(key, result);
            }
        }
    }

    public static class SortMap
            extends Mapper<Object, Text, IntWritable, Text>{
        private IntWritable count = new IntWritable();
        private Text word = new Text();
        private TreeMap<Integer, ArrayList<String>> tmap
                = new TreeMap<>(Collections.reverseOrder());
        private Integer kMap = 1;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            kMap = Integer.parseInt(conf.get("k"));
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\R");
            //String valueOut = values[0];
            //Integer keyOut = Integer.parseInt(values[1]);

            for (String str : values) {
                String[] smol = str.split("[^\\S\\r\\n]+");
                count.set(Integer.parseInt(smol[1]));
                //word.set(smol[0]);
                //context.write(count, word);
                ArrayList<String> as;
                int valGet = count.get();
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
        protected void cleanup(Context context)
                throws IOException, InterruptedException
        {
            Integer countdown = kMap;
            for (Map.Entry<Integer, ArrayList<String>> entry :
                    tmap.entrySet()) {
                count.set(entry.getKey());
                ArrayList<String> asSort = entry.getValue();
                Collections.sort(asSort);
                String res = String.join("\n", asSort);
                word.set(res);
                System.err.println(word+ " HELLO");
                System.err.println(entry.getKey());
                context.write(count, word);
            }
        }
    }

    public static class SortReduce
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private Text word = new Text();
        //private TreeMap<Integer, ArrayList<String>> tmap
                //= new TreeMap<>(Collections.reverseOrder());
        private Integer kMap = 1;
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            kMap = Integer.parseInt(conf.get("k"));
        }
        public void reduce(IntWritable key, Text values,
                           Context context
        ) throws IOException, InterruptedException {
            String[] smol = values.toString().split("\\n");
            ArrayList<String> stringList = new ArrayList<String>(Arrays.asList(smol));
            for(String omg: stringList){
                if(kMap>0) {
                    word.set(omg);
                    context.write(key, word);
                    kMap -= 1;
                }
            }
            //context.write(values, word);
            //tmap.put(keyInt, stringList);
        }
        /*
        protected void cleanup(Context context)
                throws IOException, InterruptedException
        {
            Integer countdown = kMap;
            for (Map.Entry<Integer, ArrayList<String>> entry :
                    tmap.entrySet()) {
                result.set(entry.getKey());
                ArrayList<String> asSort = entry.getValue();
                for(String omg: asSort){
                    if(countdown>0) {
                        System.err.println(omg+ " HELLO");
                        word.set("TEST");
                        context.write(result, word);
                        countdown -= 1;
                    }
                }
            }
        }*/
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        //Path interDirPath = new Path("/home/course/cs4225/cs4225_assign/temp/assign1_inter/A0223939W"); // REPLACE THIS WITH YOUR OWN ID!
        Path interDirPath = new Path(args[3]+"/../A0223939W");

        Path path = new Path(args[2]);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String data = new String();
        String line;
        line = br.readLine();
        while (line != null) {
            line = br.readLine();
            data += "\n" + line;
        }

        if(data != null){
            conf.set("stopwords", data);
        }else{
            conf.set("stopwords", "-1");
        }

        conf.set("Separator.common", "\\s+");

        //\s\t\n\r\f
        Job job = Job.getInstance(conf, "Top k Common Words");

        job.setJarByClass(TopkCommonWords.class);
        job.setReducerClass(IntCountAll.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //FileInputFormat.addInputPaths(job, args[0]+","+args[1]);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, MapperOne.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, MapperTwo.class);
        FileOutputFormat.setOutputPath(job, interDirPath);
        //FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.setInt("k", Integer.parseInt(args[4]));
        Job job2 = Job.getInstance(conf2, "Sorting");
        job2.setJarByClass(TopkCommonWords.class);
        job2.setMapperClass(SortMap.class);
        job2.setReducerClass(SortReduce.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, interDirPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        boolean hasCompleted = job2.waitForCompletion(true);
        fs.delete(interDirPath, true); // ONLY call this after your last job has completed to delete your intermediate directory
        System.exit(hasCompleted ? 0 : 1); // there should be NO MORE code below this line
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
