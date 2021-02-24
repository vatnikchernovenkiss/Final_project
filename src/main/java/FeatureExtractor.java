import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

public class FeatureExtractor extends Configured implements Tool {
    private static final String TRAIN = "";
    private static final String TEST = "";
    private static final String TYPE = "";
    private static final String URLS = "";
    private static final String QUERIES = "";
    private static final String GLOBAL = "";
    private static final String QUERY= "";

    public static class FeatureExtractorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final HashSet<String> trainSet = new HashSet<>();
        private final HashSet<String> testSet = new HashSet<>();
        private final HashMap<String, Integer> urlsMap = new HashMap<>();
        private final HashMap<String, Integer> queryMap = new HashMap<>();
   	private boolean if_host = false;

        private void file_to_map(BufferedReader reader, HashMap<String, Integer> map) throws IOException {
            String line = reader.readLine();
            while (line != null) {
                String[] splited = line.trim().split("\t");
                Integer id = Integer.parseInt(splited[0]);
                String url = splited[1];
                url = url.replace("http://","").replace("https://","").replace("www.", "");
                if (url.endsWith("/")) {
                    url = url.substring(0, url.length() - 1);
                }
                map.put(url, id);
                line = reader.readLine();
            }
        }

        private void readSet(BufferedReader reader, HashSet<String> set) throws IOException {
            String line = reader.readLine();
            while (line != null) {
                String[] splited = line.trim().split("\t");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }
        @Override
        protected void setup(Context context) throws IOException {
            if_host = context.getConfiguration().get(TYPE).equals("host");
            String urlsFileName = context.getConfiguration().get(URLS);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader urlsFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(urlsFileName)), StandardCharsets.UTF_8));
            file_to_map(urlsFileReader, urlsMap);
            urlsFileReader.close();
            String queriesFileName = context.getConfiguration().get(QUERIES);
            BufferedReader queriesFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(queriesFileName)), StandardCharsets.UTF_8));
            file_to_map(queriesFileReader, queryMap);
            queriesFileReader.close();
            String trainSetFileName = context.getConfiguration().get(TRAIN);
            BufferedReader trainSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(trainSetFileName)), StandardCharsets.UTF_8));
            readSet(trainSetFileReader, trainSet);
            trainSetFileReader.close();
            String testSetFileName = context.getConfiguration().get(TEST);
            BufferedReader testSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(testSetFileName)), StandardCharsets.UTF_8));
            readSet(testSetFileReader, testSet);
            testSetFileReader.close();
        }

        private StringBuilder make_value(IRSession session, int pos, String prefix, String url) {
            StringBuilder res = new StringBuilder();
            res.append(prefix).append(" ").append(pos).append(" ");
            int  fl = 0;
            for (int i = 0; i < session.clicked_pos.length; i++) {
                if (session.clicked_pos[i] == pos) {
                    res.append(1);
                    fl = 1;
                    break;
                }
            }
            if (fl == 0) {
                res.append(0);
            } 
            res.append(" ");
            res.append(session.clicked_pos[session.clicked_pos.length - 1]);
            res.append(" ");
            if (session.time_spent.containsKey(url)) {
            	res.append(session.time_spent.get(url));
            } else {
            	res.append(0);
           }
            return res;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sessionStr = value.toString();
            String query = new StringTokenizer(new StringTokenizer(sessionStr, "\t").nextToken(), "@").nextToken().trim();
            IRSession session = new IRSession(sessionStr);
            if (queryMap.containsKey(query)) {
                int queryId = queryMap.get(query);
                for (int i = 0; i < session.showed_urls.length; i++){
                    String url = session.showed_urls[i];
                     if (if_host) {
                        try {
                            url = new URL("www." + url).getHost();
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        String pair = queryId + " " + urlId;
                        StringBuilder res;
                        if (!trainSet.contains(pair) && !testSet.contains(pair)) {
                            res = make_value(session, i, "global", url);
                        } else {
                            res = make_value(session, i, Integer.toString(queryId), url);
                        }
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            } else {
                for (int i = 0; i < session.showed_urls.length; i++) {
                    String url = session.showed_urls[i];
                     if (if_host) {
                        try {
                            url = new URL("www." + url).getHost();
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        StringBuilder res = make_value(session, i, "global", url);
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            }
        }
    }

    public static class FeatureExtractorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private MultipleOutputs<IntWritable, Text> out;
        private final HashSet<String> trainSet = new HashSet<>();
        private final HashSet<String> testSet = new HashSet<>();
        @Override
        protected void setup(Context context) throws IOException {
            out = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StatCalc globalStats = new StatCalc();
            HashMap<Integer, StatCalc> queryStats = new HashMap<>();
            for (Text v: values) {
                String s = v.toString();
                boolean glob = s.startsWith("global");
                String[] splited = s.split(" ");
                int queryId = -1;
                if (!glob) {
                    queryId = Integer.parseInt(splited[0]);
                }
                int pos = Integer.parseInt(splited[1]);
                int click = Integer.parseInt(splited[2]);
                int lastClickPos = Integer.parseInt(splited[3]);
                int time_spent =  Integer.parseInt(splited[4]);
                globalStats.inc_stat(click, pos, lastClickPos, time_spent);
                if (!glob) {
                    StatCalc stat = queryStats.get(queryId);
                    if (stat == null) {
                        stat = new StatCalc();
                        stat.inc_stat(click, pos, lastClickPos,time_spent);
                        queryStats.put(queryId, stat);
                    } else {
                        stat.inc_stat(click, pos, lastClickPos,time_spent);
                    }
                }
            }
            StringBuilder res = globalStats.get_stat(-1);
            out.write(GLOBAL, key, new Text(res.toString()));
            for (Integer queryId: queryStats.keySet()) {
                StatCalc stat = queryStats.get(queryId);
                StringBuilder res_new = stat.get_stat(queryId);
                out.write(QUERY, key, new Text(res_new.toString()));
            }
        }
    }
    private Job getFeatureExtractorConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(FeatureExtractor.class);
        job.setJobName(FeatureExtractor.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, GLOBAL, TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, QUERY, TextOutputFormat.class, IntWritable.class, Text.class);
        job.setMapperClass(FeatureExtractorMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(FeatureExtractorReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(5);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getFeatureExtractorConf(args[0], args[1]);
        job.getConfiguration().set(URLS, args[2]);
        job.getConfiguration().set(QUERIES, args[3]);
        job.getConfiguration().set(TRAIN, args[4]);
        job.getConfiguration().set(TEST, args[5]);
        job.getConfiguration().set(TYPE, args[6]);
        job.getConfiguration().set(GLOBAL, args[7]);
        job.getConfiguration().set(QUERY, args[8]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FeatureExtractor(), args);
        System.exit(exitCode);
    }
}
