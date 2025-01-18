package app;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// First Reduce: compress in a single line all the ratings of a movie.
// Input:  movieId t:movieTitle
//         movieId r:userId|userRate
//         movieId r:userId|userRate
//         ...
// Output: 1 movieTitle1=userId|userRate,userId|userRate,...
//         1 movieTitle2=userId|userRate,userId|userRate,...
class ReducerOne extends Reducer<Text, Text, Text, Text> {

    private final Text ret = new Text();
    private final static Text one = new Text("1");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String title = "";
        StringBuilder sb = new StringBuilder();
        for (Text t : values) {
            String[] val = t.toString().split(":", 2);
            String letter = val[0];
            String content = val[1];
            if (letter.equals("t") && title.equals("")) {
                title = content;
            }
            if (letter.equals("r")) {
                sb.append(content).append(",");
            }
        }
        ret.set(title + "=" + sb.toString());
        context.write(one, ret);
    }
}

// Second Map: just pass the data
// Input:  1 movieTitle1=userId|userRate,userId|userRate,...
//         1 movieTitle2=userId|userRate,userId|userRate,...
// Output: 1 movieTitle1=userId|userRate,userId|userRate,...
//         1 movieTitle2=userId|userRate,userId|userRate,...
class SecondMapper extends Mapper<Object, Text, Text, Text> {

    private final Text id = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        id.set(key.toString());
        context.write(id, value);
    }
}

// Second Reduce: chose one random best rated movie per user
// Input:  1 movieTitle1=userId|userRate,userId|userRate,...
//         1 movieTitle2=userId|userRate,userId|userRate,...
// Output: N1 movieTitle1
//         N2 movieTitle2
//         ...
//         N movieTitleN
class AllReducer extends Reducer<Text, Text, IntWritable, Text> {

    HashMap<String, Entry<String, Float>> userFav = new HashMap();

    HashMap<String, Integer> freq = new HashMap();
    HashMap<Integer, String> inv = new HashMap();
    private final IntWritable num = new IntWritable();
    private final Text content = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // for each movie
        for (Text line : values) {
            String[] titleVotes = line.toString().split("=", 2);
            String title = titleVotes[0];
            String votes = titleVotes[1];
            String[] vals = votes.split(",");
            // for each vote
            for (String v : vals) {
                String[] val = v.split("\\|", 2);
                if (val.length < 2) {
                    continue;
                }
                String userId = val[0];
                try {
                    Float rate = Float.valueOf(val[1]);

                    if (!userFav.keySet().contains(userId)) {
                        // no entry found
                        // create a new entry
                        userFav.put(userId, new SimpleEntry<>(title, rate));
                    } else {
                        Entry<String, Float> e = userFav.get(userId);
                        Float actual = e.getValue();
                        if (actual < rate) {
                            // if rate is better than the acqual vote, then replace it
                            userFav.put(userId, new SimpleEntry<>(title, rate));
                        }
                    }
                } catch (NumberFormatException e) {
                    System.out.println(e.toString() + ": Couldn't parse " + val[1]);
                }
            }
        }

        // create the frequency map
        for (Entry<String, Entry<String, Float>> entry : userFav.entrySet()) {
            Entry<String, Float> e = entry.getValue();
            String title = e.getKey();
            if (!freq.keySet().contains(title)) {
                freq.put(title, 1);
            } else {
                int n = freq.get(title);
                freq.put(title, n + 1);
            }
        }

        // invert key and value and format the titles 
        for (HashMap.Entry<String, Integer> entry : freq.entrySet()) {
            String k = entry.getKey();
            Integer v = entry.getValue();

            if (!inv.keySet().contains(v)) {
                inv.put(v, k);
            } else {
                String list = inv.get(v);
                inv.put(v, list + " " + k);
            }
        }

        // place them in order
        Set<Integer> keySet = inv.keySet();
        Integer[] keys = keySet.toArray(new Integer[0]);
        Arrays.sort(keys);

        // write the output
        for (int i : keys) {
            String v = inv.get(i);
            num.set(i);
            content.set(v);
            context.write(num, content);
        }
    }
}

public class ChainTer extends Configured implements Tool {

    static Configuration cf;

    @Override
    public int run(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        cf = new Configuration();
        // Configuration of Job 1
        Job job1 = Job.getInstance(cf, "Job 1");
        job1.setJarByClass(ChainSec.class);
        job1.setMapperClass(app.MovieMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(ReducerOne.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediateOutput1 = new Path("intermediate_output1");
        FileOutputFormat.setOutputPath(job1, intermediateOutput1);
        intermediateOutput1.getFileSystem(cf).delete(intermediateOutput1, true);

        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        // Configure Job 2
        Job job2 = Job.getInstance(cf, "Job 2");
        job2.setJarByClass(ChainSec.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(SecondMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(AllReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, intermediateOutput1);
        Path p = new Path(args[1]);
        FileOutputFormat.setOutputPath(job2, p);
        p.getFileSystem(cf).delete(p, true);
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(cf, new ChainTer(), args);
        System.exit(res);
    }
}
