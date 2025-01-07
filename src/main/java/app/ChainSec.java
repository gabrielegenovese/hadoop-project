package app;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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


// Third Map: format each line to keep only the title
// Input:  userId1 movieTitle1,rate
//         userId2 movieTitle2,rate
//         userId3 movieTitle2,rate
//         ...
//         userIdN movieTitleN
// Output: 1 movieTitle1
//         1 movieTitle2
//         1 movieTitle2
//         ...
//         1 movieTitleN
class SimpleMap extends Mapper<Object, Text, Text, Text> {

    private final Text id = new Text();
    private final static Text one = new Text("1");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        id.set(value.toString());
        context.write(one, id);
    }
}

// Third Reduce:  Show the frequency in ascending order
// Input:  1 movieTitle1
//         1 movieTitle2
//         1 movieTitle2
//         ...
//         1 movieTitleN
// Output: N1 movieTitle1
//         N2 movieTitle2
//         ...
//         N movieTitleN
class AscendReduce extends Reducer<Text, Text, IntWritable, Text> {

    HashMap<String, Integer> freq = new HashMap();
    HashMap<Integer, String> inv = new HashMap();
    private final IntWritable num = new IntWritable();
    private final Text content = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text v : values) {
            String title = v.toString();
            if (!freq.keySet().contains(title)) {
                freq.put(title, 1);
            } else {
                int n = freq.get(title);
                freq.put(title, n + 1);
            }
        }

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
        
        Set<Integer> keySet = inv.keySet();
        Integer[] keys = keySet.toArray(new Integer[0]);
        Arrays.sort(keys);
        
        for (int i : keys) {
            String v = inv.get(i);
            num.set(i);
            content.set(v);
            context.write(num, content);
        }

    }
}

public class ChainSec extends Configured implements Tool {

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
        job1.setReducerClass(app.FirstReducer.class);
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
        job2.setMapperClass(app.UserMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(app.SecondReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, intermediateOutput1);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        Path intermediateOutput2 = new Path("intermediate_output2");
        FileOutputFormat.setOutputPath(job2, intermediateOutput2);
        intermediateOutput2.getFileSystem(cf).delete(intermediateOutput2, true);

        if (!job2.waitForCompletion(true)) {
            return 1;
        }

        // Configure Job 3
        Job job3 = Job.getInstance(cf, "Job 3");
        job3.setJarByClass(ChainSec.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(SimpleMap.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(AscendReduce.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, intermediateOutput2);
        Path p = new Path(args[1]);
        FileOutputFormat.setOutputPath(job3, p);
        p.getFileSystem(cf).delete(p, true);
        return job3.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(cf, new ChainSec(), args);
        System.exit(res);
    }
}
