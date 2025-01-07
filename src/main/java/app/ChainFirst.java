package app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Iterables;

// Objective: find the frequency of the highest rated movie per userID.
// Solution: use a chainFirst of Map/Reduce jobs.
//-----------------------------------------------------------------------------------
// First Map: join the files, for each movieId get the title and the rating of a user.
// Input:  movie.csv and rating.csv files
// Output: movieId t:movieTitle
//         movieId r:userId|userRate
//         movieId r:userId|userRate
//         ...
class MovieMapper extends Mapper<Object, Text, Text, Text> {

    private final Text id = new Text();
    private final Text content = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        if (fileName.contains("movies")) {
            String valS = value.toString();
            int last = valS.lastIndexOf(",");
            String genreS = valS.substring(0, last);
            String[] line = genreS.split(",", 2);
            String idS = line[0];
            String titleS = line[1];
            if (idS.equals("movieId")) {
                return;
            }
            id.set(idS);
            content.set("t:" + titleS);
            context.write(id, content);
        } else {
            String[] line = value.toString().split(",");
            String userId = line[0];
            String movieId = line[1];
            String rateS = line[2];
            if (movieId.equals("movieId")) {
                return;
            }
            id.set(movieId);
            content.set("r:" + userId + "|" + rateS);
            context.write(id, content);
        }
    }
}

// First Reduce: compress in a single line all the ratings of a movie.
// Input:  movieId t:movieTitle
//         movieId r:userId|userRate
//         movieId r:userId|userRate
//         ...
// Output: movieTitle userId|userRate,userId|userRate,...
class FirstReducer extends Reducer<Text, Text, Text, Text> {

    private final Text id = new Text();
    private final Text ret = new Text();

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
        id.set(title);
        ret.set(sb.toString());
        context.write(id, ret);
    }
}

// Second Map: for each movie get the ratings 
// Input:  movieTitle userId|userRate,userId|userRate,...
// Output: userId1 movieTitle|userRate
//         userId1 movieTitle|userRate
//         userId2 movieTitle|userRate
//         userId2 movieTitle|userRate
//         ...
//         userIdN movieTitle|userRate
class UserMapper extends Mapper<Object, Text, Text, Text> {

    private final Text id = new Text();
    private final Text ret = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String title = key.toString();
        String[] vals = value.toString().split(",");
        for (String v : vals) {
            String[] val = v.split("\\|", 2);
            if (val.length < 2) {
                continue;
            }
            String userId = val[0];
            String rate = val[1];
            id.set(userId);
            ret.set(title + "|" + rate);
            context.write(id, ret);
        }
    }
}

// Second Reduce: chose one random best rated movie per user
// Input:  userId1 movieTitle|userRate
//         userId1 movieTitle|userRate
//         userId2 movieTitle|userRate
//         userId2 movieTitle|userRate
//         ...
//         userIdN movieTitle|userRate
// Output: userId1 movieTitle1
//         userId2 movieTitle2
//         userId3 movieTitle2
//         ...
//         userIdN movieTitleN
class SecondReducer extends Reducer<Text, Text, Text, Text> {

    private final Text ret = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float max = 0;
        String title = "";
        for (Text t : values) {
            String[] val = t.toString().split("\\|", 2);
            if (val.length < 2) {
                continue;
            }
            String movieTitle = val[0];
            String rate = val[1];
            try {
                float rateF = Float.parseFloat(rate);
                if (max < rateF) {
                    max = rateF;
                    title = movieTitle;
                }
            } catch (NumberFormatException e) {
                System.out.println(e.toString() + ": Couldn't parse " + rate);
            }
        }
        ret.set(title);
        context.write(key, ret);
    }
}

// Third Map: chose one random best rated movie per user
// Input:  userId1 movieTitle1
//         userId2 movieTitle2
//         userId3 movieTitle2
//         ...
//         userIdN movieTitleN
// Output: movieTitle1 1
//         movieTitle2 1
//         movieTitle2 1
//         ...
//         movieTitleN 1
class FreqMapper extends Mapper<Object, Text, Text, Text> {

    private final Text id = new Text();
    private final static Text one = new Text("1");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        id.set(value.toString());
        context.write(id, one);
    }
}

// Third Reduce: Compute the frequency
// Input:  movieTitle1 1
//         movieTitle2 1
//         movieTitle2 1
//         ...
//         movieTitleN 1
// Output: movieTitle1 1
//         movieTitle2 2
//         movieTitle3 2
//         ...
//         movieTitleN N
class ThirdReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = Iterables.size(values);
        context.write(new Text(Integer.toString(sum)), key);
    }
}

// Fourth Map: Switch  number and title (use IntWritable for evaluating entries in ascending order)
// Input:  movieTitle1 1
//         movieTitle2 2
//         movieTitle3 2
//         ...
//         movieTitleN N
// Output: 1 movieTitle1
//         2 movieTitle2
//         2 movieTitle3
//         ...
//         N movieTitleN
class GroupMapper extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(Integer.parseInt(key.toString())), value);
    }
}

// Fourth Reduce: Concatenate every title (use IntWritable for evaluating entries in ascending order)
// Input:  1 movieTitle1
//         2 movieTitle2
//         2 movieTitle3
//         ...
//         N movieTitleN
// Output: 1 movieTitle1
//         2 movieTitle2
//         ...
//         N movieTitleN
class FourthReducer extends Reducer<IntWritable, Text, Text, Text> {

    private final Text id = new Text();
    private final Text ret = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String retS = "";
        for (Text t : values) {
            retS += t.toString() + " ";
        }
        id.set(Integer.toString(key.get()));
        ret.set(retS.substring(0, retS.length() - 1));
        context.write(id, ret);
    }
}

public class ChainFirst extends Configured implements Tool {

    static Configuration cf;

    @Override
    public int run(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        cf = new Configuration();
        // Configuration of Job 1
        Job job1 = Job.getInstance(cf, "Job 1");
        job1.setJarByClass(ChainFirst.class);
        job1.setMapperClass(MovieMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(FirstReducer.class);
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
        job2.setJarByClass(ChainFirst.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(UserMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(SecondReducer.class);
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
        job3.setJarByClass(ChainFirst.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(FreqMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(ThirdReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, intermediateOutput2);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        Path intermediateOutput3 = new Path("intermediate_output3");
        FileOutputFormat.setOutputPath(job3, intermediateOutput3);
        intermediateOutput3.getFileSystem(cf).delete(intermediateOutput3, true);

        if (!job3.waitForCompletion(true)) {
            return 1;
        }

        // Configure Job 4
        Job job4 = Job.getInstance(cf, "Job 4");
        job4.setJarByClass(ChainFirst.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setMapperClass(GroupMapper.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setReducerClass(FourthReducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, intermediateOutput3);
        Path p = new Path(args[1]);
        FileOutputFormat.setOutputPath(job4, p);
        p.getFileSystem(cf).delete(p, true);
        return job4.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(cf, new ChainFirst(), args);
        System.exit(res);
    }
}
