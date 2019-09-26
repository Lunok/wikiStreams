import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;



public class WikiStreams {
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new IntWritable(1));
            }

        }
    }
    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        URLConnection conn = new URL
                ("https://stream.wikimedia.org/v2/stream/recentchange").openConnection();

        BufferedReader reader = new BufferedReader
                (new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));

        String line;

        while ((line = reader.readLine()) != null) {

            System.out.println(line);
            // 1. parse interesting lines according to
            //    their prefix event, id, or data
            // 2. write filtered data to the database
        }

       /*
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WikiStreams.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true);

        if (status) {
            System.out.println("mdr");
            System.exit(0);
        } else {
            System.out.println("pas mdr");
            System.exit(1);
        }
        */

    }
}