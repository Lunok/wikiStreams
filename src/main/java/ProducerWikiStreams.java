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
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerWikiStreams {

    private final static String TOPIC = "WikiStreams";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {

            runProducer(5);

        } else {

            runProducer(Integer.parseInt(args[0]));

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

    private static Producer<Long, String> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ProducerWikiStreams");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);

    }

    static void runProducer(final int sendMessageCount) throws Exception {

        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        try {

            URLConnection conn = new URL("https://stream.wikimedia.org/v2/stream/recentchange").openConnection();
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
            String line;

            while ((line = reader.readLine()) != null) {

                // New Producer of Wikipedia
                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, time,line);

                // Send async records to Kafka
                producer.send(record, (metadata, exception) -> {

                    long elapsedTime = System.currentTimeMillis() - time;

                    if (metadata != null) {

                        System.out.printf("sent record(key=%s value=%s) " +
                                              "meta(partition=%d, offset=%d) time=%d\n",
                                              record.key(), record.value(), metadata.partition(),
                                              metadata.offset(), elapsedTime
                        );

                    } else {

                        exception.printStackTrace();

                    }

                    countDownLatch.countDown();

                });

            }

        } finally {

            producer.flush();
            producer.close();

        }
    }

}