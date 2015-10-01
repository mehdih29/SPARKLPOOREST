package main.java.com.arismore.poste.kafka;

/**
 * Created by mehdi on 9/14/15.
 */
import org.apache.spark.streaming.kafka.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

public class KafkaConsumer {

        private static final Pattern SPACE = Pattern.compile(" ");

        public static void main(String[] args) {
                if (args.length < 2) {
                        System.err.println("Usage: DirectKafkaWordCount <brokers> <topics>\n" +
                                "  <brokers> is a list of one or more Kafka brokers\n" +
                                "  <topics> is a list of one or more kafka topics to consume from\n\n");
                        System.exit(1);
                }

                String brokers = args[0];
                String topics = args[1];

                // Create context with 2 second batch interval
                SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
                JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

                HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
                HashMap<String, String> kafkaParams = new HashMap<String, String>();
                kafkaParams.put("metadata.broker.list", brokers);

                // Create direct kafka stream with brokers and topics
                JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet
                );

                // Get the lines, split them into words, count the words and print
                JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
                        @Override
                        public String call(Tuple2<String, String> tuple2) {
                                return tuple2._2();
                        }
                });
                JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                        @Override
                        public Iterable<String> call(String x) {
                                return Lists.newArrayList(SPACE.split(x));
                        }
                });
                JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                        new PairFunction<String, String, Integer>() {
                                @Override
                                public Tuple2<String, Integer> call(String s) {
                                        return new Tuple2<String, Integer>(s, 1);
                                }
                        }).reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                                @Override
                                public Integer call(Integer i1, Integer i2) {
                                        return i1 + i2;
                                }
                        });
                wordCounts.print();

                // Start the computation
                jssc.start();
                jssc.awaitTermination();
        }
}
