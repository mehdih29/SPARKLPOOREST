package main.java.com.arismore.poste.spark;

/**
 * Created by mehdi on 10/7/15.
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import javax.xml.parsers.DocumentBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

/**
 * Created by mehdi on 9/29/15.
 */

    public class SparkUriGetJob {

    private static final long serialVersionUID = 2222111112L;

    //static Logger LOG = Logger.getLogger(SparkJobsStarter.class);
    private static String FILE_RECOVERY_WINDOWS = "/spark/POC/_file_recovery_window_urls";
    private static DocumentBuilder builder = null;

    private static String topic = "urls";
    private static String catchingTopic = "catchingTopic";
    private static Producer<String, String> producer;
    private static Properties props = new Properties();

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: DirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n");
            System.exit(1);
        }
        try {


            SparkConf sparkConf = new SparkConf().setAppName("JobStarter");
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
            JavaDStream<List<String>> receiverStream = jssc.receiverStream(new HTTPCustomReceiver());

            props.put("metadata.broker.list", args[0]);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("partitioner.class", "main.java.com.arismore.poste.kafka.TopicPartitioner");
            props.put("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);


            if (receiverStream != null) {
                //receiverStream.persist();
                receiverStream.foreachRDD(new Function<JavaRDD<List<String>>, Void>() {
                    public Void call(JavaRDD<List<String>> listJavaRDD) throws Exception {
                        listJavaRDD.collect();
                        listJavaRDD.foreach(new VoidFunction<List<String>>() {
                            public void call(List<String> strings) throws Exception {
                                Integer number = Integer.parseInt(strings.get(0));
                                if (number != 0){

                                    for (int i = 1; i < number; i += STEP) {
                                        String key = "";
                                        String msg = STREAMING_API_URL + BEGINDATE + strings.get(1) + SEP
                                                + ENDDATE + strings.get(2) + SEP + STARTINDEX + i + SEP
                                                + COUNT + STEP;
                                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, msg);
                                        producer.send(data);
                                    }
                                }else{
                                    String key = "";
                                    String msg = STREAMING_API_URL + BEGINDATE + strings.get(1) + SEP
                                            + ENDDATE + strings.get(2) + SEP + STARTINDEX + "1" + SEP
                                            + COUNT + "1";
                                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(catchingTopic, key, msg);
                                    producer.send(data);
                                    try {
                                        PrintWriter out = new PrintWriter(new FileWriter(
                                                FILE_RECOVERY_WINDOWS, true));
                                        out.println(msg);
                                        out.close();

                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        });
                        return null;
                    }
                });
            } else {
                System.out.println(" Problem Here: " + receiverStream.toString());
            }

            jssc.start();
            jssc.awaitTermination();
        }catch(Exception e) {
            System.out.println("ERROOOOOOOOOOOOOOOOOOOOOOOOOOR: ");
            e.printStackTrace();
        }
    }
}