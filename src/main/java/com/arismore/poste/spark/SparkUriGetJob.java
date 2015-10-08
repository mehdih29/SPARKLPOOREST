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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by mehdi on 9/29/15.
 */

public class SparkUriGetJob {

    private static final long serialVersionUID = 2222111112L;

    //static Logger LOG = Logger.getLogger(SparkJobsStarter.class);
    private static String FILE_RECOVERY_WINDOWS = "/spark/POC/_file_recovery_window_urls";
    private static DocumentBuilder builder = null;

    private static String topic = "JsonTopic";
    private static String catchingTopic = "catchingTopicUrls";
    private static Producer<String, String> producer;
    private static Properties props = new Properties();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: DirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n");
            System.exit(1);
        }
        try {


            SparkConf sparkConf = new SparkConf().setAppName("JobStarter");
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
            JavaDStream<Map<String, String>> receiverStream = jssc.receiverStream(new HTTPUriGetReceiver(args[1], args[2], args[3]));

            props.put("metadata.broker.list", args[0]);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("partitioner.class", "main.java.com.arismore.poste.kafka.TopicPartitioner");
            props.put("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);


            if (receiverStream != null) {
                //receiverStream.persist();
                receiverStream.foreachRDD(new Function<JavaRDD<Map<String, String>>, Void>() {
                    public Void call(JavaRDD<Map<String, String>> mapJavaRDD) throws Exception {
                        mapJavaRDD.collect();
                        mapJavaRDD.foreach(new VoidFunction<Map<String, String>>() {
                            public void call(Map<String, String> strings) throws Exception {
                                Iterator<String> iter = strings.keySet().iterator();
                                String id;
                                while(iter.hasNext()){
                                    id = iter.next();
                                    if(!id.equals("0")) {
                                        System.out.println("strings");
                                        System.out.println(strings);
                                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, id, strings.get(id));
                                        producer.send(data);
                                    }else{
                                        System.out.println("********************** 0         strings:       0 ***************************");
                                        System.out.println(strings);
                                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(catchingTopic, strings.get(id), strings.get(id));
                                        producer.send(data);
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