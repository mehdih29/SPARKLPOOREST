package main.java.com.arismore.poste.spark;


import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import javax.xml.parsers.DocumentBuilder;
import java.util.List;
import java.util.Properties;

/**
 * Created by mehdi on 9/29/15.
 */
public class SparkJobsStarter {


    private static final long serialVersionUID = 2222111111L;

    //static Logger LOG = Logger.getLogger(SparkJobsStarter.class);
    private static String FILE_RECOVERY_WINDOWS = "/spark/POC/_file_recovery_window";
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
            String STREAMING_API_URL = "http://national.cpn.prd.sie.courrier.intra.laposte.fr/National/enveloppes/v1/externe?";
            String SEP = "&";
            String BEGINDATE = "dateDebut=";
            String ENDDATE = "dateFin=";
            String STARTINDEX = "startIndex=";
            String COUNT = "count=";
            int STEP = 1000;

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
                    receiverStream.persist();
                    receiverStream.print();

                    /*for (int i = 1; i < number; i += STEP) {
                        String key = "";
                        String msg = STREAMING_API_URL + BEGINDATE + dateDebut + SEP
                                + ENDDATE + dateFin + SEP + STARTINDEX + i + SEP
                                + COUNT + STEP;
                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, msg);
                        producer.send(data);
                    }*/
                } else {
                    System.out.println(receiverStream.toString());
                    /*try {
                        PrintWriter out = new PrintWriter(new FileWriter(
                                FILE_RECOVERY_WINDOWS, true));
                        out.println(STREAMING_API_URL + BEGINDATE + dateDebut + SEP
                                + ENDDATE + dateFin + SEP + STARTINDEX + "1" + SEP
                                + COUNT + "1");
                        out.close();
                        String key = "";
                        String msg = STREAMING_API_URL + BEGINDATE + dateDebut + SEP
                                + ENDDATE + dateFin + SEP + STARTINDEX + "1" + SEP
                                + COUNT + "1";
                        KeyedMessage<String, String> data = new KeyedMessage<String, String>(catchingTopic, key, msg);
                        producer.send(data);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }*/
                }

            jssc.start();
            jssc.awaitTermination();
        }catch(Exception e) {
            System.out.println("ERROOOOOOOOOOOOOOOOOOOOOOOOOOR: ");
            e.printStackTrace();
        }
    }
}