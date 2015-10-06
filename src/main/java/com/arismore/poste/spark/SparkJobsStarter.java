package main.java.com.arismore.poste.spark;


import java.util.Properties;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import main.java.com.arismore.poste.util.UniversalNamespaceCache;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

/**
 * Created by mehdi on 9/29/15.
 */
public class SparkJobsStarter {


    private static final long serialVersionUID = 2222111111L;

    static Logger LOG = Logger.getLogger(SparkJobsStarter.class);
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
            SparkConf sparkConf = new SparkConf().setAppName("JobStarter");
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
            JavaDStream<String> receiverStream = jssc.textFileStream("file:///spark/POC/2");

            String STREAMING_API_URL = "http://national.cpn.prd.sie.courrier.intra.laposte.fr/National/enveloppes/v1/externe?";
            String SEP = "&";
            String BEGINDATE = "dateDebut=";
            String ENDDATE = "dateFin=";
            String STARTINDEX = "startIndex=";
            String COUNT = "count=";
            int STEP = 1000;


            props.put("metadata.broker.list", args[0]);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("partitioner.class", "main.java.com.arismore.poste.kafka.TopicPartitioner");
            props.put("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);

            XPath xpath = XPathFactory.newInstance().newXPath();
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);

            builder = factory.newDocumentBuilder();


            CloseableHttpClient client = HttpClientBuilder.create().build();
            Date date = new Date();

            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MINUTE, -6);
            Date start = cal.getTime();

            cal.setTime(date);
            cal.add(Calendar.MINUTE, -5);
            Date end = cal.getTime();

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm':00Z'");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

            String dateDebut = formatter.format(start);
            String dateFin = formatter.format(end);

            LOG.debug("processing " + dateDebut + "  " + dateFin);

            HttpGet get = new HttpGet(STREAMING_API_URL + BEGINDATE + dateDebut
                    + SEP + ENDDATE + dateFin + SEP + STARTINDEX + "1" + SEP
                    + COUNT + "1");
            HttpResponse response;

        try {
            String url = null;

            response = client.execute(get);
            StatusLine status = response.getStatusLine();

            if (status.getStatusCode() == 200) {

                InputStream inputStream = response.getEntity().getContent();

                Document doc = builder.parse(new InputSource(inputStream));
                xpath.setNamespaceContext(new UniversalNamespaceCache(doc, true));

                int number = Integer.parseInt((String) xpath.compile(
                        "/a:feed/openSearch:totalResults").evaluate(doc,
                        XPathConstants.STRING));
                for (int i = 1; i < number; i += STEP) {
                    String key = "";
                    String msg = STREAMING_API_URL + BEGINDATE + dateDebut + SEP
                            + ENDDATE + dateFin + SEP + STARTINDEX + i + SEP
                            + COUNT + STEP;
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, msg);
                    producer.send(data);
                }
            } else {
                try {
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
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            LOG.error("Error in communication with the OREST TAE api ["
                    + get.getURI().toString() + "]");
            try {
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
            } catch (IOException a) {
                a.printStackTrace();
            }
        } catch (SAXException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (XPathExpressionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
            receiverStream.print();
            jssc.start();
            jssc.awaitTermination();
        }catch(Exception e) {
            System.out.println("ERROOOOOOOOOOOOOOOOOOOOOOOOOOR: ");
            e.printStackTrace();
        }
    }
}