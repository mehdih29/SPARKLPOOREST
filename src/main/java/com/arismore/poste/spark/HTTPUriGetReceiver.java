package main.java.com.arismore.poste.spark;

/**
 * Created by mehdi on 10/7/15.
 */

import com.google.gson.GsonBuilder;
import com.google.gson.Gson;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import main.java.com.arismore.poste.data.ParcelData;
import main.java.com.arismore.poste.data.QueryEntry;
import main.java.com.arismore.poste.util.UniversalNamespaceCache;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HTTPUriGetReceiver extends Receiver<Map<String, String>> {

    private ConsumerConnector consumer;
    private static String brokers = null;
    private static String topics = null;
    private static String groupId = null;
    private static String zookeeper = null;
    static private Gson gson = null;
    XPath xpath = null;
    static DocumentBuilder builder = null;

    public HTTPUriGetReceiver(String zookeeper, String topics, String groupId) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        HTTPUriGetReceiver.topics = topics;
        HTTPUriGetReceiver.groupId = groupId;
       HTTPUriGetReceiver.zookeeper = zookeeper;
        System.out.println(topics + "     " + zookeeper + "     " + groupId );
    }

    //Logger LOG = Logger.getLogger(this.getClass());
    public void onStart() {
        // Start the thread that receives data over a connectionHashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
       // HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        Properties kafkaParams = new Properties();
        //kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("zookeeper.connect", zookeeper);
         kafkaParams.put("group.id", groupId);
        kafkaParams.put("zookeeper.session.timeout.ms", "500");
        kafkaParams.put("zookeeper.sync.time.ms", "250");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("group.id", groupId);
        this.consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaParams));
        this.xpath = XPathFactory.newInstance().newXPath();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        try {
            HTTPUriGetReceiver.builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        HTTPUriGetReceiver.gson = new GsonBuilder().create();
        doConsume(1);
    }
    public void onStop() {

        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    public void doConsume(int threadCount) {
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        // Define thread count for each topic
        topicCount.put(HTTPUriGetReceiver.topics, threadCount);
        // Here we have used a single topic but we can also add multiple topics to topicCount MAP
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(HTTPUriGetReceiver.topics);
        System.out.println("streams length: " + streams.size());
        // Launching the thread pool
        ExecutorService  executor = Executors.newFixedThreadPool(threadCount);
        //Creating an object messages consumption
        final CountDownLatch latch = new CountDownLatch(3);
        for (final KafkaStream stream : streams) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
                    String msg = null;
                    while (consumerIte.hasNext()) {
                        msg = new String(consumerIte.next().message());
                        System.out.println("*****************************          Message from thread :: " + Thread.currentThread().getName() + " -- " + msg);
                        try{
                            CloseableHttpClient client = HttpClientBuilder.create().build();

                            HttpResponse response;

                            Map<String, String> output = new HashMap<String, String>();
                            response = client.execute(new HttpGet(msg));
                            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                            factory.setNamespaceAware(true);

                            builder = factory.newDocumentBuilder();

                            StatusLine status = response.getStatusLine();
                            ParcelData parcel;
                            String id;
                            if (status.getStatusCode() == 200) {

                                InputStream inputStream = response.getEntity().getContent();

                                Document doc = builder.parse(new InputSource(inputStream));
                                xpath.setNamespaceContext(new UniversalNamespaceCache(doc, true));

                                NodeList entries = (NodeList) xpath.compile("/a:feed/a:entry")
                                        .evaluate(doc, XPathConstants.NODESET);
                                QueryEntry entry;

                                if (entries.getLength() > 0) {
                                    for (int i = 1; i <= entries.getLength(); i++) {
                                        entry = new QueryEntry(doc, i, xpath);
                                        for (int j = 0; j < entry.getParcels().size(); j++) {
                                            parcel = (ParcelData) entry.getParcels().get(j);
                                            id = parcel.getIsie() + "|"
                                                    + parcel.getTraitement().getId();
                                            output.put(id,
                                                    HTTPUriGetReceiver.gson.toJson(parcel));
                                        }
                                    }
                                }

                            } else {
                                output.put("0", msg);
                                store(output);
                            }

                        } catch (FileNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (UnsupportedEncodingException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();

                        } catch (SAXException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (XPathExpressionException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }catch (Exception e) {
                            System.out.println("exception thrown while attempting to fetch CPN data**********************");
                            e.printStackTrace();
                            //LOG.error("Error in communication with the OREST TAE api [" + get.getURI().toString() + "]");
                            //LOG.trace(null, e);
                        }
                    }
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
