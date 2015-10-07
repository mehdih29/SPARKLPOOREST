package main.java.com.arismore.poste.spark;

/**
 * Created by mehdi on 10/7/15.
 */

import kafka.consumer.*;

import main.java.com.arismore.poste.util.UniversalNamespaceCache;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HTTPUriGetReceiver extends Receiver<List<String>> {

    private final ConsumerConnector consumer;
    private ExecutorService executor;
    String brokers = null;
    String topics = null;
    String groupId = null;

    public HTTPUriGetReceiver(String brokers, String topics, String groupId) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.brokers = brokers;
        this.topics = topics;
        this.groupId = groupId;
    }

    //Logger LOG = Logger.getLogger(this.getClass());
    public void onStart() {
        // Start the thread that receives data over a connectionHashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        //HashMap<String, String> kafkaParams = new HashMap<String, Striddddng>();
        Properties kafkaParams = new Properties();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("group.id", groupId);
        this.consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaParams));
        doConsume(3);
    }
    public void onStop() {

        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    public void doConsume(int threadCount) {
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        // Define thread count for each topicuuuuuu
        topicCount.put(this.topics, new Integer(threadCount));
        // Here we have used a single topic but we can also add multiple topics to topicCount MAP
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(this.topics);
        System.out.println("streams length: " + streams.size());
        // Launching the thread pool
        executor = Executors.newFixedThreadPool(threadCount);
        //Creating an object messages consumption
        final CountDownLatch latch = new CountDownLatch(3);
        for (final KafkaStream stream : streams) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
                    while (consumerIte.hasNext()) {
                        System.out.println("Message from thread :: " + Thread.currentThread().getName() + " -- " + new String(consumerIte.next().message()));
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

        if (executor != null)
            executor.shutdown();
    }

    public void receive() {


            DocumentBuilder builder = null;
            List<String> output = new ArrayList<String>();

            if (HTTPUriGetReceiver.slidingWindow.isEmpty()) {
                try {
                    Thread.sleep(10000);
                }catch (InterruptedException e){
                    System.out.println("********************************\n************************************************exception thrown while attempting to fetch CPN data*********************");
                    e.printStackTrace();
                }
            } else {
                ArrayList<String> window = HTTPUriGetReceiver.slidingWindow.remove(0);
                //String window = TickTimerSpout.slidingWindow.remove(0);
                dateDebut = window.get(0);
                dateFin = window.get(1);

            }

            try {
                CloseableHttpClient client = HttpClientBuilder.create().build();

                System.out.println("processing " + dateDebut + "  " + dateFin);

                HttpGet get = new HttpGet(STREAMING_API_URL + BEGINDATE + dateDebut
                        + SEP + ENDDATE + dateFin + SEP + STARTINDEX + "1" + SEP
                        + COUNT + "1");
                HttpResponse response;

                response = client.execute(get);
                XPath xpath = XPathFactory.newInstance().newXPath();
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);

                builder = factory.newDocumentBuilder();

                StatusLine status = response.getStatusLine();

                if (status.getStatusCode() == 200) {

                    InputStream inputStream = response.getEntity().getContent();

                    Document doc = builder.parse(new InputSource(inputStream));
                    xpath.setNamespaceContext(new UniversalNamespaceCache(doc, true));

                    int number = Integer.parseInt((String) xpath.compile(
                            "/a:feed/openSearch:totalResults").evaluate(doc,
                            XPathConstants.STRING));
                    output.add(Integer.toString(number));
                    output.add(dateDebut);
                    output.add(dateFin);
                    store(output);

                } else {
                    output.add("0");
                    output.add(dateDebut);
                    output.add(dateFin);
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
            }finally {
                output.add("0");
                output.add(dateDebut);
                output.add(dateFin);
                store(output);
            }
        }
}
