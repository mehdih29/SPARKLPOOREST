package main.java.com.arismore.poste.spark;

/**
 * Created by mehdi on 10/7/15.
 */

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

public class HTTPUriGetReceiver extends Receiver<List<String>> {

    private ConsumerConnector consumer;
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
        // Define thread count for each topic
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
                     DocumentBuilder builder = null;
                    ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
                    String msg = null;
                    while (consumerIte.hasNext()) {
                        msg = new String(consumerIte.next().message());
                        System.out.println("Message from thread :: " + Thread.currentThread().getName() + " -- " + new String(consumerIte.next().message()));
                     try{
                        CloseableHttpClient client = HttpClientBuilder.create().build();

                HttpResponse response;

                response = client.execute(new HttpGet(msg));
                XPath xpath = XPathFactory.newInstance().newXPath();
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);

                builder = factory.newDocumentBuilder();

                StatusLine status = response.getStatusLine();

                if (status.getStatusCode() == 200) {

                    InputStream inputStream = response.getEntity().getContent();

                    Document doc = builder.parse(new InputSource(inputStream));
                    xpath.setNamespaceContext(new UniversalNamespaceCache(doc, true));

                    NodeList entries = (NodeList) xpath.compile("/a:feed/a:entry")
                            .evaluate(document, XPathConstants.NODESET);
                    QueryEntry entry;

                    if (entries.getLength() > 0) {
                        for (int i = 1; i <= entries.getLength(); i++) {
                            entry = new QueryEntry(document, i, xpath);
                            for (int j = 0; j < entry.getParcels().size(); j++) {
                                parcel = (ParcelData) entry.getParcels().get(j);
                                id = parcel.getIsie() + "|"
                                        + parcel.getTraitement().getId();
                                this.collector.emit(tuple, new Values(id,
                                        XmlToJsonBolt.gson.toJson(parcel)));
                            }
                        }
                    }


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
}
