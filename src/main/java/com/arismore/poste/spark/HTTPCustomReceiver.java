package main.java.com.arismore.poste.spark;

/**
 * Created by mehdi on 10/6/15.
 */

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

public class HTTPCustomReceiver  extends Receiver<String> {

    public JavaCustomReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread()  {
            @Override public void run() {
                receive();
                Thread.sleep(60000);
            }
        }.start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    /** Create a socket connection and receive data until receiver is stopped */
    private void receive() {
        String STREAMING_API_URL = "http://national.cpn.prd.sie.courrier.intra.laposte.fr/National/enveloppes/v1/externe?";
        String SEP = "&";
        String BEGINDATE = "dateDebut=";
        String ENDDATE = "dateFin=";
        String STARTINDEX = "startIndex=";
        String COUNT = "count=";
        int STEP = 1000;

        try{
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

            response = client.execute(get);
            store(response)

        } catch (Exception e) {
            Logger LOG = Logger.getLogger(this.getClass());
            LOG.error("exception thrown while attempting to post", ex);
            LOG.error("Error in communication with the OREST TAE api [" + get.getURI().toString() + "]");
            LOG.trace(null, ex);
        }

    }

