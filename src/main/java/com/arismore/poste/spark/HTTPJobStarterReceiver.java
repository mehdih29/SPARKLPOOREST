package main.java.com.arismore.poste.spark;

/**
 * Created by mehdi on 10/6/15.
 */

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

public class HTTPJobStarterReceiver  extends Receiver<List<String>> {

    private static ArrayList<ArrayList<String>> slidingWindow = new ArrayList<ArrayList<String>>();
    private static Timer timer = null;


    public HTTPJobStarterReceiver() {
         super(StorageLevel.MEMORY_AND_DISK_2());
    }

    //Logger LOG = Logger.getLogger(this.getClass());
    public void onStart() {
        // Start the thread that receives data over a connection
        startTimer();
    }
    public void startTimer() {
HTTPJobStarterReceiver.timer = new Timer(); // At this line a new Thread will be created
 HTTPJobStarterReceiver.timer.schedule(new RemindTask(), 0, 60 * 1000); // delay in milliseconds

    }
    public void onStop() {

        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    class RemindTask extends TimerTask {

        @Override
        public void run() {

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm':00Z'");
            formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date date = new Date();

            Calendar cal = Calendar.getInstance();

            cal.setTime(date);
            cal.setTime(date);
            cal.add(Calendar.MINUTE, -6);
            Date start = cal.getTime();

            cal.setTime(date);
            cal.add(Calendar.MINUTE, -5);
            Date end = cal.getTime();
            ArrayList<String> temp = new ArrayList<String>();
            temp.add(formatter.format(start));
            temp.add(formatter.format(end));
            HTTPJobStarterReceiver.slidingWindow.add(temp);
            String STREAMING_API_URL = "http://national.cpn.prd.sie.courrier.intra.laposte.fr/National/enveloppes/v1/externe?";
            String SEP = "&";
            String BEGINDATE = "dateDebut=";
            String ENDDATE = "dateFin=";
            String STARTINDEX = "startIndex=";
            String COUNT = "count=";
            DocumentBuilder builder = null;
            List<String> output = new ArrayList<String>();

            String dateDebut = null;
            String dateFin = null;

            if (HTTPJobStarterReceiver.slidingWindow.isEmpty()) {
                try {
                    Thread.sleep(10000);
                }catch (InterruptedException e){
                    System.out.println("********************************\n************************************************exception thrown while attempting to fetch CPN data*********************");
                    e.printStackTrace();
                }
            } else {
                ArrayList<String> window = HTTPJobStarterReceiver.slidingWindow.remove(0);
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
}

