package main.java.com.arismore.poste.data;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class QueryEntry implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 22122110L;
	private ArrayList<ParcelData> parcels = null;
	//private Singleton instance = null;

	public QueryEntry(Document document, int index, XPath xpath) {

		try {

			//instance = Singleton.getInstance();
			PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");

			this.parcels = new ArrayList<ParcelData>();
			ParcelData parcel = new ParcelData(index, document, xpath);
			ParcelData clone = null;

			// BulkRequestBuilder bulkRequest =
			// instance.getClient().prepareBulk();

			NodeList traitement = (NodeList) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li")
					.evaluate(document, XPathConstants.NODESET);
			if (traitement.getLength() > 0) {
				for (int i = 1; i <= traitement.getLength(); i++) {
					clone = (ParcelData) parcel.clone();
					clone.setTraitement(new TraitementMachine(index, i,
							document, xpath));
					this.parcels.add(clone);
					//System.out.println(instance.getGson().toJson(clone));
					// writer.println(gson.toJson(clone));

					/*
					 * bulkRequest.add(instance .getClient() .prepareIndex(
					 * "parceldata", "traitement", clone.getIsie() + "|" +
					 * clone.getTraitement().getId())
					 * .setSource(instance.getGson().toJson(clone)));
					 */

				}
			}
			NodeList traitementManuel = (NodeList) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li")
					.evaluate(document, XPathConstants.NODESET);
			if (traitementManuel.getLength() > 0) {
				for (int i = 1; i <= traitementManuel.getLength(); i++) {
					clone = (ParcelData) parcel.clone();
					clone.setTraitement(new TraitementManuel(index, i,
							document, xpath));
					this.parcels.add(clone);

					/*
					 * bulkRequest.add(instance .getClient() .prepareIndex(
					 * "parceldata", "traitement", clone.getIsie() + "|" +
					 * clone.getTraitement().getId())
					 * .setSource(instance.getGson().toJson(clone)));
					 */
				}
			}

			/*
			 * BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			 * 
			 * if (bulkResponse.hasFailures()) {
			 * System.out.println("bulk failures");
			 * System.out.println(bulkResponse.getItems().toString()); }
			 */

			//System.out.println("parcels traitement size");
			//System.out.println(this.parcels.size());

			writer.close();

		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ArrayList<ParcelData> getParcels() {
		return parcels;
	}
	
	 /*@Override
	 public String toString(){
		 return this.parcels.toString();
	 }*/
}