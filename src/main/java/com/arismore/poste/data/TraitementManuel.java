package main.java.com.arismore.poste.data;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;

public class TraitementManuel extends Traitement {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1222111122233445L;
	private String date;
	private String type;
	private String etat;
	private String idCentreVideocodage;
	private String duree;
	private String nomOperateur;
	private String datePublication;
	private String status;
	private String raisonFin;
	private String sequenceFrappe;
	
	public String getId() {
		return datePublication;
	}

	public TraitementManuel(int index, int i, Document document, XPath xpath) {
		try {
			this.date = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i + "]/:span[@class='date']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.type = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i + "]/:span[@class='type']/text()")
					.evaluate(document, XPathConstants.STRING);

			this.etat = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i + "]/:span[@class='etat']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.status = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i + "]/:span[@class='etat']/@title")
					.evaluate(document, XPathConstants.STRING);
			this.idCentreVideocodage = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='idCentreVideocodage']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.duree = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='duree']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.nomOperateur = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='nomOperateur']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.datePublication = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='datePublication']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.sequenceFrappe = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='sequenceFrappe']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.raisonFin = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitementManuel']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='raisonFin']/text()")
					.evaluate(document, XPathConstants.STRING);
		
			if(this.date == ""){
				this.date = null;
			}
			if(this.datePublication == ""){
				this.datePublication = null;
			}

		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
