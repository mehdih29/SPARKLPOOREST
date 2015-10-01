package main.java.com.arismore.poste.data;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;

public class TraitementMachine extends Traitement {
	/**
	 * 
	 */
	private static final long serialVersionUID = 122233334445555L;
	private String debutTraitement;
	private String idPlateforme;
	private String idMachineTri;
	private String entreeMachine;
	private String programmeTriTitle;
	private String programmeTriId;
	private String programmeTri;
	private String triSanctionPartiel;
	private String etat;
	private String receptacle;
	private String receptacleType;
	private String receptacleNiveau;
	private String receptacleLang;
	private String receptacleReel;
	private String raisonModificationReceptacle;
	private String receptacleReelNiveau;
	private String receptacleReelTitle;
	private String receptacleReelLang;
	private String plateformeSuivante;
	private String valeurAdressePrioritaire;
	private String cedex;
	private String typeAdressePrioritaire;
	private String service;
	private String datePublication;

	public TraitementMachine(int index, Integer i, Document document,
			XPath xpath) {
		try {
			this.debutTraitement = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='dateDebut']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.idPlateforme = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='idPlateforme']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.idMachineTri = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='idMachineTri']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.entreeMachine = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='entreeMachine']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.programmeTriTitle = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:abbr[@class='programmeTri']/@title")
					.evaluate(document, XPathConstants.STRING);
			this.programmeTriId = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:abbr[@class='programmeTri']/@id")
					.evaluate(document, XPathConstants.STRING);
			this.programmeTri = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:abbr[@class='programmeTri']/text()")
					.evaluate(document, XPathConstants.STRING);
			// TODO a verifier
			this.triSanctionPartiel = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:dl[@class='parametresTri']/:dd/text()")
					.evaluate(document, XPathConstants.STRING);
			this.etat = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='etat']/text()").evaluate(
					document, XPathConstants.STRING);
			this.receptacle = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='receptacle']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleType = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='receptacle']/@type")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleNiveau = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='receptacle']/@niveau")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleLang = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='receptacle']/@lang")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleReel = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='receptacleReel']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.raisonModificationReceptacle = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='raisonModificationReceptacle']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleReelNiveau = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='receptacleReel']/@niveau")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleReelTitle = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='receptacleReel']/@title")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleReelLang = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='receptacleReel']/@lang")
					.evaluate(document, XPathConstants.STRING);
			this.receptacleReelTitle = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:div[@class='client']/:span[@class='receptacleReel']/@title")
					.evaluate(document, XPathConstants.STRING);

			this.plateformeSuivante = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:span[@class='plateformeSuivante']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.valeurAdressePrioritaire = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:span[@class='valeur_adresse_prioritaire']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.typeAdressePrioritaire = (String) xpath
					.compile(
							"/a:feed/a:entry["
									+ index
									+ "]/a:content/:div/:ul[@class='traitement']/:li["
									+ i
									+ "]/:span[@class='type_adresse_prioritaire']/text()")
					.evaluate(document, XPathConstants.STRING);
			this.cedex = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='cedex']/text()").evaluate(
					document, XPathConstants.STRING);
			this.service = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='service']/text()").evaluate(
					document, XPathConstants.STRING);
			this.datePublication = (String) xpath.compile(
					"/a:feed/a:entry[" + index
							+ "]/a:content/:div/:ul[@class='traitement']/:li["
							+ i + "]/:span[@class='datePublication']/text()")
					.evaluate(document, XPathConstants.STRING);
			
			if(this.debutTraitement == ""){
				this.debutTraitement = null;
			}
			if(this.datePublication == ""){
				this.datePublication = null;
			}

			/*
			 * System.out.println("debutTraitement >>>"+debutTraitement);
			 * System.out.println("idPlateforme >>>" + idPlateforme);
			 * System.out.println("idMachineTri >>>" + idMachineTri);
			 * System.out.println("entreeMachine >>>" + entreeMachine);
			 * System.out.println("programmeTriTitle >>>" + programmeTriTitle);
			 * System.out.println("programmeTriId >>>" + programmeTriId);
			 * System.out.println("triSanctionPartiel >>>" +
			 * triSanctionPartiel); System.out.println("etat >>>" + etat);
			 * System.out.println("receptacle >>>" + receptacle);
			 * System.out.println("receptacleType >>>" + receptacleType);
			 * System.out.println("receptacleNiveau >>>" + receptacleNiveau);
			 * System.out.println("receptacleLang >>>" + receptacleLang);
			 * System.out.println("receptacleReel >>>" + receptacleReel);
			 * System.out.println("raisonModificationReceptacle >>>" +
			 * raisonModificationReceptacle);
			 * System.out.println("receptacleReelNiveau >>>" +
			 * receptacleReelNiveau); System.out.println("programmeTri >>>" +
			 * programmeTri);
			 */

		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getId() {
		return datePublication;
	}
	
	public String getDebutTraitement() {
		return debutTraitement;
	}

	public void setDebutTraitement(String debutTraitement) {
		this.debutTraitement = debutTraitement;
	}
}
