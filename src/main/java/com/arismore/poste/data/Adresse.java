package main.java.com.arismore.poste.data;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;

public class Adresse {

	private String originAdresse;
	private String CodeAdresse;
	private String codeAcheDistri;
	private String idRao;
	private String typeRao;
	private String idRaoLivraison;
	private String typeRaoLivraison;

	public Adresse(int index, Integer i, Document document, XPath xpath) {
		try {
			this.originAdresse = (String) xpath.compile(
							"/a:feed/a:entry[" + index + "]/a:content/:div/:ul[@class='adresses']/:li["+ i.toString()+ "]/:span[@class='type']/text()").evaluate(
							document, XPathConstants.STRING);
			this.CodeAdresse = (String) xpath.compile(
					"/a:feed/a:entry[" + index + "]/a:content/:div/:ul[@class='adresses']/:li["+ i.toString()+ "]/:span[@class='code']/text()").evaluate(
					document, XPathConstants.STRING);
			this.codeAcheDistri = (String) xpath.compile(
					"/a:feed/a:entry[" + index + "]/a:content/:div/:ul[@class='adresses']/:li["+ i.toString()+ "]/:div[@class='client']/:span[@class='codeAcheDistri']/text()").evaluate(
					document, XPathConstants.STRING);
			this.idRao = (String) xpath.compile(
					"/a:feed/a:entry[" + index + "]/a:content/:div/:ul[@class='adresses']/:li["+ i.toString()+ "]/:span[@class='idRao']/text()").evaluate(
					document, XPathConstants.STRING);
			this.typeRao = (String) xpath.compile(
					"/a:feed/a:entry[" + index + "]/a:content/:div/:ul[@class='adresses']/:li["+ i.toString()+ "]/:span[@class='typeRao']/text()").evaluate(
					document, XPathConstants.STRING);
			this.idRaoLivraison = (String) xpath.compile(
					"/a:feed/a:entry[" + index + "]/a:content/:div/:ul[@class='adresses']/:li["+ i.toString()+ "]/:span[@class='idRaoLivraison']/text()").evaluate(
					document, XPathConstants.STRING);
			this.typeRaoLivraison = (String) xpath.compile(
					"/a:feed/a:entry[" + index + "]/a:content/:div/:ul[@class='adresses']/:li["+ i.toString()+ "]/:span[@class='typeRaoLivraison']/text()").evaluate(
					document, XPathConstants.STRING);
		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
