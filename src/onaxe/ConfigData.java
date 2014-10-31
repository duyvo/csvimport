package onaxe;

import java.io.File;




import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.net.util.Base64;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConfigData {
	public String url,ftpUser,ftpPass,port,pathServer,csvFileName,statFileName,lastCsvFileName,pathLocal,jdbc,sqlUser,sqlPass;
    public String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public char separator = ';';

	public String logPath = "";
    
    static Logger logger = Logger.getRootLogger();
    
	public void doReadConfig(String path) {

	    try {
	 
			File fXmlFile = new File(path);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			Element eElement ;
			Node nNode;
			NodeList nList;
			String tempPass;
			doc.getDocumentElement().normalize();
			//read ftp
			nList = doc.getElementsByTagName("ftp");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					eElement = (Element) nNode;
					url=eElement.getElementsByTagName("url").item(0).getTextContent();
					ftpUser=eElement.getElementsByTagName("ftpUser").item(0).getTextContent();
					tempPass=eElement.getElementsByTagName("ftpPass").item(0).getTextContent();
					ftpPass = new String(Base64.decodeBase64(tempPass));
					
					port=eElement.getElementsByTagName("port").item(0).getTextContent();
					pathServer=eElement.getElementsByTagName("pathServer").item(0).getTextContent();
					csvFileName=eElement.getElementsByTagName("csvFileName").item(0).getTextContent();
					statFileName=eElement.getElementsByTagName("statFileName").item(0).getTextContent();
					lastCsvFileName=eElement.getElementsByTagName("lastCsvFileName").item(0).getTextContent();
					pathLocal=eElement.getElementsByTagName("pathLocal").item(0).getTextContent();
					
					logPath = eElement.getElementsByTagName("pathLog").item(0).getTextContent();
				}
			}
			
			//read sql
			nList = doc.getElementsByTagName("sql");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					eElement = (Element) nNode;
					jdbc=eElement.getElementsByTagName("jdbc").item(0).getTextContent();
					sqlUser=eElement.getElementsByTagName("sqlUser").item(0).getTextContent();

					tempPass=eElement.getElementsByTagName("sqlPass").item(0).getTextContent();
					sqlPass = new String(Base64.decodeBase64(tempPass));
				}
			}
			
			logger.info("The config file has been loaded correctly");
	    } catch (Exception e) {
	    	e.printStackTrace();
			logger.info("Problem while loading the config file", e);
	    }
	  }
}
