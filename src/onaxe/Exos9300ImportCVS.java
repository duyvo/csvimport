package onaxe;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Exos9300ImportCVS{
  static Logger logger = Logger.getRootLogger();
  public static ConfigData configData = new ConfigData();

  public static void main(String[] args){
		PropertyConfigurator.configure("rc\\log4j.properties");
		Date d = new Date();

		// Get config file
		logger.info("");
		logger.info("=========================================================================");
		logger.info("------------------------------------------------------------------------");
		logger.info("");
		Constant.write2Log(":::::: Execution started at: " + d.toString());
		logger.info("");
		logger.info("------------------------------------------------------------------------");
		
		configData.doReadConfig("conf.xml");
		try {
			// Connect to FTP
			ConnectFTP connFTP=new ConnectFTP();
			
			if (args.length == 0 || args[0].compareTo("import") == 0) {
				if (connFTP.connectAndDownload()) {
					Constant.write2Log("\nDownloaded file from FTP server successfully ...");				
					ReadCSV readCSV=new ReadCSV(); 
					if (readCSV.getCSVFiles()){
						Constant.write2Log("\nStart to process data from the CSV file into the Exos9300 database......");
						readCSV.processCSV2DB();
					} else {
						Constant.write2LogError("Cannot read file or no data to be processed");
					}
				}
			} else if (args[0].compareTo("export") == 0) {
				if (args.length == 2) {
					try {
						int month = Integer.parseInt(args[1]);
						configData.setMonth(month);
					} catch (Exception e) {
					}
				}
				if (connFTP.connectAndUpload()) {
					Constant.write2Log("\nUpload file to FTP server successfully ...");
				} else {
					Constant.write2LogError("\nError while uploading file to FTP server ...");
				}
				
			}
		} catch (Exception e) {
			Constant.write2Log("An error occurred during the execution of Exos9300 Import CSV." + e);
		}
		
		d = new Date();
		logger.info("------------------------------------------------------------------------");
		logger.info("");
		Constant.write2Log(":::::: Execution finished at: " + d.toString());
		logger.info("");
		logger.info("------------------------------------------------------------------------");
		logger.info("=========================================================================");
		logger.info("");
  }
}