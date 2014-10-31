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
		logger.info(":::::: Execution started at: " + d.toString());
		logger.info("");
		logger.info("------------------------------------------------------------------------");
		System.out.println(":::::: Execution started at: " + d.toString());
		
		configData.doReadConfig("conf.xml");
		try {
				
			// Connect to FTP
			ConnectFTP connFTP=new ConnectFTP();
			if (connFTP.connectAndDownload()) {
				System.out.println("\nDownloaded file from FTP server successfully ...");				
				ReadCSV readCSV=new ReadCSV(); 
				if (readCSV.getCSVFiles()){
					logger.info("\nStart to process data from the CSV file into the Exos9300 database......");
					System.out.println("\nStart to process data from the CSV into Exos9300........\n");
//					readCSV.processCSV2DB();
				} else {
					logger.info("Cannot read file or no data to be processed");
					System.out.println("Cannot read file or no data to be processed");
				}
			}
		} catch (Exception e) {
			logger.error("An error occurred during the execution of Exos9300 Import CSV." + e);
		}
		
		d = new Date();
		logger.info("------------------------------------------------------------------------");
		logger.info("");
		logger.info(":::::: Execution finished at: " + d.toString());
		logger.info("");
		logger.info("------------------------------------------------------------------------");
		logger.info("=========================================================================");
		logger.info("");
		System.out.println("\n:::::: Execution finish at: " + d.toString());
  }
}