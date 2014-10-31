package onaxe;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

public class ConnectFTP {
	static Logger logger = Logger.getRootLogger();

	public boolean downloadSingleFile(FTPClient ftpClient, String remoteFilePath, String savePath) throws IOException {
		File downloadFile = new File(savePath);

		File parentDir = downloadFile.getParentFile();
		if (!parentDir.exists()) {
			parentDir.mkdir();
		}
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
		try {
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			return ftpClient.retrieveFile(remoteFilePath, outputStream);
		} catch (IOException ex) {
			throw ex;
		} finally {
			if (outputStream != null) {
				outputStream.close();
			}
		}
	}
	
	public boolean downloadFile(FTPClient ftpClient) throws IOException {
		ConfigData configData = Exos9300ImportCVS.configData;		
		String dirToList = configData.pathServer;
		FTPFile[] subFiles = ftpClient.listFiles(dirToList);

		if (subFiles != null && subFiles.length > 0) {
			for (FTPFile aFile : subFiles) {
				String currentFileName = aFile.getName();
				if (currentFileName.compareToIgnoreCase(configData.csvFileName) != 0) {
					// search until found the same file as of defined in csvFileName
					continue;
				}
				String filePath = configData.pathServer + "/" + currentFileName;
				String newDirPath = configData.pathLocal + configData.pathServer + File.separator + currentFileName;

				if (aFile.isFile()) {
					boolean success = downloadSingleFile(ftpClient, filePath, newDirPath);
					if (success) {
						String msg = "Downloaded the file successfully: " + filePath;
						Constant.write2Log(msg);
						return true;
					} else {
						String msg = "Could not download the file: " + filePath;
						Constant.write2LogError(msg);
					}
				}
			}
		}
		return false;
	}

	public boolean connectAndDownload() throws SocketException, IOException {
		ConfigData configData = Exos9300ImportCVS.configData;
		File fCheck = new File(configData.pathLocal + configData.pathServer);
		if (!fCheck.exists())
			fCheck.mkdirs();

		Constant.write2Log("Connecting to the FTP server: " + configData.url + " for dowdloading the file " + configData.csvFileName);
		int port = Integer.valueOf(configData.port);
		FTPClient ftpClient = new FTPClient();

		int i = 1;
		while (true) {
			try{
				// connect and login to the server
				Constant.write2Log("Trying to connect to the ftp server for downloading file ... counter = " + i);
				ftpClient.connect(configData.url, port);
				break;
			} catch (Exception e) {
				if (i == 6) {
					Constant.write2LogError("An error occurred during the execution of Exos9300 Import CSV." + e);
					return false;
				}
				i++;
			}
		}
		ftpClient.login(configData.ftpUser, configData.ftpPass);
		ftpClient.enterLocalPassiveMode();
		boolean result = downloadFile(ftpClient);
		
		// log out and disconnect from the server
		ftpClient.logout();
		ftpClient.disconnect();

		Constant.write2Log("Disconnected from FTP server: " + configData.url);
		return result;
	}

	public boolean connectAndUpload() throws SocketException, IOException {
		ConfigData configData = Exos9300ImportCVS.configData;
		Constant.write2Log("Connecting to the FTP server: " + configData.url + " for uploading the statistic file.");
		int port = Integer.valueOf(configData.port);
		FTPClient ftpClient = new FTPClient();

		int i = 1;
		while (true) {
			try{
				// connect and login to the server
				Constant.write2Log("Trying to connect to the ftp server for downloading file ... counter = " + i);
				ftpClient.connect(configData.url, port);
				break;
			} catch (Exception e) {
				if (i == 6) {
					Constant.write2LogError("An error occurred during the execution of Exos9300 Import CSV." + e);
					return false;
				}
				i++;
			}
		}
		ftpClient.login(configData.ftpUser, configData.ftpPass);
		boolean result = uploadFile(ftpClient);
		
		// log out and disconnect from the server
		ftpClient.logout();
		ftpClient.disconnect();

		Constant.write2Log("Disconnected from FTP server: " + configData.url);
		return result;
	}
	
	
	public boolean uploadFile(FTPClient ftpClient) throws IOException {
		ConfigData configData = Exos9300ImportCVS.configData;		
		String fileName = getUploadFileName();
		String newDirPath = configData.pathStatLocal + File.separator + fileName;

		File uploadFile = new File(newDirPath);
		if (uploadFile.exists()) {
			InputStream inputStream = new FileInputStream(uploadFile);
			try {
				Constant.write2Log("Start uploading file: " + fileName);
				
				String serverFilePath = configData.pathStatServer + "/" + fileName;
				boolean done = ftpClient.storeFile(serverFilePath, inputStream);
		        
				Constant.write2Log("Finished uploading file to " + serverFilePath + " successfully!");
				inputStream.close();
				return done;
			} catch (IOException ex) {
				Constant.write2LogError("Sorry, there is a problem occurred during the uploading to ftp!" + ex);
				throw ex;
			}
		} else {
			Constant.write2LogError("Sorry, the given " + newDirPath + " file does not exist!");
		}
		return false;
	}

	private String getUploadFileName() {
		Date today = new Date();  
		ConfigData configData = Exos9300ImportCVS.configData;
		
		Calendar calendar = Calendar.getInstance();  
		calendar.setTime(today);

		// Get the month of today, or if given from the parameter
		int month = calendar.get(Calendar.MONTH) - 1;		
		if (configData.month != -1) {
			month = configData.month;
		}
		// Get the first day of the given month
		calendar.set(Calendar.DAY_OF_MONTH, 1);  
		calendar.set(Calendar.MONTH, month);
		Date firstDayOfMonth = calendar.getTime();

		// Get the last day of the given month
		int lastDate = calendar.getActualMaximum(Calendar.DATE);
	    calendar.set(Calendar.DATE, lastDate); 
		Date lastDayOfMonth = calendar.getTime();

		DateFormat sdf = new SimpleDateFormat("yyyyMMdd");  
		String firstDay = sdf.format(firstDayOfMonth);
		String strLastDay = sdf.format(lastDayOfMonth);
		// Format: Transactions_20140901_20140930.csv
		return "Transactions_" + firstDay + "_" + strLastDay + ".csv";
	}
}
