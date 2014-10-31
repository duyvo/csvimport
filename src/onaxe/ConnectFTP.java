package onaxe;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.sql.Statement;

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
						System.out.println("Downloaded the file successfully: " + filePath);
						logger.info("Downloaded the file successfully: " + filePath);
						return true;
					} else {
						System.out.println("COULD NOT download the file: " + filePath);
						logger.error("Could not download the file: " + filePath);
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

		System.out.println("Connecting to the FTP server: " + configData.url + " for dowdloading the file " + configData.csvFileName);
		int port = Integer.valueOf(configData.port);
		FTPClient ftpClient = new FTPClient();

		int i = 1;
		while (true) {
			try{
				// connect and login to the server
				logger.info("Trying to connect to the ftp server for downloading file ... counter = " + i);
				ftpClient.connect(configData.url, port);
				break;
			} catch (Exception e) {
				if (i == 6) {
					logger.error("An error occurred during the execution of Exos9300 Import CSV." + e);
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

		logger.info("Disconnected from FTP server: " + configData.url);
		System.out.println("Disconnected from FTP server: " + configData.url);
		return result;
	}
	
}
