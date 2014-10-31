package onaxe;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

public class ReadCSV {
	public List<CsvBean> csvBeanList = new ArrayList<CsvBean>();
	static Logger logger = Logger.getRootLogger();
	Connection connection;
	Statement st;
	DBHelper dbHelper;
	Date today = new Date();

	public ReadCSV() throws SQLException {
		connection = openConnection();
		st = connection.createStatement();
		dbHelper = new DBHelper(st,connection);
	}
	
	public boolean getCSVFiles() throws Exception {
		ConfigData configData = Exos9300ImportCVS.configData;
		
		String parentDirectory = configData.pathLocal + configData.pathServer;
		File[] filesInDirectory = new File(parentDirectory).listFiles();
		for (File f : filesInDirectory) {
			if (f.isFile() && (f.getName().compareToIgnoreCase(configData.csvFileName) == 0)) {
				String csvFilePath = f.getAbsolutePath();
				csvBeanList = readCSVFileToBeanList(csvFilePath);
				
				File backupFolder = new File(parentDirectory + "/backup");
				if (!backupFolder.exists())
					backupFolder.mkdir();
				DateFormat df = new SimpleDateFormat("yyyy_MM_dd_");
				String dateName = df.format(new Date());
				
				File backupFile = new File(backupFolder + "/" + dateName + "_" + f.getName());
				System.out.println(backupFile.getName());
				Files.copy(f.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				String message = "The file " + f.getName() + " has been copied to the backup folder under the name " + backupFile.getName();
				logger.info(message);
				System.out.println(message);
				return true;
			} 
		}
		return false;
	}

	public Connection openConnection() {
		ConfigData configData = Exos9300ImportCVS.configData;
		try {
			Class.forName(configData.driver).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Problem with creating the instance for DB: ", e);
		}
		Connection c = null;
		try {
			c = DriverManager.getConnection(configData.jdbc, configData.sqlUser, configData.sqlPass);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Problem with getting the connection: ", e);
		}
		return c;
	}

	public List<CsvBean> readCSVFileToBeanList(String csvPath) throws Exception {
		CSVReader reader = new CSVReader(new FileReader(csvPath), Exos9300ImportCVS.configData.separator);
		
		List<CsvBean> csvlist = new ArrayList<CsvBean>();
		String[] dataRow = null;
		CsvBean bean = null;
		int line = 1;
		Date dateLastWeek = DateUtils.addDays(today, -7);
		Date dateLast30Days = DateUtils.addDays(today, -30);
		int compareToGivenDate;
		try{
			while ((dataRow = reader.readNext()) != null) {
				if (dataRow.length < 6) {
					continue;
				}
				// set all values from the row into the csvbean
				bean = createBeanFromRow(dataRow, line);

				// After having retrieved the line, check the date and status if the line needs to be stored for processing or not
				switch (bean.status) {
				case Constant.PERDUE:
				case Constant.DECES:
				case Constant.ENDOMMAGEE:
				case Constant.REMPLACEE:
					// For all these cases, if the event date is older than 30 days, then do not need to process the bean, it should have already been processed
					compareToGivenDate = bean.dateEventVsGivenDate(dateLast30Days);
					if (compareToGivenDate == Constant.FUTURE)
						csvlist.add(bean);
					if (compareToGivenDate == Constant.INVALID && dbHelper.checkBeanExistInDB(bean))
						csvlist.add(bean);
					break;
				case Constant.BLOQUEE:
					// For the card bloquée, after 30 days, the card and person will be deleted, so, check the date end and add to list if needed
					compareToGivenDate = bean.dateEventVsGivenDate(dateLast30Days);
					if (compareToGivenDate == Constant.FUTURE && dbHelper.checkBeanExistInDB(bean))
						csvlist.add(bean);
					break;
				case Constant.RESTITUTION: // same as Depart right below
				case Constant.DEPART:
					// These 2 cases are similar: if the card is returned, or the person departs, then if the dateEnd is:
					// 1. Today or in the past: add this line for processing
					// 2. In the future: don't do anything
					compareToGivenDate = bean.dateEventVsGivenDate(today);
					if (compareToGivenDate <= Constant.SAME_DAY  && dbHelper.checkBeanExistInDB(bean))
						csvlist.add(bean);
					break;
				case Constant.AUCUN:
				case Constant.NOUVELLE:
					// New card: if the dateStart is from 7 days old to today, then add the line for processing -- but still be checked during the process
					// all lines with dateStart older than last week, just ignore, they should have already been processed before!
					compareToGivenDate = bean.dateStartVsGivenDate(dateLastWeek);
					if (compareToGivenDate >= Constant.FUTURE)
						csvlist.add(bean);
					break;
				}				
				line++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Problem while reading csv line number: " + line, e);
		}
		reader.close();
		List<CsvBean> finallist = new ArrayList<CsvBean>();
		
		// now sort the list by date start to make sure the earlier will be executed first
		while (csvlist.size() > 0) {
			int el = 0;
			Date early = csvlist.get(0).getStartDate();
			for (int i = 1; i < csvlist.size(); i++){
				Date dd = csvlist.get(i).getStartDate();
				if (early.compareTo(dd) > 0) {
					early = dd;
					el = i;
				}
			}
			finallist.add(csvlist.get(el));
			csvlist.remove(el);
		}
		return finallist;
	}

	private CsvBean createBeanFromRow(String[] dataRow, int line) {
		for (int i = 0; i < dataRow.length; i++)
			if (dataRow[i].indexOf("'") < 0)
				dataRow[i] = dataRow[i].replace("'", "''");

		// String personalNr, String lastName, String firstName, String cardNr, String dateStart,String dateEnd
		CsvBean bean = new CsvBean(dataRow[0], dataRow[1], dataRow[2], strFixLength(dataRow[3]), dataRow[5], dataRow[6], line);
		if (dataRow.length == 8) {
			bean.status = "";
		}
		if (dataRow.length == 9) {
			bean.status = dataRow[8].trim();
		}
		if (dataRow.length == 10) {
			bean.status = dataRow[8].trim();
			bean.dateEvent = dataRow[9];
		}	
		return bean;
	}
	
	public static String strFixLength(String CardNr) {
		// fix length = 6
		for (int i = CardNr.trim().length(); i < 6; i++)
			CardNr = "0" + CardNr;
		return CardNr;
	}

	StringBuilder strBuiderPerdue = new StringBuilder("\n\nPerdue: ");
	StringBuilder strBuiderDeces = new StringBuilder("\n\nDï¿½cï¿½s:");
	StringBuilder strBuiderRemplace = new StringBuilder("\n\nRemplacï¿½e:");
	StringBuilder strBuiderEndommage = new StringBuilder("\n\nEndomagï¿½e:");
	StringBuilder strBuiderRestitution = new StringBuilder("\n\nRestitution:");
	StringBuilder strBuiderDepart = new StringBuilder("\n\nDï¿½part:");
	StringBuilder strBuiderBloque = new StringBuilder("\n\nBloquï¿½e:");
	StringBuilder strBuiderAjoute = new StringBuilder("\n\nAjoutï¿½e:");
	int cPerdue, cDeces, cRemplacee, cEndommagee, cRestitution, cDepart, cBloquee, cNew = 0;
	String dbCardNr, dbPersonNr;
	Date dbEndDate;

	public void resetStrBuider() {
		strBuiderPerdue = new StringBuilder("\n\nPerdue: ");
		strBuiderDeces = new StringBuilder("\n\nDï¿½cï¿½s");
		strBuiderRemplace = new StringBuilder("\n\nRemplacï¿½e");
		strBuiderEndommage = new StringBuilder("\n\nEndomagï¿½e");
		strBuiderRestitution = new StringBuilder("\n\nRestitution");
		strBuiderDepart = new StringBuilder("\n\nDï¿½part");
		strBuiderBloque = new StringBuilder("\n\nBloquï¿½e");
		strBuiderAjoute = new StringBuilder("\n\nAjoutï¿½e");
		cPerdue = cDeces = cRemplacee = cEndommagee = cRestitution = cDepart = cBloquee = cNew = 0;
	}

	public void processCSV2DB() {
		try {
			cPerdue = cDeces = cRemplacee = cEndommagee = cRestitution = cDepart = cBloquee = cNew = 0;

			for (CsvBean bean : csvBeanList) {
				String message = "";
				
				switch (bean.status) {
				case Constant.PERDUE:
					if (dbHelper.checkBeanExistInDB(bean)) {
						if (dbHelper.deletePersonAndAssociatedCard(bean.personNr) != 0) {
							message = "\nDeleted " + CsvBean.outPerson(bean) + " and its associated badge: " + bean.cardNr;
							cPerdue++;
							strBuiderPerdue.append(message);
						}
					}
					break;
				case Constant.DECES:
					if (dbHelper.checkBeanExistInDB(bean)) {
						if (dbHelper.deleteBoth(bean) != 0) {
							message = "\nDeleted " + CsvBean.outPerson(bean) + " and its associated badge: " + bean.cardNr;
							cDeces++;
							strBuiderDeces.append(message);
						}
					}
					break;

				case Constant.REMPLACEE: // same as Remplacï¿½e right below
					if (dbHelper.checkBeanExistInDB(bean)) {
						if (dbHelper.deleteBoth(bean) != 0) {
							message = "\nDeleted " + CsvBean.outPerson(bean) + " and its associated badge: " + bean.cardNr;
							cRemplacee++;
							strBuiderRemplace.append(message);
						}
					} 
					break;
				case Constant.ENDOMMAGEE:
					if (dbHelper.checkBeanExistInDB(bean)) {
						if (dbHelper.deletePersonAndAssociatedCard(bean.personNr) != 0) {
							message = "\nDeleted " + CsvBean.outPerson(bean) + " and its associated badge: " + bean.cardNr;
							cEndommagee++;
							strBuiderEndommage.append(message);
						}
					}
					break;
				case Constant.RESTITUTION: // same as Depart right below
					if (dbHelper.checkBeanExistInDB(bean)) {
						if (dbHelper.deletePersonAndAssociatedCard(bean.personNr) != 0) {
							message = "\nDeleted " + CsvBean.outPerson(bean) + " and its associated badge: " + bean.cardNr;
							cRestitution++;
							strBuiderRestitution.append(message);
						}
					}					
					break;
				case Constant.DEPART:
					if (dbHelper.checkBeanExistInDB(bean)) {
						int ret = bean.dateEventVsGivenDate(today);
						if (ret <= Constant.SAME_DAY) { // dateEnd in the past or today, or empty
							if (dbHelper.deletePersonAndAssociatedCard(bean.personNr) != 0) {
								message = "\nDeleted " + CsvBean.outPerson(bean) + " and its associated badge: " + bean.cardNr;
								cDepart++;
								strBuiderDepart.append(message);
							}
						}
					}					
					break;
				case Constant.BLOQUEE:
					if (dbHelper.checkBeanExistInDB(bean)) {
						dbEndDate = dbHelper.getEndDate(bean.personNr);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
						Date csvDate;
						try {
							if (!isEmpty(bean.dateEvent))
								csvDate = sdf.parse(bean.dateEvent);
							else
								csvDate = new Date();
							if (dbEndDate == null || dbEndDate.after(csvDate)) {
								if (dbHelper.blockCard(bean) != 0) { // set the Date fin to become earlier than defined in DB
									message = "\nBlocked the badge: " + bean.cardNr+ " for " + CsvBean.outPerson(bean);
									cBloquee++;
									strBuiderBloque.append(message);
								}
							} else {
								Date dateLast30Days = DateUtils.addDays(today, -30);
								int dateEndVsLast30Days = bean.dateEventVsGivenDate(dateLast30Days);
								if (dateEndVsLast30Days == Constant.FUTURE && dbHelper.checkCardExist(bean.cardNr))
									if (dbHelper.deleteBoth(bean) != 0) {
										message = "\nDestroy blocked badge: " + bean.cardNr + " for: " + CsvBean.outPerson(bean) + " after 30 days.";
										cBloquee++;
										strBuiderBloque.append(message);
									}
								}
						
						} catch (ParseException e) {
							e.printStackTrace();
							logger.error("Error while parsing date" + bean.dateEnd, e);
						}
					}
					break;
				case Constant.AUCUN: // new card
				case Constant.NOUVELLE:
					if (dbHelper.checkBeanExistInDB(bean)) // if the bean exists already, ignore!
						break;
					if (dbHelper.checkCardExist(bean.cardNr)){
						if (dbHelper.checkIfCardExistAndPreactivated(bean.cardNr)) {
							if (dbHelper.checkPersonExist(bean.personNr)) {
								String tempCard = dbHelper.getCardNr(bean.personNr);
								if (!isEmpty(tempCard)) {
									strBuiderAjoute.append("\n!!!! Sorry, the matricule: " + bean.personNr + " has already the badge " + tempCard + "! Cannot reassign another badge!!!");
									break;
								}
							}
							if (dbHelper.assignExistingCardToNewPerson(bean) == 1){
								strBuiderAjoute.append("\nCreated new " + CsvBean.outPerson(bean) + " and assigned to the preactivated badge: " + bean.cardNr);
								cNew++;
							}
						} else {
							String personNr = dbHelper.getPersonNr(bean.cardNr);
							strBuiderAjoute.append("\n!!!! Sorry the badge: " + bean.cardNr + " has been assigned to the matricule: " + personNr + "! Cannot assign to a new matricule.");
						}
					} else {
						if (dbHelper.insertBoth(bean) != 0) {
							strBuiderAjoute.append("\nCreated new " + CsvBean.outPerson(bean) + " and associated new badge: " + bean.cardNr);
							cNew++;
						}
					}
					break;
				}
			}
			connection.close();
			String message = "Summary of the execution task of importing the csv file from the FTP server into the Exos9300 Database: " + 
					"\nPerdu: " + cPerdue + "\tDeces:" + cDeces + "\tRemplacee:" + cRemplacee + "\tEndommagee:" + cEndommagee + "\tRestitution:" + cRestitution
					+ "\tDepart:" + cDepart + "\tBloquee:" + cBloquee + "\tAjoute:" + cNew + strBuiderPerdue.toString() + strBuiderDeces.toString() + strBuiderRemplace.toString()
					+ strBuiderEndommage.toString() + strBuiderRestitution.toString() + strBuiderDepart.toString() + strBuiderBloque.toString() + strBuiderAjoute.toString();
			
			WriteLog.doWrite(message);
			System.out.println(message);
			
			message = "Perdue: " + cPerdue + " - Deces:" + cDeces + " - Remplacee:" + cRemplacee + " - Endommagee:" + cEndommagee + " - Restitution:" + cRestitution
					+ " - Depart:" + cDepart + " - Bloquee:" + cBloquee + " - Cree:" + cNew; 
			logger.info("\nProcessing in DB has been successfully done with the following result: ");
			logger.info(message);
			System.out.println(message);
			
			resetStrBuider();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("\nThere is an error while performing database action: ", e);
		}
	}

	private boolean isEmpty(String str) {
		return str == null || "".compareToIgnoreCase(str) == 0;
	}
}
