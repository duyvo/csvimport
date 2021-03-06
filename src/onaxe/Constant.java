package onaxe;

import org.apache.log4j.Logger;

public class Constant {
	public static final String PERDUE = "Perdue";	
	public static final String REMPLACEE = "Remplac�e";	
	public static final String RESTITUTION = "Restitution";	
	public static final String DECES = "D�c�s";	
	public static final String ENDOMMAGEE = "Endommag�e";	
	public static final String BLOQUEE = "Bloqu�e";	
	public static final String DEPART = "D�part";
	public static final String AUCUN = "";
	public static final String NOUVELLE = "Nouvelle";
	public static final String INSTALLATION_NUMBER = "5888";	
	

	public static final int SAME_DAY = 0;	
	public static final int FUTURE = 1;	
	public static final int PAST = -1;	
	public static final int INVALID = -1000;	

	static Logger logger = Logger.getRootLogger();	
	
	public static void write2Log(String msg) {
		System.out.println(msg);
		logger.info(msg);		
	}
	public static void write2LogError(String msg) {
		System.out.println("ERROR: " + msg);
		logger.error("ERROR: " + msg);
	}
}
