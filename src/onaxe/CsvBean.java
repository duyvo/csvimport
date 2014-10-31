package onaxe;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;


public class CsvBean {
	public String personNr;
	public String lastName;
	public String firstName;
	public String cardNr;//ClientFK,LanguageFK
	public String dateStart;
	public String dateEnd;
	public String installationNr;
	public String status;
	public String dateEvent;
	public int line;

	public CsvBean(){
		this.personNr = "";
		this.lastName = "";
		this.firstName = "";
		this.cardNr = "";//ClientFK,LanguageFK
		this.dateStart = "";
		this.dateEnd = "";
		this.installationNr = "";
		this.status = "";
		this.dateEvent = "";
	}
	
	public CsvBean(String personalNr, String lastName, String firstName, String cardNr,
			String dateStart,String dateEnd, int line){
		this.personNr = personalNr;
		this.lastName = lastName;
		this.firstName = firstName;
		this.cardNr = cardNr;//ClientFK,LanguageFK
		this.dateStart = dateStart;
		this.dateEnd = dateEnd;
		this.installationNr = Constant.INSTALLATION_NUMBER;
		this.status = "";
		this.dateEvent = "";
		this.line = line;
	}
	
	private boolean isEmpty(String str) {
		return str == null || "".compareToIgnoreCase(str) == 0;
	}
	
	private boolean equals(CsvBean bean){
		if (this.personNr.compareToIgnoreCase(bean.personNr) != 0) 
			return false;
		if (this.cardNr.compareToIgnoreCase(bean.cardNr) != 0) 
			return false;
		
		boolean isStatusThis = isEmpty(this.status); 
		boolean isStatusBean = isEmpty(bean.status); 
		
		if ((isStatusThis && !isStatusBean) || (!isStatusThis && isStatusBean)) 
			return false;
		if (this.status.compareToIgnoreCase(bean.status) != 0) {
			return false;
		}
		if (this.dateStart.compareToIgnoreCase(bean.dateStart) != 0) {
			return false;
		}
		if (this.dateEnd.compareToIgnoreCase(bean.dateEnd) != 0) {
			return false;
		}
		if (this.dateEvent.compareToIgnoreCase(bean.dateEvent) != 0) {
			return false;
		}
		return true;
	}

	public boolean checkBeanInOldCsvFile(List<CsvBean> csvBeanOldList) {
		for (CsvBean b : csvBeanOldList) {
			if (this.equals(b))
				return true;
		}
		return false;
	}

	@Deprecated
	public int dateEndVsGivenDate(Date today) {
		return dateVsGivenDate(this.dateEnd, today);
	}

	public int dateStartVsGivenDate(Date today) {
		return dateVsGivenDate(this.dateStart, today);
	}

	public int dateEventVsGivenDate(Date today) {
		return dateVsGivenDate(this.dateEvent, today);
	}

	public boolean isDateStartWithin14Days(Date today) {
		Date dateLastWeek = DateUtils.addDays(today, -7);
		Date dateNextWeek = DateUtils.addDays(today, 7);
		
		Date dateE = createDate(dateStart);
		if (dateE != null && dateE.before(dateNextWeek) && dateE.after(dateLastWeek))
			return true;
		return false;
	}
	
	public int dateVsGivenDate(String givenDate, Date today) {
		Date dateE = createDate(givenDate);
		if (dateE == null)
			return Constant.INVALID;
		return dateVsGivenDate(dateE, today);
	}

	public int dateVsGivenDate(Date givenDate, Date today) {
        if (DateUtils.isSameDay(today, givenDate)){
        	return Constant.SAME_DAY;
        } else if (today.before(givenDate)) {
        	return Constant.FUTURE;
        }
        return Constant.PAST;
	}

	
	public Date getStartDate() {
		return createDate(dateStart);
	}
	
	private Date createDate(String givenDateString) {
		if (isEmpty(givenDateString) || givenDateString.length() != 8) // Date must be in the format of YYYYMMDD, ex: 20141230
			return null;
		Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, Integer.parseInt(givenDateString.substring(0,4)));
        c.set(Calendar.MONDAY, Integer.parseInt(givenDateString.substring(4,6)) - 1);
        c.set(Calendar.DAY_OF_MONTH, Integer.parseInt(givenDateString.substring(6,8)));
        return c.getTime();
	}
	
	public static String outPerson(CsvBean bean) {
		return " Matricule: " + bean.personNr + " (" + bean.lastName + "," + bean.firstName + ", line " + bean.line + " in csv file) ";
	}
}
