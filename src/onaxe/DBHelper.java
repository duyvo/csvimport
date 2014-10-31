package onaxe;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

public class DBHelper {
	public Statement st;
	public Connection cn;
	static Logger logger = Logger.getRootLogger();

	public DBHelper(Statement st, Connection cn) throws SQLException {
		this.st = st;
		this.cn = cn;
	}

	private boolean isEmpty(String str) {
		return str == null || "".compareToIgnoreCase(str) == 0;
	}

	// DELETE
	public int deletePerson(String personalNr) {
		if (checkPersonExist(personalNr)) {
			try {
				String sql = "Delete  from hPerson WHERE PersonalNr='" + personalNr + "'";
				st.execute(sql);
				return 1;
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("Error while deleting Person: " + personalNr, e);
			}
		}
		return 0;
	}

	public int deleteCard(String cardNr) {
		if (checkCardExist(cardNr)) {
			try {
				cn.setAutoCommit(false);
				String sql = "Delete  FROM bidcard WHERE cardNr='" + cardNr + "'";
				st.execute(sql);
				sql = "Delete  FROM bbadge WHERE badgeNr='" + cardNr + "'";
				st.execute(sql);
				cn.commit();
				cn.setAutoCommit(true);
				return 1;
			} catch (SQLException e) {
				try {
					cn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
				logger.error("Error while deleting Card: " + cardNr, e);
			}finally
			{
				try {
					cn.setAutoCommit(false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

	public int deleteBoth(CsvBean bean) {
		if (checkPersonExist(bean.personNr) && checkCardExist(bean.cardNr)) {
			try {
				cn.setAutoCommit(false);
				String sql = "Delete  FROM bidcard WHERE cardNr='" + bean.cardNr + "'";
				st.execute(sql);
				sql = "Delete  FROM bbadge WHERE badgeNr='" + bean.cardNr + "'";
				st.execute(sql);
				sql = "Delete  from hPerson WHERE PersonalNr='" + bean.personNr + "'";
				st.execute(sql);
				
				cn.commit();
				cn.setAutoCommit(true);
				return 1;
			} catch (Exception e) {
				try {
					cn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
				logger.error("Error deleteBoth status=" + bean.status + " idPerson=" + bean.personNr + " idCard=" + bean.cardNr, e);
			}finally
			{
				try {
					cn.setAutoCommit(false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

	public int deletePersonAndAssociatedCard(String personalNr) {
		if (checkPersonExist(personalNr)) {
			String dbCardNr = getCardNr(personalNr);
			if(isEmpty(dbCardNr))
			{
				deletePerson(personalNr);
				return 1;
			}
			
			if(checkCardExist(dbCardNr))
			{
				try{
					cn.setAutoCommit(false);
					String sql = "Delete  FROM bidcard WHERE cardNr='" + dbCardNr + "'";
					st.execute(sql);
					sql = "Delete  FROM bbadge WHERE badgeNr='" + dbCardNr + "'";
					st.execute(sql);
					sql = "Delete  from hPerson WHERE PersonalNr='" + personalNr + "'";
					st.execute(sql);
					
					cn.commit();
					cn.setAutoCommit(true);
					return 1;
				} catch (Exception e) {
					try {
						cn.rollback();
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
					e.printStackTrace();
					logger.error("Error delete Person = "+personalNr+" and associated card ", e);
				}finally
				{
					try {
						cn.setAutoCommit(false);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return 0;
	}

	// UPDATE
	public int blockCard(CsvBean bean) {
		if (isEmpty(bean.cardNr)) {
			return 0;
		}

		if (checkCardExist(bean.cardNr)) {
			try {
				String sql = "UPDATE bIDCard SET ObsolFlag='1' where CardNr='" + bean.cardNr + "'";
				st.execute(sql);
				return 1;
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("\nError while updating Date for Person: " + bean.personNr, e);
			}
		}

		return 0;
	}

	private String formatDateForDB(String strD) {
		Date dDate;
		try {
			dDate = new Date(new SimpleDateFormat("yyyyMMdd").parse(strD).getTime());
			return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(dDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	// INSERT
	public int insertBoth(CsvBean bean) {
		try {
			// If the card already exists in the system, meaning it is occupied by someone else, so do nothing 
			if (!checkCardExist(bean.cardNr)) {
				cn.setAutoCommit(false);
				
				// if this person exists in the system, just delete it and recreate again with new info
				if (checkPersonExist(bean.personNr))
					deletePerson(bean.personNr);
				
				String persKF = insertPersonalAndRef(bean);
				if (isEmpty(persKF))
					return 0;
				
				String sql;
				//insert card
				sql = "insert into bidcard(cardnr,clientfk,cardtypefk,persfk,role,roleauth,ObsolFlag) values ('" + bean.cardNr + "','1','1','" + persKF
						+ "','2','2','0')";
				st.execute(sql);
				
				//insert bagde
				sql = "insert into bbadge(badgenr,cardfk,lexicafk) values ('" + bean.cardNr + "','" + getCardFK(bean.cardNr) + "','1')";
				st.execute(sql);
				
				cn.commit();
				cn.setAutoCommit(true);
				
				return 1;
			}
			
		} catch (Exception e) {
			try {
				cn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			logger.error("Error while inserting person and card into the DB: Person" + bean.personNr + " idCard=" + bean.cardNr, e);
		}finally
		{
			try {
				cn.setAutoCommit(true);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	private String insertPersonalAndRef(CsvBean bean) throws SQLException {
		if(isEmpty(bean.dateEnd))
			bean.dateEnd="21001231";
		
		String dEnd = formatDateForDB(bean.dateEnd);
		String dStart = formatDateForDB(bean.dateStart);
		if (dStart == null)
			return null;
		
		String authGrpAc = getAuthGrpAc();
			
		//insert person
		String sql = "INSERT INTO hPerson (ClientFK,LanguageFK,PersonalNr,LastName,FirstName,FullName,CurrentCardNr,DateLeftAC,DateEntCompany, AuthGrpAc) "
				+ " VALUES ('1','GER'," + "'" + bean.personNr + "','" + bean.lastName + "','" + bean.firstName + "','" + bean.lastName + "," + bean.firstName + "','"
				+ bean.cardNr + "','" + dEnd + "','" + dStart + "','"+ authGrpAc + "')";
		
		st.execute(sql);
		// insert hpersonClient
		String persKF = getPersKF(bean.personNr);
		
		sql = "INSERT INTO hPersonClient (PersFK,ClientFK,Text1) "
				+ " VALUES ('"+persKF+"','1','5888')";
		st.execute(sql);

		// Insert into table aPersonProfile
		sql = "INSERT INTO aPersonProfile (PersFK,ClientFK,AccessDomainFK,ProfileFK, DateFrom) "
				+ " VALUES ('"+persKF+"',1,1,2,'" + dStart + "')";
		st.execute(sql);
		return persKF;
	}

	public int assignExistingCardToNewPerson(CsvBean bean) {
		if (isEmpty(bean.dateStart) || isEmpty(bean.cardNr))
			return 0;
		if (isEmpty(bean.dateEnd))
			bean.dateEnd = "21001231";

		try {
			cn.setAutoCommit(false);
			String sql;
			
			if (checkPersonExist(bean.personNr)) {
				sql = "Delete  from hPerson WHERE PersonalNr='" + bean.personNr + "'";
				st.execute(sql);
			}
			String persKF = insertPersonalAndRef(bean);
			if (isEmpty(persKF)) {
				return 0;
			}

			// insert card
			sql = "update bidcard set persfk = '" + persKF + "', ObsolFlag='1' where cardNr='" + bean.cardNr + "'";
			st.execute(sql);

			cn.commit();
			cn.setAutoCommit(true);

			return 1;
		} catch (Exception e) {
			try {
				cn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			logger.error("Error while updating the card " + bean.cardNr, e);
		} finally {
			try {
				cn.setAutoCommit(true);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}
		
	// GET
	private String getPersKF(String personalNr) {
		try {
			String sql = "select persid from hperson where PersonalNr='" + personalNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				return rs.getString("PersID");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Error while performing database action for idPerson=" + personalNr, e);
		}
		return "";
	}

	private String getAuthGrpAc() {
		try {
			String sql = "select AuthGrpAC from [Exos9300].[dbo].[hPerson] where AuthGrpAC is not null group by AuthGrpAC";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				return rs.getString("AuthGrpAC");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Error while getting the AuthGrpAC from hPerson", e);
		}
		return "";
	}
	
	private String getCardFK(String cardNr) {
		try {
			String sql = "select cardid from bidcard where cardnr='" + cardNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				return rs.getString("CardID");
			}
		} catch (SQLException e) {
			logger.error("\nError idCard=" + cardNr, e);
		}
		return "";
	}

	public String getCardNr(String personalNr) {

		try {
			String sql = "select currentcardnr from hperson where personalnr= '" + personalNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				return rs.getString("currentcardnr");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Error while getting Card info for Person " + personalNr, e);
		}
		return "";
	}

	public String getPersonNr(String cardNr) {
		if (isEmpty(cardNr)) {
			return "";
		}
		try {
			String sql = "select PersonalNr from hperson where currentcardnr= '" + cardNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				return rs.getString("PersonalNr");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Error while getting Person with the card " + cardNr, e);
		}
		return "";
	}

	public Date getEndDate(String personalNr) {
		try {
			String sql = "select dateleftac from hperson where personalnr= '" + personalNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				String tmp = rs.getString("dateleftac");
				if (tmp == null)
					return null;
				Date convDate = null;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				try {
					convDate = new Date(sdf.parse(tmp).getTime());
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return convDate;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Error while retrieving EndDate of Person: " + personalNr, e);
		}
		return null;
	}

	// CHECK
	
	public boolean checkPersonExist(String personNr) {
		try {
			String sql = "select PersonalNr from hperson where PersonalNr= '" + personNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
		}
		return false;
	}
	public boolean checkCardExist(String cardNr) {
		try {
			String sql = "select PersFK from bidcard where cardnr='" + cardNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
		}
		return false;
	}	

	public boolean checkIfCardExistAndPreactivated(String cardNr) {
		try {
			String sql = "select PersFK from bidcard where cardnr='" + cardNr + "'";
			ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				String persFk = rs.getString("PersFK");
				if (isEmpty(persFk)){
					return true; 
				}
				return false;
			}
		} catch (SQLException e) {
		}
		return false;
	}	

	/*
	 * Check if the given bean exists in the DB, meaning cardNr and personalNr must exist both on the same record 
	 */
	public boolean checkBeanExistInDB(CsvBean bean) {
		try {
			if (!isEmpty(bean.cardNr) && !isEmpty(bean.personNr)) {  
				String sql = "select PersonalNr from hperson where PersonalNr= '" + bean.personNr + "' and CurrentCardNr='" + bean.cardNr  + "'" ;
				ResultSet rs = st.executeQuery(sql);
				if (rs.next()) {
					return true;
				}
			}
		} catch (SQLException e) {
		}
		return false;
	}	

	/*
	 * Check if the given bean exists in the DB, meaning cardNr and personalNr must exist both on the same record 
	 */
	public boolean checkBeanExistInDB(String personNr, String cardNr) {
		try {
			if (!isEmpty(cardNr) && !isEmpty(personNr)) {  
				String sql = "select PersonalNr from hperson where PersonalNr= '" + personNr + "' and CurrentCardNr='" + cardNr  + "'" ;
				ResultSet rs = st.executeQuery(sql);
				if (rs.next()) {
					return true;
				}
			}
		} catch (SQLException e) {
		}
		return false;
	}	
}
