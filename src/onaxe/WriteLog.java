package onaxe;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class WriteLog {
	public static void doWrite(String input) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd.HH.mm.ss");
		Calendar cal = Calendar.getInstance();
		String filename = "\\" + dateFormat.format(cal.getTime()) + ".txt";

		BufferedWriter out = null;
		FileWriter fw=null;
		try {
			fw=new FileWriter(Exos9300ImportCVS.configData.logPath + filename);
			out = new BufferedWriter(fw);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			out.write(input);
		} catch (IOException e1) {
			System.out.println("\nError during reading/writing");
		} finally {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
}
