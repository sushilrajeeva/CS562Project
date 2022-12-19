package project;
/**
 * Sushil Rajeeva Bhandary - 20015528
 * Narmit Mashruwala - 20011284
 * */
import java.sql.*;
import java.util.*;
import java.io.*;

/**Auto-generated File! 
**/

class dbTuple{
	String	prod;
	int	month;
	int	year;
	String	state;
	int	quant;
	int	day;
	String	cust;
}

class MF_structure{
	String	prod;
	int	month;
	int	sum_quant_1;
	int	sum_quant_2;
	String formatName(String name){
		Integer maxLen = 20;
		Integer len = name.length();
		Integer difference = maxLen - len;
		for(int i=0; i<=difference; i++){
			 name+= ' ';
		}
		 return name + '|';
}
	String formatName(Integer number){
		String numStr = number.toString();
		Integer maxLen = 20;
		Integer len = numStr.length();
		Integer difference = maxLen - len - 2;
		for(int i=0; i<=difference; i++){
			 numStr = ' ' + numStr;
		}
		 return numStr + "  |";
}
	void output(){
		System.out.printf("\t"+formatName(prod));
		System.out.printf("\t"+formatName(month));
			System.out.printf("\t"+formatName(sum_quant_1));
			System.out.printf("\t"+formatName(sum_quant_2));
		System.out.printf("\n");
	}
}

class EMFOutput {
	String username ="postgres";
	String password ="CS562";
	String url = "jdbc:postgresql://localhost:5432/salesdb";
	ArrayList<MF_structure> result_list = new ArrayList<MF_structure>();
	int	sum_quant_1 = 0;
	int	sum_quant_2 = 0;

	public static void main(String[] args) {
		EMFOutput emf = new EMFOutput();
		emf.connect();
		long start = System.currentTimeMillis();
		emf.retrieve();
		emf.output();
		long end = System.currentTimeMillis();
		long time = end-start;
		System.out.println();
		System.out.println("Time taken in milliseconds : " + time);
	}
	public void connect(){
		try {
		Class.forName("org.postgresql.Driver");
		System.out.println("Success loading Driver!");
		} catch(Exception exception) {
		exception.printStackTrace();
		}
	}
	void retrieve(){
		try {
		Connection con = DriverManager.getConnection(url, username, password);
		System.out.println("Success connecting server!");
		ResultSet rs;
		boolean more;
		Statement st = con.createStatement();
		String ret = "select * from sales";
		rs = st.executeQuery(ret);
		more=rs.next();
		while(more){
			dbTuple nextrow = new dbTuple();
			nextrow.prod = rs.getString("prod");
			nextrow.month = rs.getInt("month");
			nextrow.year = rs.getInt("year");
			nextrow.state = rs.getString("state");
			nextrow.quant = rs.getInt("quant");
			nextrow.day = rs.getInt("day");
			nextrow.cust = rs.getString("cust");
			sum_quant_1 += nextrow.quant;
			sum_quant_2 += nextrow.quant;
			if(true){
				boolean found = false;
				for (MF_structure temp : result_list){
					 if(compare(temp.prod,nextrow.prod) && compare(temp.month,nextrow.month)){
						found=true;
						break;
					}
				}
				if (found == false){
					MF_structure newrow = new MF_structure();
					newrow.prod = nextrow.prod;
					newrow.month = nextrow.month;
					newrow.sum_quant_1 = 0;
					newrow.sum_quant_2 = 0;
					result_list.add(newrow);
				}
			}
			more=rs.next();
		}

		rs = st.executeQuery(ret);
		more=rs.next();
		while(more){
			dbTuple nextrow = new dbTuple();
			nextrow.prod = rs.getString("prod");
			nextrow.month = rs.getInt("month");
			nextrow.year = rs.getInt("year");
			nextrow.state = rs.getString("state");
			nextrow.quant = rs.getInt("quant");
			nextrow.day = rs.getInt("day");
			nextrow.cust = rs.getString("cust");
			if(true){
				for (MF_structure temp : result_list){
					if (nextrow.prod.equals(temp.prod)&&nextrow.month==temp.month){
						temp.sum_quant_1 += nextrow.quant;
					}
				}
			}
			more=rs.next();
		}

		rs = st.executeQuery(ret);
		more=rs.next();
		while(more){
			dbTuple nextrow = new dbTuple();
			nextrow.prod = rs.getString("prod");
			nextrow.month = rs.getInt("month");
			nextrow.year = rs.getInt("year");
			nextrow.state = rs.getString("state");
			nextrow.quant = rs.getInt("quant");
			nextrow.day = rs.getInt("day");
			nextrow.cust = rs.getString("cust");
			if(true){
				for (MF_structure temp : result_list){
					if (nextrow.prod.equals(temp.prod)&&nextrow.month==temp.month){
						temp.sum_quant_2 += nextrow.quant;
					}
				}
			}
			more=rs.next();
		}

		}catch(Exception e) {
			System.out.println("errors!");
			e.printStackTrace();
		}
	}
	void output(){
		for (MF_structure temp : result_list)
			temp.output();
	}
	boolean compare(String s1, String s2){
		return s1.equals(s2);
	}
	boolean compare(int i1, int i2){
		return (i1 == i2);
	}
}
		
		
		
		
		
