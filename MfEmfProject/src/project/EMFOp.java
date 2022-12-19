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
	String	cust;
	int	sum_quant_1;
	int	count_quant_1;
	int	sum_quant_2;
	int	count_quant_2;
	int	sum_quant_3;
	int	count_quant_3;
	int	sum_quant_4;
	int	count_quant_4;
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
		System.out.printf("\t"+formatName(cust));
		if (count_quant_1 == 0)
			System.out.printf("\t "+formatName(0));
		else
			System.out.printf("\t"+formatName(sum_quant_1/count_quant_1));
		if (count_quant_2 == 0)
			System.out.printf("\t "+formatName(0));
		else
			System.out.printf("\t"+formatName(sum_quant_2/count_quant_2));
		if (count_quant_3 == 0)
			System.out.printf("\t "+formatName(0));
		else
			System.out.printf("\t"+formatName(sum_quant_3/count_quant_3));
		if (count_quant_4 == 0)
			System.out.printf("\t "+formatName(0));
		else
			System.out.printf("\t"+formatName(sum_quant_4/count_quant_4));
		System.out.printf("\n");
	}
}

class EMFOutput {
	String username ="postgres";
	String password ="CS562";
	String url = "jdbc:postgresql://localhost:5432/salesdb";
	ArrayList<MF_structure> result_list = new ArrayList<MF_structure>();
	int	sum_quant_1 = 0;
	int	count_quant_1 = 0;
	int	sum_quant_2 = 0;
	int	count_quant_2 = 0;
	int	sum_quant_3 = 0;
	int	count_quant_3 = 0;
	int	sum_quant_4 = 0;
	int	count_quant_4 = 0;

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
			count_quant_1 ++;
			sum_quant_2 += nextrow.quant;
			count_quant_2 ++;
			sum_quant_3 += nextrow.quant;
			count_quant_3 ++;
			sum_quant_4 += nextrow.quant;
			count_quant_4 ++;
			if(nextrow.year==2016){
				boolean found = false;
				for (MF_structure temp : result_list){
					 if(compare(temp.cust,nextrow.cust)){
						found=true;
						break;
					}
				}
				if (found == false){
					MF_structure newrow = new MF_structure();
					newrow.cust = nextrow.cust;
					newrow.sum_quant_1 = 0;
					newrow.count_quant_1 = 0;
					newrow.sum_quant_2 = 0;
					newrow.count_quant_2 = 0;
					newrow.sum_quant_3 = 0;
					newrow.count_quant_3 = 0;
					newrow.sum_quant_4 = 0;
					newrow.count_quant_4 = 0;
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
			if(nextrow.year==2016){
				for (MF_structure temp : result_list){
					if (nextrow.cust.equals(temp.cust)&&nextrow.state.equals("NY")){
						temp.sum_quant_1 += nextrow.quant;
						temp.count_quant_1 ++;
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
			if(nextrow.year==2016){
				for (MF_structure temp : result_list){
					if (nextrow.cust.equals(temp.cust)&&nextrow.state.equals("NJ")){
						temp.sum_quant_2 += nextrow.quant;
						temp.count_quant_2 ++;
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
			if(nextrow.year==2016){
				for (MF_structure temp : result_list){
					if (nextrow.cust.equals(temp.cust)&&nextrow.state.equals("CT")){
						temp.sum_quant_3 += nextrow.quant;
						temp.count_quant_3 ++;
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
			if(nextrow.year==2016){
				for (MF_structure temp : result_list){
					if (nextrow.cust.equals(temp.cust)){
						temp.sum_quant_4 += nextrow.quant;
						temp.count_quant_4 ++;
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
		
		
		
		
		
