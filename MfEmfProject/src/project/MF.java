package project;
import java.sql.*;
import java.util.*;
public class MF {
	//Variables to connect to DB
	private static final String usr = "postgres";
	private static final String pwd = "CS562";
	private static final String url = "jdbc:postgresql://localhost:5432/salesdb";
	//Variables to generate the output
	List<Result> output_attributes = new ArrayList<Result>();
	List<MF_Structure> mfStruct = new ArrayList<MF_Structure>();

	 /** 
	 * This class contains the DB schema 
	 */ 
	public class DBStruct{
		String prod;
		int month;
		int year;
		String state;
		int quant;
		int day;
		String cust;
	}

	 /** 
	 *  Selection attributes hi 
	 */ 

	 /** 
	 *  This result set class stores only the attributes in selection attribute 
	 */ 
	public class Result{
		String cust;
		String prod;
		int sum_quant_1;
		int sum_quant_2;
		int sum_quant_3;
		int count_month_1;
		int count_month_2;
		int count_month_3;
	}

	 /** 
	 * f-vect attributes 
	 * and group by attribues 
	 */ 

	 /** 
	 * Contains all the required attributes that needs to be computed 
	 */ 
	public class MF_Structure{
		String cust;
		String prod;
		int sum_quant_1;
		int sum_quant_2;
		int sum_quant_3;
		int count_month_1;
		int count_month_2;
		int count_month_3;
	}

	 /** 
	 * The Main method 
	 */ 
	public static void main(String [] args){

		MF mf = new MF();
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		} catch(Exception exception) {
		exception.printStackTrace();
		}
		mf.retrive();

		mf.addToOutput();

		mf.outputTable();
	}

	 /** 
	 * Logic to establish connection to Data Base 
	 * executing Single scan (SELECT * FROM SALES) and retriving  
	 * Storing in Data Set if it satisfies the condition in MF Query 
	 
	 */ 
	public void retrive(){
		try {
			Connection con = DriverManager.getConnection(url, usr, pwd);
			ResultSet result_set;
			boolean more;
			Statement st = con.createStatement();
			String query = "select * from sales";


			 /** 
			 * Generating while loops for each grouping variable. 
			 */ 

			//While loop for grouping variable 1.
			result_set = st.executeQuery(query);
			more = result_set.next();
			while(more){
				DBStruct currentRow = new DBStruct();
				currentRow.prod = result_set.getString("prod");
				currentRow.month = result_set.getInt("month");
				currentRow.year = result_set.getInt("year");
				currentRow.state = result_set.getString("state");
				currentRow.quant = result_set.getInt("quant");
				currentRow.day = result_set.getInt("day");
				currentRow.cust = result_set.getString("cust");
				if(true){
					if (currentRow.state.equals("PA")){
						boolean found = false;
						for(MF_Structure row: mfStruct){
							if(compare(row.cust,currentRow.cust) && compare(row.prod,currentRow.prod)){
								found = true;
								row.sum_quant_1 += currentRow.quant;
								row.count_month_1++;
							}
						}
						if(found == false){
							MF_Structure addCurrentRow = new MF_Structure();
							addCurrentRow.cust = currentRow.cust;
							addCurrentRow.prod = currentRow.prod;
							addCurrentRow.sum_quant_1 = currentRow.quant;
							addCurrentRow.count_month_1++;
							mfStruct.add(addCurrentRow);
						}
					}
				}
				more = result_set.next();
			}

			//While loop for grouping variable 2.
			result_set = st.executeQuery(query);
			more = result_set.next();
			while(more){
				DBStruct currentRow = new DBStruct();
				currentRow.prod = result_set.getString("prod");
				currentRow.month = result_set.getInt("month");
				currentRow.year = result_set.getInt("year");
				currentRow.state = result_set.getString("state");
				currentRow.quant = result_set.getInt("quant");
				currentRow.day = result_set.getInt("day");
				currentRow.cust = result_set.getString("cust");
				if(true){
					if (currentRow.state.equals("NJ")){
						boolean found = false;
						for(MF_Structure row: mfStruct){
							if(compare(row.cust,currentRow.cust) && compare(row.prod,currentRow.prod)){
								found = true;
								row.sum_quant_2 += currentRow.quant;
								row.count_month_2++;
							}
						}
						if(found == false){
							MF_Structure addCurrentRow = new MF_Structure();
							addCurrentRow.cust = currentRow.cust;
							addCurrentRow.prod = currentRow.prod;
							addCurrentRow.sum_quant_2 = currentRow.quant;
							addCurrentRow.count_month_2++;
							mfStruct.add(addCurrentRow);
						}
					}
				}
				more = result_set.next();
			}

			//While loop for grouping variable 3.
			result_set = st.executeQuery(query);
			more = result_set.next();
			while(more){
				DBStruct currentRow = new DBStruct();
				currentRow.prod = result_set.getString("prod");
				currentRow.month = result_set.getInt("month");
				currentRow.year = result_set.getInt("year");
				currentRow.state = result_set.getString("state");
				currentRow.quant = result_set.getInt("quant");
				currentRow.day = result_set.getInt("day");
				currentRow.cust = result_set.getString("cust");
				if(true){
					if (currentRow.state.equals("CT")){
						boolean found = false;
						for(MF_Structure row: mfStruct){
							if(compare(row.cust,currentRow.cust) && compare(row.prod,currentRow.prod)){
								found = true;
								row.sum_quant_3 += currentRow.quant;
								row.count_month_3++;
							}
						}
						if(found == false){
							MF_Structure addCurrentRow = new MF_Structure();
							addCurrentRow.cust = currentRow.cust;
							addCurrentRow.prod = currentRow.prod;
							addCurrentRow.sum_quant_3 = currentRow.quant;
							addCurrentRow.count_month_3++;
							mfStruct.add(addCurrentRow);
						}
					}
				}
				more = result_set.next();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	 /** 
	 * These are comapare methods to compare two string values orinteger values. 
	 * @return boolean true if same or else false. 
	 */ 
	boolean compare(String str1, String str2){
		return str1.equals(str2);
	}
	boolean compare(int num1, int num2){
		return (num1 == num2);
	}

	 /** 
	 * filtering output data if having conditions exists. 
	 */ 
	public void addToOutput(){
		for(MF_Structure ms: mfStruct){
			Result result = new Result();
				result.cust = ms.cust;
				result.prod = ms.prod;
			if(true){
				result.sum_quant_1 = ms.sum_quant_1;
				result.sum_quant_2 = ms.sum_quant_2;
				result.sum_quant_3 = ms.sum_quant_3;
				result.count_month_1 = ms.count_month_1;
				result.count_month_2 = ms.count_month_2;
				result.count_month_3 = ms.count_month_3;
				output_attributes.add(result);
			}
		}
	}

	 /** 
	 * This method will create format for outputting the data table. 
	 */ 
	public void outputTable(){
		System.out.printf("%-4s","cust\t");
		System.out.printf("%-4s","prod\t");
		System.out.printf("%-11s","sum_quant_1\t");
		System.out.printf("%-11s","sum_quant_2\t");
		System.out.printf("%-11s","sum_quant_3\t");
		System.out.printf("%-13s","count_month_1\t");
		System.out.printf("%-13s","count_month_2\t");
		System.out.printf("%-13s","count_month_3\t");
		System.out.printf("\n");
		System.out.printf("====\t====\t===========\t===========\t===========\t=============\t=============\t=============\t ");
		for(Result ra: output_attributes){
			System.out.printf("\n");
			System.out.printf("%-4s\t", ra.cust);
			System.out.printf("%-4s\t", ra.prod);
			System.out.printf("%11s\t", ra.sum_quant_1);
			System.out.printf("%11s\t", ra.sum_quant_2);
			System.out.printf("%11s\t", ra.sum_quant_3);
			System.out.printf("%13s\t", ra.count_month_1);
			System.out.printf("%13s\t", ra.count_month_2);
			System.out.printf("%13s\t", ra.count_month_3);
		}
	}
}
