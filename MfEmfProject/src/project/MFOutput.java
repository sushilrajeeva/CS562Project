package project;
/**
 * Sushil Rajeeva Bhandary - 20015528
 * Narmit Mashruwala - 20011284
 * */
import java.sql.*;
import java.util.*;
public class MFOutput {
	//Variables to connect to DB
	private static final String username = "postgres";
	private static final String password = "CS562";
	private static final String url = "jdbc:postgresql://localhost:5432/salesdb";
	//Variables to generate the output
	List<MF_Structure> mfStructureList = new ArrayList<MF_Structure>();
	List<Result> outputAttributeList = new ArrayList<Result>();

	 /** 
	 * This class contains the DB schema 
	 */ 
	public class DB_Structure{
		String prod;
		int month;
		int year;
		String state;
		int quant;
		int day;
		String cust;
	}

	 /** 
	 *  Selection attributes 
	 */ 

	 /** 
	 *  This result set class stores only the attributes in selection attribute 
	 */ 
	public class Result{
		String prod;
		String cust;
		int sum_quant_1;
		int sum_quant_2;
		int sum_quant_3;
		int count_quant_3;
		int avg_quant_1;
		int avg_quant_2;
		int avg_quant_3;
	}

	 /** 
	 * f-vect attributes 
	 * and group by attribues 
	 */ 

	 /** 
	 * Contains all the required attributes that needs to be computed 
	 */ 
	public class MF_Structure{
		String prod;
		String cust;
		int sum_quant_1;
		int count_quant_1;
		int avg_quant_1;
		int sum_quant_2;
		int count_quant_2;
		int avg_quant_2;
		int sum_quant_3;
		int count_quant_3;
		int avg_quant_3;
	}

	 /** 
	 * The Main method 
	 */ 
	public static void main(String [] args){

		MFOutput mfOutput = new MFOutput();
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		} catch(Exception exception) {
		exception.printStackTrace();
		}
		long start = System.currentTimeMillis();
		mfOutput.retrive();

		mfOutput.addToOutput();

		mfOutput.outputTable();
		long end = System.currentTimeMillis();
		long time = end-start;
		System.out.println();
		System.out.println("Time taken in milliseconds : " + time);
	}

	 /** 
	 * Logic to establish connection to Data Base 
	 * executing Single scan (SELECT * FROM SALES) and retriving  
	 * Storing in Data Set if it satisfies the condition in MF Query 
	 
	 */ 
	public void retrive(){
		try {
			Connection connection = DriverManager.getConnection(url, username, password);
			ResultSet result_set;
			boolean isNext;
			Statement statement = connection.createStatement();
			String query = "select * from sales";


			 /** 
			 * Generating while loops for each grouping variable. 
			 */ 

			//While loop for grouping variable 1.
			result_set = statement.executeQuery(query);
			isNext = result_set.next();
			while(isNext){
				DB_Structure currentRow = new DB_Structure();
				currentRow.prod = result_set.getString("prod");
				currentRow.month = result_set.getInt("month");
				currentRow.year = result_set.getInt("year");
				currentRow.state = result_set.getString("state");
				currentRow.quant = result_set.getInt("quant");
				currentRow.day = result_set.getInt("day");
				currentRow.cust = result_set.getString("cust");
				if(true){
					if (currentRow.state.equals("NY")){
						boolean found = false;
						for(MF_Structure row: mfStructureList){
							if(compare(row.prod,currentRow.prod) && compare(row.cust,currentRow.cust)){
								found = true;
								row.sum_quant_1 += currentRow.quant;
								row.count_quant_1 ++;
								if(row.count_quant_1 !=0){
									row.avg_quant_1 = row.sum_quant_1/row.count_quant_1;
								}
							}
						}
						if(found == false){
							MF_Structure addCurrentRow = new MF_Structure();
							addCurrentRow.prod = currentRow.prod;
							addCurrentRow.cust = currentRow.cust;
							addCurrentRow.sum_quant_1 = currentRow.quant;
							addCurrentRow.count_quant_1++;
							if(addCurrentRow.count_quant_1 !=0){
								addCurrentRow.avg_quant_1 = addCurrentRow.sum_quant_1/addCurrentRow.count_quant_1;
							}
							mfStructureList.add(addCurrentRow);
						}
					}
				}
				isNext = result_set.next();
			}

			//While loop for grouping variable 2.
			result_set = statement.executeQuery(query);
			isNext = result_set.next();
			while(isNext){
				DB_Structure currentRow = new DB_Structure();
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
						for(MF_Structure row: mfStructureList){
							if(compare(row.prod,currentRow.prod) && compare(row.cust,currentRow.cust)){
								found = true;
								row.sum_quant_2 += currentRow.quant;
								row.count_quant_2 ++;
								if(row.count_quant_2 !=0){
									row.avg_quant_2 = row.sum_quant_2/row.count_quant_2;
								}
							}
						}
						if(found == false){
							MF_Structure addCurrentRow = new MF_Structure();
							addCurrentRow.prod = currentRow.prod;
							addCurrentRow.cust = currentRow.cust;
							addCurrentRow.sum_quant_2 = currentRow.quant;
							addCurrentRow.count_quant_2++;
							if(addCurrentRow.count_quant_2 !=0){
								addCurrentRow.avg_quant_2 = addCurrentRow.sum_quant_2/addCurrentRow.count_quant_2;
							}
							mfStructureList.add(addCurrentRow);
						}
					}
				}
				isNext = result_set.next();
			}

			//While loop for grouping variable 3.
			result_set = statement.executeQuery(query);
			isNext = result_set.next();
			while(isNext){
				DB_Structure currentRow = new DB_Structure();
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
						for(MF_Structure row: mfStructureList){
							if(compare(row.prod,currentRow.prod) && compare(row.cust,currentRow.cust)){
								found = true;
								row.sum_quant_3 += currentRow.quant;
								row.count_quant_3 ++;
								if(row.count_quant_3 !=0){
									row.avg_quant_3 = row.sum_quant_3/row.count_quant_3;
								}
							}
						}
						if(found == false){
							MF_Structure addCurrentRow = new MF_Structure();
							addCurrentRow.prod = currentRow.prod;
							addCurrentRow.cust = currentRow.cust;
							addCurrentRow.sum_quant_3 = currentRow.quant;
							addCurrentRow.count_quant_3++;
							if(addCurrentRow.count_quant_3 !=0){
								addCurrentRow.avg_quant_3 = addCurrentRow.sum_quant_3/addCurrentRow.count_quant_3;
							}
							mfStructureList.add(addCurrentRow);
						}
					}
				}
				isNext = result_set.next();
			}
		}catch(Exception exception) {
			exception.printStackTrace();
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
		for(MF_Structure mfStructure: mfStructureList){
			Result result = new Result();
				result.prod = mfStructure.prod;
				result.cust = mfStructure.cust;
			if((mfStructure.avg_quant_1 < 2 * mfStructure.avg_quant_2) && (mfStructure.avg_quant_1 < 2 * mfStructure.avg_quant_2) && (mfStructure.avg_quant_1 < mfStructure.avg_quant_3)){
				result.sum_quant_1 = mfStructure.sum_quant_1;
				result.sum_quant_2 = mfStructure.sum_quant_2;
				result.sum_quant_3 = mfStructure.sum_quant_3;
				result.count_quant_3 = mfStructure.count_quant_3;
				result.avg_quant_1 = mfStructure.avg_quant_1;
				result.avg_quant_2 = mfStructure.avg_quant_2;
				result.avg_quant_3 = mfStructure.avg_quant_3;
				outputAttributeList.add(result);
			}
		}
	}

	 /** 
	 * This method will create format for outputting the data table. 
	 */ 
	public void outputTable(){
		System.out.printf("%-4s","prod\t");
		System.out.printf("%-4s","cust\t");
		System.out.printf("%-11s","sum_quant_1\t");
		System.out.printf("%-11s","sum_quant_2\t");
		System.out.printf("%-11s","sum_quant_3\t");
		System.out.printf("%-13s","count_quant_3\t");
		System.out.printf("%-11s","avg_quant_1\t");
		System.out.printf("%-11s","avg_quant_2\t");
		System.out.printf("%-11s","avg_quant_3\t");
		System.out.printf("\n");
		System.out.printf("====\t====\t===========\t===========\t===========\t=============\t===========\t===========\t===========\t ");
		for(Result result: outputAttributeList){
			System.out.printf("\n");
			System.out.printf("%-4s\t", result.prod);
			System.out.printf("%-4s\t", result.cust);
			System.out.printf("%11s\t", result.sum_quant_1);
			System.out.printf("%11s\t", result.sum_quant_2);
			System.out.printf("%11s\t", result.sum_quant_3);
			System.out.printf("%13s\t", result.count_quant_3);
			System.out.printf("%11s\t", result.avg_quant_1);
			System.out.printf("%11s\t", result.avg_quant_2);
			System.out.printf("%11s\t", result.avg_quant_3);
		}
	}
}
