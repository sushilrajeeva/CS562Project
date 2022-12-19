package project;

/**
 * Sushil Rajeeva Bhandary - 20015528
 * Narmit Mashruwala - 20011284
* */

import java.io.PrintWriter;
import java.sql.*;
import java.util.HashMap;

/**
 *
 * This class helps in connecting our application to the Database
 * gets schema of the Database in use
 * outputs the column names and their JAVA equivalent datatype
 */
public class Schema {
    //Saving Authentication info to connect to DB
    private static final String username = "postgres";
    private static final String password = "CS562";
    private static final String url = "jdbc:postgresql://localhost:5432/salesdb";

    //To store the column data along with its datatype in a hashMap
    private static HashMap<String, String> dataType = new HashMap<String, String>();


    //Gets the schema of the Database
    public static HashMap<String, String> getSchema() {
        //Error handling using try-catch
        try {
            Connection connection = DriverManager.getConnection(url, username, password);
            System.out.println("Successfully Established Connection to the Database!");

            ResultSet resultSet;
            boolean isNext;
            Statement statement = connection.createStatement();

            //SQL Query to get schema form the sales table
            String query = "select data_type, column_name from information_schema.columns where table_name= 'sales'"; //SQL query as a string
            resultSet = statement.executeQuery(query); //Passing the SQL query as string to the executeQuery method of statement that executes the query
            isNext = resultSet.next(); //returns boolean - moves the cursor to the next row and returns true if there are rows else false
            while (isNext) {
                if (resultSet.getString("data_type").contains("character")) {
                    //If the resulting datatype contains "charecter" then add the column name with type string in dataType hashmap
                    dataType.put(resultSet.getString("column_name"), "String");
                } else {
                    //If the resulting datatype contains "charecter" then add the column name with type string in dataType hashmap
                    dataType.put(resultSet.getString("column_name"), "int");

                }
                isNext = resultSet.next(); //goes to next row
            }

        } catch (SQLException sqlException) {
            //handling Exceptions
            System.out.println("Oops! Something went wrong, Couldn't establish connection!!");
            sqlException.printStackTrace();
        }
        //returns a hashmap containing the column names of sales table along with its respective data_type
        return dataType;

    }

}

