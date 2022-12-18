package project;


import java.io.PrintWriter;
import java.sql.*;
import java.util.HashMap;

/**
 * This class helps in connecting our application to the Database
 * gets schema of the Database in use
 * outputs the column names and their JAVA equivalent datatype
 */
public class Schema {
    //Authentication info
    private static final String username = "postgres";
    private static final String password = "CS562";
    private static final String url = "jdbc:postgresql://localhost:5432/salesdb";

    //To store the column data along with its datatype in a hashMap
    private static HashMap<String, String> dataType = new HashMap<String, String>();


    //Gets the schema of the Database
    public static HashMap<String, String> getSchema() {
        //Error handling using try-catch
        try {
            Connection con = DriverManager.getConnection(url, username, password);
            System.out.println("Connection successful");

            ResultSet resultSet;
            boolean more;
            Statement st = con.createStatement();

            //SQL Query to get schema
            String query = "select data_type, column_name from information_schema.columns where table_name= 'sales'";
            resultSet = st.executeQuery(query);
            more = resultSet.next();
            while (more) {
                if (resultSet.getString("data_type").contains("character")) {
                    dataType.put(resultSet.getString("column_name"), "String");
                } else {
                    dataType.put(resultSet.getString("column_name"), "int");

                }
                more = resultSet.next();
            }

        } catch (SQLException e) {
            System.out.println("Connection error");
            e.printStackTrace();
        }
        return dataType;

    }

}

