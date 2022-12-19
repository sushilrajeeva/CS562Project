package project;

/**
 * Sushil Rajeeva Bhandary - 20015528
 * Narmit Mashruwala - 20011284
 * */

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class MainProject {

    // Declaring the DataStructures for phi operator
    //Narmit - Creating Variables to hold data of input file

    //Stores the Select attributes
    private static List<String> select = new ArrayList<String>();

    //stores number of grouping variables
    private static int number;

    //Stores group by clause attributes
    private static List<String> groupby = new ArrayList<String>();;

    //Stores FVect variables - i.e group variables in list
    private static List<GroupVariable> fvect_variable = new ArrayList<GroupVariable>();;

    //Stores conditions in such that clause in this list
    private static List<SuchThat> suchthat = new ArrayList<SuchThat>();;

    //Stores having clause conditions in this list
    private static List<String> having = new ArrayList<String>();

    //Stores where clause conditions in this list
    private static List<String> where_condition = new ArrayList<String>();

    //Stores the schema output of Sales Table that contains hashmap the column names along with its respective data_type
    public static HashMap<String, String> dataType = new HashMap<String, String>();

    // Testing if we can connect to PostgreSQL
    private void connect() { //Narmit - to establish connection
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Driver connected successfully!");

        } catch (Exception exception) {
            System.out.println("Oops!! Driver loading has failed!");
            exception.printStackTrace();
        }
    }

    /**
     * adding arguments of phi operator to the data structures
     * @param input
     */
    public void addPhiArguments(File input) { // Narmit - handles logic of taking input file data and storing it in our userdefined java variables

        //adding try catch for handling errors
        try {

            //reads the input file and stores it in sc
            Scanner scanner = new Scanner(input);
            String inputLine;

            String[] select_attributes = null;
            String[] grouping_atributes = null;
            String[] fvect = null;
            String[] select_condition = null;
            String[] where = null;
            String[] having_condition = null;
            int noGV = 0; // this is number of grouping variables

            /*
                Logic to scan each inputLine and smartly transform it to required form
                and populate it into its respective data holders / variables.
             */

            while (scanner.hasNextLine()) {
                inputLine = scanner.nextLine();
                if (inputLine.contains("select_attribute")) {
                    inputLine = inputLine.replaceAll(".+:", "");
                    select_attributes = inputLine.split(", ");
                } else if (inputLine.contains("no_gv")) {
                    inputLine = inputLine.replaceAll(".+:", "");
                    noGV = Integer.parseInt(inputLine);
                } else if (inputLine.contains("grouping_attributes")) {
                    inputLine = inputLine.replaceAll(".+:", "");
                    grouping_atributes = inputLine.split(", ");
                } else if (inputLine.contains("where")) {
                    // If there is no where condition set it to null.
                    inputLine = inputLine.replaceAll(".+:", "");
                    if (inputLine.equals("")) {
                        where = null;
                    } else {
                        where = inputLine.split(", ");
                    }

                } else if (inputLine.contains("fvect")) {
                    inputLine = inputLine.replaceAll(".+:", "");
                    fvect = inputLine.split(", ");
                } else if (inputLine.contains("select")) {
                    inputLine = inputLine.replaceAll(".+:", "");
                    select_condition = inputLine.split(", ");
                } else if (inputLine.contains("having_condition")) {
                    // If there is no having condition set it to null.
                    inputLine = inputLine.replaceAll(".+:", "");
                    if (inputLine.equals("")) {
                        having_condition = null;
                    } else {
                        having_condition = inputLine.split(", ");
                    }

                } else {
                    continue;
                }
            }

            getArguments(select_attributes, grouping_atributes, fvect, select_condition, noGV, where, having_condition);

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Taking all operators and parse them to a list whenever required
     *
     * @param select_attributes
     * @param grouping_atributes
     * @param fvect
     * @param select_condition
     * @param noGV
     * @param where
     * @param having_condition
     */

    //Sushil - logic for storing input file data in user defined java variables
    private static void getArguments(String[] select_attributes, String[] grouping_atributes, String[] fvect,
                                     String[] select_condition, int noGV, String[] where, String[] having_condition) {

        /*  Converts select attributes of type string into a grouping variable object
            and stores the transformed attribute i.e getString() stringin selectList
         */
        for (String str : select_attributes) {
            if (str.contains("_")) {
                String[] value = str.split("_");
                GroupVariable gv = new GroupVariable(value[0], value[1], value[2]);
                select.add(gv.getString());

            } else {
                select.add(str);
            }
        }

        number = noGV;
        for (String str : grouping_atributes) {
            groupby.add(str);
        }

        if (where != null) {
            for (String str : where) {
                where_condition.add(str);
            }
        }

        for (String str : fvect) {
            String[] value = str.split("_");
            GroupVariable gv = new GroupVariable(value[0], value[1], value[2]);
            fvect_variable.add(gv);
        }

        for (String str : select_condition) {
            String[] value = str.split("_");
            SuchThat pair = new SuchThat(Integer.parseInt(value[0]), value[1]);
            suchthat.add(pair);
        }

        if (having_condition != null) {
            for (String str : having_condition) {
                having.add(str);
            }
        }

    }

    // Sushil
    public List<String> getSelect() {
        return select;
    }

    public int getNumber() {
        return number;
    }

    public List<String> getGroupby() {
        return groupby;
    }

    public List<GroupVariable> getFvect() {
        return fvect_variable;
    }

    public List<SuchThat> getSuchthat() {
        return suchthat;
    }

    public List<String> getHaving() {
        return having;
    }

    public List<String> getWhere() {
        return where_condition;
    }

    public int getSizeWhere() {
        return where_condition.size();
    }

    public int getSizeHaving() {
        return having.size();
    }

    //Sushil - scan the file and print the java data variables
    public static void main(String args[]) {
        File input;
        MainProject mainProject = new MainProject();
        mainProject.connect();

        dataType = Schema.getSchema();

        //Displaying the schema of the database

        System.out.println();
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("The datatype of the given sales table: ");
        System.out.println(dataType);
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println();

        //Asking User Input for selecting the type of Input File - i.e MF/EMF
        System.out.println("Please enter MF of EMF depending on what type of query you want to run:");
        Scanner in = new Scanner(System.in);

        String query = in.nextLine();

        //Replacing spaces with no spaces
        query = query.replace(" ", "");
        //Converting all the upper case so that we can make the user input case insensitive
        query = query.toUpperCase();

        try {
            if (query.equals("MF")) {
                System.out.println("You have selected MF Compiler!");

                System.out.println("Please Select the file number associated with your MFQuery input !");
                Integer inputQueryNum = in.nextInt();
                String Str = "Inputs/MFQuery"+inputQueryNum+".txt";
                input = new File(Str);
                //calls addPhiArguments to take care the logic of populating input file variable values to its respective java variables
                mainProject.addPhiArguments(input);

                //Let's now check the result of calling the above function - the input variable values are stored in respective java variables
                System.out.println("Select attributes (S):");
                System.out.println(mainProject.getSelect());

                System.out.println("Number of GV (n):");
                System.out.println(mainProject.getNumber());

                System.out.println("GroupBy (V):");
                System.out.println(mainProject.getGroupby());

                System.out.println("Fvect attributes (F):");
                System.out.println(mainProject.getFvect());

                System.out.println("SuchThat (phi):");
                System.out.println(mainProject.getSuchthat());

                System.out.println("getHaving clause:");
                System.out.println(mainProject.getHaving());

                System.out.println("getWhere clause:");
                System.out.println(mainProject.getWhere());

                System.out.println("where clause list size:");
                System.out.println(mainProject.getSizeWhere());

                System.out.println("having clause list size:");
                System.out.println(mainProject.getSizeHaving());

                //passing datatype containing sales table schema to the EMFCodeGenerator file that then goes on to generate a dynamic java file to execute our EMF Query
                MFCodeGenerator.MFCode(dataType);

                System.out.println("MFOutput Generated Successfully Successful!!");
                System.out.println("\n\n");
                System.out.println();
            } else if (query.equals("EMF")) {
                System.out.println("You have selected EMF Compiler!");
                input = new File("Inputs/EMFQuery1.txt");
                //calls addPhiArguments to take care the logic of populating input file variable values to its respective java variables
                mainProject.addPhiArguments(input);

                //Let's now check the result of calling the above function - the input variable values are stored in respective java variables
                System.out.println("Select");
                System.out.println(mainProject.getSelect());
                System.out.println("Number");
                System.out.println(mainProject.getNumber());
                System.out.println("GroupBy");
                System.out.println(mainProject.getGroupby());
                System.out.println("Fvect");

                System.out.print("[");
                for (GroupVariable groupVariable : mainProject.getFvect()) {
                    System.out.print(groupVariable.getString() + " ");
                }
                System.out.print("]\n");
                System.out.println("SuchThat");

                for (SuchThat ak : mainProject.getSuchthat()) {
                    System.out.print(ak.getIndex()+ "_" +ak.getAttribute());
                }
                System.out.println("\n");

                System.out.println("getHaving : ");
                System.out.println(mainProject.getHaving());

                System.out.println("getWhere : ");
                System.out.println(mainProject.getWhere());

                System.out.println("where list size");
                System.out.println(mainProject.getSizeWhere());

                System.out.println("Having list size");
                System.out.println(mainProject.getSizeHaving());

                System.out.println("EMFOutput Generated Successfully!");
                System.out.println("\n\n");
                System.out.println();

                // START CODE
                // GroupVariable gv = new GroupVariable();
                //passing datatype containing sales table schema to the EMFCodeGenerator file that then goes on to generate a dynamic java file to execute our EMF Query
                EMFCodeGenerator.EMFCode(dataType);

                // END CODE

            } else {
                System.out.println("Please enter a valid option");
            }
        }catch (Exception exception){
            System.out.println("Oops!! something went wrong");
            System.out.println("Please check if you have selected MF/EMF and the correct file number");
            exception.printStackTrace();
        }
    }
}

/* Converts the Grouping Variables in form 1_abc_xyz to abc_xyz_1
   This is because our input file has it in this form but it is a bad java variable name so we are transforming it.
 */
//Narmit - Logic for renaming input variables and also extracting index, aggregate and attributes of each input variables from input file
class GroupVariable {

    String aggregate, attribute, index;

    GroupVariable(String index, String aggregate, String attribute) {
        this.index = index;
        this.aggregate = aggregate;
        this.attribute = attribute;
    }

    //returns the string in aggregate_attribute_index format - example sum_quant_1
    public String getString() {
        return aggregate + "_" + attribute + "_" + index;
    }
}

//This class creates a SuchThat object that tracks the index and attribute also provides setter and getter for the same
//Sushil - handling Such That elements
class SuchThat {

    public int index;
    public String attribute;

    public SuchThat(int index, String attribute) {
        this.index = index;
        this.attribute = attribute;
    }

    public int getIndex() {
        return index;
    } //returns index of SuchThat object

    public void setIndex(int index) {
        this.index = index;
    }

    //returns attribute of SuchThat object
    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }
}

