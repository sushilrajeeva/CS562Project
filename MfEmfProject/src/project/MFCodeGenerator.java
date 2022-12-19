package project;

/**
 * Sushil Rajeeva Bhandary - 20015528
 * Narmit Mashruwala - 20011284
 * */

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MFCode class to generate MFOutput.java file
 */
public class MFCodeGenerator {

    /**
     * First method of the code, where execution begins
     *
     * @param dataType
     */
    public static void MFCode(HashMap<String, String> dataType) {
        try {
            //Narmit
            //Writing logic to create a file
            File output = new File("src/project/MFOutput.java");
            System.out.println("New MFOutput Output file generated successfully!!");
            PrintWriter writer = new PrintWriter(output);


            //Narmit - Writing SQL Connection logic
            writer.print("package project;\n");
            writer.print("/**\n" +
                    " * Sushil Rajeeva Bhandary - 20015528\n" +
                    " * Narmit Mashruwala - 20011284\n" +
                    " * */\n");
            writer.print("import java.sql.*;\n");
            writer.print("import java.util.*;\n");
            //Narmit - creating main class
            writer.print("public class MFOutput {\n");
            writer.print("\t//Variables to connect to DB\n");
            // sushil
           writer.print("\tprivate static final String username = \"postgres\";\n"
                   + "	private static final String password = \"CS562\";\n"
                   + "	private static final String url = \"jdbc:postgresql://localhost:5432/salesdb\";\n");

            // narmit
            // writer.print("\tprivate static final String username = \"postgres\";\n"
            //         + "	private static final String password = \"admin\";\n"
            //         + "	private static final String url = \"jdbc:postgresql://localhost:5432/postgres\";\n");

            writer.print("\t//Variables to generate the output\n");
            writer.print("\tList<MF_Structure> mfStructureList = new ArrayList<MF_Structure>();\n");
            writer.print("\tList<Result> outputAttributeList = new ArrayList<Result>();\n");

            writer.print("\n");
            // Generate DataBase Structure class of sales table
            writer.print("\t /** \n\t * This class contains the DB schema \n\t */ \n");
            writer.print("\tpublic class DB_Structure{\n");
            for (Map.Entry<String, String> entry : dataType.entrySet())
                writer.print("\t\t" + entry.getValue() + " " + entry.getKey() + ";\n");

            writer.print("\t}\n");

            // Generating select attributes
            writer.print("\n\t /** \n\t *  Selection attributes \n\t */ \n");
            MainProject mainProject = new MainProject();

            // Sushil - Class Result
            writer.print("\n\t /** \n\t *  This result set class stores only the attributes in selection attribute \n\t */ \n");
            writer.print("\tpublic class Result{\n");
            for (String str : mainProject.getSelect()) {
                if (dataType.get(str) != null) {
                    writer.print("\t\t" + dataType.get(str) + " " + str + ";\n");
                } else {
                    writer.print("\t\tint " + str + ";\n");
                }
            }
            writer.print("\t}\n");

            //Sushil - generate f-vect and groupby attributes
            writer.print("\n\t /** \n\t * f-vect attributes \n\t * and group by attribues \n\t */ \n");
            List<String> added_elements = new ArrayList<String>();

            // Sushil - Class MF_Structure
            writer.print("\n\t /** \n\t * Contains all the required attributes that needs to be computed \n\t */ \n");
            writer.print("\tpublic class MF_Structure{\n");
            //writing logic for creating [datatype groupByVariableName] for each groupby in groupbyList
            for (String groupByItem : mainProject.getGroupby()) {
                for (Map.Entry<String, String> entry : dataType.entrySet())
                    if (groupByItem.equals(entry.getKey()))
                        writer.print("\t\t" + entry.getValue() + " " + entry.getKey() + ";\n");
            }
            //writing logic to handle avg - sum - count and creating variable names for the same
            for (GroupVariable groupVariable : mainProject.getFvect()) {
                if (groupVariable.aggregate.equals("avg")) {
                    String sum = "sum_" + groupVariable.attribute + "_" + groupVariable.index;
                    String count = "count_" + groupVariable.attribute + "_" + groupVariable.index;
                    if (!added_elements.contains(sum)) {
                        added_elements.add(sum);
                        writer.print("\t\tint" + " sum_" + groupVariable.attribute + "_" + groupVariable.index + ";\n");
                    }
                    if (!added_elements.contains(count)) {
                        added_elements.add(count);
                        writer.print("\t\tint" + " count_" + groupVariable.attribute + "_" + groupVariable.index + ";\n");
                    }
                    if (!added_elements.contains(groupVariable.getString())) {
                        writer.print("\t\tint " + groupVariable.getString() + ";\n");
                        added_elements.add(groupVariable.getString());
                    }

                }
                else {
                    if (!added_elements.contains(groupVariable.getString())) {
                        writer.print("\t\tint " + groupVariable.getString() + ";\n");
                        added_elements.add(groupVariable.getString());
                    }
                }
            }
            writer.print("\t}\n");

            // Narmit - Writing the main method of the output file.
            writer.print("\n\t /** \n\t * The Main method \n\t */ \n");
            writer.print("\tpublic static void main(String [] args){\n");

            //logic to check if the application is able to connect to db
            writer.print("\n\t\tMFOutput mfOutput = new MFOutput();\n");
            writer.print("\t\ttry {\n");
            writer.print("\t\t\tClass.forName(\"org.postgresql.Driver\");\n");
            writer.print("\t\t\tSystem.out.println(\"Success loading Driver!\");\n");
            writer.print("\t\t} catch(Exception exception) {\n");
            writer.print("\t\texception.printStackTrace();\n");
            writer.print("\t\t}\n");

            //writing the methods we want to run in sequence --- logic for these will be written below
            writer.print("\t\tlong start = System.currentTimeMillis();\n");//logic to calculate runtime of our algorithm
            writer.print("\t\tmfOutput.retrive();\n\n");
            writer.print("\t\tmfOutput.addToOutput();\n\n");
            writer.print("\t\tmfOutput.outputTable();\n");
            writer.print("\t\tlong end = System.currentTimeMillis();\n");
            writer.print("\t\tlong time = end-start;\n");
            writer.print("\t\tSystem.out.println();\n");
            writer.print("\t\tSystem.out.println(\"Time taken in milliseconds : \" + time);\n");
            writer.print("\t}\n");

            //Sushil - Execution of main logic of the code
            writer.print("\n\t /** \n\t * Logic to establish connection to Data Base \n\t * executing Single scan (SELECT * FROM SALES) and retriving  \n\t * Storing in Data Set if it satisfies the condition in MF Query \n\t \n\t */ \n");
            writer.print("\tpublic void retrive(){\n");
            writer.print("\t\ttry {\n");
            writer.print("\t\t\tConnection connection = DriverManager.getConnection(url, username, password);\n");
            // Declaring variables
            writer.print("\t\t\tResultSet result_set;\n");
            writer.print("\t\t\tboolean isNext;\n");
            writer.print("\t\t\tStatement statement = connection.createStatement();\n");
            writer.print("\t\t\tString query = \"select * from sales\";\n");
            writer.print("\n");
            //using AlgorithmMFLoop Funciton which contains the main logic for filtering the retrived data according to MF Query condition
            AlgorithmMFLoop(writer, mainProject, dataType);
            writer.print("\t\t}catch(Exception exception) {\n");
            writer.print("\t\t\texception.printStackTrace();\n");
            writer.print("\t\t}\n");
            writer.print("\t}\n");
            // Create compare Methods to check
            writer.print(
                    "\n\t /** \n\t * These are comapare methods to compare two string values orinteger values. \n\t * @return boolean true if same or else false. \n\t */ \n");
            writer.print("\tboolean compare(String str1, String str2){\n");
            writer.print("\t\treturn str1.equals(str2);\n\t}\n");
            writer.print("\tboolean compare(int num1, int num2){\n");
            writer.print("\t\treturn (num1 == num2);\n\t}\n");
            // Create addToOutput to build result
            writer.print("\n\t /** \n\t * filtering output data if having conditions exists. \n\t */ \n");
            writer.print("\tpublic void addToOutput(){\n");
            writer.print("\t\tfor(MF_Structure mfStructure: mfStructureList){\n");
            writer.print("\t\t\tResult result = new Result();\n");
            for (String str : mainProject.getGroupby())
                writer.print("\t\t\t\tresult." + str + " = mfStructure." + str + ";\n");
            //Narmit - handling if having condition is false
            writer.print("\t\t\tif(");
            // Declaring variable to set to true if the second having condition exists
            boolean isSecondHaving = false;

            //Narmit -  Putting the having condition in the output file for filtering the output.
            if (mainProject.getSizeHaving() != 0) {
                for (String str : mainProject.getHaving()) {
                    if (str.contains("sum"))
                        str = str.replace("sum", "mfStructure.sum");
                    if (str.contains("max"))
                        str = str.replace("max", "mfStructure.max");
                    if (str.contains("count"))
                        str = str.replace("count", "mfStructure.count");
                    if (str.contains("min"))
                        str = str.replace("min", "mfStructure.min");
                    if (str.contains("avg"))
                        str = str.replace("avg", "mfStructure.avg");

                    if (isSecondHaving == false) {
                        writer.print("(" + str + ")");
                        isSecondHaving = true;
                    }
                    if (isSecondHaving == true)
                        writer.print(" && (" + str + ")");
                }
            }
            //Narmit -  If there is no having condition put "true".
            else
                writer.print("true");
            writer.print("){\n");

            //Sushil - saving the result
            for (String str : mainProject.getSelect()) {
                for (GroupVariable groupVariable : mainProject.getFvect()) {
                    if (str.equals(groupVariable.getString())) {
                        writer.print("\t\t\t\tresult." + groupVariable.getString() + " = mfStructure." + groupVariable.getString() + ";\n");
                    }
                }

            }
            writer.print("\t\t\t\toutputAttributeList.add(result);\n");
            writer.print("\t\t\t}\n");

            writer.print("\t\t}\n");
            writer.print("\t}\n");

            //Narmit - Generate method to print output
            writer.print("\n\t /** \n\t * This method will create format for outputting the data table. \n\t */ \n");
            int length;
            writer.print("\tpublic void outputTable(){\n");

            for (String str : mainProject.getSelect()) {
                length = str.length();
                writer.print("\t\tSystem.out.printf(\"%-" + length + "s\",\"" + str + "\\t\");\n");
            }
            writer.print("\t\tSystem.out.printf(\"\\n\");\n");
            writer.print("\t\tSystem.out.printf(\"");
            for (String str : mainProject.getSelect()) {
                length = str.length();
                for (int i = 0; i < length; i++) {
                    writer.print("=");
                }
                writer.print("\\t");
            }
            writer.print(" \");\n");
            writer.print("\t\tfor(Result result: outputAttributeList){\n");
            writer.print("\t\t\tSystem.out.printf(\"\\n\");\n");
            for (String str : mainProject.getSelect()) {
                for (String str1 : mainProject.getGroupby()) {
                    if (str.equals(str1)) {
                        length = str.length();
                        if (str.equals("month") || str.equals("year") || str.equals("days") || str.equals("quant")) {
                            writer.print("\t\t\tSystem.out.printf(\"%" + length + "s\\t\", result." + str + ");\n");
                        } else {
                            writer.print("\t\t\tSystem.out.printf(\"%-" + length + "s\\t\", result." + str + ");\n");
                        }

                    }
                }
                for (GroupVariable fv : mainProject.getFvect()) {
                    if (str.equals(fv.getString())) {
                        length = str.length();
                        writer.print("\t\t\tSystem.out.printf(\"%" + length + "s\\t\", result." + str + ");\n");
                    }
                }

            }
            writer.print("\t\t}\n");
            writer.print("\t}\n");
            writer.print("}\n");

            writer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * The core logic of writing the AlgorithmMFLoop is based on the number of grouping variables
     *
     * @param writer
     * @param mainProject
     * @param dataType
     */
    private static void AlgorithmMFLoop(PrintWriter writer, MainProject mainProject, HashMap<String, String> dataType) {
        // TODO Auto-generated method stub
        List<String> added_elements = new ArrayList<String>();
        List<String> updated_elements = new ArrayList<String>();

        // Generating number of while loops equal to number of Grouping variables.
        writer.print("\n\t\t\t /** \n\t\t\t * Generating while loops for each grouping variable. \n\t\t\t */ \n");
        for (int i = 0; i < mainProject.getNumber(); i++) {
            writer.print("\n\t\t\t//While loop for grouping variable " + (i + 1) + ".\n");
            writer.print("\t\t\tresult_set = statement.executeQuery(query);\n");
            writer.print("\t\t\tisNext = result_set.next();\n");
            writer.print("\t\t\twhile(isNext){\n");
            writer.print("\t\t\t\tDB_Structure currentRow = new DB_Structure();\n");
            for (Map.Entry<String, String> entry : dataType.entrySet()) {
                if (entry.getValue().equals("String")) {
                    writer.print("\t\t\t\tcurrentRow." + entry.getKey() + " = result_set.getString(\"" + entry.getKey()
                            + "\");\n");
                } else if (entry.getValue().equals("int")) {
                    writer.print("\t\t\t\tcurrentRow." + entry.getKey() + " = result_set.getInt(\"" + entry.getKey()
                            + "\");\n");
                }
            }

            boolean isSecondWhere = false;
            boolean isSecondSuchThat = false;
            boolean not = false;
            // Filtering data if it has where conditions and making necessary modifications
            // in case it encounters = operator
            writer.print("\t\t\t\tif(");
            /*  This is when where is not empty then we loop through the condition
                and evaluate what condition are present on the where clause and we
                associate that condition with the if statement body as currntRow.condition
                we also write logic if there are multiple where elements

                This logic handles all the valid where clause conditions
                "<=", ">=", "==", ">", "<", "!="
             */
            if (mainProject.getSizeWhere() != 0) {
                for (String str : mainProject.getWhere()) {
                    str = str.replace(" ", "");
                    if (str.contains("<=") || str.contains(">=") || str.contains(">") || str.contains("<")
                            || str.contains("!=")) {
                        str = str;
                    } else if (str.contains("=")) {
                        str = str.replace("=", "==");
                    }

                    if (str.contains("prod==") || str.contains("state==") || str.contains("cust==")) {
                        String[] nameVal = str.split("=");
                        str = str.replace(str, nameVal[0] + ".equals(\"" + nameVal[2] + "\")");
                    }
                    if (str.contains("prod!=") || str.contains("state!=") || str.contains("cust!=")) {
                        not = true;
                        String[] nameVal = str.split("!=");
                        str = str.replace(str, nameVal[0] + ".equals(\"" + nameVal[1] + "\")");
                    }
                    if (isSecondWhere == false) {
                        if (not == true) {
                            writer.print("!currentRow." + str);
                            isSecondWhere = true;
                            not = false;
                        } else {
                            writer.print("currentRow." + str);
                            isSecondWhere = true;
                        }

                    } else if (isSecondWhere == true) {
                        if (not == true) {
                            writer.print(" && !currentRow." + str);
                            not = false;
                        } else {
                            writer.print(" && currentRow." + str);
                        }

                    }

                }
            } else {
                writer.print(true);
            }
            writer.print("){\n");

            // Putting the such that conditions if any.
            not = false;
            writer.print("\t\t\t\t\tif (");
            for (SuchThat such_that : mainProject.getSuchthat()) {
                not = false;
                String str = such_that.getAttribute();
                str = str.replace(" ", "");
                if (str.contains("<=") || str.contains(">=") || str.contains(">") || str.contains("<")) {
                    str = str;
                } else if (str.contains("=")) {
                    str = str.replace("=", "==");
                }
                if (str.contains("prod==") || str.contains("state==") || str.contains("cust==")) {
                    String[] nameVal = str.split("=");
                    str = str.replace(str, nameVal[0] + ".equals(" + nameVal[2] + ")");
                }
                if (str.contains("prod!=") || str.contains("state!=") || str.contains("cust!=")) {
                    not = true;
                    String[] nameVal = str.split("!==");
                    str = str.replace(str, nameVal[0] + ".equals(\"" + nameVal[1] + "\")");
                }
                if (such_that.getIndex() == i + 1 && isSecondSuchThat == false) {
                    if (not == true) {
                        isSecondSuchThat = true;
                        writer.print("!currentRow." + str);
                    } else {
                        isSecondSuchThat = true;
                        writer.print("currentRow." + str);
                    }
                } else if (such_that.getIndex() == i + 1 && isSecondSuchThat == true) {
                    if (not == true) {
                        writer.print(" && !currentRow." + str);
                    } else {
                        writer.print(" && currentRow." + str);
                    }
                }
            }
            if (isSecondSuchThat == false) {
                writer.print("true");
            }
            writer.print("){\n");
            writer.print("\t\t\t\t\t\tboolean found = false;\n");
            //loop through each mf structure list if it is present it will just update the value else it will add the current row to mf struct
            writer.print("\t\t\t\t\t\tfor(MF_Structure row: mfStructureList){\n");
            boolean isSecondGroupByVariable = false;
            writer.print("\t\t\t\t\t\t\tif(compare(row.");
            for (String str : mainProject.getGroupby()) {
                if (isSecondGroupByVariable == false) {
                    writer.print(str + ",currentRow." + str + ")");
                    isSecondGroupByVariable = true;
                } else {
                    writer.print(" && compare(row." + str + ",currentRow." + str + ")");
                }
            }
            writer.print("){\n");

            writer.print("\t\t\t\t\t\t\t\tfound = true;\n");

            // Outputting the aggregate functions if record is added already.
            for (GroupVariable gv : mainProject.getFvect()) {
                if (Integer.parseInt(gv.index) == i + 1) {
                    if (gv.aggregate.equals("avg")) {
                        String sum = "sum_" + gv.attribute + "_" + gv.index;
                        String count = "count_" + gv.attribute + "_" + gv.index;
                        if (!updated_elements.contains(sum)) {
                            updated_elements.add(sum);
                            writer.print("\t\t\t\t\t\t\t\trow." + sum + " += currentRow." + gv.attribute + ";\n");
                        }
                        if (!updated_elements.contains(count)) {
                            updated_elements.add(count);
                            writer.print("\t\t\t\t\t\t\t\trow." + count + " ++;\n");
                        }
                        if (!updated_elements.contains(gv.getString())) {
                            updated_elements.add(gv.getString());
                            writer.print("\t\t\t\t\t\t\t\tif(row." + count + " !=0){\n");
                            writer.print("\t\t\t\t\t\t\t\t\trow." + gv.getString() + " = row." + sum + "/row." + count
                                    + ";\n");
                            writer.print("\t\t\t\t\t\t\t\t}\n");
                        }

                    }
                    if (!updated_elements.contains(gv.getString()) && gv.aggregate.equals("sum")) {
                        writer.print(
                                "\t\t\t\t\t\t\t\trow." + gv.getString() + " += currentRow." + gv.attribute + ";\n");
                        updated_elements.add(gv.getString());
                    }
                    if (!updated_elements.contains(gv.getString()) && gv.aggregate.equals("max")) {
                        writer.print("\t\t\t\t\t\t\t\trow." + gv.getString() + " = (row." + gv.getString()
                                + "< currentRow." + gv.attribute + ") ? currentRow." + gv.attribute + " :row."
                                + gv.getString() + ";\n");
                        updated_elements.add(gv.getString());
                    }
                    if (!updated_elements.contains(gv.getString()) && gv.aggregate.equals("min")) {
                        writer.print("\t\t\t\t\t\t\t\trow." + gv.getString() + " = (row." + gv.getString()
                                + "> currentRow." + gv.attribute + ") ? currentRow." + gv.attribute + " :row."
                                + gv.getString() + ";\n");
                        updated_elements.add(gv.getString());
                    }
                    if (!updated_elements.contains(gv.getString()) && gv.aggregate.equals("count")) {
                        writer.print("\t\t\t\t\t\t\t\trow." + gv.getString() + "++;\n");
                        updated_elements.add(gv.getString());
                    }
                }
            }

            writer.print("\t\t\t\t\t\t\t}\n");
            writer.print("\t\t\t\t\t\t}\n");
            // If record is found for the first time - i.e no record in mfStructureList
            writer.print("\t\t\t\t\t\tif(found == false){\n");
            writer.print("\t\t\t\t\t\t\tMF_Structure addCurrentRow = new MF_Structure();\n");
            for (String str : mainProject.getGroupby()) {
                writer.print("\t\t\t\t\t\t\taddCurrentRow." + str + " = currentRow." + str + ";\n");
            }
            for (GroupVariable gv : mainProject.getFvect()) {
                if (Integer.parseInt(gv.index) == i + 1) {
                    if (gv.aggregate.equals("avg")) {
                        String sum = "sum_" + gv.attribute + "_" + gv.index;
                        String count = "count_" + gv.attribute + "_" + gv.index;
                        if (!added_elements.contains(sum)) {
                            added_elements.add(sum);
                            writer.print("\t\t\t\t\t\t\taddCurrentRow." + "sum_" + gv.attribute + "_" + gv.index
                                    + " = currentRow." + gv.attribute + ";\n");
                        }
                        if (!added_elements.contains(count)) {
                            added_elements.add(count);
                            writer.print("\t\t\t\t\t\t\taddCurrentRow." + "count_" + gv.attribute + "_" + gv.index
                                    + "++;\n");
                        }
                        if (!added_elements.contains(gv.getString())) {
                            added_elements.add(gv.getString());
                            writer.print("\t\t\t\t\t\t\tif(addCurrentRow." + count + " !=0){\n");
                            writer.print("\t\t\t\t\t\t\t\taddCurrentRow." + gv.getString() + " = addCurrentRow." + sum
                                    + "/addCurrentRow." + count + ";\n");
                            writer.print("\t\t\t\t\t\t\t}\n");
                        }

                    } else {
                        if (!added_elements.contains(gv.getString())) {
                            if (gv.aggregate.equals("count")) {
                                writer.print("\t\t\t\t\t\t\taddCurrentRow." + "count_" + gv.attribute + "_" + gv.index
                                        + "++;\n");
                            } else {
                                writer.print("\t\t\t\t\t\t\taddCurrentRow." + gv.getString() + " = currentRow."
                                        + gv.attribute + ";\n");
                            }
                            added_elements.add(gv.getString());
                        }

                    }
                }

            }
            writer.print("\t\t\t\t\t\t\tmfStructureList.add(addCurrentRow);\n");
            writer.print("\t\t\t\t\t\t}\n");
            writer.print("\t\t\t\t\t}\n");
            writer.print("\t\t\t\t}\n");
            writer.print("\t\t\t\tisNext = result_set.next();\n");
            writer.print("\t\t\t}\n");

        }
    }

}
