import java.sql.*;
import java.util.Properties;
import java.io.*;
import java.text.ParseException;
import java.sql.Date;

/**
 *
 * $> javac Project1
 * 
 */
/*
TODO: 1. set up password authentication for the db on a file not part of git
2. Connect to the database



*/





public class Project1 {

    private static Connection connection;

    private Statement statement;

    private PreparedStatement preparedStatement;

    private ResultSet resultSet;

    private String query;

    public Project1(int example, String[] args) {
        switch (example) {
            case 0:
                databaseInit();
                break;
            // case 1:
//                 selectAllRegistrations();
//                 break;
//             case 2:
//                 if (args.length < 2) {
//                     System.err.println("arguments needed for query 2: <query-#> <student-name>");
//                     break;
//                 }
//                 selectStudentRegistration(args[1]);
//                 break;
//             case 3:
//                 if (args.length < 3) {
//                     System.err.println("arguments needed for query 3: <query-#> <student-name> <class-id>");
//                     break;
//                 }
//                 insertNewRegistration(args[1], Integer.parseInt(args[2]));
//                 break;
//             case 4:
//                 if (args.length < 3) {
//                     System.err.println("arguments needed for query 4: <query-#> <student-name> <new-student-name>");
//                     break;
//                 }
//                 updateStudentName(args[1], args[2]);
//                 break;
            default:
                System.out.println("Example not found for your entry: " + example);
                try {
                    connection.close();
                } catch(Exception Ex)  {
                    System.out.println("Error connecting to database.  Machine Error: " +
                            Ex.toString());
                }
                break;
        }
    }

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
		
		Properties properties = new Properties(); // using Properties class for username and password
		try {
		properties.load(new FileInputStream(new File("credentials.properties")));
		} catch(Exception e) {
			System.out.println("Error reading the credentials file");
		}
		
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");
		
		System.out.println(username);
		System.out.println(password);

        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        //Class.forName("oracle.jdbc.OracleDriver");
        String url = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";

        if (args.length < 1) {
            System.err.println("usage: need at least one argument with the query-number.");
            System.exit(1);
        }
        connection = DriverManager.getConnection(url, username, password);
        Project1 demo = new Project1(Integer.parseInt(args[0]), args);
		System.out.println("Test");
        connection.close();
    }

	public void databaseInit() {
	        System.out.println("Initializing database...");
	        String startTransaction = "SET TRANSACTION READ WRITE";
			
	        //String dropTableClass = "drop table class cascade constraints";
	        //String createTableClass = "create table class (" + 
	            // "classid number(2)," +
// 	            "max_num_students number(2)," +
// 	            "cur_num_students number(2)," +
// 	            "primary key(classid) )";
// 	        String dropTableRegister = "drop table register cascade constraints";
// 	        String createTableRegister = "create table register (" +
// 	            "student_name varchar2(10)," +
// 	            "classid number(2)," +
// 	            "date_registered date," +
// 	            "primary key(student_name, classid)," +
// 	            "foreign key (classid) references class(classid) )";
// 	        String insertQueryClassOne = "insert into class values(1, 2, 1)";
// 	        String insertQueryClassTwo = "insert into class values (2,4,0)";
// 	        String insertQueryRegister = "insert into register values ('Mary',1, '03-JAN-2012')";
	        try {
	            statement = connection.createStatement();
	            statement.executeUpdate(startTransaction);
	            // statement.executeUpdate(dropTableClass);
// 	            statement.executeUpdate(createTableClass);
// 	            statement.executeUpdate(dropTableRegister);
// 	            statement.executeUpdate(createTableRegister);
// 	            statement.executeUpdate(insertQueryClassOne);
// 	            statement.executeUpdate(insertQueryClassTwo);
// 	            statement.executeUpdate(insertQueryRegister);
	            statement.executeUpdate("COMMIT");
	        } catch(SQLException Ex) {
	            System.out.println("Error running the sample queries.  Machine Error: " +
	                    Ex.toString());
	        } finally{
	            try {
	                if (statement != null)
	                    statement.close();
	                if (preparedStatement != null)
	                    preparedStatement.close();
	            } catch (SQLException e) {
	                System.out.println("Cannot close Statement. Machine error: "+e.toString());
	            }
	        }
	    } 


}