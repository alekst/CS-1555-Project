import java.sql.*;
import java.text.ParseException;
import java.sql.Date;

/**
 * 
 * $> javac Project1
 * 
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
            case 1:
                selectAllRegistrations();
                break;
            case 2:
                if (args.length < 2) {
                    System.err.println("arguments needed for query 2: <query-#> <student-name>");
                    break;
                }
                selectStudentRegistration(args[1]);
                break;
            case 3:
                if (args.length < 3) {
                    System.err.println("arguments needed for query 3: <query-#> <student-name> <class-id>");
                    break;
                }
                insertNewRegistration(args[1], Integer.parseInt(args[2]));
                break;
            case 4:
                if (args.length < 3) {
                    System.err.println("arguments needed for query 4: <query-#> <student-name> <new-student-name>");
                    break;
                }
                updateStudentName(args[1], args[2]);
                break;
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
        String username = "username";
        String password = "password";

        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        //Class.forName("oracle.jdbc.OracleDriver");
        String url = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";

        if (args.length < 1) {
            System.err.println("usage: need at least one argument with the query-number.");
            System.exit(1);
        }
        connection = DriverManager.getConnection(url, username, password);
        Project1 demo = new Project1(Integer.parseInt(args[0]), args);

        connection.close();
    }

 


}