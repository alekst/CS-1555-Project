/******************************
* DBLoader.java
*
* Aleks Tapinsh
* alt99@pitt.edu
* Clint Wadley
* cvw5@pitt.edu
*
* 11/12/15
* CS1555
* Term Project
*
* Program for loading the grocery delivery database with generated data
*/

import java.sql.*;
import java.io.FileInputStream;
import java.util.Scanner;
import java.io.IOException;

public class DBLoader
{
    private static String username;
    private static String password;

    // address of the server
    private static final String SERVER_ADDR = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";


    /**
    * Main method
    * The path to a credential file is passed as the first argument.
    * The credential file should have the username and password to the database
    * on the first and second lines of the file, respectively.
    */
    public static void main(String[] args)
    {
        // check the number of arguments
        if (args.length < 1)
        {
            System.out.println("Usage: java DBLoader [credential file]");
            System.exit(0);
        }

        // open the credential file passed as an argument
        Scanner infile = null;
        try
        {
            infile = new Scanner(new FileInputStream(args[0]));
            infile.useDelimiter("\\n");
        }
        catch (IOException e)
        {
            System.out.println("Error reading credential file. Please check the path.");
            System.exit(1);
        }

        // read in the username and password from the credential file
        username = infile.next();
        password = infile.next();

        infile.close();

        // open the connection to the server
        Connection con = openConnection();

        // ask whether the user wants to drop the tables or not
        Scanner scan = new Scanner(System.in);
        System.out.print("\nDo you want to drop and recreate the tables in the database? (y/n): ");
        String answer = scan.nextLine();



        // drop and recreate the tables
        if (answer.toUpperCase().equals("Y"))
        {
            dropTables(con);
            createTables(con);
        }

        // populate the tables with generated data
        populateTables(con);

        scan.close();
    }



    /**
    * Opens a connection to the database server
    * @return Connection object representing the open connection
    */
    private static Connection openConnection()
    {
        Connection connection = null;
        try
        {
            // open the SQL connection
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            connection = DriverManager.getConnection(SERVER_ADDR, username, password);
        }
        catch (SQLException e)
        {
            System.out.println("Error opening connection with the server.");
            e.printStackTrace();
            System.exit(4);
        }

        return connection;
    }



    private static void dropTables(Connection con)
    {

    }



    private static void createTables(Connection con)
    {
		System.out.println("creating tables...");
		String startTransaction = "SET TRANSACTION READ WRITE";
		String createwarehouses = "create table Warehouses (" +
			"warehouse_id number(3), " + 
			"name varchar2(20), " +
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(2), " +
			"zip number(5), " +
			"tax_rate number (3, 2), " +
			"sum_sales number (9, 2), " +
			"primary key(warehouse_id) )";
		String createStations = "create table Stations (" +
			"station_id number(3), " +
			"warehouse_id number(3)" +
			"name varchar2(20), " +
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(5), " +
			"zip number(5), " +
			"tax_rate number(3, 2), " +
			"sum_sales(9, 2) )";
    }



    private static void populateTables(Connection con)
    {

    }
}