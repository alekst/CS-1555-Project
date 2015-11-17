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
	private Statement statement;
	
	private PreparedStatement preparedStatement;
	
	private static Connection con;
	
    private static String username;
    private static String password;

    // address of the server
    private static final String SERVER_ADDR = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";
	
	public DBLoader(String answer) {
        // drop and recreate the tables
        if (answer.toUpperCase().equals("Y"))
        {
			initDatabase();
        }

        // populate the tables with generated data
        populateTables();
		
		
	}

    /**
    * Main method
    * The path to a credential file is passed as the first argument.
    * The credential file should have the username and password to the database
    * on the first and second lines of the file, respectively.
    */
		
		
	
    public static void main(String[] args) throws SQLException
    {
		System.out.println(args[0]);
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
        con = openConnection();
		
        // ask whether the user wants to drop the tables or not
        Scanner scan = new Scanner(System.in);
        System.out.print("\nDo you want to drop and recreate the tables in the database? (y/n): ");
        String answer = scan.nextLine();
		
		DBLoader loader = new DBLoader(answer);

        scan.close();
		con.close();
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



    public void initDatabase()
    {
		System.out.println("creating tables...");
		String startTransaction = "SET TRANSACTION READ WRITE";
		String dropWarehouses = "drop table Warehouses cascade constraints";
		
		String createWarehouses = "create table Warehouses (" +
			"warehouse_id number(3), " + 
			"name varchar2(20), " +
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(2), " +
			"zip number(5), " +
			"tax_rate number (3, 2), " +
			"sum_sales number (9, 2), " +
			"constraint Warehouses_pk primary key(warehouse_id) )";
		
		String dropStations = "drop table Stations cascade constraints";
		String createStations = "create table Stations (" +
			"station_id number(3), " +
			"warehouse_id number(3)" +
			"name varchar2(20), " +
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(5), " +
			"zip number(5), " +
			"tax_rate number(3, 2), " +
			"sum_sales(9, 2) ), " +
			"constraint Stations_pk primary key(station_id, warehouse_id), " +
			"constraint Stations_fk foreign key(warehouse_id) references Warehouses(warehouse_id) )";
	
		String dropCustomers = "drop table Customers cascade constraints";
		String createCustomers = "create table Customers (" +
			"customer_id number(6), " +
			"station_id number(3), " +
			"fname varchar2(10), " +
			"mi varchar2(1), " +
			"lname varchar2(20), " + 
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(5), " +
			"zip number(5), " +
			"phone varchar(13), " +
			"join_date date, " +
			"discount number(3, 2), " +
			"balance number(7, 2), " +
			"paid_amount number (7, 2), " +
			"total_payments number (9, 2), " +
			"total_deliveries number, " +
			"constraint Customers_pk primary key(customer_id, station_id), " +
			"constraint Customers_ak alternate key(phone), " +
			"constraint Customers_fk foreign key(station_id) references Stations(station_id) )";
		
		String dropOrders = "drop table Orders cascade constraints";
		String createOrders = "create table Orders (" +
			"order_id number(10), " +
			"customer_id number(6), " +
			"order_date date, " +
			"completed number(1), " +
			"items varchar2(25), " +
			"station_id number(3), " +
			"warehouse_id number(3)," +
			"constraint Orders_pk primary key(customer_id, order_id), " +
			"constraint Orders_fk1 foreign key(station_id) references Stations(station_id), " +
			"constraint Orders_fk2 foreign key(warehouse_id) references Warehouses(warehouse_id) )";
		
		String dropLineItems = "drop table LineItems cascade constraints";
		String createLineItems = "create table LineItems (" +
			"item_id number(15), " +
			"order_id number(10), " +
			"item_number number(2), " +
			"quantity number(5), " +
			"amount number (5, 2), " +
			"delivered number(1), " +
			"constraint LineItems_pk primary key(item_id, order_id), " +
			"constraint LineItems_fk foreign key(order_id) references Orders(order_id) )";
		
		String dropStockItems = "drop table StockItems cascade constraints";
		String createStockItems = "create table StockItems (" +
			"item_id number(15), " +
			"name varchar2(20), " +
			"price number(5, 2), " +
			"in_stock number(1), " +
			"sold_this_year number(4), " +
			"included_in_orders number(4), " +
			"warehouse_id number(3)" +
			"constraint StockItems_pk primary key(item_id, warehouse_id), " +
			"constraint StockItems_fk foreign key(warehouse_id) references Warehouses(warehouse_id) )";
		
		try {
			statement = con.createStatement();
			statement.executeUpdate(startTransaction);
			statement.executeUpdate(dropWarehouses);
			statement.executeUpdate(createWarehouses);
			
		} catch(SQLException Ex) {
			System.out.println("Error running create queries. " + Ex.toString());
		} 
    }



    private static void populateTables()
    {

    }
}