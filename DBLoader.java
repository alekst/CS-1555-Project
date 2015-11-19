/******************************
* DBLoader.java
*
* Aleks Tapinsh
* alt99@pitt.edu
* Clint Wadley
* cvw5@pitt.edu
*
* 11/18/15
* CS1555
* Term Project
*
* Program for loading the grocery delivery database with generated data
*/

import java.sql.*;
import java.io.FileInputStream;
import java.util.Scanner;
import java.io.IOException;
import java.io.IOError;
import java.io.Console;
import java.util.Random;
import java.util.HashMap;

public class DBLoader
{
	private Statement statement;
	private PreparedStatement preparedStatement;
	private Connection con;
    private String server;
	private String username;
	private String password;
    private Scanner scan;
    private final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private final String NUMBERS = "0123456789";
    private final float[] TAXES = {0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15};
    private final String[] STREET_SUFFIXES = {"St.", "Ave.", "Rd.", "Way"};
    private final int MAX_ADDRESS = 2000;
    private final int NAME_MIN = 4;
    private final int NAME_MAX = 15;
    private final int MAX_SOLD = 15000;

    // address of the server
	private static final String SERVER_ADDR = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";
	
    /**
    * Main method
    */
    public static void main(String[] args)
    {
        new DBLoader();
    }
	

    /**
    * Constructor for the DBLoader. Opens the database connection with the
    * credentials passed from the user.
    */
	public DBLoader()
    {
        scan = new Scanner(System.in);

        // ask the user if they want to switch databases
        System.out.println("Welcome to the database loader!");
        System.out.println("The default database is : " + SERVER_ADDR);
        System.out.print("Do you want to switch to a different database? (y/n): ");
        String answer = scan.nextLine();

        if (answer.toUpperCase().equals("Y"))
        {
            System.out.println("Ok, please enter the address of the server you want to use:");
            server = scan.nextLine();

            do
            {
                System.out.println("You entered: " + server);
                System.out.print("Is this correct? (y/n): ");
                answer = scan.nextLine();
            }
            while (answer.toUpperCase().equals("N"));
        }
        else
        {
            server = SERVER_ADDR;
        }

        // prompt the user for their credentials
        System.out.print("Please enter your username: ");
        username = scan.nextLine();

        Console console = System.console();
        System.out.print("Password for " + username + ": ");
        char[] pwd = console.readPassword();

        password = new String(pwd);

        // open the connection to the server
        con = openConnection();
		
        // ask whether the user wants to drop the tables or not
        System.out.print("\nDo you want to drop and recreate the tables in the database? (y/n): ");
        answer = scan.nextLine();

      	// drop and recreate the tables
    	if (answer.toUpperCase().equals("Y"))
    	{
			initDatabase();
    	}

    	// populate the tables with generated data
    	//populateTables();

        scan.close();
        try
        {
            con.close();
        }
        catch (SQLException e)
        {
            System.out.println("Error closing connection.");
        }
	}

    /**
    * Opens a connection to the database server
    * @return Connection object representing the open connection
    */
    private Connection openConnection() 
    {
        Connection connection = null;
        try
        {
            // open the SQL connection
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            connection = DriverManager.getConnection(server, username, password);
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
        String[] dropStatements = new String[6];

		dropStatements[0] = "drop table Warehouses cascade constraints";
		
		String createWarehouses = "create table Warehouses (" +
			"warehouse_id number(3) not null, " + 
			"name varchar2(20), " +
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(2), " +
			"zip varchar2(5), " +
			"tax_rate number (3, 2), " +
			"sum_sales number (9, 2), " +
			"constraint Warehouses_pk primary key(warehouse_id) )";
		
		dropStatements[1] = "drop table Stations cascade constraints";
		String createStations = "create table Stations (" +
			"station_id number(3) not null, " +
			"warehouse_id number(3) not null, " +
			"name varchar2(20), " +
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(5), " +
			"zip varchar2(5), " +
			"tax_rate number(3, 2), " +
			"sum_sales number(9, 2), " +
			"constraint Stations_pk primary key(station_id), " +
			"constraint Stations_fk foreign key(warehouse_id) references Warehouses(warehouse_id) )";
	
		dropStatements[2] = "drop table Customers cascade constraints";


		String createCustomers = "create table Customers (" +
			"customer_id number(6) not null, " +
			"station_id number(3) not null, " +
			"fname varchar2(10), " +
			"mi varchar2(1), " +
			"lname varchar2(20), " + 
			"address varchar2(20), " +
			"city varchar2(20), " +
			"state varchar2(5), " +
			"zip varchar2(5), " +
			"phone varchar2(13), " +
			"join_date date, " +
			"discount number(3, 2), " +
			"balance number(7, 2), " +
			"paid_amount number(7, 2), " +
			"total_payments number(9, 2), " +
			"total_deliveries number(10), " +
			"constraint Customers_pk primary key(customer_id), " +
			"constraint Customers_fk foreign key(station_id) references Stations(station_id) )";
		
		dropStatements[3] = "drop table Orders cascade constraints";
		String createOrders = "create table Orders (" +
			"order_id number(10), " +
			"customer_id number(6), " +
			"order_date date, " +
			"completed boolean, " +
			"line_item_count number(10), " +
			"station_id number(3), " +
			"warehouse_id number(3), " +
			"constraint Orders_pk primary key(order_id), " +
			"constraint Orders_fk1 foreign key(station_id) references Stations(station_id), " +
			"constraint Orders_fk2 foreign key(warehouse_id) references Warehouses(warehouse_id), " +
            "constraint Orders_fk3 foreign key(customer_id) references Customers(customer_id) )";
		
		dropStatements[4] = "drop table LineItems cascade constraints";
		String createLineItems = "create table LineItems (" +
			"line_id number(15), " +
			"order_id number(10), " +
			"item_id number(15), " +
			"quantity number(5), " +
			"amount number (5, 2), " +
			"delivered boolean, " +
			"constraint LineItems_pk primary key(line_id), " +
			"constraint LineItems_fk1 foreign key(order_id) references Orders(order_id), " +
            "constraint LineItems_fk2 foreign key(item_id) references StockItems(item_id) )";


		dropStatements[5] = "drop table StockItems cascade constraints";

		String createStockItems = "create table StockItems (" +
			"item_id number(15), " +
			"name varchar2(20), " +
			"price number(5, 2), " +
			"in_stock number(1), " +
			"sold_this_year number(10), " +
			"included_in_orders number(4), " +
			"warehouse_id number(3), " +
			"constraint StockItems_pk primary key(item_id, warehouse_id), " +
			"constraint StockItems_fk foreign key(warehouse_id) references Warehouses(warehouse_id) )";

 	  // System.out.println(
		   
		  //createWarehouses + "\n" + createStations + "\n" + 
	   //createCustomers + "\n" + createOrders + "\n" + createLineItems + "\n" + createStockItems);

        try
        {
		  statement = con.createStatement();
            statement.executeUpdate(startTransaction);
        }
        catch (SQLException e)
        {
            System.out.println("Error creating statement");
        }
        for (int i = 0; i < dropStatements.length; i++)
        {
            try
            {
                statement.executeUpdate(dropStatements[i]);
            }
            catch (SQLException e)
            {
            }
        }
        try
        {
            statement.executeUpdate("COMMIT");
        }
        catch (SQLException e)
        {
            System.out.println("Commitment failure.");
        }


		try {
			statement.executeUpdate(startTransaction);
			statement.executeUpdate(createWarehouses);
            System.out.println("Warehouses");
			statement.executeUpdate(createStations);
            System.out.println("Stations");
			statement.executeUpdate(createCustomers);
            System.out.println("Customers");
			statement.executeUpdate(createOrders);
            System.out.println("Orders");
            statement.executeUpdate(createStockItems);
            System.out.println("StockItems");
			statement.executeUpdate(createLineItems);
            System.out.println("LineItems");
			statement.executeUpdate("COMMIT");
			
		} catch(SQLException Ex) {
			System.out.println("Error running create queries. " + Ex.toString());
		}

        System.out.println("Tables successfully created.");
    }



    private void populateTables()
    {
        String warehouses = 0;
        int stations = 0;
        int customers = 0;
        int orders = 0;
        int items = 0;

        // ask how many of each data item to make
        do
        {
            System.out.print("How many warehouses do you want to create?: ");
            warehouses = scan.nextLine();
        }
        while(true);


        // define the insert statements
        String warehousesString = "insert into Warehouses (warehouse_id, name, address, city, state, zip, tax_rate, sum_sales)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?)";
        String stationsString = "insert into Stations (station_id, warehouse_id, name, address, city, state, zip, tax_rate, sum_sales)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String customersString = "insert into Customers (customer_id, station_id, fname, mi, lname, address, city, state, zip, phone, "
        + "join_date, discount, balance, paid_amount, total_payments, total_deliveries)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String ordersString = "insert into Orders (order_id, customer_id, order_date, completed, line_item_count, station_id, warehouse_id)"
        + "values (?, ?, ?, ?, ?, ?, ?)";
        String lineItemsString = "insert into LineItems (line_id, order_id, item_id, quantity, amount, delivered)"
        + "values (?, ?, ?, ?, ?, ?)";
        String stockItemsString = "insert into StockItems (item_id, name, price, in_stock, sold_this_year, included_in_orders, warehouse_id)"
        + "values (?, ?, ?, ?, ?, ?, ?)";

        // instantiate the prepared statements
        PreparedStatement insertWarehouses = con.prepareStatement(warehousesString);
        PreparedStatement insertStations = con.prepareStatement(stationsString);
        PreparedStatement insertCustomers = con.prepareStatement(customersString);
        PreparedStatement insertOrders = con.prepareStatement(ordersString);
        PreparedStatement insertLineItems = con.prepareStatement(lineItemsString);
        PreparedStatement insertStockItems = con.prepareStatement(stockItemsString);


        /********
        * Generate the data
        *********/
        Random rand = new Random(System.nanoTime());
        float warehouseTotal = 0;
        float stationTotal = 0;
        float customerTotal = 0;
        HashMap<Integer, Integer> ytdSoldCounts = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> itemOrderCounts = new HashMap<Integer, Integer>();
        try
        {
            // generate the warehouses
            for (int i = 1; i <= warehouses; i++)
            {
                insertWarehouses.setString(1, i);
                insertWarehouses.setString(2, getName(rand));
                insertWarehouses.setString(3, getAddress(rand));
                insertWarehouses.setString(4, getName(rand));
                insertWarehouses.setString(5, getState(rand));
                insertWarehouses.setString(6, getZip(rand));
                insertWarehouses.setString(7, getTax(rand));
                insertWarehouses.setString(8, "0");
                insertWarehouses.addBatch();

                // generate the stock items
                for (int j = 1; j <= items; j++)
                {
                    ytdSoldCounts.put(j, 0);
                    itemOrderCounts.put(j, 0);
                    insertStockItems.setString(1, j);
                    insertStockItems.setString(2, getName());
                    insertStockItems.setString(3, getPrice());
                    insertStockItems.setString(4, Math.round(rand.nextFloat()));
                    insertStockItems.setString(5, "0");
                    insertStockItems.setString(6, "0");
                    insertStockItems.setString(7, i);
                    insertStockItems.addBatch();
                }

                // generate the stations
                for (int j = 1; j <= stations; j++)
                {
                    insertStations.setString(1, j);
                    insertStations.setString(2, i);
                    insertStations.setString(3, getName(rand));
                    insertStations.setString(4, getAddress(rand));
                    insertStations.setString(5, getName(rand));
                    insertStations.setString(6, getState(rand));
                    insertStations.setString(7, getZip(rand));
                    insertStations.setString(8, getTax(rand));
                    insertStations.setString(9, "0");
                    insertStations.addBatch();

                    // generate the customers
                    for (int k = 1; k <= customers; k++)
                    {
                        insertCustomers.setString(1, k);
                        insertCustomers.setString(2, j);
                        insertCustomers.setString(3, getName(rand));
                        insertCustomers.setString(4, new String(ALPHABET.charAt(rand.nextInt(ALPHABET.length()))));
                        insertCustomers.setString(5, getName(rand));
                        insertCustomers.setString(6, getAddress());
                        insertCustomers.setString(7, getName());
                        insertCustomers.setString(8, getState());
                        insertCustomers.setString(9, getZip());
                        insertCustomers.setString(10, getPhone());
                        insertCustomers.setString(11, getDate());
                        insertCustomers.setString(12, getDiscount());
                        insertCustomers.setString(13, balance);
                        insertCustomers.setString(14, paidamount);
                        insertCustomers.setString(15, totalpayments);
                        insertCustomers.setString(16, totaldeliveries);
                        insertCustomers.addBatch();

                        // generate the orders
                        for (int l = 1; l <= orders; l++)
                        {
                            int lineCount = rand.nextInt(15) + 1;
                            Date theDate = getDate();
                            insertOrders.setInt(1, l);
                            insertOrders.setInt(2, k);
                            insertOrders.setDate(3, theDate);
                            insertOrders.setBoolean(4, rand.nextBoolean());
                            insertOrders.setString(5, lineCount);
                            insertOrders.setInt(6, j);
                            insertOrders.setInt(7, i);
                            insertOrders.addBatch();

                            // generate the line items for the order
                            for (int m = 1; m <= lineCount; m++)
                            {
                                int itemID = rand.nextInt(items) + 1;
                                int itemCount = rand.nextInt(10) + 1;

                                // update the hashmaps to reflect the sold and order numbers
                                ytdSoldCounts.put(itemID, new Integer(ytdSoldCounts.get(itemID).intValue() + itemCount));
                                insertLineItems.setInt(1, m);
                                insertLineItems.setInt(2, l);
                                insertLineItems.setInt(3, itemID);
                                insertLineItems.setInt(4, itemCount);
                                insertLineItems.setFloat(5, amount);
                                insertLineItems.setBoolean(6, rand.nextBoolean());
                                insertLineItems.addBatch();
                            }

                        }
                    }
                }

            }
        }
        catch (SQLException e)
        {
            System.out.println("Error generating statements");
        }
    }

    /**
    * Generates and returns a random alphanumeric name of length between
    * NAME_MIN and NAME_MAX
    * @param rand Random number generator object
    * @return A String object containing the name
    */
    private String getName(Random rand)
    {
        // generate the length
        int length = rand.nextInt((NAME_MAX - NAME_MIN) + 1) + NAME_MIN;

        // generate the name
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
        }

        return sb.toString();
    }

    /**
    * Generates and returns a random street number between 1 and MAX_ADDRESS
    * @param rand Random number generator object
    * @return int containing the street number
    */
    private int getStreetNum(Random rand)
    {
        return (rand.nextInt(MAX_ADDRESS) + 1);
    }

    /**
    * Returns a random street suffix
    * @param rand Random number generator object
    * @return String containing the street suffix
    */
    private String getStreetSuffix(Random rand)
    {
        return STREET_SUFFIXES[rand.nextInt(STREET_SUFFIXES.length)];

	}

    /**
    * Returns a full address, concatenated together.
    * @param rand Random number generator object
    * @return String containing the address
    */
    private String getAddress(Random rand)
    {
        return getStreetNum(rand) + " " + getName(rand) + " " + getStreetSuffix(rand);
    }

    /**
    * Returns a random state abbreviation
    * @param rand Random number generator object
    * @return String containing the two letter state abbreviation
    */
    private String getState(Random rand)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2; i++)
        {
            sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
        }

        return sb.toString();
    }

    /**
    * Returns a random zip code
    * @param rand Random number generator object
    * @return String containing the zip code
    */
    private String getZip(Random rand)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++)
        {
            sb.append(NUMBERS.charAt(rand.nextInt(NUMBERS.length())));
        }

        return sb.toString();
    }

    /**
    * Returns a random tax rate
    * @param rand Random number generator object
    * @return String containing the tax rate
    */
    private String getTax(Random rand)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(TAXES[rand.nextInt(TAXES.length)]);

        return sb.subString(0, 4);
    }

    /**
    * Returns a random phone number
    * @param rand Random number generator object
    * @return String containing the phone number
    */
    private String getPhone(Random rand)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++)
        {
            sb.append(rand.nextInt(10));
        }

        return sb.toString();
    }

    /**
    * Returns a random date
    * @param rand Random number generator object
    * @return Date object containing the date
    */
    private Date getDate(Random rand)
    {
        int year = 2010 + rand.nextInt(6);
        int month = rand.nextInt(12) + 1;
        int day = rand.nextInt(28) + 1;
        return new Date(year, month, day);
    }

    /**
    * Returns a random discount
    * @param rand Random number generator object
    * @return String containing the discount
    */
    private String getDiscount(Random rand)
    {
        int intPortion = rand.nextInt(75);
        float floatPortion = rand.nextFloat();
        StringBuilder sb = new StringBuilder();
        sb.append((float)intPortion + floatPortion);
        return sb.toString();

    }

}