/******************************
* DBLoader.java
*
* Aleks Tapinsh
* alt99@pitt.edu
* Clint Wadley
* cvw5@pitt.edu
*
* 12/6/15
* CS1555
* Term Project
*
* Program for loading the grocery delivery database with generated data
*/

import java.sql.*;
import java.math.*;
import java.text.*;
import java.io.FileInputStream;
import java.util.Scanner;
import java.io.IOException;
import java.io.IOError;
import java.io.Console;
import java.util.Random;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
import java.util.Calendar;
import java.util.Locale;

public class DBLoader
{
	private Statement statement;
	private PreparedStatement preparedStatement;
	private Connection con;
	private ResultSet resultSet, rs;
    private String server;
	private String username;
	private String password;
    private Scanner scan;
    private String startTransaction = "SET TRANSACTION READ WRITE";
    private final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private final String NUMBERS = "0123456789";
    private final double[] TAXES = {0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14, 0.15};
    private final String[] STREET_SUFFIXES = {"St.", "Ave.", "Rd.", "Way"};
    private final int MAX_ADDRESS = 2000;
    private final int NAME_MIN = 4;
    private final int NAME_MAX = 15;
    private final int MAX_SOLD = 15000;
    private final int MAX_PRICE = 50;
    private HashMap<Integer, Float> itemCost;
    private int currWarehouseID, currStationID, currCustomerID;
    private HashMap<String, Integer> currOrderID, currLineID;
    private String[][][] last20Orders;
    private int[][] last20Index;
	
	Locale currentLocale = Locale.getDefault();
	NumberFormat cf = NumberFormat.getCurrencyInstance(currentLocale); //currency formatter

    // constants defining the amount of data to generate
    public int WAREHOUSES = 1;
    public int STATIONS_PER_WAREHOUSE = 8;
    public int CUSTOMERS_PER_STATION = 100;
    public int ITEMS = 1000;
    public final int MAX_ORDERS_PER_CUSTOMER = 50;
    public final int MIN_LINE_ITEMS_PER_ORDER = 3;
    public final int MAX_LINE_ITEMS_PER_ORDER = 10;
    public final int AVE_ITEMS_IN_STOCK_PER_WAREHOUSE = 100;
    public final int MIN_ITEMS_IN_STOCK_PER_WAREHOUSE = 1;
    //private final int MAX_ITEMS_IN_STOCK_PER_WAREHOUSE = 200;
    

    // address of the server
	private static final String SERVER_ADDR = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";
	
    /**
    * Main method
    */
    public static void main(String[] args)
    {
        new DBLoader(true);
    }

    /**
    * Copy constructor
    */
    public DBLoader(DBLoader loader)
    {
        this.con = loader.getConnection();
        this.itemCost = loader.getItemCost();
        this.currOrderID = loader.getCurrOrderID();
        this.currLineID = loader.getCurrLineID();
        this.last20Orders = loader.getLast20Orders();
        this.last20Index = loader.getLast20Index();
    }
	

    /**
    * Constructor for the DBLoader. Opens the database connection with the
    * credentials passed from the user.
    */
	public DBLoader(boolean askForParameters)
    {
        currOrderID = new HashMap<String, Integer>();
        currLineID = new HashMap<String, Integer>();
        

        scan = new Scanner(System.in);

        // ask the user if they want to switch databases
        System.out.println("\nWelcome to the database loader!");
		System.out.println("Today is " + getTodaysDate());
        System.out.println("\nThe default database is : " + SERVER_ADDR);
        String answer = null;
        boolean done = false;
        do
        {
            System.out.print("If you wish to use a different database, enter the address now,\nor press enter to use the default: ");
            answer = scan.nextLine();

            if (!answer.equals(""))
            {
                System.out.println("You entered: " + answer);
                System.out.print("Is this correct? (y/n): ");
                answer = scan.nextLine();

                if (answer.toUpperCase().equals("Y"))
                {
                    System.out.println("Using " + answer + " as the database server.");
                    server = answer;
                    done = true;
                }        
            }
            else
            {
                System.out.println("Using default database.");
                server = SERVER_ADDR;
                done = true;
            }
        }
        while (!done);

        // prompt the user for their credentials
        System.out.print("Please enter your username: ");
        username = scan.nextLine();

        Console console = System.console();
        System.out.print("Password for " + username + ": ");
        char[] pwd = console.readPassword();

        password = new String(pwd);

        // open the connection to the server
        try
        {
            con = openConnection();
            con.setAutoCommit(false);
        }
		catch (SQLException e)
        {
            System.out.println("Error setting up connection.");
        }

        ///////////////////////////////////
        // Load the data into the database
        ///////////////////////////////////

      	// drop and recreate the tables
        // ask for the number of warehouses, stations, customers, and items they want to create
        boolean gotIt = false;
        int oldWarehouses = WAREHOUSES;
        int oldStations = STATIONS_PER_WAREHOUSE;
        int oldCustomers = CUSTOMERS_PER_STATION;
        int oldItems = ITEMS;
        String readIn;

        // ask for user input if askForParameters is true
        if (askForParameters)
        {
            do
            {
                WAREHOUSES = oldWarehouses;
                STATIONS_PER_WAREHOUSE = oldStations;
                CUSTOMERS_PER_STATION = oldCustomers;
                ITEMS = oldItems;
                try
                {
                    System.out.print("Number of warehouses? (Enter for default of " + WAREHOUSES + "): ");
                    readIn = scan.nextLine();
                    if (!readIn.equals(""))
                        WAREHOUSES = Integer.parseInt(readIn);
                    System.out.print("Number of stations per warehouse? (Enter for default of "+ STATIONS_PER_WAREHOUSE + "): ");
                    readIn = scan.nextLine();
                    if (!readIn.equals(""))
                        STATIONS_PER_WAREHOUSE = Integer.parseInt(readIn);
                    System.out.print("Number of customers per station? (Enter for default of " + CUSTOMERS_PER_STATION + "): ");
                    readIn = scan.nextLine();
                    if (!readIn.equals(""))
                        CUSTOMERS_PER_STATION = Integer.parseInt(readIn);
                    System.out.print("Number of unique items? (Enter for default of " + ITEMS + "): ");
                    readIn = scan.nextLine();
                    if (!readIn.equals(""))
                        ITEMS = Integer.parseInt(readIn);

                    gotIt = true;
                }
                catch (NumberFormatException e)
                {
                    System.out.println("Invalid number format, whole integers only please.");
                }
            }
            while (!gotIt);
        }
        else
        {
            System.out.println("Using default values to populate database.");
        }

        last20Orders = new String[WAREHOUSES][STATIONS_PER_WAREHOUSE][20];
        last20Index = new int[WAREHOUSES][STATIONS_PER_WAREHOUSE];

        for (int i = 0; i < last20Index.length; i++)
        {
            for (int j = 0; j < last20Index[i].length; j++)
                last20Index[i][j] = 0;
        }

        // initialize the database
		initDatabase();
		
    	// populate the tables with generated data
    	populateTables();






        ///////////////////////////////////////
        // Loop to ask user for function input
        ///////////////////////////////////////
        if (askForParameters)
        {
            do
            {
                System.out.println("\nWhat would you like to do?");
                System.out.println("-------------------------------------------");
                System.out.println("c - Create a new order");
                System.out.println("p - Process a payment");
                System.out.println("s - Check on the status of a recent order");
                System.out.println("d - Perform a warehouse delivery");
                System.out.println("l - Check the stock level of a station");
    			System.out.println("r - Rebuild the database");
                System.out.println("q - Quit");
                System.out.print("(c, p, s, d, l, r, q) ?: ");
                answer = scan.nextLine();

                // create an order
                if (answer.toUpperCase().equals("C"))
                {
                    try
                    {
                        System.out.print("Enter the warehouse ID: ");
                        int warehouseNum = Integer.parseInt(scan.nextLine());
                        System.out.print("Enter the station ID: ");
                        int stationNum = Integer.parseInt(scan.nextLine());
                        System.out.print("Enter the customer ID: ");
                        int customerNum = Integer.parseInt(scan.nextLine());
                        System.out.print("How many items in the order?: ");
                        int itemNum = Integer.parseInt(scan.nextLine());

                        int[] items = new int[itemNum];
                        int[] counts = new int[itemNum];

                        for (int i = 0; i < itemNum; i++)
                        {
                            System.out.print("Item " + (i + 1) + " ID: ");
                            items[i] = Integer.parseInt(scan.nextLine());
                            System.out.print("Item " + (i + 1) + " count: ");
                            counts[i] = Integer.parseInt(scan.nextLine());
                        }

                        newOrder(warehouseNum, stationNum, customerNum, items, counts, itemNum);
                    }
                    catch (NumberFormatException e)
                    {
                        System.out.println("Error parsing input. " + e.toString());
                    }
                }


                // process a payment
                else if (answer.toUpperCase().equals("P"))
                {
    				try
    				{
    					System.out.print("Enter the warehouse ID: ");
    					int warehouse = Integer.parseInt(scan.nextLine());
    					System.out.print("Enter the station ID: ");
    					int station = Integer.parseInt(scan.nextLine());
                        System.out.print("Enter the customer ID: ");
                        int customer = Integer.parseInt(scan.nextLine());
    					System.out.print("Enter the payment amount: ");
    					String ans = scan.nextLine();
    					BigDecimal payment = new BigDecimal(ans);
    					processPayment(warehouse, customer, station, payment);
    				}
    				catch (NumberFormatException e)
    				{
    					System.out.println("Error parsing input. " + e.toString());
    				}
    				
                }


                // check order status
                else if (answer.toUpperCase().equals("S"))
                {
    				try
    				{
    					System.out.print("Enter the warehouse ID: ");
    					int warehouse = Integer.parseInt(scan.nextLine());
                        System.out.print("Enter the station ID: ");
                        int station = Integer.parseInt(scan.nextLine());
    					System.out.print("Enter the customer ID: ");
    					int customer = Integer.parseInt(scan.nextLine());
    					getOrderStatus(warehouse, station, customer);
    				}
    				catch (NumberFormatException e)
    				{
    					System.out.println("Error parsing input " + e.toString());
    				}
                }


                // warehouse delivery
                else if (answer.toUpperCase().equals("D"))
                {
    				System.out.print("Enter the warehouse ID: ");
    				int warehouse = Integer.parseInt(scan.nextLine());
    				getDeliveryTransaction(warehouse); 
                }


                // check stock level
                else if (answer.toUpperCase().equals("L"))
                {
                    try
                    {
                        System.out.print("Enter the warehouse ID: ");
                        int warehouseNum = Integer.parseInt(scan.nextLine());
                        System.out.print("Enter the station ID: ");
                        int stationNum = Integer.parseInt(scan.nextLine());
                        System.out.print("Enter the stock threshold: ");
                        int thresholdNum = Integer.parseInt(scan.nextLine());

                        stockLevel(warehouseNum, stationNum, thresholdNum);
                    }
                    catch (NumberFormatException e)
                    {
                        System.out.println("Error parsing input. " + e.toString());
                    }
                }
    			//reinitializes the database (Hi, Nick!)
    			else if (answer.toUpperCase().equals("R"))
    			{
    				initDatabase();
    				populateTables();
    			}
    			
                else if (!answer.toUpperCase().equals("Q"))
                {
                    System.out.println("Invalid input, please try again.\n");
                }
            }
            while (!answer.toUpperCase().equals("Q"));


            // close the connection and scanner
            try
            {
                con.close();
            }
            catch (SQLException e)
            {
                System.out.println("Error closing connection.");
            }
            scan.close();
        }
	}












/********************************************************************************************************************************************
* Methods for the intialization of the database - Milestone 1
*********************************************************************************************************************************************/
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
            System.exit(4);
        }

        return connection;
    }

    /**
    * Creates the tables in the database
    */
    public void initDatabase()
    {
		System.out.println("\ncreating tables...");
        String[] dropStatements = new String[7];

		dropStatements[0] = "drop table Warehouses cascade constraints";
		String createWarehouses = "create table Warehouses (" +
			"warehouse_id number(3) not null, " + 
			"name varchar2(20), " +
			"address varchar2(30), " +
			"city varchar2(20), " +
			"state varchar2(2), " +
			"zip varchar2(5), " +
			"tax_rate number (3, 2), " +
			"sum_sales number (20, 2), " +
			"constraint Warehouses_pk primary key(warehouse_id) )";
		

		dropStatements[1] = "drop table Stations cascade constraints";
		String createStations = "create table Stations (" +
			"station_id number(3) not null, " +
			"warehouse_id number(3) not null, " +
			"name varchar2(20), " +
			"address varchar2(30), " +
			"city varchar2(20), " +
			"state varchar2(5), " +
			"zip varchar2(5), " +
			"tax_rate number(3, 2), " +
			"sum_sales number(12, 2), " +
			"constraint Stations_pk primary key(station_id, warehouse_id), " +
			"constraint Stations_fk foreign key(warehouse_id) references Warehouses(warehouse_id) )";
	

		dropStatements[2] = "drop table Customers cascade constraints";
		String createCustomers = "create table Customers (" +
			"customer_id number(6) not null, " +
			"station_id number(3) not null, " +
            "warehouse_id number(3) not null, " +
			"fname varchar2(20), " +
			"mi varchar2(1), " +
			"lname varchar2(20), " + 
			"address varchar2(30), " +
			"city varchar2(20), " +
			"state varchar2(2), " +
			"zip varchar2(6), " +
			"phone varchar2(14), " +
			"join_date varchar2(10), " +
			"discount number(5, 2), " +
			"balance number(20, 2), " +
			"paid_amount number(20, 2), " +
			"total_payments number(20), " +
			"total_deliveries number(20), " +
			"constraint Customers_pk primary key(customer_id, station_id), " +
			"constraint Customers_fk foreign key(station_id, warehouse_id) references Stations(station_id, warehouse_id) )";
		

		dropStatements[3] = "drop table Orders cascade constraints";
		String createOrders = "create table Orders (" +
			"order_id number(10) not null, " +
			"customer_id number(6), " +
            "station_id number(3), " +
            "warehouse_id number(3), " +
			"order_date varchar2(10), " +
			"completed number(1), " +
			"line_item_count number(10), " +
			"constraint Orders_pk primary key(order_id, customer_id, station_id, warehouse_id), " +
			"constraint Orders_fk1 foreign key(station_id, warehouse_id) references Stations(station_id, warehouse_id), " +
            "constraint Orders_fk2 foreign key(customer_id, station_id) references Customers(customer_id, station_id) )";
		
        dropStatements[4] = "drop table Items cascade constraints";
        String createItems = "create table Items (" +
            "item_id number(15) not null, " +
            "name varchar2(20), " +
            "price number(5, 2)," +
            "constraint Items_pk primary key(item_id) )";
		
		dropStatements[5] = "drop table StockItems cascade constraints";
		String createStockItems = "create table StockItems (" +
			"item_id number(15) not null, " +
            "warehouse_id number(3), " +
			"in_stock number(20), " +
			"sold_this_year number(15), " +
			"included_in_orders number(15), " +
			"constraint StockItems_pk primary key(item_id, warehouse_id), " +
            "constraint StockItems_fk1 foreign key(item_id) references Items(item_id), " +
			"constraint StockItems_fk2 foreign key(warehouse_id) references Warehouses(warehouse_id) )";
		

		dropStatements[6] = "drop table LineItems cascade constraints";
		String createLineItems = "create table LineItems (" +
			"line_id number(15) not null, " +
            "order_id number(10), " +
            "customer_id number(6), " +
            "station_id number(3), " +
            "warehouse_id number(3), " + 
			"item_id number(15), " +
			"quantity number(5), " +
			"amount number (5, 2), " +
			"delivery_date varchar2(10), " +
			"constraint LineItems_pk primary key(line_id, order_id, customer_id, station_id, warehouse_id), " +
			"constraint LineItems_fk1 foreign key(order_id, customer_id, station_id, warehouse_id) references Orders(order_id, customer_id, station_id, warehouse_id), " +
            "constraint LineItems_fk2 foreign key(item_id, warehouse_id) references StockItems(item_id, warehouse_id) )";


        // create the statement and start a transaction
        try
        {
            statement = con.createStatement();
            statement.executeUpdate(startTransaction);
        }
        catch (SQLException e)
        {
            System.out.println("Error creating statement");
        }

        // drop the sequences, triggers, and tables, if they exist
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


        // create the tables
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
            statement.executeUpdate(createItems);
            System.out.println("Items");
            statement.executeUpdate(createStockItems);
            System.out.println("StockItems");
			statement.executeUpdate(createLineItems);
            System.out.println("LineItems");
			statement.executeUpdate("COMMIT");
			
		} catch(SQLException Ex) {
			System.out.println("Error running create queries. " + Ex.toString());
            System.exit(1);
		}

        System.out.println("Tables successfully created.\n");
    }



    /**
    * Generates random data to populate the created tables
    */
    private void populateTables()
    {
        System.out.println("Generating data...");
        // define the insert statements
        String warehousesString = "insert into Warehouses (warehouse_id, name, address, city, state, zip, tax_rate, sum_sales)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?)";
        String stationsString = "insert into Stations (station_id, warehouse_id, name, address, city, state, zip, tax_rate, sum_sales)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String customersString = "insert into Customers (customer_id, station_id, warehouse_id, fname, mi, lname, address, city, state, zip, phone, "
        + "join_date, discount, balance, paid_amount, total_payments, total_deliveries)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String ordersString = "insert into Orders (order_id, customer_id, station_id, warehouse_id, order_date, completed, line_item_count)"
        + "values (?, ?, ?, ?, ?, ?, ?)";
        String lineItemsString = "insert into LineItems (line_id, order_id, customer_id, station_id, warehouse_id, item_id, quantity, amount, delivery_date)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String itemsString = "insert into Items (item_id, name, price) values (?, ?, ?)";
        String stockItemsString = "insert into StockItems (item_id, warehouse_id, in_stock, sold_this_year, included_in_orders)"
        + "values (?, ?, ?, ?, ?)";

        // instantiate the prepared statements
        PreparedStatement insertWarehouses = null;
        PreparedStatement insertStations = null;
        PreparedStatement insertCustomers = null;
        PreparedStatement insertOrders = null;
        PreparedStatement insertLineItems = null;
        PreparedStatement insertItems = null;
        PreparedStatement insertStockItems = null;
        try
        {
            insertWarehouses = con.prepareStatement(warehousesString);
            insertStations = con.prepareStatement(stationsString);
            insertCustomers = con.prepareStatement(customersString);
            insertOrders = con.prepareStatement(ordersString);
            insertLineItems = con.prepareStatement(lineItemsString);
            insertItems = con.prepareStatement(itemsString);
            insertStockItems = con.prepareStatement(stockItemsString);
        }
        catch (SQLException e)
        {
            System.out.println("Error creating prepared statements.");
            System.exit(1);
        }


        /********
        * Generate the data
        *********/
        Random rand = new Random(System.nanoTime());
        float warehouseTotal = 0;
        float stationTotal = 0;
        float customerTotal = 0;

        // HashMaps are used to keep track of item costs, item order counts, and ytd sold counts.
        // this enables us to update the counts without querying the database
        HashMap<Integer, Integer> ytdSoldCounts = new HashMap<Integer, Integer>(ITEMS);
        HashMap<Integer, Integer> itemOrderCounts = new HashMap<Integer, Integer>(ITEMS);
        itemCost = new HashMap<Integer, Float>(ITEMS);
        try
        {
            // generate the items
            for (int i = 1; i <= ITEMS; i++)
            {
                // add initial value to hashmap
                float cost = getPrice(rand);
                itemCost.put(i, new Float(cost));

                insertItems.setInt(1, i);
                insertItems.setString(2, getName(rand));
                insertItems.setString(3, twoDecimals(cost));
                insertItems.addBatch();
            }
            //System.out.println("Items statements created.");

            // generate the warehouses
            for (currWarehouseID = 1; currWarehouseID <= WAREHOUSES; currWarehouseID++)
            {
                warehouseTotal = 0;

                // generate the stock items
                for (int j = 1; j <= ITEMS; j++)
                {
                    // insert initial entries into the hashmaps
                    ytdSoldCounts.put(j, 0);
                    itemOrderCounts.put(j, 0);

                    insertStockItems.setInt(1, j);
                    insertStockItems.setInt(2, currWarehouseID);
                    insertStockItems.setInt(3, randomMean(rand));
                    insertStockItems.setString(4, "0");
                    insertStockItems.setString(5, "0");
                    insertStockItems.addBatch();
                }
                //System.out.println("StockItems statements created.");

                // generate the stations
                for (currStationID = 1; currStationID <= STATIONS_PER_WAREHOUSE; currStationID++)
                {
                    stationTotal = 0;
                    double taxRate = getRandomTax(rand);

                    // generate the customers
                    for (currCustomerID = 1; currCustomerID <= CUSTOMERS_PER_STATION; currCustomerID++)
                    {
                        customerTotal = 0;

                        // generate the orders
                        int orderNum = rand.nextInt(MAX_ORDERS_PER_CUSTOMER) + 1;
                        currOrderID.put(getCustomerKey(currWarehouseID, currStationID, currCustomerID), orderNum + 1);          // store the current orderID sequence for the customer
                        for (int l = 1; l <= orderNum; l++)
                        {
                            // date for order placement
                            String theDate = getDate(rand);

                            // generate the number of line items to put in the order
                            int lineCount = rand.nextInt((MAX_LINE_ITEMS_PER_ORDER - MIN_LINE_ITEMS_PER_ORDER) + 1) + MIN_LINE_ITEMS_PER_ORDER;
                            currLineID.put(getOrderKey(currWarehouseID, currStationID, currCustomerID, l), lineCount + 1);      // store the current lineID sequence for the order

                            // generate the line items for the order
                            for (int m = 1; m <= lineCount; m++)
                            {
                                int itemID = rand.nextInt(ITEMS) + 1;
                                int itemCount = rand.nextInt(10) + 1;
                                double lineTotal = (itemCost.get(itemID).floatValue() * itemCount) * taxRate;
                                customerTotal += lineTotal;

                                // update the hashmaps to reflect the sold and order numbers
                                ytdSoldCounts.put(itemID, new Integer(ytdSoldCounts.get(itemID).intValue() + itemCount));
                                itemOrderCounts.put(itemID, new Integer(itemOrderCounts.get(itemID).intValue() + 1));

                                insertLineItems.setInt(1, m);
                                insertLineItems.setInt(2, l);
                                insertLineItems.setInt(3, currCustomerID);
                                insertLineItems.setInt(4, currStationID);
                                insertLineItems.setInt(5, currWarehouseID);
                                insertLineItems.setInt(6, itemID);
                                insertLineItems.setInt(7, itemCount);
                                insertLineItems.setString(8, twoDecimals(lineTotal));
                                insertLineItems.setString(9, getDate(rand, theDate));
                                insertLineItems.addBatch();
                            }
                            //System.out.println("LineItems statements created.");

                            insertOrders.setInt(1, l);
                            insertOrders.setInt(2, currCustomerID);
                            insertOrders.setInt(3, currStationID);
                            insertOrders.setInt(4, currWarehouseID);
                            insertOrders.setString(5, theDate);
							insertOrders.setInt(6, 1);
                            insertOrders.setInt(7, lineCount);
                            insertOrders.addBatch();

                            enqueueOrder(currWarehouseID, currStationID, currCustomerID, l);

                        }
                        //System.out.println("Orders statements created.");

                        float balance = getPrice(rand, customerTotal);

                        insertCustomers.setInt(1, currCustomerID);
                        insertCustomers.setInt(2, currStationID);
                        insertCustomers.setInt(3, currWarehouseID);
                        insertCustomers.setString(4, getName(rand));
                        insertCustomers.setString(5, getMI(rand));
                        insertCustomers.setString(6, getName(rand));
                        insertCustomers.setString(7, getAddress(rand));
                        insertCustomers.setString(8, getName(rand));
                        insertCustomers.setString(9, getState(rand));
                        insertCustomers.setString(10, getZip(rand));
                        insertCustomers.setString(11, getPhone(rand));
                        insertCustomers.setString(12, getDate(rand));
                        insertCustomers.setString(13, getDiscount(rand));
                        insertCustomers.setString(14, twoDecimals(balance));
                        insertCustomers.setString(15, twoDecimals(customerTotal - balance));
                        insertCustomers.setInt(16, rand.nextInt(20) + 1);
                        insertCustomers.setInt(17, rand.nextInt(30) + 1);
                        insertCustomers.addBatch();
                        stationTotal += customerTotal;
                    }
                    //System.out.println("Customers statements created.");

                    insertStations.setInt(1, currStationID);
                    insertStations.setInt(2, currWarehouseID);
                    insertStations.setString(3, getName(rand));
                    insertStations.setString(4, getAddress(rand));
                    insertStations.setString(5, getName(rand));
                    insertStations.setString(6, getState(rand));
                    insertStations.setString(7, getZip(rand));
                    insertStations.setString(8, twoDecimals(taxRate));
                    insertStations.setString(9, twoDecimals(stationTotal));
                    insertStations.addBatch();
                    warehouseTotal += stationTotal;
                }
                //System.out.println("Stations statements created.");

                insertWarehouses.setInt(1, currWarehouseID);
                insertWarehouses.setString(2, getName(rand));
                insertWarehouses.setString(3, getAddress(rand));
                insertWarehouses.setString(4, getName(rand));
                insertWarehouses.setString(5, getState(rand));
                insertWarehouses.setString(6, getZip(rand));
                insertWarehouses.setString(7, twoDecimals(getRandomTax(rand)));
                insertWarehouses.setString(8, twoDecimals(warehouseTotal));
                insertWarehouses.addBatch();

            }
            //System.out.println("Warehouses statements created.");
        }
        catch (SQLException e)
        {
            System.out.println("Error generating statements");
        }

        // execute the batch inserts
        try
        {
            statement.executeUpdate(startTransaction);

            insertItems.executeBatch();
            System.out.println("Items inserted");
            insertWarehouses.executeBatch();
            System.out.println("Warehouses inserted");
            insertStockItems.executeBatch();
            System.out.println("StockItems inserted");
            insertStations.executeBatch();
            System.out.println("Stations inserted");
            insertCustomers.executeBatch();
            System.out.println("Customers inserted");
            insertOrders.executeBatch();
            System.out.println("Orders inserted");
            insertLineItems.executeBatch();
            System.out.println("LineItems inserted");

            insertItems.close();
            insertWarehouses.close();
            insertStockItems.close();
            insertStations.close();
            insertCustomers.close();
            insertOrders.close();
            insertLineItems.close();

            statement.executeUpdate("COMMIT");
        }
        catch (SQLException e)
        {
            System.out.println("Error executing the prepared statements. - " + e.toString());
            System.out.println(e.getErrorCode());
            System.exit(1);
        }

        // update the item order count and sold count on each item
        try
        {
            System.out.println("Updating StockItem counts...");
            String itemsSoldString = "update StockItems set sold_this_year = ? where item_id = ?";
            String orderCountString = "update StockItems set included_in_orders = ? where item_id = ?";
            PreparedStatement itemsSold = con.prepareStatement(itemsSoldString);
            PreparedStatement orderCount = con.prepareStatement(orderCountString);

            for (int i = 1; i <= ITEMS; i++)
            {
                itemsSold.setInt(1, ytdSoldCounts.get(i).intValue());
                itemsSold.setInt(2, i);
                orderCount.setInt(1, itemOrderCounts.get(i).intValue());
                orderCount.setInt(2, i);
                itemsSold.addBatch();
                orderCount.addBatch();
            }

            // execute update queries
            statement.executeUpdate(startTransaction);
            itemsSold.executeBatch();
            orderCount.executeBatch();
            itemsSold.close();
            orderCount.close();
            statement.executeUpdate("COMMIT");
            statement.close();
            System.out.println("StockItem counts successfully updated.");
        }
        catch (SQLException e)
        {
            System.out.println("Error updating item entries." + e.toString());
            System.exit(1);
        }

        System.out.println("All data inserted. Success!");
    }
	












/********************************************************************************************************************************************
* Methods for executing transations - Milestone 2
*********************************************************************************************************************************************/

    /**
    * Creates a new order with the passed information (3.1 in the milestone description document)
    * @param warehouse int containing the warehouse_id
    * @param station int containing the station_id
    * @param customer int containing the customer_id for the new order
    * @param items int array containing the item numbers in the order
    * @param counts int array containing the item counts in the order
    * @param totalCount int containing the sum of all line items in the order
    */
    public void newOrder(int warehouse, int station, int customer, int[] items, int[] counts, int totalItems)
    {
        //Random rand = new Random(System.nanoTime());
		Date date = getTodaysDate();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = df.format(date);
        String addOrderString = "insert into Orders (order_id, customer_id, station_id, warehouse_id, order_date, completed, line_item_count)"
        + "values (?, ?, ?, ?, ?, ?, ?)";
        String addLineItemString = "insert into LineItems (line_id, order_id, customer_id, station_id, warehouse_id, item_id, quantity, amount, delivery_date)"
        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Savepoint save = null;

        try
        {
            // prepare the statements
            PreparedStatement addOrder = con.prepareStatement(addOrderString);
            PreparedStatement addLineItem = con.prepareStatement(addLineItemString);
            int thisOrderID = currOrderID.get(getCustomerKey(warehouse, station, customer)).intValue();

            // set the fields in the addOrder statement
            addOrder.setInt(1, thisOrderID);
            addOrder.setInt(2, customer);
            addOrder.setInt(3, station);
            addOrder.setInt(4, warehouse);
            addOrder.setString(5, dateString);
            addOrder.setInt(6, 0);
            addOrder.setInt(7, totalItems);



            // iterate through items array and prepare the lineItem batch
            int thisLineID = 1;
            PreparedStatement updateStock = null;
			//System.out.println("items is this long " + items.length);
            for (int i = 0; i < items.length; i++)
            {
				//System.out.println("items " + i + "is " + items[i]);
                float lineTotal = itemCost.get(items[i]).floatValue() * counts[i];

                addLineItem.setInt(1, thisLineID);
                addLineItem.setInt(2, thisOrderID);
                addLineItem.setInt(3, customer);
                addLineItem.setInt(4, station);
                addLineItem.setInt(5, warehouse);
                addLineItem.setInt(6, items[i]);
                addLineItem.setInt(7, counts[i]);
                addLineItem.setString(8, twoDecimals(lineTotal));
                addLineItem.setString(9, "");
                addLineItem.addBatch();

                thisLineID++;

                // decrement the stock values of the warehouse
                updateStock = updateStock(warehouse, items[i], counts[i]);
            }

            // set up the transaction
            save = con.setSavepoint();
            statement = con.createStatement();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            //statement.executeUpdate("SET TRANSACTION READ WRITE");

            // execute the statements
            updateStock.executeBatch();
            addOrder.execute();
            addLineItem.executeBatch();

            // commit the transaction
            statement.executeUpdate("COMMIT");
            addOrder.close();
            addLineItem.close();
            statement.close();

            enqueueOrder(warehouse, station, customer, thisOrderID);

            // update the order and line numbers for that customer
            Integer temp = currOrderID.remove(getCustomerKey(warehouse, station, customer));
            currOrderID.put(getCustomerKey(warehouse, station, customer), temp + 1);
            currLineID.put(getOrderKey(warehouse, station, customer, thisOrderID), thisLineID);

            System.out.println("Order number " + thisOrderID + " successfully placed for:");
            System.out.println("customer " + customer + " of station " + station + ", warehouse " + warehouse);
			//con.commit();
            System.out.println("\n---------------------------------------------");
            System.out.println("New Order transaction committed successfully.");
            System.out.println("---------------------------------------------\n");
        }
        catch (SQLException e)
        {
            try
            {
                con.rollback(save);
            }
            catch (SQLException f)
            {}
			System.out.println("Error in new order is " + e.toString());
            System.out.println("\n-----------------------------------------------");
            System.out.println("Error inserting order, transaction rolled back.");
            System.out.println("-----------------------------------------------\n");
        }
    }

    /**
    * Decreases the stock of the passed item in the passed warehouse by count.
    * Called as part of the newOrder() method.
    * @param warehouse int containing the warehouse_id
    * @param item int containing the item_id
    * @param count int containing the item count to be decremented
    */
    private PreparedStatement updateStock(int warehouse, int item, int count) throws SQLException
    {
        String getCountsString = "select in_stock, sold_this_year, included_in_orders " +
            "from StockItems where item_id = ? and warehouse_id = ?";
        String updateStockString = "update StockItems set in_stock = ?, sold_this_year = ?, included_in_orders = ? " +
            "where item_id = ? and warehouse_id = ?";

        // prepare the statements
        PreparedStatement getCounts = con.prepareStatement(getCountsString);
        PreparedStatement updateStock = con.prepareStatement(updateStockString);
        getCounts.setInt(1, item);
        getCounts.setInt(2, warehouse);

        // get the current counts from the database
        ResultSet results = getCounts.executeQuery();
        results.next();

        // update the values appropriately
        int newStock = results.getInt("in_stock");
        int newSold = results.getInt("sold_this_year");
        int newOrders = results.getInt("included_in_orders");
        newStock = newStock - count;
        newSold = newSold + count;
        newOrders++;

        // add the values to the batch
        updateStock.setInt(1, newStock);
        updateStock.setInt(2, newSold);
        updateStock.setInt(3, newOrders);
        updateStock.setInt(4, item);
        updateStock.setInt(5, warehouse);
        updateStock.addBatch();

        return updateStock;
    }

	/*
    * processes the Payment, updates appropriate tuples, uses BigDecimal for payment as it is recommended to be used with currencies
	* (3.2 in the milestone description document)
    */
	public void processPayment(int warehouse_id, int customer_id, int station_id, BigDecimal payment) 
	{
		Savepoint save = null;
		try 
		{
            System.out.println("Starting to process payment transaction for customer " + customer_id + " of station " + station_id);
            System.out.println("Here is the pre-payment account status:");

            PreparedStatement updateBalance = decrementBalance(warehouse_id, customer_id, station_id, payment);
			
            PreparedStatement updatePaidAmount = updatePaidAmount(warehouse_id, customer_id, station_id, payment);
			
            PreparedStatement updateTotalPayments = updateTotalPayments(warehouse_id, customer_id, station_id);
			
            PreparedStatement updateYTDSalesWare = updateYTDSalesWare(warehouse_id, payment);
			
			PreparedStatement updateYTDSalesStation = updateYTDSalesStation(warehouse_id, station_id, payment);

    		
            // set the transaction
            save = con.setSavepoint();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            //Statement startTransaction = con.createStatement();
            //startTransaction.executeUpdate("set transaction read write");

            // execute the statements
			showAccountStatus(warehouse_id, station_id, customer_id);
            updateBalance.executeBatch();
			//System.out.println("We...");
            updatePaidAmount.executeBatch();
			//System.out.println("Are...");
            updateTotalPayments.executeBatch();
			//System.out.println("Here...");
            updateYTDSalesWare.executeBatch();
			updateYTDSalesStation.executeBatch();
            

            System.out.println("Here is the post-payment account status:");
            showAccountStatus(warehouse_id, station_id, customer_id);
            // commit the transaction
            con.commit();
			
            System.out.println("\n---------------------------------------------------");
            System.out.println("Process payment transaction committed successfully.");
            System.out.println("---------------------------------------------------\n");
		}
		catch (SQLException e)
		{
            try
            {
                con.rollback(save);
            }
            catch (SQLException f)
            {}
			System.out.println("Error in proccess payment is " + e.toString());
            System.out.println("\n--------------------------------------------------");
			System.out.println("Error processing payment, transaction rolled back.");
            System.out.println("--------------------------------------------------\n");
		}
	}
	
	/* 
	* A helper method. Used in processPayment method. 	
	* Displays the customer's account status, which includes the customer's name, discount, balance, paid amount, the number of payments and deliveries
	*/ 
	private void showAccountStatus(int warehouse_id, int station_id, int customer_id) throws SQLException
	{
		//System.out.println("The following is the account status for the customer " + customer_id + " of the station " + station_id + " of the warehouse " + warehouse_id);
		String getAccountStatusString = "select * from Customers where warehouse_id = ? and station_id = ? and customer_id = ?";
		PreparedStatement getAccountStatus = con.prepareStatement(getAccountStatusString);
		getAccountStatus.setInt(1, warehouse_id);
		getAccountStatus.setInt(2, station_id);
		getAccountStatus.setInt(3, customer_id);
		resultSet = getAccountStatus.executeQuery();
		while (resultSet.next())
		{
			String name = resultSet.getString(4) + " " + resultSet.getString(5) + " " + resultSet.getString(6);
			String address = resultSet.getString(7) + " " + resultSet.getString(8) + ", " + resultSet.getString(9) + " " + resultSet.getString(10);
			System.out.print("Name: " + name + "\n" + address + "\n");
			System.out.println("Discount....." + resultSet.getDouble(13)); 
			System.out.println("Balance......" + cf.format(resultSet.getBigDecimal(14))); 
			System.out.println("Paid Amount.." + cf.format(resultSet.getBigDecimal(15)));
			System.out.println("Payments....." + resultSet.getInt(16));
			System.out.println("Deliveries..." + resultSet.getInt(17));
		}
		resultSet.close();
	}
	

	/**
	* The main method for the Order status transaction (3.3 in the milestone 2 description document) 
	* @param warehouse_id, station_id, customer_id
	* prints the table of the order status
	*/
	public void getOrderStatus(int warehouse_id, int station_id, int customer_id)
	{
        Savepoint save = null;
		try 
		{
            // set transaction and execute
            save = con.setSavepoint();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            // Statement stmt = con.createStatement();
 //            stmt.executeUpdate("SET TRANSACTION READ ONLY");
			//System.out.println("Getting order status for " + customer_id + " from the station " + station_id + "of the warehouse " + warehouse_id); //mostly for debugging purposes	
			ResultSet rs1 = getMostRecentOrders(warehouse_id, station_id, customer_id);
            
            
			while (rs1.next())
			{
				int order_id = rs1.getInt(1);
				String order_date = rs1.getString(5);
				getOrderDetails(order_id, customer_id, station_id, warehouse_id, order_date);
			}
			
			con.commit();
			rs1.close();
            System.out.println("\n---------------------------------------------");
            System.out.println("Order status transaction committed successfully.");
            System.out.println("---------------------------------------------\n");
		}
		
		catch (SQLException e)
		{
            try
            {
                con.rollback(save);
            }
            catch (SQLException f)
            {}
				System.out.println("Error in order status is: " + e.toString());
            System.out.println("\n--------------------------------------------------------");
			System.out.println("Error getting the order status. Transaction rolled back.");
            System.out.println("--------------------------------------------------------\n");
		}
		
	}
	
	/** 
	* A helper method. Used in getOrderStatus() method
	* Gathers and displays the details of a specific order
 	*/
	private void getOrderDetails(int order_id, int customer_id, int station_id, int warehouse_id, String order_date) throws SQLException
	{
		System.out.println("Checking on the order details for the order number " + order_id + " placed on " + order_date);
		String getOrderDetailsString = "select item_id, quantity, amount, delivery_date from LineItems where warehouse_id =? and station_id = ? and customer_id = ? and order_id = ?";
		preparedStatement = con.prepareStatement(getOrderDetailsString);
		preparedStatement.setInt(1, warehouse_id);
		preparedStatement.setInt(2, station_id);
		preparedStatement.setInt(3, customer_id);
		preparedStatement.setInt(4, order_id);
		resultSet = preparedStatement.executeQuery();
		System.out.println("Item number \t Quantity \t Amount Due \t Delivery Date\t ");
		System.out.println("---------- \t --------- \t -----------\t -------------\t ");
		while (resultSet.next())
		{
			System.out.print("" + resultSet.getInt(1));
			System.out.print("\t\t " + resultSet.getInt(2));
			System.out.print("\t\t" + cf.format(resultSet.getBigDecimal(3)));
			System.out.println("\t\t" + resultSet.getString(4));
		}
		resultSet.close();
	}
	/**
	* A helper method. Used in getOrderStatus() method
	* Returns a ResultSet object containing the information for the most recent order. 
	*
	*/
	private ResultSet getMostRecentOrders(int warehouse_id, int station_id, int customer_id) throws SQLException
	{
		System.out.println("Getting the most recent order... ");
		
		String getMostRecentOrdersString = "select * from Orders where order_date in" +
            "(select max(order_date) from Orders where warehouse_id = ? and station_id=? and customer_id= ? )" +
            "and warehouse_id = ? and station_id = ? and customer_id = ? ";
		PreparedStatement getMostRecentOrders = con.prepareStatement(getMostRecentOrdersString);
		getMostRecentOrders.setInt(1, warehouse_id);
		getMostRecentOrders.setInt(2, station_id);
		getMostRecentOrders.setInt(3, customer_id);
		getMostRecentOrders.setInt(4, warehouse_id);
		getMostRecentOrders.setInt(5, station_id);
		getMostRecentOrders.setInt(6, customer_id);
		
		resultSet = getMostRecentOrders.executeQuery();

		return resultSet;
	}
	
	/**
	* The main method for the Delivery Transaction (3.4 in the Milestone 2 document)
	* Processes the delivery for the whole warehouse. 
	*/
	public void getDeliveryTransaction(int warehouse_id) 
	{
		System.out.println("Preparing the delivery transaction...");
        Savepoint save = null;
    	try
        {
            save = con.setSavepoint();
            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            Statement stmt = con.createStatement();
            //stmt.executeUpdate("SET TRANSACTION READ WRITE");

			String theDeliveredString = "select * from Orders where completed = ? and warehouse_id = ?";
			PreparedStatement theDelivered = con.prepareStatement(theDeliveredString);
			theDelivered.setInt(1, 0);
			theDelivered.setInt(2, warehouse_id);
			resultSet = theDelivered.executeQuery();
			//con.commit();
			if (!resultSet.isBeforeFirst() ) {    
			 System.out.println("All line items in this warehouses have been delivered. No new deliveries will be made."); 
			} 
			else
			{
				while (resultSet.next())
				 {
					 int order = resultSet.getInt(1);
					 int customer = resultSet.getInt(2);
					 int station = resultSet.getInt(3);
					 int warehouse = resultSet.getInt(4);
					 setCompleted(order, customer, station, warehouse); //set completed to 1
					 addDeliveryDate(order,customer,station, warehouse); //add the delivery date
					 updateDeliveries(warehouse, customer, station);//update the delivery count
					 System.out.println("The following line items for the order " + order + " have been delivered and added to the customer's total:");
					 System.out.println("item id\tquantity\tamount\ttax rate\ttax amount\ttotal\tdelivery date" );
					 System.out.println("-------\t--------\t------\t--------\t----------\t-----\t-------------" );
					 ResultSet rs = getCharge(order, customer, station, warehouse);
					 while (rs.next())
					 {
						 System.out.print(rs.getInt(6) + "\t" + rs.getInt(7) + "\t\t");
						 BigDecimal cost = rs.getBigDecimal(8); //getting the amount
						 System.out.print(cf.format(cost) + "\t");
						 //int quantity = rs.getInt(7); // getting the quantity
						 BigDecimal tax = getTax(station); //getting the tax
						 BigDecimal tax_amount = cost.multiply(tax);//tax amount
						 System.out.print(tax + "\t\t"+ cf.format(tax_amount) + "\t\t");
						 BigDecimal total = calculateCost(cost, tax);
						 System.out.print(cf.format(total) + "\t" + rs.getString(9) + "\n");
						 incrementBalance(warehouse, customer, station, total); //increment customer's balance
					 }
					 rs.close();
			 	 }
			 }
			 resultSet.close();
             con.commit();

            System.out.println("\n--------------------------------------------");
            System.out.println("Delivery transaction committed successfully.");
            System.out.println("--------------------------------------------\n");
		 }
		 catch(SQLException e)
		 {
            try
            {
                con.rollback(save);
            }
            catch (SQLException f)
            {}
			System.out.println("Error in delivery is: " + e.toString());	
			System.out.println("\n---------------------------------------------------------------");
            System.out.println("Error processing delivery transaction, transaction rolled back.");
            System.out.println("---------------------------------------------------------------\n");
		 }
	}
	
	/**
	* A helper method used to calculate the price with the tax. 
	*/
	private BigDecimal calculateCost(BigDecimal amount, BigDecimal tax)
	{
		BigDecimal taxAmount = amount.multiply(tax);
		BigDecimal totalCost = amount.add(taxAmount);
		
		return totalCost;
	}
	
	/**
	* A helper method used to add the delivery date to the LineItems of the Order
	*/
	private void addDeliveryDate(int order_id, int customer_id, int station_id, int warehouse_id) throws SQLException
	{
		// Savepoint save = null;
//         save = con.setSavepoint();
//         con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		
		Date date = getTodaysDate();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = df.format(date);
		//System.out.println("Adding today's date of " + dateString + " to the database");

		String addDeliveryString = "update LineItems set delivery_date = ? where order_id=? and customer_id=? and station_id=? and warehouse_id=?";
		PreparedStatement addDelivery = con.prepareStatement(addDeliveryString);
		addDelivery.setString(1, dateString);
		addDelivery.setInt(2, order_id);
		addDelivery.setInt(3, customer_id);
		addDelivery.setInt(4, station_id);
		addDelivery.setInt(5, warehouse_id);
		addDelivery.executeUpdate();
		
		//con.commit();
	}

    /*
	* A helper method. Gets the amount to charge the customer.
	*/
	private ResultSet getCharge(int order_id, int customer_id, int station_id, int warehouse_id) throws SQLException
	{ 
		//System.out.println("Getting the charge for the order number " + order_id + " of the customer number " + customer_id + " in the station " + station_id + "of the warehouse " + warehouse_id);
		String getChargeString = "select * from LineItems where order_id = ? and customer_id = ? and station_id = ? and warehouse_id = ?";
		PreparedStatement getCharge = con.prepareStatement(getChargeString);
		getCharge.setInt(1, order_id);
		getCharge.setInt(2, customer_id);
		getCharge.setInt(3, station_id);
		getCharge.setInt(4, warehouse_id);
		rs = getCharge.executeQuery();

		return rs;
	}

	/*
	* A helper method. Returns the tax rate for a distribution station
	*/
	private BigDecimal getTax(int station_id) throws SQLException
	{
		BigDecimal rate = new BigDecimal(0);

		String getTaxString = "select * from Stations where station_id = ?";
		PreparedStatement getTax = con.prepareStatement(getTaxString);
		getTax.setInt(1, station_id);
		resultSet = getTax.executeQuery();
		while (resultSet.next())
		{
			rate = resultSet.getBigDecimal(8);
		}
		//System.out.println("The rate is " + rate);

		return rate;
	}

	/* 
	* A helper method. Flips the completed attribute to 1(true) in the Orders table to keep track of completed orders.
	*/
	private void setCompleted(int order_id, int customer_id, int station_id, int warehouse_id) throws SQLException
	{
		//System.out.println("Updating the order of id " + order_id + " to complete.");
        // Savepoint save = null;
// 		save = con.setSavepoint();
//         con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		
		String setCompletedString = "update Orders set completed=1 where order_id = ? and customer_id = ? and station_id = ? and warehouse_id = ?";
		PreparedStatement setCompleted = con.prepareStatement(setCompletedString);
		setCompleted.setInt(1, order_id);
		setCompleted.setInt(2, customer_id);
		setCompleted.setInt(3, station_id);
		setCompleted.setInt(4, warehouse_id);
		setCompleted.executeUpdate();
		//con.commit();
	}
	
	/* 
	* A helper method. It returns the current date as the Date object
	*/
	private Date getTodaysDate()
	{
		Date date = Calendar.getInstance().getTime();
		//java.sql.Date sqlDate = new java.sql.Date(date.getTime());
 		//System.out.println(date);
 		//System.out.println("sqldate is " + sqlDate);
		return date;
	}
	
	/* 
	* A helper method. It adds the current order charge to the customer's total
	*/
	private void incrementBalance(int warehouse_id, int customer_id, int station_id, BigDecimal charge) throws SQLException
	{
		String incrementBalanceString = "update Customers set balance = balance + ? where warehouse_id = ? and customer_id = ? and station_id = ?";
		//System.out.println(incrementBalanceString);
		PreparedStatement incrementBalance = con.prepareStatement(incrementBalanceString);
		incrementBalance.setBigDecimal(1, charge);
		incrementBalance.setInt(2, warehouse_id);
		incrementBalance.setInt(3, customer_id);
		incrementBalance.setInt(4, station_id);
		incrementBalance.executeUpdate();
	}

	/*
	* A helper method. It increments the number of deliveries by one
	*/
	private void updateDeliveries(int warehouse_id, int customer_id, int station_id) throws SQLException
	{
		System.out.println("Incrementing the total deliveries for the customer number " + customer_id + "of the station " + station_id + " in the warehouse " + warehouse_id);
		String updateDeliveriesString = "update Customers set total_deliveries = total_deliveries + 1 where warehouse_id = ? and customer_id = ? and station_id = ?";
		PreparedStatement updateDeliveries = con.prepareStatement(updateDeliveriesString);
		updateDeliveries.setInt(1, warehouse_id);
		updateDeliveries.setInt(2, customer_id);
		updateDeliveries.setInt(3, station_id);
		updateDeliveries.executeUpdate();
	}
	
	/**
	* Decrements the outstanding balance based on the payment amount
	* @param customer_id, station_id and payment
	*/
	private PreparedStatement decrementBalance(int warehouse_id, int customer_id, int station_id, BigDecimal payment) throws SQLException
	{
		String updateBalanceString = "update Customers set balance = balance - ? where customer_id = ? and station_id = ?";
		PreparedStatement updateBalance = con.prepareStatement(updateBalanceString);
		updateBalance.setBigDecimal(1, payment);
		updateBalance.setInt(2, customer_id);
		updateBalance.setInt(3, station_id);
        updateBalance.addBatch();

        return updateBalance;
	}

	/**
	* Updates the amount paid for the year 
	*/
	private PreparedStatement updatePaidAmount(int warehouse_id, int customer_id, int station_id, BigDecimal payment) throws SQLException
	{
		String updatePaidAmountString = "update Customers set paid_amount = paid_amount + ? where customer_id = ? and station_id = ?";
		PreparedStatement updatePaidAmount = con.prepareStatement(updatePaidAmountString);
		updatePaidAmount.setBigDecimal(1, payment);
		updatePaidAmount.setInt(2, customer_id);
		updatePaidAmount.setInt(3, station_id);
        updatePaidAmount.addBatch();
		
        return updatePaidAmount;
	}
	
	/**
	* A helper method. Updates the year to date sales in a warehouse and in a station
	*/

	private PreparedStatement updateYTDSalesWare(int warehouse_id, BigDecimal amount) throws SQLException
	{
		String updateYTDSalesWString = "update Warehouses set sum_sales = sum_sales + ? where warehouse_id = ?";
		PreparedStatement updateYTDSalesW = con.prepareStatement(updateYTDSalesWString);
        updateYTDSalesW.setBigDecimal(1, amount);
        updateYTDSalesW.setInt(2, warehouse_id);
        

        return updateYTDSalesW;
	}
	
	private PreparedStatement updateYTDSalesStation(int warehouse_id, int station_id, BigDecimal amount) throws SQLException
	{
		String updateYTDSalesSString = "update Stations set sum_sales = sum_sales + ? where station_id = ?";
		PreparedStatement updateYTDSalesS = con.prepareStatement(updateYTDSalesSString);
        updateYTDSalesS.setBigDecimal(1, amount);
        updateYTDSalesS.setInt(2, station_id);
		return updateYTDSalesS;
	}
	
	/**
	* A helper method. Increments number of payments made for a customer 
	*/
	private PreparedStatement updateTotalPayments(int warehouse_id, int customer_id, int station_id) throws SQLException
	{
		String updateTotalPaymentsString = "update Customers set total_payments = total_payments + 1 where customer_id = ? and station_id = ?";
		PreparedStatement updateTotalPayments = con.prepareStatement(updateTotalPaymentsString);
		updateTotalPayments.setInt(1, customer_id);
		updateTotalPayments.setInt(2, station_id);
        updateTotalPayments.addBatch();
		
        return updateTotalPayments;
	}


    /**
    * Returns a count of the number of stocked items in the passed station's 20 most recent orders
    * are under the passed threshold (3.5 in the Milestone 2 document)
    * @param station int containing the stationID
    * @param threshold int containing the count threshold
    */
    public void stockLevel(int warehouse, int station, int threshold)
    {
        int order_id, customer_id, item_id, in_stock;
		int underStockCount = 0;
		String getLast20OrdersString = "select * from " + 
			"(select * from Orders order by order_date desc) " + 
				"where rownum < 21 and warehouse_id = ? and station_id = ?";

        String getLineString = "select item_id from LineItems " +
            "where order_id = ? and customer_id = ? and station_id = ? and warehouse_id = ?";
        String getStockString = "select in_stock from StockItems " +
            "where item_id = ? and warehouse_id = ?";
		
		Savepoint save = null;
		
		try
		{
			save = con.setSavepoint();
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			// Statement stmt = con.createStatement();
// 			stmt.executeUpdate("SET TRANSACTION READ ONLY");
			PreparedStatement getLast20Orders = con.prepareStatement(getLast20OrdersString);
			getLast20Orders.setInt(1, warehouse);
			getLast20Orders.setInt(2, station);
			PreparedStatement getLine = con.prepareStatement(getLineString);
			PreparedStatement getStock = con.prepareStatement(getStockString);
			ResultSet orderResults = getLast20Orders.executeQuery();
			while (orderResults.next())
			{
				
				order_id = orderResults.getInt(1);
				customer_id = orderResults.getInt(2);
				getLine.setInt(1, order_id);
				getLine.setInt(2, customer_id);
				getLine.setInt(3, station);
				getLine.setInt(4, warehouse);
				ResultSet itemResults = getLine.executeQuery();
				while (itemResults.next())
				{
					item_id = itemResults.getInt(1);
					getStock.setInt(1, item_id);
					getStock.setInt(2, warehouse);
					ResultSet stockResults = getStock.executeQuery();
					while (stockResults.next())
					{
						in_stock = stockResults.getInt(1); //
						if (in_stock < threshold)
							underStockCount++;
					}
					stockResults.close();
				}
				itemResults.close();
			}
			orderResults.close();
		

            System.out.println("\nStock items from the last 20 orders of station " + station + " in warehouse " + warehouse);
            System.out.println("which are below the threshold of " + threshold + ": " + underStockCount);

            con.commit();
            System.out.println("\n-----------------------------------------------");
            System.out.println("Stock level transaction committed successfully.");
            System.out.println("-----------------------------------------------\n");
            
        }
        catch (SQLException e)
        {
            try
            {
				System.err.println("The transaction is being rolled back" + e.toString());
                con.rollback(save);
            }
            catch (SQLException f)
            {}

            System.out.println("Error in stock level is: " + e.toString());
            System.out.println("\n---------------------------------------------------------------");
            System.out.println("Error processing stock level transaction, transaction rolled back.");
            System.out.println("---------------------------------------------------------------\n");
        }
    }
	
	




/********************************************************************************************************************************************
* Various helper methods for generating the data and executing the transations
*********************************************************************************************************************************************/

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
    * Returns a single character string for use as a middle initial
    * @param rand Random number generator object
    * @return String containing the initial
    */
    private String getMI(Random rand)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(ALPHABET.charAt(rand.nextInt(ALPHABET.length())));
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
    * @return double containing the tax rate
    */
    private double getRandomTax(Random rand)
    {
        return TAXES[rand.nextInt(TAXES.length)];
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
    * Returns a random date greater than the passed year, month and day
    * @param rand Random number generator object
    * @param year Year of the date after which the returned date should fall, must be less than 2015.
    * @param month Month of the date after which the returned date should fall, 0 if not specified.
    * @param day Day of the date after which the returned date should fall, 0 if not specified.
    * @return String containing the date
    */
    private String getDate(Random rand, int year, int month, int day)
    {
        if (year > 2015 || month > 12 || month < 0 || day > 31 || day < 0)
            return null;

        // compute the offsets for the random date generator
        int yearRange = 2015 - year;
        int monthRange = 12 - month;
        int dayRange = 31 - day;

        // compute the random date
        int newYear, newMonth, newDay;
        if (yearRange == 0)
            newYear = year;
        else
            newYear = year + rand.nextInt(yearRange);
        if (monthRange == 0)
            newMonth = month;
        else
            newMonth = month + rand.nextInt(monthRange) + 1;
        if (dayRange == 0)
            newDay = day;
        else
            newDay = day + rand.nextInt(dayRange) + 1;

        // correct for any date errors
        if (newMonth > 12)
            newMonth = 12;
        if ((newMonth == 9 || newMonth == 4 || newMonth == 6 || newMonth == 11) && newDay > 30)
            newDay = 30;
        if (newMonth == 2 && newDay > 28)
            newDay = 28;

        // build the output string
        String monthString = null;
        String dayString = null;
        if (newMonth < 10)
            monthString = "0" + newMonth;
        else
            monthString = "" + newMonth;
        if (newDay < 10)
            dayString = "0" + newDay;
        else
            dayString = "" + newDay;

        String output = newYear + "-" + monthString + "-" + dayString;
        return output;
    }

    /**
    * Returns a random date after 2010
    * @param rand Random number generator object
    * @return String containing the date
    */
    private String getDate(Random rand)
    {
        return getDate(rand, 2010, 0, 0);
    }

    /**
    * Returns a random date after the passed date string
    * @param rand Random number generator object
    * @param date Date string after which the returned date should fall
    * @return String containing the date
    */
    private String getDate(Random rand, String date)
    {
        // parse the date
        String[] splitDate = date.split("-");

        int oldYear = 0, oldMonth = 0, oldDay = 0;
        try
        {
            oldYear = Integer.parseInt(splitDate[0]);
            oldMonth = Integer.parseInt(splitDate[1]);
            oldDay = Integer.parseInt(splitDate[2]);
        }
        catch(NumberFormatException e)
        {
            System.out.println("Error parsing date string");
        }

        return getDate(rand, oldYear, oldMonth, oldDay);
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
        return twoDecimals(intPortion + floatPortion);

    }

    /**
    * Returns a random price that is less than or equal to MAX_PRICE + 1
    * @param rand Random number generator object
    * @return float containing the price
    */
    private float getPrice(Random rand)
    {
        int intPortion = rand.nextInt(MAX_PRICE);
        float floatPortion = rand.nextFloat();
        return (float)intPortion + floatPortion;
    }

    /**
    * Returns a random price that is less than or equal to the passed total value
    * @param rand Random number generator object
    * @param total float value containing the value the price should be less than or equal to
    */
    private float getPrice(Random rand, float total)
    {
        float balance;
        do
        {
            balance = total - getPrice(rand);
        }
        while (balance < 0.0);

        return balance;
    }

    /**
    * Rounds the passed value to two decimal places and returns it as a string
    * @param value float value to be rounded
    * @return String containing the rounded value
    */
    private String twoDecimals(float value)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(value);

        int dotIndex = sb.indexOf(".");
        int endIndex = dotIndex + 3;
        while (endIndex > sb.length())
        {
            endIndex--;
        }
        return sb.substring(0, endIndex);
    }

    /**
    * Rounds the passed value to two decimal places and returns it as a string
    * @param value double value to be rounded
    * @return String containing the rounded value
    */
    private String twoDecimals(double value)
    {
        return twoDecimals((float)value);
    }

    /**
    * Generates a random number which trends towards a mean of AVE_ITEMS_IN_STOCK_PER_WAREHOUSE.
    * Used to supply the random stock number.
    * @param rand Random number generator object
    */
    private int randomMean(Random rand)
    {
        // get a Gaussian value
        double gauss = rand.nextGaussian();

        // adjust the random Gaussian value to the mean
        gauss = gauss + 1.0;
        gauss = gauss + AVE_ITEMS_IN_STOCK_PER_WAREHOUSE;

        return (int)Math.round(gauss);
    }

    /**
    * Enqueues the passed order in the recent order queue
    * @param warehouse int containing the warehouse_id
    * @param station int containing the station_id
    * @param customer int containing the customer_id
    * @param order int containing the order_id
    */
    private void enqueueOrder(int warehouse, int station, int customer, int order)
    {
        last20Orders[warehouse - 1][station - 1][last20Index[warehouse - 1][station - 1]] = getOrderKey(warehouse, station, customer, order);
        last20Index[warehouse - 1][station - 1] = (last20Index[warehouse - 1][station - 1] + 1) % last20Orders[warehouse - 1][station - 1].length;
    }

    /**
    * Generates a unique UUID for the customer.
    * @param warehouse int containing the warehouse_id
    * @param station int containing the station_id
    * @param customer int containing the customer_id
    * @return String containing the UUID
    */
    private String getCustomerKey(int warehouse, int station, int customer)
    {
        return warehouse + "-" + station + "-" + customer;
    }

    /**
    * Generates a unique UUID for the order.
    * @param warehouse int containing the warehouse_id
    * @param station int containing the station_id
    * @param customer int containing the customer_id
    * @param order int containing the order_id
    * @return String containing the UUID
    */
    private String getOrderKey(int warehouse, int station, int customer, int order)
    {
        return warehouse + "-" + station + "-" + customer + "-" + order;
    }

    /************************
    * Various getters
    *************************/
    public Connection getConnection()
    {
        return con;
    }

    public HashMap<Integer, Float> getItemCost()
    {
        return itemCost;
    }

    public HashMap<String, Integer> getCurrOrderID()
    {
        return currOrderID;
    }

    public HashMap<String, Integer> getCurrLineID()
    {
        return currLineID;
    }

    public String[][][] getLast20Orders()
    {
        return last20Orders;
    }

    public int[][] getLast20Index()
    {
        return last20Index;
    }

}
=======
>>>>>>> External Changes
