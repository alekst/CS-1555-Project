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
import java.util.ArrayList;

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
    private final String[] TAXES = {"0.01", "0.02", "0.03", "0.04", "0.05", "0.06", "0.07", "0.08", "0.09", "0.10", "0.11", "0.12", "0.13", "0.14", "0.15"};
    private final String[] STREET_SUFFIXES = {"St.", "Ave.", "Rd.", "Way"};
    private final int MAX_ADDRESS = 2000;
    private final int NAME_MIN = 4;
    private final int NAME_MAX = 15;
    private final int MAX_SOLD = 15000;
    private final int MAX_PRICE = 50;

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
        System.out.println("\nWelcome to the database loader!");
        System.out.println("\nThe default database is : " + SERVER_ADDR);
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
        System.out.print("\nDo you want to create or recreate the tables in the database? (y/n): ");
        answer = scan.nextLine();

      	// drop and recreate the tables
    	if (answer.toUpperCase().equals("Y"))
    	{
			initDatabase();
    	}

    	// populate the tables with generated data
    	populateTables();

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
			"address varchar2(30), " +
			"city varchar2(20), " +
			"state varchar2(2), " +
			"zip varchar2(5), " +
			"tax_rate number (3, 2), " +
			"sum_sales number (9, 2), " +
			"constraint Warehouses_pk primary key(warehouse_id) )";
		

		dropStatements[1] = "drop table Stations cascade constraints";
		String createStations = "create table Stations (" +
			"station_id number(3) unique not null, " +
			"warehouse_id number(3) not null, " +
			"name varchar2(20), " +
			"address varchar2(30), " +
			"city varchar2(20), " +
			"state varchar2(5), " +
			"zip varchar2(5), " +
			"tax_rate number(3, 2), " +
			"sum_sales number(9, 2), " +
			"constraint Stations_pk primary key(station_id, warehouse_id), " +
			"constraint Stations_fk foreign key(warehouse_id) references Warehouses(warehouse_id) )";
	

		dropStatements[2] = "drop table Customers cascade constraints";
		String createCustomers = "create table Customers (" +
			"customer_id number(6) unique not null, " +
			"station_id number(3) not null, " +
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
			"constraint Customers_fk foreign key(station_id) references Stations(station_id) )";
		

		dropStatements[3] = "drop table Orders cascade constraints";
		String createOrders = "create table Orders (" +
			"order_id number(10) unique not null, " +
			"customer_id number(6), " +
			"order_date varchar2(10), " +
			"completed number(1), " +
			"line_item_count number(10), " +
			"station_id number(3), " +
			"warehouse_id number(3), " +
			"constraint Orders_pk primary key(order_id, customer_id), " +
			"constraint Orders_fk1 foreign key(station_id) references Stations(station_id), " +
			"constraint Orders_fk2 foreign key(warehouse_id) references Warehouses(warehouse_id), " +
            "constraint Orders_fk3 foreign key(customer_id) references Customers(customer_id) )";
		

		dropStatements[4] = "drop table LineItems cascade constraints";
		String createLineItems = "create table LineItems (" +
			"line_id number(15) unique not null, " +
			"order_id number(10), " +
			"item_id number(15), " +
			"quantity number(5), " +
			"amount number (5, 2), " +
			"delivered number(1), " +
			"constraint LineItems_pk primary key(line_id, order_id), " +
			"constraint LineItems_fk1 foreign key(order_id) references Orders(order_id), " +
            "constraint LineItems_fk2 foreign key(item_id) references StockItems(item_id) )";


		dropStatements[5] = "drop table StockItems cascade constraints";
		String createStockItems = "create table StockItems (" +
			"item_id number(15) unique not null, " +
			"name varchar2(20), " +
			"price number(5, 2), " +
			"in_stock number(20), " +
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


    /**
    * Generates random data to populate the created tables
    */
    private void populateTables()
    {
        int warehouses = 1;
        int stations = 5;
        int customers = 50;
        int orders = 40;
        int items = 200;

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
        PreparedStatement insertWarehouses = null;
        PreparedStatement insertStations = null;
        PreparedStatement insertCustomers = null;
        PreparedStatement insertOrders = null;
        PreparedStatement insertLineItems = null;
        PreparedStatement insertStockItems = null;
        try
        {
            insertWarehouses = con.prepareStatement(warehousesString);
            insertStations = con.prepareStatement(stationsString);
            insertCustomers = con.prepareStatement(customersString);
            insertOrders = con.prepareStatement(ordersString);
            insertLineItems = con.prepareStatement(lineItemsString);
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
        ArrayList<Integer> itemLog = new ArrayList<Integer>();
        HashMap<Integer, Integer> ytdSoldCounts = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> itemOrderCounts = new HashMap<Integer, Integer>();
        HashMap<Integer, Float> itemCost = new HashMap<Integer, Float>();
        try
        {
            // generate the warehouses
            for (int i = 1; i <= warehouses; i++)
            {
                warehouseTotal = 0;

                // generate the stock items
                for (int j = 1; j <= items; j++)
                {
                    // insert initial entries into the hashmaps
                    float cost = getPrice(rand);
                    itemLog.add(j);
                    ytdSoldCounts.put(j, 0);
                    itemOrderCounts.put(j, 0);
                    itemCost.put(j, new Float(cost));

                    insertStockItems.setInt(1, j);
                    insertStockItems.setString(2, getName(rand));
                    insertStockItems.setString(3, twoDecimals(cost));
                    insertStockItems.setInt(4, rand.nextInt(200));
                    insertStockItems.setString(5, "0");
                    insertStockItems.setString(6, "0");
                    insertStockItems.setInt(7, i);
                    insertStockItems.addBatch();
                }

                // generate the stations
                for (int j = 1; j <= stations; j++)
                {
                    stationTotal = 0;

                    // generate the customers
                    for (int k = 1; k <= customers; k++)
                    {
                        customerTotal = 0;

                        // generate the orders
                        for (int l = 1; l <= orders; l++)
                        {
                            int lineCount = rand.nextInt(15) + 1;

                            // generate the line items for the order
                            for (int m = 1; m <= lineCount; m++)
                            {
                                int itemID = rand.nextInt(items) + 1;
                                int itemCount = rand.nextInt(10) + 1;
                                float lineTotal = itemCost.get(itemID).floatValue() * itemCount;
                                customerTotal += lineTotal;

                                // update the hashmaps to reflect the sold and order numbers
                                ytdSoldCounts.put(itemID, new Integer(ytdSoldCounts.get(itemID).intValue() + itemCount));
                                itemOrderCounts.put(itemID, new Integer(itemOrderCounts.get(itemID).intValue() + 1));

                                insertLineItems.setInt(1, m);
                                insertLineItems.setInt(2, l);
                                insertLineItems.setInt(3, itemID);
                                insertLineItems.setInt(4, itemCount);
                                insertLineItems.setString(5, twoDecimals(lineTotal));
                                insertLineItems.setInt(6, Math.round(rand.nextFloat()));
                                insertLineItems.addBatch();
                            }

                            String theDate = getDate(rand);
                            insertOrders.setInt(1, l);
                            insertOrders.setInt(2, k);
                            insertOrders.setString(3, theDate);
                            insertOrders.setInt(4, Math.round(rand.nextFloat()));
                            insertOrders.setInt(5, lineCount);
                            insertOrders.setInt(6, j);
                            insertOrders.setInt(7, i);
                            insertOrders.addBatch();

                        }

                        float balance = getPrice(rand, customerTotal);
                        insertCustomers.setInt(1, k);
                        insertCustomers.setInt(2, j);
                        insertCustomers.setString(3, getName(rand));
                        insertCustomers.setString(4, getMI(rand));
                        insertCustomers.setString(5, getName(rand));
                        insertCustomers.setString(6, getAddress(rand));
                        insertCustomers.setString(7, getName(rand));
                        insertCustomers.setString(8, getState(rand));
                        insertCustomers.setString(9, getZip(rand));
                        insertCustomers.setString(10, getPhone(rand));
                        insertCustomers.setString(11, getDate(rand));
                        insertCustomers.setString(12, getDiscount(rand));
                        insertCustomers.setString(13, twoDecimals(balance));
                        insertCustomers.setString(14, twoDecimals(customerTotal - balance));
                        insertCustomers.setInt(15, rand.nextInt(20) + 1);
                        insertCustomers.setInt(16, rand.nextInt(30) + 1);
                        insertCustomers.addBatch();
                        stationTotal += customerTotal;
                    }

                    insertStations.setInt(1, j);
                    insertStations.setInt(2, i);
                    insertStations.setString(3, getName(rand));
                    insertStations.setString(4, getAddress(rand));
                    insertStations.setString(5, getName(rand));
                    insertStations.setString(6, getState(rand));
                    insertStations.setString(7, getZip(rand));
                    insertStations.setString(8, getTax(rand));
                    insertStations.setString(9, twoDecimals(stationTotal));
                    insertStations.addBatch();
                    warehouseTotal += stationTotal;
                }

                insertWarehouses.setInt(1, i);
                insertWarehouses.setString(2, getName(rand));
                insertWarehouses.setString(3, getAddress(rand));
                insertWarehouses.setString(4, getName(rand));
                insertWarehouses.setString(5, getState(rand));
                insertWarehouses.setString(6, getZip(rand));
                insertWarehouses.setString(7, getTax(rand));
                insertWarehouses.setString(8, twoDecimals(warehouseTotal));
                insertWarehouses.addBatch();

            }
        }
        catch (SQLException e)
        {
            System.out.println("Error generating statements");
        }

        // execute the batch inserts
        try
        {
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
            String itemsSoldString = "update StockItems set sold_this_year = ? where item_id = ";
            String orderCountString = "update StockItems set included_in_orders = ? where item_id = ";
            PreparedStatement itemsSold = con.prepareStatement(itemsSoldString);
            PreparedStatement orderCount = con.prepareStatement(orderCountString);

            for (int i = 0; i < itemLog.size(); i++)
            {
                int soldQuant = ytdSoldCounts.get(itemLog.get(i)).intValue();
                int orderQuant = itemOrderCounts.get(itemLog.get(i)).intValue();
                itemsSold.setInt(1, soldQuant);
                itemsSold.setInt(2, itemLog.get(i));
                orderCount.setInt(1, orderQuant);
                orderCount.setInt(2, itemLog.get(i));
                itemsSold.execute();
                orderCount.execute();
            }
        }
        catch (SQLException e)
        {
            System.out.println("Error updating item entries.");
            System.exit(1);
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
    * @return String containing the tax rate
    */
    private String getTax(Random rand)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(TAXES[rand.nextInt(TAXES.length)]);

        return sb.substring(0, 4);
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
    * @return String containing the date
    */
    private String getDate(Random rand)
    {
        int year = 2010 + rand.nextInt(6);
        int month = rand.nextInt(12) + 1;
        int day = rand.nextInt(28) + 1;

        String monthString = null;
        String dayString = null;
        if (month < 10)
            monthString = "0" + month;
        else
            monthString = "" + month;
        if (day < 10)
            dayString = "0" + day;
        else
            dayString = "" + day;

        String output = year + "-" + monthString + "-" + dayString;
        return output;
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

}