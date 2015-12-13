import java.sql.*;
import java.math.*;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.pool.*;
import java.util.Random;

public class ThreadEx extends Thread
{
	private static int NUM_OF_THREADS = 15;
	
	static int m_myId;
	
	static int c_nextId = 0;
	static boolean share_connection = true;	
	
	static DBLoader db = null;
	
	synchronized static int getNextId()
	{
		return c_nextId++;
	}
	
	public static void main (String args [])
	  {
	    try  
	    {  

		  db = new DBLoader(false);

	      // get a shared connection
          // OracleDataSource ods = new OracleDataSource();
 // 		  ods.setURL(db.server);
 // 		  ods.setUser(db.username);
 // 		  ods.setPassword(db.password);
 // 		  Connection s_conn = ods.getConnection();
		  
	      // Create the threads
	      Thread[] threadList = new Thread[NUM_OF_THREADS];

	      // spawn threads
	      for (int i = 0; i < NUM_OF_THREADS; i++)
	      {
	          threadList[i] = new ThreadEx(db);
	          threadList[i].start();
	      }
    
	      // Start everyone at the same time
	      long start = System.nanoTime();
	      setGreenLight ();

	      // wait for all threads to end
	      for (int i = 0; i < NUM_OF_THREADS; i++)
	      {
	          threadList[i].join();
	      }
	      long end = System.nanoTime();
	      double elapsed = (double)(end - start) / (double)1000000000;

		  System.out.println("The threading is complete.");
		  System.out.println("Time elapsed for threading: " + elapsed + " sec\n");
	      // if (share_connection)
// 	      {
// 	          s_conn.close();
// 	          s_conn = null;
// 	      }
          
	    }
	    catch (Exception e)
	    {
	       e.printStackTrace();
	    }
	}
	
	public ThreadEx(DBLoader db)
	{
		super();
		this.db = new DBLoader(db);
		//Assign an ID to the thread
		m_myId = getNextId();
	}
	
	public void run()
	  {
	    ResultSet resultSet = null;
	    Statement statement = null;
		
		int newId = m_myId % 5;

		while (!getGreenLight())
			yield();

	    switch (newId)
	    {
	    	case 0:	//System.out.println("Creating order");
					createNewOrder();
	    			break;
	    	case 1:	//System.out.println("Making payment");
					makePayment();
	    			break;
	    	case 2:	//System.out.println("Order status");
					orderStatus();
	    			break;
	    	case 3:	//System.out.println("Make delivery");
					makeDelivery();
	    			break;
	    	case 4:	//System.out.println("Get stock");
					getStock();
	    			break;
	    }

  	}
	
	public void createNewOrder()
	{
		Random rand = new Random(System.nanoTime());
		int warehouse = 1;
		int randomStation = rand.nextInt(db.STATIONS_PER_WAREHOUSE) + 1;
		int randomCustomer = rand.nextInt(db.CUSTOMERS_PER_STATION) + 1;
		int randomItemsLength = rand.nextInt(db.MAX_LINE_ITEMS_PER_ORDER) + 3; //from 3 to 10
		//System.out.println("Random Items Length is " + randomItemsLength);
		int[] items = new int[randomItemsLength]; //creates items array
		int[] counts = new int[randomItemsLength];
		int totalItems = 0;
		
		for (int ind = 0; ind < randomItemsLength; ++ind)
		{
			items[ind] = rand.nextInt(db.ITEMS) + 1;
			counts[ind] = rand.nextInt(10) + 1;
			totalItems += counts[ind];
		}
		db.newOrder(warehouse, randomStation, randomCustomer, items, counts, totalItems);	
	}


  	private void makePayment()
  	{
  		Random rand = new Random(System.nanoTime());
  		int warehouse = 1;
  		int station = rand.nextInt(db.STATIONS_PER_WAREHOUSE) + 1;
  		int customer = rand.nextInt(db.CUSTOMERS_PER_STATION) + 1;
  		BigDecimal payment = new BigDecimal(rand.nextInt(50) + rand.nextFloat());

  		db.processPayment(warehouse, station, customer, payment);
  	}

  	private void orderStatus()
  	{
  		Random rand = new Random(System.nanoTime());
  		int warehouse = 1;
  		int station = rand.nextInt(db.STATIONS_PER_WAREHOUSE) + 1;
  		int customer = rand.nextInt(db.CUSTOMERS_PER_STATION) + 1;

  		db.getOrderStatus(warehouse, station, customer);
  	}

  	private void makeDelivery()
  	{
  		db.getDeliveryTransaction(1);
  	}

  	private void getStock()
  	{
  		Random rand = new Random(System.nanoTime());
  		int warehouse = 1;
  		int station = rand.nextInt(db.STATIONS_PER_WAREHOUSE) + 1;
  		int threshold = rand.nextInt(500) + 1;

  		db.stockLevel(warehouse, station, threshold);
	}
	
	
	static boolean greenLight = false;
    static synchronized void setGreenLight () { greenLight = true; }
    synchronized boolean getGreenLight () { return greenLight; }
}