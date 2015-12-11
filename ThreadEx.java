import java.sql.*;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.pool.*;
import java.util.Random;

public class ThreadEx extends Thread
{
	private static int NUM_OF_THREADS = 15;
	
	int m_myId;
	
	static int c_nextId = 0;
	static boolean share_connection = true;	
	
	synchronized static int getNextId()
	{
		return c_nextId++;
	}
	
	public static void main (String args [])
	  {
	    try  
	    {  
  
	      // If NoOfThreads is specified, then read it
	      // if ((args.length > 2)  ||
    // 	           ((args.length > 1) && !(args[1].equals("share"))))
    // 	      {
    // 	         System.out.println("Error: Invalid Syntax. ");
    // 	         System.out.println("java JdbcMTSample [NoOfThreads] [share]");
    // 	         System.exit(0);
    // 	      }
    //
    // 	      if (args.length > 1)
    // 	      {
    // 	         share_connection = true;
    // 	         System.out.println
    // 	                ("All threads will be sharing the same connection");
    // 	      }

		  DBLoader db = new DBLoader(false);

	      // get a shared connection
          OracleDataSource ods = new OracleDataSource();          
		  ods.setURL(db.server);          
		  ods.setUser(db.username);          
		  ods.setPassword(db.password);          
		  Connection s_conn = ods.getConnection();
		  
	      // Create the threads
	      Thread[] threadList = new Thread[NUM_OF_THREADS];

	      // spawn threads
	      for (int i = 0; i < NUM_OF_THREADS; i++)
	      {
	          threadList[i] = new ThreadEx();
	          threadList[i].start();
	      }
    
	      // Start everyone at the same time
	      setGreenLight ();

	      // wait for all threads to end
	      for (int i = 0; i < NUM_OF_THREADS; i++)
	      {
	          threadList[i].join();
	      }

	      if (share_connection)
	      {
	          s_conn.close();
	          s_conn = null;
	      }
          
	    }
	    catch (Exception e)
	    {
	       e.printStackTrace();
	    }
	}
	
	public ThreadEx()
	{
		super();
		//Assign an ID to the thread
		m_myId = getNextId();
	}
	
	public void run()
	  {
	    ResultSet resultSet = null;
	    Statement statement = null;

	    switch (m_myID % 5)
	    {
	    	case 0:	createNewOrder();
	    			break;
	    	case 1:	makePayment();
	    			break;
	    	case 2:	orderStatus();
	    			break;
	    	case 3:	makeDelivery();
	    			break;
	    	case 4:	getStock();
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
		int[] items = new int[randomItemsLength]; //creates items array
		int[] counts = new int[randomItemsLength];
		int totalItems = 0;
		
		for (int ind = 0; ind < randomItemsLength - 1; ++ind)
		{
			items[ind] = rand.nextInt(db.ITEMS) + 1;
			counts[ind] = rand.nextInt(10) + 1;
			totalItems += counts[ind];
		}
		newOrder(warehouse, randomStation, randomCustomer, items, counts, totalItems);	
	}


  	private void makePayment()
  	{
  		Random rand = new Random(System.nanoTime());
  		int warehouse = 1;
  		int station = rand.nextInt(db.STATIONS_PER_WAREHOUSE) + 1;
  		int customer = rand.nextInt(db.CUSTOMES_PER_STATION) + 1;
  		BigDecimal payment = new BigDecimal(rand.nextInt(50) + rand.nextFloat());

  		db.processPayment(warehouse, station, customer, payment);
  	}

  	private void orderStatus()
  	{
  		Random rand = new Random(System.nanoTime());
  		int warehouse = 1;
  		int station = rand.nextInt(db.STATIONS_PER_WAREHOUSE) + 1;
  		int customer = rand.nextInt(db.CUSTOMES_PER_STATION) + 1;

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
  		int station = rand.nextInt(db.STATIONS_PER_WAREHOUSE);
  		int threshold = rand.nextInt(30);

  		db.stockLevel(warehouse, station, threshold);
	}
	
	
	static boolean greenLight = false;
    static synchronized void setGreenLight () { greenLight = true; }
    synchronized boolean getGreenLight () { return greenLight; }
}