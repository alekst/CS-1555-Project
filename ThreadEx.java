import java.sql.*;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.pool.*;

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

	    switch (m_myID)
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

	    try
	    {    
	      // Using shared connection
		  // Instantiate items and counts arrays
		  int[] items = {2, 3}; 
		  int[] counts = {2, 2};
		  
		  while (!getGreenLight())
	        yield();
          
		  db.newOrder(1, 1, 1, items, counts, 4);
	     
	  }
  	}
	
	
	static boolean greenLight = false;
    static synchronized void setGreenLight () { greenLight = true; }
    synchronized boolean getGreenLight () { return greenLight; }
}