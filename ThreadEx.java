import java.sql.*;
import oracle.jdbc.OracleStatement;

public class ThreadEx extends Thread
{
	private static int NUM_OF_THREADS = 15;
	
	int m_myId;
	
	static int c_nextID = 0;
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
  
	      // get a shared connection
	      if (share_connection) 
	      {
	          OracleDataSource ods = new OracleDataSource();          
			  ods.setURL("jdbc:oracle:" +args[1]);          
			  ods.setUser("scott");          
			  ods.setPassword("tiger");          
			  Connection s_conn = ods.getConnection();
	      }
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
	
	
	static boolean greenLight = false;
    static synchronized void setGreenLight () { greenLight = true; }
    synchronized boolean getGreenLight () { return greenLight; }
}