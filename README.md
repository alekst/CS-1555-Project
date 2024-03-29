# Project for CS 1555
# Milestone 2

DBLoader.java loads and randomly populates the database with data. 

The structure of the source is as follows:

The main function creates a new DBLoader object.
The constructor of this object contains the main running code of the program.

The constructor gets credential and preference input from the user, and then calls the
initDatabase() and populateTables() methods to create the tables, and populate them with
randomly generated data. The preference's default setting is established in the Milestone 2 document:

	* 1 warehouse
	* 8 distribution stations per warehouse
	* 100 customers per distribution station
	* 1,000 items
	* 1,000 stock listings per warehouse
	* 50 max orders
	* 10 max line items per order

The constructor then enters a menu loop which contains the database functions required for
milestone 2. The menu includes an option to reinitialize the database, after which the database
is recreated with randomly generated data. 

Separate methods are used to implement the functionality of the database initialization
and query functions. These methods are grouped into two blocks, with a third block containing
various helper methods for these functions.

--------------------------------------------------------------------------------------------------------------------------------------------
# Milestone 3

The ThreadEx class implements a multi-threaded program which instantiates a DBLoader object,
thereby loading the database with generated data, and then spawning 15 threads, three of each
of the five types of queries that the DBLoader object offers.

These threads are run concurrently, testing the ability of the database to cope with multiple
conflicting queries.

A basic timing loop was inserted around the query code, and the total execution time of all
15 threads is reported at the end of execution.

Run the application as: java ThreadEx