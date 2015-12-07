# Project for CS 1555

DBLoader.java loads and randomly populates the database with data. 

The structure of the source is as follows:

The main function creates a new DBLoader object.
The constructor of this object contains the main running code of the program.

The constructor gets credential and preference input from the user, and then calls the
initDatabase() and populateTables() methods to create the tables, and populate them with
randomly generated data. The default setting of the preference is the one set out in the Milestone 2 document:

	* 1 warehouse
	* 8 distribution stations per warehouse
	* 3,000 customers per distribution station
	* 10,000 items
	* 10,000 stock listings per warehouse
	* 100 max orders
	* 15 max line items per order

The constructor then enters a menu loop which contains the database functions required for
milestone 2. The menu includes an option to reinitialize the database, after which the database is recreated with randomly generated data. 

Separate methods are used to implement the functionality of the database initialization
and query functions. These methods are grouped into two blocks, with a third block containing
various helper methods for these functions.