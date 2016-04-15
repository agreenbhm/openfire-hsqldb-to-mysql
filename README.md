# OpenFire hsqlDB to MySQL

### Requirements
* JayDeBeAPI (Python library) - install via pip
* Java JRE (matching architecture of machine, ie 32-bit if OS is 32-bit, 64-bit if OS is 64-bit)
* Python 2.7 (matching architecture of machine; see above)

The architecture match is important, as if you are using 32-bit Python on a 64-bit machine the program will likely crash.


### Usage
1. Rename config.py.example to config.py.
2. Adjust settings in config.py to reflect your environment.
3. Shutdown Openfire.  Restart Openfire, then shutdown again (recommended in conversion guides).
4. Copy entire contents of Openfire embedded-db folder to folder containing this script.  This includes:
 * openfire.script
 * openfire.properties
 * openfire.log
 * and anything else.  If there is a .lck file, copy that too, but you may need to delete it if you get an error.
5. Follow existing guides to setup Openfire with MySQL.  This will create the necessary tables in MySQL.
 * If migrating the application to a new server as well make sure to install any plugins you had previously as these sometimes create their own tables.
6. Run main.py (remember - run it using the Python interpreter matching your system's architecture).
7. If you have a lot of data (and you probably do, otherwise you wouldn't need this script) prepare to wait a while.
 * There is a time estimate for each table as it runs; this time is for the current table, not overall.



### Issues
None known at the moment.  This has only been tested in a limited lab environment.  Do not use in production without testing beforehand.

If you find a bug please report it.


### How It Works
This script uses JayDeBeAPI (which in turns uses JPype, a Python-to-Java bridge) to interface with JDBC drivers
for HSQLDB and MySQL.  The script analyzes the tables in the source and destination databases.  For tables found
in both locations (and with more than 0 rows in the source) data is selected from the source and inserted into
the destination, table by table, row by row.  After each successful row is completed, a local sqlite db is updated
to reflect progress (so in the event of a crash the program can start where it left off).  The same is done for each
completed table (so no completed tables are re-processed).


### To-Do (maybe)
* Add multi-row processing
* Add multi-threading for simultaneous table processing

