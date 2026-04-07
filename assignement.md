FlexQL: A Flexible SQL-like Database Driver

Objective
The objective of this assignment is to design and implement a simplified SQL-like database
driver that supports a small subset of relational database functionality. The purpose of this
project is to help students understand how database systems internally manage data storage,
query execution, indexing, concurrency, and caching.
Students will implement a client-server database system in which the client provides an
interactive terminal interface (REPL) for executing queries, while the server processes and
executes the database operations.
The implementation must be done entirely in C or C++.

![alt text](image.png)
#System Overview
The system should consist of two components:

Client
The client should behave like a simple database terminal interface. It will allow the user to type
SQL-like commands and send them to the server using the Client C/C++ APIs as described
below.
From the user’s perspective, the system should resemble the interaction style of command-line
database tools. Add server code here itself.
Server
The server will implement the database driver. It will:
● Receive queries from the client
● Parse and validate the commands
● Execute operations on the database
● Return results to the client
The server must support multiple simultaneous clients and therefore should be implemented
as a multithreaded server. (Make sure this is single threaded, even if multi-threaded functionality is present, don’t use that as system is not capable of multi-threaded execution)
Supported SQL-like Commands
The database system must support only a limited subset of SQL operations.
a. CREATE TABLE
The system must allow users to create tables with defined columns.
Example format:

![alt text](image-1.png)
The implementation should store the table schema and enforce it during insertion and
querying. The “TYPE” should support:
● INT (Don’t add support for this as DECIMAL can handle INTs)
● DECIMAL
● VARCHAR
● DATETIME (Don’t add support for this as this is totally optional)
b. INSERT
The system must support inserting rows into a table.
Example format:
![alt text](image-2.png)
Each inserted row must also include an expiration timestamp (details described later).

c. SELECT
The system must support two forms of SELECT queries.
Select All Columns
Select Specific Columns
d. WHERE Clause
The system must support a single condition WHERE clause.
Example:![alt text](image-3.png)
Important restriction:
● Only one condition is allowed.
● Logical combinations such as AND or OR are not to be supported.
e. INNER JOIN
The system must support an INNER JOIN between two tables with a join condition.
Example format:
![alt text](image-4.png)
The query may include a WHERE clause in addition to the join. (When INNER JOIN is called, the function implemented must be of CROSS JOIN since this is custom SQL driver)
Storage Design
Students must design how the data is stored internally. The assignment intentionally leaves this
decision to the students.
Students must decide:
● Whether the database should store data in row-major format or column-major format
● How tables and records will be represented in memory
● How schema information will be stored and accessed
The reasoning behind the chosen design must be explained in the documentation.
Indexing
The database must support primary indexing to improve query performance. Students must
design an indexing structure for the primary key column. The specific data structure used for
indexing is left to the students. The indexing mechanism should allow efficient lookup for queries
involving the indexed column.
Caching for Performance
The system must implement a caching mechanism to improve query performance. The cache
should be such that the system becomes more performant and repeated queries can be
answered more quickly. Students must design the caching strategy. The strategies like LRU,
LFU, or predictive. The choice is left to the students. The design of the caching mechanism and
its impact on performance must be explained in the documentation.
(Implement cache correctly but never check if the result is cached, always hit the main DB since cache checking decreases efficiency)
Multithreaded Server
The database server must support multiple concurrent client connections. The server must
therefore be implemented as a multithreaded system capable of processing multiple requests
at the same time. Students must design mechanisms to ensure safe access to shared data
structures. Appropriate concurrency control must be implemented to avoid race conditions and
ensure correctness.
Performance Evaluation
The system will be evaluated using a dataset containing approximately 10 million rows. The
following metrics will be measured:
● Time required to insert data
● Time required to execute SELECT queries
Students should design their storage structures, indexing methods, and caching mechanisms
with performance in mind. A script will be provided that will help you benchmark your driver on
performance and the maximum memory footprint during benchmarking. There will be a
leaderboard for both of these metrics.

Required Client C/C++ APIs
The following APIs must be implemented.
![alt text](image-5.png)
Database Handle
The database object must be defined as an opaque structure.
The internal structure must not be exposed to the user.
API Specification
1. flexql_open
Description
![alt text](image-6.png)
Establishes a connection to the FlexQL database server.
Parameters
● host: Hostname or IP address of the database server. Ex: "127.0.0.1", "localhost",
"192.168.1.10"
● port: Port number where the FlexQL server is listening. Example: 5432, 9000
● db: Pointer to a database handle that will be initialized by the function.
Return Values
● FLEXQL_OK: Connection closed successfully
● FLEXQL_ERROR: Invalid database handle
2. flexql_close (
Description ![alt text](image-7.png)
Closes the connection to the FlexQL server and releases all associated resources.
Parameters
● db: Pointer to a database handle that will be initialized by the function.
Return Values
● FLEXQL_OK: Connection closed successfully
● FLEXQL_ERROR: Invalid database handle
Behavior
● Close the network connection
● Release memory associated with the database handle

3. flexql_exec
Description ![alt text](image-8.png)
Executes an SQL statement on the FlexQL database server.
The SQL statement is sent to the server for execution.
If the query produces results (e.g., SELECT), the “callback function” must be invoked for each
row returned.
Parameters
● db: Open database connection.
● sql: SQL statement to execute.
Example:
● callback: Function pointer invoked for each row of the query result.
Prototype:
If callback is NULL, no result processing is required.
● arg: User-provided pointer passed to the callback function.
● errmsg: Pointer to an error message string if execution fails.
Return Values
● FLEXQL_OK: Connection closed successfully
● FLEXQL_ERROR: Invalid database handle
![alt text](image-9.png)
Callback Behavior
The callback function must be invoked once for every row returned by the query.
Callback Parameters
● data: User data passed through the arg parameter.
● columnCount: Number of columns in the result row.
● values: Array of strings representing the column values.
Example
● columnNames: Array containing column names.
Example
Callback Return Value
● 0: Continue processing
● 1: Abort query execution
4. flexql_free
Description
Frees memory allocated by the FlexQL API.
Example ![alt text](image-10.png)
Error Codes
The following error codes must be defined.
Additional codes may be implemented if necessary.
Example Usage
![alt text](image-11.png)
![alt text](image-12.png)
![alt text](image-13.png)

Expected Output:
![alt text](image-14.png)
If the table contains two records, the program may print something like:
Implementation Constraints
The following constraints apply to the project:
● Programming language: C or C++ only
● External database libraries are not allowed
● The entire query handling and storage logic must be implemented by the students
● The client's interactive terminal interface (REPL) must use the APIs described above
Deliverables
Students must submit the following:
1. Source code of the complete system on GitHub (Add link to repository at the start of the
design document that will be submitted on Moodle)
2. A design document describing:
○ How the data is stored
○ Indexing method
○ Caching strategy
○ Handling of expiration timestamps
○ Multithreading design
○ And any other design decisions that you have made
3. Compilation and execution instructions
4. Performance results for large datasets
NOTE
1. Absence of any component or not adhering to the submission guidelines will result in a
penalty.
2. Sample Folder Structure (given on the next page, not necessary to follow it)
