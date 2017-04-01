### 0.8.3

* Fixed not to ignore property with false value

### 0.8.2

* Allow :driver parameter to be an object that responds to `connect`

### 0.8.1

* Allow :driver parameter to be in either String or Class

### 0.8.0

0.8.0 introduces a few backward-incompatible changes.

* `Connection#ResultSetEnumerator` is renamed to `Connection::ResultSet`
* `Connection#query` method will return a ResultSet object instead of an Array
  * `Connection#enumerate` method is now retired, and just a synonym for query method
  * Partially consumed ResultSet must be closed explicitly
* `ResultSet#each` method will return an enumerator when block is not given
* Refined TableWrapper interface with external [sql_helper](https://github.com/junegunn/sql_helper) gem
  * The use of `JDBCHelper::SQL` is deprecated
* Added MariaDB connector
* Added SQLite connector

### 0.7.7 / 2013/01/0?
* `PreparedStatment`s and `TableWrapper`s now inherit the fetch size of the connection
* Added `JDBCHelper::TableWrapper#fetch_size`
* Added `JDBCHelper::TableWrapper#execute_batch`
* Added `JDBCHelper::TableWrapper#clear_batch`
* `execute_batch` method returns the sum of all update counts
* Removed undocumented operation statistics

### 0.7.6 / 2012/08/26
* Added FileMaker Pro connector (statonjr)

### 0.7.5 / 2012/07/28
* `require 'jdbc-helper/sql'` is allowed in non-Java environments

### 0.7.4 / 2012/07/09
* Revised Connectors
 * Use `JDBCHelper::MySQL` instead of `JDBCHelper::MySQLConnector`
  * Optional `timeout` parameter has been removed. For backward-compatibility, `XXXConnector` still receives timeout parameter, though deprecated.
* Added Cassandra connector
 * `JDBCHelper::Cassandra#connect`

### 0.7.3 / 2012/05/11
* Fixed occasional error when rolling back transaction: "Can't call rollback when autocommit=true"

### 0.7.2 / 2012/03/07
* Added JDBCHelper::Connection#execute, JDBCHelper::PreparedStatement#execute
 * Returns update count as Fixnum or JDBCHelper::Connection::ResultSetEnumerator
   depending on the type of the given statement.
* Bug fix: Setting nil value for PreparedStatements fails on SQL Server
 * Modified to refer to ParameterMetaData when calling setNull method
 * Current version of SQL Server driver has some crazy bugs with PreparedStatements.
   You may notice that SQL Server complains on a perfectly fine SQL statement
   only when you try to access the metadata of its wrapping PreparedStatement.
   For such cases, where we can't refer to ParameterMetaData (and you will see a warning log),
   setting nulls should still fail.

### 0.7.1 / 2012/02/28
* Modified nextval/currval syntax in JDBCHelper::SequenceWrapper
 * PostgreSQL: nextval('[sequence]')
 * Others: [sequence].nextval
* Bug fix: JDBCHelper::Connection#inspect returns invalid data

### 0.7.0 / 2012/01/30
* Helper methods for generating where clauses for TableWarppers
 * `JDBCHelper::SQL.expr` (`JDBCHelper::SQL` deprecated in favor of `expr`)
 * `JDBCHelper::SQL.ne`
 * `JDBCHelper::SQL.gt`
 * `JDBCHelper::SQL.ge`
 * `JDBCHelper::SQL.lt`
 * `JDBCHelper::SQL.le`
 * `JDBCHelper::SQL.like`
 * `JDBCHelper::SQL.not_like`
 * `JDBCHelper::SQL.not_null`
* Tested on PostgreSQL
* Tested on MS SQL Server
* JDBCHelper::PostgresConnector added.
* JDBCHelper::SqlServerConnector added.


### 0.6.3 / 2011/11/09
* Added flexibility when writing TableWrapper conditions with the new ActiveRecord-like Array-expression.

```ruby
table.count(["title != ? or year <= ?", 'N/A', '2011'], :month => (1..3), :day => 10)
```

### 0.6.2 / 2011/10/27
* Bug fix: JDBCHelper::Connection#initialize *still* modifies its input Hash

### 0.6.1 / 2011/09/05
* Minor fix for an inefficient type conversion
* More tests. Test coverage over 97%.

### 0.6.0 / 2011/09/05
* Datatype handling
 * Proper BLOB handling.
 * Proper handling of Numeric types.
  * Numeric value will be returned as Fixnum, Bignum, Float or BigDecimal. (No more Strings!)
 * ParameterizedStatement#set_param achieves better precision with Time#to_f
* Increased test coverage

### 0.5.1 / 2011/08/29
* Bug fix: TableWrapper objects are cached to prevent proliferation of PreparedStatements.

```ruby
# In the previous version, codes like the following generated too many prepared statements.
(0..10000).each do |i|
  db.table(:my_table).count(:id => i)
end
```

### 0.5.0 / 2011/08/29
* TableWrapper internally uses PreparedStatements instead of regular statements for better performance. <b>However, this makes it impossible to serialize batched operations.</b> Thus, you should not use batched statements with TableWrapper when the order of operations is important.
* TableWrapper#close closes cached PreparedStatements.
* Connection#execute_batch and Connection#clear_batch have been modified to execute or clear every PreparedStatment of the Connection.
* Connection overrides clone method to create another connection to the same database.

### 0.4.10 / 2011/08/09
* Sadly, I found that with Oracle, precision and scale information from ResultSetMetaData is completely unreliable. Going conservative.

### 0.4.9 / 2011/08/09
* Oracle NUMBER datatype is retrived as Fixnum or Bignum or String depending on its precision and scale.

### 0.4.8 / 2011/07/12
* Improved fetch size interface
* Supports non-string property values in the input parameter for Connection object

### 0.4.7 / 2011/06/28
* PreparedStatement performance improvement. Minimized unneccesary to_s call when setting parameters. 10% to 30% improvement observed for usual bulk insertion.

### 0.4.6 / 2011/06/20
* Bug fix: Invalid alias for JDBCHelper::TableWrapper#drop!
* Bug fix: JDBCHelper::Connection#initialize modifies its input Hash
* Added attr_readers for url and driver to JDBCHelper::Connection

### 0.4.5 / 2011/06/10
* JDBCHelper::TableWrapper#default method added which allows you to specify common default values for the subsequent inserts and updates.

### 0.4.4 / 2011/06/08
* Where conditions have become accumulative, which means you can chain multiple JDBCHelper::TableWrapper#where methods to further limit the scope.

```ruby
users = conn.table(:users).where(:status => 1).where(:age => 20..30).to_a
```

### 0.4.3 / 2011/06/06
* Improved handling of hash parameters for JDBCHelper::ProcedureWrapper. Missing parameters in the given Hash are treated as nulls. And now it works for both Oracle and MySQL.
* Users are encouraged to update their MySQL JDBC driver to the latest version. Some features of ProcedureWrapper may not work properly on older versions.

### 0.4.2 / 2011/06/03
* JDBCHelper::SequenceWrapper added.
* JDBCHelper::TableWrapper#drop!, JDBCHelper::TableWrapper#truncate!

### 0.4.1 / 2011/06/01
* Methods in JDBCHelper::TableWrapper which take where conditions as their arguments are modified to take varible length arguments.

### 0.4.0 / 2011/05/31
* Rewrote JDBCHelper::TableWrapper interface
* JDBCHelper::TableWrapper has become Enumerable with new select, where, and order methods
* JDBCHelper::TableWrapper now supports batch updates
* WARNING: JDBCHelper::TableWrapper#select and methods in JDBCHelper::SQL are not backward-compatible.

### 0.3.2 / 2011/05/25
* JDBCHelper::MySQLConnector.connect and JDBCHelper::OracleConnector.connect can take a block

### 0.3.0 / 2011/05/21
* Supports CallableStatement with IN/INOUT/OUT parameters
* Added JDBCHelper::FunctionWrapper
* Added JDBCHelper::ProcedureWrapper
* Performance tuning (10-20% improvement for some scenarios)
* Removed PreparedStatement caching since PreparedStatement can be invalidated after DDLs
* Revised JDBCHelper::SQL methods (JDBCHelper::SQL#where now prepends `where')
* Minor bug fixes
* Tested with MySQL and Oracle

### 0.2.1 / 2011/05/19
* JDBCHelper::Connection::Row can now be accessed with Range, Symbol and *[offset, length] index

### 0.2.0 / 2011/05/19
* Feature: JDBCHelper::TableWrapper added to reduce the hassle of writing SQLs.
* YARD documentation

### 0.1.3 / 2011/04/22
* Bug fix: setTimestamp now works correctly for java.sql.Timestamp
* Usability: Ruby Time object is automatically converted to java.sql.Timestamp object if the type of the target column is Timestamp.

### 0.1.1-0.1.2 / 2011/04/01
* Yanked bad gem.

### 0.1.0 / 2011/03/31
* Created. Based on samdorr-db 0.2.7. samdorr-db will be no longer maintained from now on.

