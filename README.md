```
    _     _ _                 _          _
   (_)   | | |               | |        | |
    _  __| | |__   ___ ______| |__   ___| |_ __   ___ _ __
   | |/ _` | '_ \ / __|______| '_ \ / _ \ | '_ \ / _ \ '__|
   | | (_| | |_) | (__       | | | |  __/ | |_) |  __/ |
   | |\__,_|_.__/ \___|      |_| |_|\___|_| .__/ \___|_|
  _/ |                                    | |
 |__/                                     |_|
```

# jdbc-helper

A JDBC helper for Ruby/Database developers.
JDBCHelper::Connection object wraps around a JDBC connection and provides far nicer interface to
crucial database operations from primitive selects and updates to more complex ones involving
batch updates, prepared statements and transactions.
As the name implies, this gem only works on JRuby.

Tested on MySQL 5.5, Oracle 11g R2, PostgreSQL 9.0.4, MS SQL Server 2008 R2 and Cassandra 1.1.1 (CQL3).

## Installation
### Install gem
```
gem install jdbc-helper
```

### Setting up CLASSPATH
Add the appropriate JDBC drivers to the CLASSPATH.

```
export CLASSPATH=$CLASSPATH:~/lib/mysql-connector-java-5.1.16-bin.jar:~/lib/ojdbc6.jar
```

### In Ruby
```ruby
require 'jdbc-helper'
```

## Examples
### Connecting to a database
```ruby
# :driver and :url must be given
conn = JDBCHelper::Connection.new(
             driver: 'com.mysql.jdbc.Driver',
             url:    'jdbc:mysql://localhost/test')
conn.close


# Optional :user and :password
conn = JDBCHelper::Connection.new(
             driver:   'com.mysql.jdbc.Driver',
             url:      'jdbc:mysql://localhost/test',
             user:     'mysql',
             password: password)
conn.close
```

### Shortcut connectors

jdbc-helper provides shortcut connectors for the following databases
so that you don't have to specify lengthy class names and JDBC URLs.

- MySQL (`JDBCHelper::MySQL`)
- MariaDB (`JDBCHelper::MariaDB`)
- Oracle (`JDBCHelper::Oracle`)
- PostgreSQL (`JDBCHelper::PostgreSQL`)
- MS SQL Server (`JDBCHelper::MSSQL`)
- Cassandra (`JDBCHelper::Cassandra`)
- FileMaker Pro (`JDBCHelper::FileMaker`)
- SQLite (`JDBCHelper::SQLite`)

```ruby
# MySQL shortcut connector
mc = JDBCHelper::MySQL.connect(host, user, password, db)

# MariaDB shortcut connector
mc = JDBCHelper::MariaDB.connect(host, user, password, db)

# Oracle shortcut connector
oc = JDBCHelper::Oracle.connect(host, user, password, service_name)

# PostgreSQL shortcut connector
pc = JDBCHelper::PostgreSQL.connect(host, user, password, db)

# MS SQL Server shortcut connector
sc = JDBCHelper::MSSQL.connect(host, user, password, db)

# Cassandra CQL3 connector
cc = JDBCHelper::Cassandra.connect(host, keyspace)

# FileMaker Pro shortcut connector
fmp = JDBCHelper::FileMaker.connect(host, user, password, db)

# SQLite connector
sc = JDBCHelper::SQLite.connect(file_path)

# Extra parameters
mc = JDBCHelper::MySQL.connect(host, user, password, db,
       rewriteBatchedStatements: true)

# With connection timeout of 30 seconds
mc = JDBCHelper::MySQL.connect(host, user, password, db,
       rewriteBatchedStatements: true, timeout: 30)

# When block is given, connection is automatically closed after the block is executed
JDBCHelper::Cassandra.connect(host, keyspace) do |cc|
  # ...
end
```

### Querying database table

```ruby
conn.query('SELECT a, b, c FROM T') do |row|
  row.labels
  row.rownum

  a, b, c = row
  a, b, c = row.a,    row.b,    row.c    # Dot-notation
  a, b, c = row[0],   row[1],   row[2]   # Numeric index
  a, b, c = row['a'], row['b'], row['c'] # String index. Case-insensitive.
  a, b, c = row[:a],  row[:b],  row[:c]  # Symbol index. Case-insensitive.

  row[0..-1]                   # Range index. Returns an array of values.
  row[0, 3]                    # Offset and length. Returns an array of values.

  row.to_h                     # Row as a Hash
end

# You can even nest queries
conn.query('SELECT a FROM T') do |row1|
  conn.query("SELECT * FROM T_#{row1.a}") do |row2|
    # ...
  end
end

# Connection::ResultSet object is returned when block is not given
# - ResultSet is automatically closed when entirely iterated
rows = conn.query('SELECT * FROM T')
uniq_rows = rows.to_a.uniq

# However, partially consumed ResultSet objects *must be closed* manually
rset = conn.query('SELECT * FROM T')
rows = rset.take(2)
rset.close

# Enumerator chain
conn.query('SELECT * FROM LARGE_T').each_slice(1000).with_index do |slice, idx|
  slice.each do |row|
    # ...
  end
end
```

### Updating database table
```ruby
del_count = conn.update('DELETE FROM T')
```

### Executing any SQL
```ruby
rset = conn.execute('SELECT * FROM T')
rset.each do |row|
  # Returned result must be used or closed
end

del_count = conn.execute('DELETE FROM T')
```

### Transaction
```ruby
committed = conn.transaction do |tx|
  # ...
  # Transaction logic here
  # ...

  if success
    tx.commit
  else
    tx.rollback
  end
  # You never reach here.
end
```

### Using batch interface
```ruby
conn.add_batch('DELETE FROM T')
conn.execute_batch
conn.add_batch('DELETE FROM T')
conn.clear_batch
```

### Using prepared statements
```ruby
p_sel = conn.prepare('SELECT * FROM T WHERE b = ? and c = ?')
p_sel.query(100, 200) do |row|
  p row
end
p_sel.close

p_upd = conn.prepare('UPDATE T SET a = ? WHERE b = ?')
count = 0
100.times do |i|
  count += p_upd.update('updated a', i)
end

p_upd.add_batch('pstmt + batch', 10)
p_upd.add_batch('pstmt + batch', 20)
p_upd.add_batch('pstmt + batch', 30)
p_upd.execute_batch
p_upd.close
```

### Accessing underlying Java object with `java` method

```ruby
conn.java.setAutoCommit false

pstmt = conn.prepare(sql)
pstmt.java.getMetaData
```

### Using table wrappers (since 0.2.0)
```ruby
# Creates a table wrapper
table = conn.table('test.data')
# Or equievalently,
table = conn['test.data']

# Counting the records in the table
table.count
table.count(a: 10)
table.where(a: 10).count

table.empty?
table.where(a: 10).empty?

# Selects the table by combining select, where, order, limit and fetch_size methods
table.select('a apple', :b).where(c: (1..10)).order('b desc', 'a asc').fetch_size(100).limit(1000).each do |row|
  puts row.apple
end

# Build select SQL
sql = table.select('a apple', :b).where(c: (1..10)).order('b desc', 'a asc').sql

# Updates with conditions
table.where(c: 3).update(a: 'hello', b: { sql: 'now()' })

# Insert into the table
table.insert(a: 10, b: 20, c: { sql: '10 + 20' })
table.insert_ignore(a: 10, b: 20, c: 30)
table.replace(a: 10, b: 20, c: 30)

# Update with common default values
with_defaults = table.default(a: 10, b: 20)
with_defaults.insert(c: 30)
with_defaults.where('a != 10 or b != 20').update   # sets a => 10, b => 20

# Batch updates with batch method
table.batch.insert(a: 10, b: 20, c: { sql: '10 + 20' })
table.batch.insert_ignore(a: 10, b: 20, c: 30)
table.batch.where(a: 10).update(a: 20)
table.execute_batch :insert, :update

# Delete with conditions
table.delete(c: 3)
# Or equivalently,
table.where(c: 3).delete

# Truncate or drop table (Cannot be undone)
table.truncate!
table.drop!
```

#### Building complex where clauses
```ruby
# With any number of Strings, Arrays and Hashes
scope = table.where(
  "x <> 'hello'",                # x <> 'hello'
  ["y = ? or z > ?", 'abc', 10], # and (y = 'abc' or z > 10)
  a: 'abc',                      # and a = 'abc'
  b: (1..10),                    # and b between 1 and 10
  c: (1...10),                   # and c >= 1 and c < 10
  d: %w[a b c],                  # and d in ('a', 'b', 'c')
  e: { expr: 'sysdate' },        # and e = sysdate
  f: { not: nil },               # and f is not null
  g: { gt: 100, le: 200 },       # and g > 100 and g <= 200
  h: { lt: 100 },                # and h < 100
  i: { like: 'ABC%' },           # and i like 'ABC%'
  j: { not: { like: 'ABC%' } },  # and j not like 'ABC%'
  k: { le: { expr: 'sysdate' } } # and k <= sysdate
)
scope.update(a: 'xyz')
```

#### Invalid use of plain String conditions

A TableWrapper object internally builds SQL strings
and creates JDBC PreparedStatement object for each distinct SQL.

If you build many number of where-clause Strings as shown in the following code,
soon there will be too many open PreparedStatements,
and if the number exceeds the system limit, an error will be thrown.

```ruby
table = connection['table']

# Leads to 10000 PreparedStatements !!
10000.times do |idx|
  table.count("id = #{idx}")
    # select count(*) from table where id = 0
    # select count(*) from table where id = 1
    # select count(*) from table where id = 2
    # select count(*) from table where id = 3
    # ...
end
```

In that case, you can `close` the table wrapper to close all the open PreparedStatements.

```ruby
table.close
```

However, you should always prefer using much more efficient Hash or Array expression over plain String,
so you don't have to worry about the proliferation of PreparedStatements.

```ruby
# 20000 queries but only a single PreparedStatement
10000.times do |idx|
  # 1. with Hash
  table.count('id' => idx)
    # select count(*) from table where id = ?

  # 2. with Array
  table.count(["id = ?", idx])
    # select count(*) from table where id = ?
end
```

### Using function wrappers (since 0.2.2)
```ruby
conn.function(:mod).call 5, 3
conn.function(:coalesce).call(nil, nil, 'king')
```

### Using procedure wrappers (since 0.3.0)
```ruby
# Working with IN/INOUT/OUT parameteres
# Bind by ordinal number
conn.procedure(:update_and_fetch_something).call(
         100,                 # Input parameter
         ["value", String],   # Input/Output parameter
         Fixnum               # Output parameter
)

# Bind by parameter name
conn.procedure(:update_and_fetch_something).call(
         a: 100, b: ["value", String], c: Fixnum)
```

### Using sequence wrappers (since 0.4.2)
```ruby
seq = conn.sequence(:my_seq)
next = seq.nextval
curr = seq.currval
seq.reset!
seq.reset! 100
```

## Contributing to jdbc-helper

* Check out the latest master to make sure the feature hasn't been implemented or the bug hasn't been fixed yet
* Check out the issue tracker to make sure someone already hasn't requested it and/or contributed it
* Fork the project
* Start a feature/bugfix branch
* Commit and push until you are happy with your contribution
* Make sure to add tests for it. This is important so I don't break it in a future version unintentionally.
* Please try not to mess with the Rakefile, version, or history. If you want to have your own version, or is otherwise necessary, that is fine, but please isolate to its own commit so I can cherry-pick around it.

## Contributors

* [Larry Staton Jr.](https://github.com/statonjr)

## Copyright

Copyright (c) 2011 Junegunn Choi. See LICENSE.txt for
further details.
