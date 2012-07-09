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
             :driver => 'com.mysql.jdbc.Driver',
             :url    => 'jdbc:mysql://localhost/test')
conn.close


# Optional :user and :password
conn = JDBCHelper::Connection.new(
             :driver   => 'com.mysql.jdbc.Driver',
             :url      => 'jdbc:mysql://localhost/test',
             :user     => 'mysql',
             :password => password)
conn.close
```

### Shortcut connectors

jdbc-helper provides shortcut connectors for the following databases
so that you don't have to specify lengthy class names and JDBC URLs.

* MySQL (`JDBCHelper::MySQL`)
* Oracle (`JDBCHelper::Oracle`)
* PostgreSQL (`JDBCHelper::PostgreSQL`)
* MS SQL Server (`JDBCHelper::MSSQL`)
* Cassandra (`JDBCHelper::Cassandra`)

```ruby
# MySQL shortcut connector
mc = JDBCHelper::MySQL.connect(host, user, password, db)

# Oracle shortcut connector
oc = JDBCHelper::Oracle.connect(host, user, password, service_name)

# PostgreSQL shortcut connector
pc = JDBCHelper::PostgreSQL.connect(host, user, password, db)
 
# MS SQL Server shortcut connector
sc = JDBCHelper::MSSQL.connect(host, user, password, db)

# Cassandra CQL3 connector
cc = JDBCHelper::Cassandra.connect(host, keyspace)

# Extra parameters
mc = JDBCHelper::MySQL.connect(host, user, password, db,
       :rewriteBatchedStatements => true)

# With connection timeout of 30 seconds
mc = JDBCHelper::MySQL.connect(host, user, password, db,
       :rewriteBatchedStatements => true, :timeout => 30)

# When block is given, connection is automatically closed after the block is executed
JDBCHelper::Cassandra.connect(host, keyspace) do |cc|
  # ...
end
```

### Querying database table

```ruby
conn.query("SELECT a, b, c FROM T") do |row|
  row.labels
  row.rownum

  row.a, row.b, row.c          # Dot-notation
  row[0], row[1], row[2]       # Numeric index
  row['a'], row['b'], row['c'] # String index. Case-insensitive.
  row[:a], row[:b], row[:c]    # Symbol index. Case-insensitive.

  row[0..-1]                   # Range index. Returns an array of values.
  row[0, 3]                    # Offset and length. Returns an array of values.
end

# Returns an array of rows when block is not given
rows = conn.query("SELECT b FROM T")
uniq_rows = rows.uniq

# You can even nest queries
conn.query("SELECT a FROM T") do |row1|
  conn.query("SELECT * FROM T_#{row1.a}") do |row2|
    # ...
  end
end

# `enumerate' method returns an Enumerable object if block is not given.
# When the result set of the query is expected to be large and you wish to
# chain enumerators, `enumerate' is much preferred over `query'. (which returns the
# array of the entire rows)
conn.enumerate("SELECT * FROM LARGE_T").each_slice(1000) do |slice|
  slice.each do | row |
    # ...
  end
end
```

### Updating database table
```ruby
del_count = conn.update("DELETE FROM T")
```

### Executing any SQL
```ruby
rset = conn.execute("SELECT * FROM T")
rset.each do |row|
  # Returned result must be used or closed
end

del_count = conn.execute("DELETE FROM T")
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
conn.add_batch("DELETE FROM T");
conn.execute_batch
conn.add_batch("DELETE FROM T");
conn.clear_batch
```

### Using prepared statements
```ruby
p_sel = conn.prepare("SELECT * FROM T WHERE b = ? and c = ?")
p_sel.query(100, 200) do |row|
  p row
end
p_sel.close

p_upd = conn.prepare("UPDATE T SET a = ? WHERE b = ?")
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

### Using table wrappers (since 0.2.0)
```ruby
# For more complex examples, refer to test/test_object_wrapper.rb
SQL = JDBCHelper::SQL

# Creates a table wrapper
table = conn.table('test.data')

# Counting the records in the table
table.count
table.count(:a => 10)
table.where(:a => 10).count

table.empty?
table.where(:a => 10).empty?

# Selects the table by combining select, where, and order methods
table.select('a apple', :b).where(:c => (1..10)).order('b desc', 'a asc') do |row|
  puts row.apple
end

# Build select SQL
sql = table.select('a apple', :b).where(:c => (1..10)).order('b desc', 'a asc').sql

# Updates with conditions
table.where(:c => 3).update(:a => 'hello', :b => SQL.expr('now()'))

# Insert into the table
table.insert(:a => 10, :b => 20, :c => SQL.expr('10 + 20'))
table.insert_ignore(:a => 10, :b => 20, :c => 30)
table.replace(:a => 10, :b => 20, :c => 30)

# Update with common default values
with_defaults = table.default(:a => 10, :b => 20)
with_defaults.insert(:c => 30)
with_defaults.where('a != 10 or b != 20').update   # sets a => 10, b => 20

# Batch updates with batch method
table.batch.insert(:a => 10, :b => 20, :c => SQL.expr('10 + 20'))
table.batch.insert_ignore(:a => 10, :b => 20, :c => 30)
conn.execute_batch

# Delete with conditions
table.delete(:c => 3)
# Or equivalently,
table.where(:c => 3).delete

# Truncate or drop table (Cannot be undone)
table.truncate!
table.drop!
```

#### Building complex where clauses
```ruby
SQL = JDBCHelper::SQL                  # Shortcut. Or you can just include JDBCHelper

# With Hash
scope = table.where(
  :a => 'abc',                         # a = 'abc'
  :b => (1..10),                       # and b >= 1 and b <= 10
  :c => (1...10),                      # and c >= 1 and c < 10
  :d => %w[a b c],                     # and d in ('a', 'b', 'c')
  :e => SQL.expr('sysdate'),           # and e = sysdate
  :f => SQL.not_null,                  # and f is not null
  :g => SQL.gt(100),                   # and g > 100
  :h => SQL.lt(100),                   # and h < 100
  :i => SQL.like('ABC%'),              # and i like 'ABC%'
  :j => SQL.not_like('ABC%'),          # and j not like 'ABC%'
  :k => SQL.le( SQL.expr('sysdate') )  # and k <= sysdate
)
scope.update(:a => 'xyz')

# With Array
scope = table.where(["a = ? or b > ?", 'abc', 10])
```

#### Invalid use of dynamic conditions

TableWrapper object internally creates JDBC PreparedStatements.
If you dynamically build many condition-strings as the following example, 
it would soon fail because there will be too many open PreparedStatements.

```ruby
10000.times do |idx|
  table.count("id = #{idx}")
end
```

Correct ways of doing the same would be as follows.

```ruby
10000.times do |idx|
  # 1. with Hash
  table.count('id' => idx)

  # 2. with Array
  table.count(["id = ?", idx])
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
         :a => 100, :b => ["value", String], :c => Fixnum)
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

## Copyright

Copyright (c) 2011 Junegunn Choi. See LICENSE.txt for
further details.

