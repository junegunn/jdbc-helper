# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

require 'insensitive_hash/minimal'

require 'jdbc-helper/connection/type_map'
require 'jdbc-helper/connection/parameterized_statement'
require 'jdbc-helper/connection/prepared_statement'
require 'jdbc-helper/connection/callable_statement'
require 'jdbc-helper/connection/statement_pool'
require 'jdbc-helper/connection/result_set'
require 'jdbc-helper/connection/row'

require 'jdbc-helper/wrapper/object_wrapper'
require 'jdbc-helper/wrapper/table_wrapper'
require 'jdbc-helper/wrapper/table_wrapper'
require 'jdbc-helper/wrapper/sequence_wrapper'
require 'jdbc-helper/wrapper/function_wrapper'
require 'jdbc-helper/wrapper/procedure_wrapper'

module JDBCHelper
# Encapsulates JDBC database connection.
# Lets you easily execute SQL statements and access their results.
#
# @example Prerequisites
#  # Add JDBC driver of the DBMS you're willing to use to your CLASSPATH
#  export CLASSPATH=$CLASSPATH:~/lib/mysql-connector-java.jar
#
#
# @example Connecting to a database
#
#  # :driver and :url must be given
#  conn = JDBCHelper::Connection.new(
#               :driver => 'com.mysql.jdbc.Driver',
#               :url    => 'jdbc:mysql://localhost/test')
#  conn.close
#
#
#  # Optional :user and :password
#  conn = JDBCHelper::Connection.new(
#               :driver   => 'com.mysql.jdbc.Driver',
#               :url      => 'jdbc:mysql://localhost/test',
#               :user     => 'mysql',
#               :password => '')
#  conn.close
#
#
#  # MySQL shortcut connector
#  conn = JDBCHelper::MySQLConnector.connect('localhost', 'mysql', '', 'test')
#  conn.close
#
# @example Querying database table
#
#  conn.query("SELECT a, b, c FROM T") do | row |
#      p row.labels
#      p row.rownum
#
#      puts row.a, row.b, row.c
#      puts row[0], row[1], row[2]
#      puts row['a'], row['b'], row['c']
#  end
#
#  # Returns an array of rows when block is not given
#  rows = conn.query("SELECT b FROM T")
#  uniq_rows = rows.uniq
#
#  # You can even nest queries
#  conn.query("SELECT a FROM T") do | row1 |
#      conn.query("SELECT * FROM T_#{row1.a}") do | row2 |
#          # ...
#      end
#  end
# @example Updating database table
#  del_count = conn.update("DELETE FROM T")
#
# @example Transaction
#  committed = conn.transaction do | tx |
#      # ...
#      # Transaction logic here
#      # ...
#
#      if success
#          tx.commit
#      else
#          tx.rollback
#      end
#  end
#
# @example Using batch interface
#  conn.add_batch("DELETE FROM T");
#  conn.execute_batch
#  conn.add_batch("DELETE FROM T");
#  conn.clear_batch
#
# @example Using prepared statements
#  p_sel = conn.prepare("SELECT * FROM T WHERE b = ? and c = ?")
#  p_sel.query(100, 200) do | row |
#      p row
#  end
#  p_sel.close
#
#  p_upd = conn.prepare("UPDATE T SET a = ? WHERE b = ?")
#  count = 0
#  100.times do | i |
#      count += p_upd.update('updated a', i)
#  end
#
#  p_upd.add_batch('pstmt + batch', 10)
#  p_upd.add_batch('pstmt + batch', 20)
#  p_upd.add_batch('pstmt + batch', 30)
#  p_upd.execute_batch
#  p_upd.close
class Connection
  # JDBC URL of the connection
  # @return [String]
  attr_reader :url

  # JDBC driver of the connection
  # @return [String|Class|#connect]
  attr_reader :driver

  # Returns the underlying JDBC Connection object.
  # Only use this when you really need to access it directly.
  def jdbc_conn
    @conn
  end
  alias java_obj jdbc_conn
  alias java     jdbc_conn

  # Creates a database connection.
  # - `args` hash must include :driver (or "driver") and :url (or "url")
  # - and takes optional :user and :password tuples (or "user", "password")
  # - You can also specify :timeout (or "timeout") to override the default connection timeout (60 seconds)
  #
  # Must be closed explicitly if not used.
  # If a block is given, the connection is automatically closed after executing the block.
  # @param [Hash] args
  def initialize(args = {})
    # Subsequent deletes should not affect the input
    @args = args
    args = InsensitiveHash[ @args ]

    raise ArgumentError.new("driver not given") unless args.has_key? :driver
    raise ArgumentError.new("url not given") unless args.has_key? :url

    @driver = args.delete :driver
    @url = args.delete :url

    timeout = args.has_key?(:timeout) ? args.delete(:timeout) : Constants::DEFAULT_LOGIN_TIMEOUT
    if timeout
      if timeout.is_a?(Fixnum) == false || timeout <= 0
        raise ArgumentError.new("Timeout must be a positive integer")
      end
      Java::java.sql.DriverManager.setLoginTimeout timeout
    end

    props = Java::java.util.Properties.new
    args.each do |k, v|
      props.setProperty(k.to_s, v.to_s) unless v.nil?
    end

    @conn = case @driver
            when String
              # NameError will be thrown for invalid drivers
              Java::JavaClass.for_name @driver
              Java::java.sql.DriverManager.get_connection(@url, props)
            when Class
              @driver.new.connect(@url, props)
            else
              if @driver.respond_to?(:connect)
                @driver.connect(@url, props)
              else
                raise ArgumentError.new('Invalid type for :driver')
              end
            end
    @spool = StatementPool.send :new, self
    @bstmt = nil
    @fetch_size = nil

    @pstmts = []

    @table_wrappers = {}

    if block_given?
      begin
        yield self
      ensure
        close rescue nil
      end
    end
  end

  # Creates another connection with the same parameters as this Connection.
  # @return [JDBCHelper::Connection]
  def clone
    nc = JDBCHelper::Connection.new @args
    nc.fetch_size = @fetch_size if @fetch_size
    nc
  end

  # Creates a prepared statement, which is also an encapsulation of Java PreparedStatement object
  # @param [String] qstr SQL string
  def prepare(qstr)
    check_closed

    pstmt = PreparedStatement.send(:new, self, qstr, @conn.prepare_statement(qstr))
    pstmt.set_fetch_size @fetch_size if @fetch_size

    @pstmts << pstmt
    pstmt
  end

  # @return [Array] Prepared statements currently opened for this connection
  def prepared_statements
    @pstmts
  end

  # Creates a callable statement.
  # @param [String] qstr SQL string
  def prepare_call(qstr)
    check_closed

    CallableStatement.send(:new, self, qstr, @conn.prepare_call(qstr))
  end

  # Executes the given code block as a transaction. Returns true if the transaction is committed.
  # A transaction object is passed to the block, which only has commit and rollback methods.
  # The execution breaks out of the code block when either of the methods is called.
  # @yield [JDBCHelper::Connection::Transaction] Responds to commit and rollback.
  # @return [Boolean] True if committed
  def transaction
    check_closed

    raise ArgumentError.new("Transaction block not given") unless block_given?
    tx = Transaction.send :new, @conn
    ac = @conn.get_auto_commit
    status = :unknown
    begin
      @conn.set_auto_commit false
      yield tx
      @conn.commit
      status = :committed
    rescue Transaction::Commit
      status = :committed
    rescue Transaction::Rollback
      status = :rolledback
    ensure
      @conn.rollback if status == :unknown && @conn.get_auto_commit == false
      @conn.set_auto_commit ac
    end
    status == :committed
  end

  # Executes an SQL and returns the count of the update rows or a ResultSet object
  # depending on the type of the given statement.
  # If a ResultSet is returned, it must be enumerated or closed.
  # @param [String] qstr SQL string
  # @return [Fixnum|ResultSet]
  def execute(qstr)
    check_closed

    stmt = @spool.take
    begin
      if stmt.execute(qstr)
        ResultSet.send(:new, stmt.getResultSet) { @spool.give stmt }
      else
        rset = stmt.getUpdateCount
        @spool.give stmt
        rset
      end
    rescue Exception => e
      @spool.give stmt
      raise
    end
  end

  # Executes an update and returns the count of the updated rows.
  # @param [String] qstr SQL string
  # @return [Fixnum] Count of affected records
  def update(qstr)
    check_closed

    @spool.with do | stmt |
      ret = stmt.execute_update(qstr)
    end
  end

  # Executes a select query.
  # When a code block is given, each row of the result is passed to the block one by one.
  # If not given, ResultSet is returned, which can be used to enumerate through the result set.
  # ResultSet is closed automatically when all the rows in the result set is consumed.
  #
  # @example Nested querying
  #   conn.query("SELECT a FROM T") do | trow |
  #     conn.query("SELECT * FROM U_#{trow.a}").each_slice(10) do | urows |
  #       # ...
  #     end
  #   end
  # @param [String] qstr SQL string
  # @yield [JDBCHelper::Connection::Row]
  # @return [Array]
  def query(qstr, &blk)
    check_closed

    stmt = @spool.take
    begin
      rset = stmt.execute_query(qstr)
    rescue Exception => e
      @spool.give stmt
      raise
    end

    enum = ResultSet.send(:new, rset) { @spool.give stmt }
    if block_given?
      enum.each do |row|
        yield row
      end
    else
      enum
    end
  end
  alias enumerate query

  # Adds a statement to be executed in batch
  # Adds to the batch
  # @param [String] qstr
  # @return [NilClass]
  def add_batch(qstr)
    check_closed

    @bstmt ||= @spool.take
    @bstmt.add_batch qstr
  end

  # Executes batched statements including prepared statements. No effect when no statement is added
  # @return [Fixnum] Sum of all update counts
  def execute_batch
    check_closed

    cnt = 0

    if @bstmt
      cnt += @bstmt.execute_batch.inject(:+) || 0
      @spool.give @bstmt
      @bstmt = nil
    end

    @pstmts.each do |pstmt|
      cnt += pstmt.execute_batch
    end

    cnt
  end

  # Clears the batched statements including prepared statements.
  # @return [NilClass]
  def clear_batch
    check_closed

    if @bstmt
      @bstmt.clear_batch
      @spool.give @bstmt
      @bstmt = nil
    end

    @pstmts.each do |stmt|
      stmt.clear_batch
    end
  end

  # Gives the JDBC driver a hint of the number of rows to fetch from the database by a single interaction.
  # This is only a hint. It may have no effect at all.
  # @param [Fixnum] fsz
  # @return [NilClass]
  def set_fetch_size(fsz)
    check_closed

    @fetch_size = fsz
    @spool.each { | stmt | stmt.set_fetch_size @fetch_size }
  end
  alias fetch_size= set_fetch_size

  # Returns the fetch size of the connection. If not set, nil is returned.
  # @return [Fixnum]
  attr_reader :fetch_size

  # Closes the connection
  # @return [NilClass]
  def close
    return if closed?
    @spool.close
    @conn.close
    @conn = @spool = nil
  end

  # Returns if this connection is closed or not
  # @return [Boolean]
  def closed?
    @conn.nil?
  end

  # Returns a table wrapper for the given table name
  # @since 0.2.0
  # @param [String/Symbol] table_name Name of the table to be wrapped
  # @return [JDBCHelper::TableWrapper]
  def table table_name
    table = JDBCHelper::TableWrapper.new(self, table_name)
    table = table.fetch_size(@fetch_size) if @fetch_size
    @table_wrappers[table_name] ||= table
  end
  alias [] table

  # Returns a sequence wrapper for the given name
  # @since 0.4.2
  # @param [String/Symbol] sequence_name Name of the sequence to be wrapped
  # @return [JDBCHelper::SequenceWrapper]
  def sequence sequence_name
    JDBCHelper::SequenceWrapper.new self, sequence_name
  end

  # Returns a function wrapper for the given function name
  # @since 0.2.2
  # @param [String/Symbol] func_name Name of the function to be wrapped
  # @return [JDBCHelper::FunctionWrapper]
  def function func_name
    JDBCHelper::FunctionWrapper.new self, func_name
  end

  # Returns a procedure wrapper for the given procedure name
  # @since 0.3.0
  # @param [String/Symbol] proc_name Name of the procedure to be wrapped
  # @return [JDBCHelper::ProcedureWrapper]
  def procedure proc_name
    JDBCHelper::ProcedureWrapper.new self, proc_name
  end

  # @return [String]
  def inspect
    InsensitiveHash[@args].merge({ :closed? => closed? }).tap { |c|
      c.delete(:password)
    }.inspect
  end

private
  # Transaction object passed to the code block given to transaction method
  class Transaction
    # Commits the transaction
    # @raise [JDBCHelper::Transaction::Commit]
    def commit
      @conn.commit
      raise Commit
    end
    # Rolls back this transaction
    # @raise [JDBCHelper::Transaction::Rollback]
    def rollback
      @conn.rollback
      raise Rollback
    end
  private
    def initialize(conn) # :nodoc:
      @conn = conn
    end
    class Commit < Exception # :nodoc:
    end
    class Rollback < Exception # :nodoc:
    end
  end

  def create_statement # :nodoc:
    stmt = @conn.create_statement
    stmt.set_fetch_size @fetch_size if @fetch_size
    stmt
  end

  def close_pstmt pstmt
    @pstmts.delete pstmt
  end

  def check_closed
    raise RuntimeError.new('Connection already closed') if closed?
  end
end#Connection
end#JDBCHelper

