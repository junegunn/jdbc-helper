# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

require 'jdbc-helper/connection/statement_pool'
require 'jdbc-helper/connection/prepared_statement'
require 'jdbc-helper/connection/result_set_enumerator'
require 'jdbc-helper/connection/row'

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
	# Returns the statistics of the previous operation
	# @return [JDBCHelper::Connection::Stat] The statistics of the previous operation.
	def prev_stat
		@prev_stat.dup
	end

	# Returns the accumulated statistics of each operation
	# @return [Hash] Accumulated statistics of each type of operation
	attr_reader :stats

	# Returns the underlying JDBC Connection object.
	# Only use this when you really need to access it directly.
	def jdbc_conn
		@conn
	end
	alias java_obj jdbc_conn

	# Creates a database connection.
	# - `args` hash must include :driver (or "driver") and :url (or "url")
	# - and takes optional :user and :password tuples (or "user", "password")
	# - You can also specify :timeout (or "timeout") to override the default connection timeout (60 seconds)
	#
	# Must be closed explicitly if not used.
	# If a block is given, the connection is automatically closed after executing the block.
	# @param [Hash] args
	def initialize(args = {})
		# String-tolerance..
		%w[driver url user password timeout].each do | strk |
			args[strk.to_sym] = args.delete strk if args.has_key? strk
		end

		raise ArgumentError.new("driver not given") unless args.has_key? :driver
		raise ArgumentError.new("url not given") unless args.has_key? :url

		begin
			Java::JavaClass.for_name args[:driver]
		rescue Exception
			# TODO
			raise
		end

		timeout = args.has_key?(:timeout) ? args.delete(:timeout) : Constants::DEFAULT_LOGIN_TIMEOUT
		JavaSql::DriverManager.setLoginTimeout timeout if timeout

		props = java.util.Properties.new
		(args.keys - [:url, :driver]).each do | key |
			props.setProperty(key.to_s, args[key]) if args[key]
		end
		
		@conn = JavaSql::DriverManager.get_connection(args[:url], props)
		@spool = StatementPool.send :new, self
		@pstmts = {}
		@bstmt = nil

		@stats = Hash.new { | h, k | h[k] = Stat.new(k, 0, 0, 0) }
		@prev_stat = Stat.new(nil, 0, 0, 0)

		if block_given?
			begin
				yield self
			ensure
				close rescue nil
			end
		end
	end

	# Creates a prepared statement, which is also an encapsulation of Java PreparedStatement object
	# @param [String] qstr SQL string
	def prepare(qstr)
		check_closed

		return @pstmts[qstr] if @pstmts.has_key? qstr

		pstmt =	PreparedStatement.send(:new, self, @pstmts, qstr,
									  measure_exec(:prepare) { @conn.prepare_statement(qstr) })
		@pstmts[qstr] = pstmt
		pstmt
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
			@conn.rollback if status == :unknown
			@conn.set_auto_commit ac
		end
		status == :committed
	end

	# Executes an update and returns the count of the updated rows.
	# @param [String] qstr SQL string
	# @return [Fixnum] Count of affected records
	def update(qstr)
		check_closed

		@spool.with do | stmt |
			ret = measure_exec(:update) { stmt.execute_update(qstr) }
		end
	end

	# Executes a select query.
	# When a code block is given, each row of the result is passed to the block one by one.
	# If a code block not given, this method will return the array of the entire result rows.
	# (which can be pretty inefficient when the result set is large. In such cases, use enumerate instead.)
	#
	# The concept of statement object of JDBC is encapsulated, so there's no need to do additional task,
	# when you nest select queries, for example.
	#
	#   conn.query("SELECT a FROM T") do | trow |
	#       conn.query("SELECT * FROM U_#{trow.a}") do | urow |
	#           # ... and so on ...
	#       end
	#   end
	# @param [String] qstr SQL string
	# @yield [JDBCHelper::Connection::Row]
	# @return [Array]
	def query(qstr, &blk)
		check_closed

		@spool.with do | stmt |
			measure_exec(:query) { stmt.execute(qstr) }
			process_and_close_rset(stmt.get_result_set, &blk)
		end
	end

	# Returns an enumerable object of the query result.
	# "enumerate" method is preferable when dealing with a large result set,
	# since it doesn't have to build a large array.
	#
	# The returned enumerator is automatically closed after enumeration.
	#
	#   conn.enumerate('SELECT * FROM T').each_slice(10) do | slice |
	#       slice.each { | row | print row }
	#       puts
	#   end
	#
	# @param [String] qstr SQL string
	# @yield [JDBCHelper::Connection::Row] Yields each record if block is given
	# @return [JDBCHelper::Connection::ResultSetEnumerator] Returns an enumerator if block is not given
	def enumerate(qstr, &blk)
		check_closed

		return query(qstr, &blk) if block_given?

		stmt = @spool.take
		begin
			measure_exec(:query) { stmt.execute(qstr) }
		rescue Exception
			@spool.give stmt
			raise
		end

		ResultSetEnumerator.send(:new, stmt.get_result_set) { @spool.give stmt }
	end

	# Adds a statement to be executed in batch
	# Adds to the batch
	# @param [String] qstr
	# @return [NilClass]
	def add_batch(qstr)
		check_closed

		@bstmt ||= @spool.take
		@bstmt.add_batch qstr
	end

	# Executes batched statements. No effect when no statment is added
	# @return [NilClass]
	def execute_batch
		check_closed

		return unless @bstmt
		ret = measure_exec(:execute_batch) { @bstmt.execute_batch }
		@spool.give @bstmt
		@bstmt = nil
		ret
	end

	# Clears the batched statements
	# @return [NilClass]
	def clear_batch
		check_closed

		return unless @bstmt
		@bstmt.clear_batch
		@spool.give @bstmt
		@bstmt = nil
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

	# Closes the connection
	# @return [NilClass]
	def close
		return if closed?
		@pstmts.each { | q, pstmt | pstmt.close }
		@spool.close
		@conn.close
		@conn = @spool = nil
	end

	# Returns if this connection is closed or not
	# @return [Boolean]
	def closed?
		@conn.nil?
	end

	# @param [String] table_name Name of the table to be wrapped
	# @return [JDBCHelper::TableWrapper]
	def table table_name
		JDBCHelper::TableWrapper.new self, table_name
	end

	# Statistics
	class Stat
		attr_accessor :type, :elapsed, :success_count, :fail_count

		def initialize(t, e, s, f)
			self.type = t
			self.elapsed = e
			self.success_count = s
			self.fail_count = f
		end
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

	def process_and_close_rset(rset) # :nodoc:
		enum = ResultSetEnumerator.send :new, rset
		rows = []

		begin
			enum.each do | row |
				if block_given?
					yield row
				else
					rows << row
				end
			end
			block_given? ? nil : rows
		ensure
			enum.close
		end
	end

	def update_stat(type, elapsed, success_count, fail_count) # :nodoc:
		@prev_stat.type = type
		@prev_stat.elapsed = elapsed
		@prev_stat.success_count = success_count
		@prev_stat.fail_count = fail_count

		accum = @stats[type]
		accum.elapsed += elapsed
		accum.success_count += success_count
		accum.fail_count += fail_count
	end

	def measure_exec(type)
		begin
			st = Time.now
			ret = yield
			elapsed = Time.now - st
			update_stat(type, elapsed, 1, 0)
		rescue Exception
			update_stat(type, 0, 0, 1)
			raise
		end
		ret
	end
	
	def check_closed
		raise RuntimeError.new('Connection already closed') if closed?
	end
end#Connection
end#JDBCHelper

