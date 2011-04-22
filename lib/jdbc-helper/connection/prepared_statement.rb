# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# An encapsulation of Java PreparedStatment object.
# Used to execute parameterized queries efficiently.
# Has a very similar set of interface as JDBCHelper::Connection.
#
# e.g.
#
#   pstmt = conn.prepare("SELECT * FROM T WHERE a = ? and b = ?")
#   rows = pstmt.query(10, 20)
#   enum = pstmt.enumerate(10, 20)
#
class Connection
class PreparedStatement
	attr_reader :sql

	# Returns the encapsulated JDBC PreparedStatement object.
	def java_obj
		@pstmt
	end

	# Returns the number of parameters required
	def parameter_count
		@pmd ||= @pstmt.get_parameter_meta_data
		@pmd.get_parameter_count
	end

	def update(*params)
		check_closed

		set_params(params)
		measure(:p_update) { @pstmt.execute_update }
	end

	def query(*params, &blk)
		check_closed

		set_params(params)
		# sorry, ignoring privacy
		@conn.send(:process_and_close_rset,
				   measure(:p_query) { @pstmt.execute_query }, &blk)
	end

	def enumerate(*params, &blk)
		check_closed

		return query(*params, &blk) if block_given?

		set_params(params)
		ResultSetEnumerator.new(measure(:p_query) { @pstmt.execute_query })
	end

	# batch interface
	def add_batch(*params)
		check_closed

		set_params(params)
		@pstmt.add_batch
	end
	def execute_batch
		check_closed

		measure(:p_execute_batch) {
			@pstmt.executeBatch
		}
	end
	def clear_batch
		check_closed

		@pstmt.clear_batch
	end

	# Gives the JDBC driver a hint of the number of rows to fetch from the database by a single interaction.
	# This is only a hint. It may no effect at all.
	def set_fetch_size(fsz)
		check_closed

		@pstmt.set_fetch_size fsz
	end

	def close
		return if closed?
		@pstmt.close
		@pstmts.delete @sql
		@pstmt = @pstmts = nil
	end

	def closed?
		@pstmt.nil?
	end
private
	def initialize(conn, pstmts, sql, pstmt) # :nodoc:
		@conn = conn
		@pstmts = pstmts
		@sql = sql
		@pstmt = pstmt
	end

	def set_params(params) # :nodoc:
		idx = 0
		params.each do | param |
			if param.nil?
				@pstmt.set_null(idx += 1, java.sql.Types::NULL)
			elsif setter = SETTER_MAP[param.class.to_s]
				if setter == :setBinaryStream
					@pstmt.send(setter, idx += 1, param.getBinaryStream, param.length)
				elsif setter == :setTimestamp && param.is_a?(Time)
					@pstmt.send(setter, idx += 1, java.sql.Timestamp.new(param.to_i * 1000))
				else
					@pstmt.send(setter, idx += 1, param)
				end
			else
				@pstmt.set_string(idx += 1, param.to_s)
			end
		end
	end

	def measure(type, &blk)	# :nodoc:
		@conn.send(:measure, type, &blk)
	end

	def check_closed
		raise RuntimeError.new("Prepared statement already closed") if closed?
	end

	SETTER_MAP =
	{
 		'Java::JavaSql::Date' => :setDate,
 		'Java::JavaSql::Time' => :setTime,
 		'Java::JavaSql::Timestamp' => :setTimestamp,
 		'Time'                     => :setTimestamp,
		'Java::JavaSql::Blob' => :setBinaryStream,

		# Only available when MySQL JDBC driver is loaded.
		# So we use the string representation of the class.
		'Java::ComMysqlJdbc::Blob' => :setBinaryStream

		# FIXME-MORE
	} # :nodoc:

end#PreparedStatment
end#Connection
end#JDBCHelper

