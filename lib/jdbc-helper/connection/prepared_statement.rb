# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class Connection
# An encapsulation of Java PreparedStatment object.
# Used to execute parameterized queries efficiently.
# Has a very similar set of interface to that of JDBCHelper::Connection.
#
# @example
#   pstmt = conn.prepare("SELECT * FROM T WHERE a = ? and b = ?")
#   rows = pstmt.query(10, 20)
#   enum = pstmt.enumerate(10, 20)
class PreparedStatement < ParameterizedStatement
	# Returns the number of parameters required
	# @return [Fixnum]
	def parameter_count
		@pmd ||= @java_obj.get_parameter_meta_data
		@pmd.get_parameter_count
	end

	# @return [Fixnum]
	def update(*params)
		check_closed

		set_params(params)
		measure_exec(:p_update) { @java_obj.execute_update }
	end

	# @return [Array] Returns an Array if block is not given
	def query(*params, &blk)
		check_closed

		set_params(params)
		# sorry, ignoring privacy
		@conn.send(:process_and_close_rset,
				   measure_exec(:p_query) { @java_obj.execute_query }, &blk)
	end

	# @return [JDBCHelper::Connection::ResultSetEnumerator]
	def enumerate(*params, &blk)
		check_closed

		return query(*params, &blk) if block_given?

		set_params(params)
		ResultSetEnumerator.new(measure_exec(:p_query) { @java_obj.execute_query })
	end

	# Adds to the batch
	# @return [NilClass]
	def add_batch(*params)
		check_closed

		set_params(params)
		@java_obj.add_batch
	end
	# Executes the batch
	def execute_batch
		check_closed

		measure_exec(:p_execute_batch) {
			@java_obj.executeBatch
		}
	end
	# Clears the batch
	# @return [NilClass]
	def clear_batch
		check_closed

		@java_obj.clear_batch
	end

	# Gives the JDBC driver a hint of the number of rows to fetch from the database by a single interaction.
	# This is only a hint. It may no effect at all.
	# @return [NilClass]
	def set_fetch_size(fsz)
		check_closed

		@java_obj.set_fetch_size fsz
	end

private
	def set_params(params) # :nodoc:
		params.each_with_index do | param, idx |
			set_param(idx + 1, param)
		end
	end

	def initialize(*args)
		super(*args)
	end
end#PreparedStatment
end#Connection
end#JDBCHelper

