# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class Connection
# Base class for CallableStatement and PreparedStatment
# @abstract
class ParameterizedStatement
	# SQL string
	# @return [String]
	attr_reader :sql

	# Underlying Java object
	attr_reader :java_obj

	# @param [JDBCHelper::Connection] conn
	# @param [String] cstmt_str
	def initialize(conn, sql, obj)
		@conn = conn
		@sql = sql
		@java_obj = obj
	end

	def set_param(idx, param)
		if param.nil?
			@java_obj.set_null idx, java.sql.Types::NULL
		elsif setter = JDBCHelper::Connection::SETTER_MAP[param.class.to_s]
			if setter == :setBinaryStream
				@java_obj.send setter, idx, param.getBinaryStream, param.length
			elsif setter == :setTimestamp && param.is_a?(Time)
				@java_obj.send setter, idx, java.sql.Timestamp.new(param.to_i * 1000)
			else
				@java_obj.send setter, idx, param
			end
		else
			@java_obj.set_string idx, param.to_s
		end
	end

	# @return [NilClass]
	def close
		@java_obj.close
		@java_obj = nil
	end

	# @return [Boolean]
	def closed?
		@java_obj.nil?
	end

private
	def measure_exec(type, &blk)	# :nodoc:
		@conn.send(:measure_exec, type, &blk)
	end

	def check_closed
		raise RuntimeError.new("Object already closed") if closed?
	end

end#ParameterizedStatement
end#Connection
end#JDBCHelper


