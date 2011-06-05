# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class Connection
# Interface to Java CallableStatment
class CallableStatement < ParameterizedStatement
	# Array or Hash (for named parameters)
	# - IN parameter: value
	# - OUT parameter: class
	# - INOUT parameter: [value, class]
	#   (Although class can be inferred from the value, 
	#   we still need a way to figure out if it's INOUT parameter)
	def call *params
		check_closed

		out_params = set_params(params)
		measure_exec(:call) { @java_obj.execute }

		result = {}
		out_params.each do |idx, jtype|
			getter = JDBCHelper::Connection::GETTER_MAP.fetch(jtype, :get_string)
			value = @java_obj.send(getter, idx.is_a?(Symbol) ? idx.to_s : idx)
			result[idx] = @java_obj.was_null ? nil : value
		end
		result
	end

private
	def set_params(params) # :nodoc:
		hash_params = 
			if params.first.kind_of? Hash
				raise ArgumentError.new("More than one Hash given") if params.length > 1
				params.first
			else
				params.each_with_index.inject({}) { |hash, pair|
					hash[pair.last + 1] = pair.first
					hash
				}
			end

		out_params = {}
		hash_params.each do | idx, value |
			# Symbols need to be transformed into string
			idx_ = idx.is_a?(Symbol) ? idx.to_s : idx
			case value
			# OUT parameter
			when Class
				jtype = JDBCHelper::Connection::RUBY_SQL_TYPE_MAP[value] || java.sql.Types::VARCHAR
				@java_obj.registerOutParameter(idx_, jtype)
				out_params[idx] = jtype

			# INOUT parameter
			when Array
				set_param(idx_, value.first)
				jtype = JDBCHelper::Connection::RUBY_SQL_TYPE_MAP[value.last] || java.sql.Types::VARCHAR
				@java_obj.registerOutParameter(idx_, jtype)
				out_params[idx] = jtype
						
			# IN parameter
			else
				set_param(idx_, value)
			end
		end
		out_params
	end

	def initialize(*args)
		super(*args)
	end
end#CallableStatment
end#Connection
end#JDBCHelper

