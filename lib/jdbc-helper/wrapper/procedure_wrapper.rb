# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# A wrapper object representing a database procedure.
# @since 0.3.0
# @example Usage
#  conn.function(:coalesce).call(nil, nil, 'king')
class ProcedureWrapper < ObjectWrapper
	# Returns the name of the procedure
	# @return [String]
	alias to_s name

	# Executes the procedure and returns the values of INOUT & OUT parameters in Hash
	# @return [Hash]
	def call(*args)
		param_count = args.first.kind_of?(Hash) ? args.first.keys.length : args.length

		cstmt = @connection.prepare_call "{call #{name}(#{Array.new(param_count){'?'}.join ', '})}"
		begin
			cstmt.call *args
		ensure
			cstmt.close
		end
	end
end#ProcedureWrapper
end#JDBCHelper

