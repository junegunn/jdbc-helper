# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# A wrapper object representing a database function.
# @since 0.2.2
# @example Usage
#  conn.function(:coalesce).call(nil, nil, 'king')
class FunctionWrapper < ObjectWrapper
	# Returns the name of the function
	# @return [String]
	alias to_s name

	# Returns the result of the function call with the given parameters
	def call(*args)
		pstmt = @connection.prepare("select #{name}(#{args.map{'?'}.join ','}) from dual")
		begin
			pstmt.query(*args)[0][0]
		ensure
			pstmt.close
		end
	end
end#FunctionWrapper
end#JDBCHelper


