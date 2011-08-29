# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class Connection
# Internal implementation for supporting query nesting.
# Not thread-safe. (Sharing a JDBC connection between threads is not the best idea anyway.)
# @private
class StatementPool
	def initialize(conn, max_size = 20)
		@conn = conn
		@max_size = max_size # TODO
		@free = []
		@occupied = []
	end

	def with
		begin
			yield stmt = take
		ensure
			give stmt
		end
	end

	def take
		if @free.empty?
			raise Exception.new("Statement nesting level is too deep (likely a bug)") if
					@occupied.length >= @max_size
			@occupied << nstmt = @conn.send(:create_statement)
			nstmt
		else
			stmt = @free.pop
			@occupied << stmt
			stmt
		end
	end

	def give(stmt)
		raise Exception.new("Not my statement") unless
				@occupied.include? stmt

		@occupied.delete stmt
		@free << stmt
	end

	def close
		(@free + @occupied).each do | stmt |
			stmt.close
		end
		@conn = nil
		@free = []
		@occupied = []
	end

	def each
		(@free + @occupied).each do | stmt |
			yield stmt
		end
	end
end#StatementPool
end#Connection
end#JDBCHelper
