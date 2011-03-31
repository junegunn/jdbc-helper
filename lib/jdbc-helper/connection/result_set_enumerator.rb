# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# Class for enumerating query results
# Automatically closed after used. When not used, you must close it explicitly by calling "close".
class Connection
class ResultSetEnumerator
	include Enumerable

	def each
		return if closed?

		count = -1
		begin
			while @rset.next
				yield Connection::Row.new(
						@col_labels,
						(1..@num_col).map { | i |
							v = @rset.send(GETTER_MAP.fetch(@cols_meta[i-1], :get_string), i)
							@rset.was_null ? nil : v
						},
						count += 1)
			end
		ensure			
			close
		end
	end

	def close
		return if closed?

		@rset.close
		@close_callback.call if @close_callback
		@closed = true
	end

	def closed?
		@closed
	end

private
	def initialize(rset, &close_callback) # :nodoc:
		unless rset.respond_to? :get_meta_data
			rset.close if rset
			@closed = true
			return
		end

		@close_callback = close_callback
		@rset = rset
		@rsmd = @rset.get_meta_data
		@num_col = @rsmd.get_column_count
		@cols_meta = []
		@col_labels = []
		(1..@num_col).each do | i |
			@cols_meta << @rsmd.get_column_type(i)
			@col_labels << @rsmd.get_column_label(i)
		end

		@closed = false
	end

	GETTER_MAP =
	{
		java.sql.Types::TINYINT => :get_int,
		java.sql.Types::SMALLINT => :get_int,
		java.sql.Types::INTEGER => :get_int,
		java.sql.Types::BIGINT => :get_long,

		java.sql.Types::CHAR => :get_string,
		java.sql.Types::VARCHAR => :get_string,
		java.sql.Types::LONGVARCHAR => :get_string,
		(java.sql.Types::NCHAR        rescue nil) => :get_string,
		(java.sql.Types::NVARCHAR     rescue nil) => :get_string,
		(java.sql.Types::LONGNVARCHAR rescue nil) => :get_blob, # FIXME: MySQL
		java.sql.Types::BINARY => :get_string,
		java.sql.Types::VARBINARY => :get_string,
		java.sql.Types::LONGVARBINARY => :get_blob,	# FIXME: MySQL

		java.sql.Types::REAL => :get_double,
		java.sql.Types::FLOAT => :get_float,
		java.sql.Types::DOUBLE => :get_double,
		java.sql.Types::NUMERIC => :get_string, # FIXME
		java.sql.Types::DECIMAL => :get_string, # FIXME

		java.sql.Types::DATE => :get_date,
		java.sql.Types::TIME => :get_time,
		java.sql.Types::TIMESTAMP => :get_timestamp,

		java.sql.Types::BLOB => :get_blob,
		java.sql.Types::CLOB => :get_string,
		(java.sql.Types::NCLOB rescue nil) => :get_string,

		java.sql.Types::BOOLEAN => :get_boolean
	} # :nodoc:
end#ResultSetEnumerator
end#Connection
end#JDBCHelper

