# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class Connection
	RUBY_SQL_TYPE_MAP = {
		Fixnum => java.sql.Types::INTEGER,
		Bignum => java.sql.Types::BIGINT,
		String => java.sql.Types::VARCHAR,
		Float  => java.sql.Types::DOUBLE,
		Time   => java.sql.Types::TIMESTAMP
	}

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
		java.sql.Types::NUMERIC => :get_string, # FIXME: get_big_decimal=no inherent jruby support
		java.sql.Types::DECIMAL => :get_string, # FIXME: get_big_decimal

		java.sql.Types::DATE => :get_date,
		java.sql.Types::TIME => :get_time,
		java.sql.Types::TIMESTAMP => :get_timestamp,

		java.sql.Types::BLOB => :get_blob,
		java.sql.Types::CLOB => :get_string,
		(java.sql.Types::NCLOB rescue nil) => :get_string,

		java.sql.Types::BOOLEAN => :get_boolean
	} # :nodoc:
end#Connection
end#JDBCHelper

