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
		Fixnum => :setInt,
		String => :setString,
		NilClass => :setNull,
		Float => :setDouble,

		# See there's a caveat. Check out ParameterizedStatement#set_param
		Time => :setTimestamp,

		Java::JavaSql::Date => :setDate,
		Java::JavaSql::Time => :setTime,
		Java::JavaSql::Timestamp => :setTimestamp,
		Java::JavaSql::Blob => :setBinaryStream,

		#########
		# MySQL #
		#########
		# Only available when MySQL JDBC driver is loaded.
		# So we use the string representation of the class.
		'Java::ComMysqlJdbc::Blob' => :setBinaryStream

		# FIXME-MORE
	} # :nodoc:

	GETTER_MAP =
	{
		java.sql.Types::TINYINT => :getInt,
		java.sql.Types::SMALLINT => :getInt,
		java.sql.Types::INTEGER => :getInt,
		java.sql.Types::BIGINT => :getLong,

		java.sql.Types::CHAR => :getString,
		java.sql.Types::VARCHAR => :getString,
		java.sql.Types::LONGVARCHAR => :getString,
		(java.sql.Types::NCHAR        rescue nil) => :getString,
		(java.sql.Types::NVARCHAR     rescue nil) => :getString,
		(java.sql.Types::LONGNVARCHAR rescue nil) => :getBlob, # FIXME: MySQL
		java.sql.Types::BINARY => :getString,
		java.sql.Types::VARBINARY => :getString,
		java.sql.Types::LONGVARBINARY => :getBlob,	# FIXME: MySQL

		java.sql.Types::REAL => :getDouble,
		java.sql.Types::FLOAT => :getFloat,
		java.sql.Types::DOUBLE => :getDouble,
		java.sql.Types::NUMERIC => :getString, # FIXME: get_big_decimal=no inherent jruby support
		java.sql.Types::DECIMAL => :getString, # FIXME: get_big_decimal

		java.sql.Types::DATE => :getDate,
		java.sql.Types::TIME => :getTime,
		java.sql.Types::TIMESTAMP => :getTimestamp,

		java.sql.Types::BLOB => :getBlob,
		java.sql.Types::CLOB => :getString,
		(java.sql.Types::NCLOB rescue nil) => :getString,

		java.sql.Types::BOOLEAN => :getBoolean
	} # :nodoc:
end#Connection
end#JDBCHelper

