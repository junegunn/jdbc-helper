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
    java.sql.Types::LONGVARBINARY => :getBlob,  # FIXME: MySQL

    java.sql.Types::REAL => :getDouble,
    java.sql.Types::FLOAT => :getFloat,
    java.sql.Types::DOUBLE => :getDouble,
    #java.sql.Types::NUMERIC => :getBigDecimal,
    #java.sql.Types::DECIMAL => :getBigDecimal,

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

