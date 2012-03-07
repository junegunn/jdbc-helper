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
    @conn     = conn
    @sql      = sql
    @java_obj = obj
  end

  def set_param(idx, param)
    case param
    when NilClass
      set_null idx, param
    when Fixnum
      @java_obj.setLong idx, param
    when Bignum
      @java_obj.setString idx, param.to_s # Safer
    when BigDecimal
      @java_obj.setBigDecimal idx, param.to_java
    when String
      @java_obj.setString idx, param
    when Float
      @java_obj.setDouble idx, param
    when Time
      @java_obj.setTimestamp idx, java.sql.Timestamp.new((param.to_f * 1000).to_i)
    when java.sql.Date
      @java_obj.setDate idx, param
    when java.sql.Time
      @java_obj.setTime idx, param
    when java.sql.Timestamp
      @java_obj.setTimestamp idx, param
    when java.sql.Blob
      @java_obj.setBinaryStream idx, param.getBinaryStream#, param.length
    when java.io.InputStream
      @java_obj.setBinaryStream idx, param
    else
      @java_obj.setString idx, param.to_s
    end
  end

  # @return [NilClass]
  def close
    @java_obj.close
    @java_obj = nil
  end

  # @return [Boolean]
  def closed?
    @java_obj.nil? || @java_obj.isClosed
  end

private
  def set_null idx, param
    @java_obj.setNull idx, java.sql.Types::NULL
  end

  def measure_exec(type, &blk)  # :nodoc:
    @conn.send(:measure_exec, type, &blk)
  end

  def check_closed
    raise RuntimeError.new("Object already closed") if closed?
  end

end#ParameterizedStatement
end#Connection
end#JDBCHelper


