# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

require 'logger'

module JDBCHelper
class Connection
# An encapsulation of Java PreparedStatment object.
# Used to execute parameterized queries efficiently.
# Has a very similar set of interface to that of JDBCHelper::Connection.
#
# @example
#   pstmt = conn.prepare("SELECT * FROM T WHERE a = ? and b = ?")
#   rows = pstmt.query(10, 20)
#   enum = pstmt.enumerate(10, 20)
class PreparedStatement < ParameterizedStatement
  # Returns the number of parameters required
  # @return [Fixnum]
  def parameter_count
    @pmd.getParameterCount
  end

  # @return [NilClass]
  def close
    @conn.send(:close_pstmt, self)
    @java_obj.close
    @java_obj = nil
  end

  # @return [Fixnum|ResultSet]
  def execute(*params)
    check_closed

    set_params(params)
    if @java_obj.execute
      ResultSet.new(@java_obj.getResultSet)
    else
      @java_obj.getUpdateCount
    end
  end

  # @return [Fixnum]
  def update(*params)
    check_closed

    set_params(params)
    @java_obj.execute_update
  end

  # @return [Array] Returns an Array if block is not given
  def query(*params, &blk)
    check_closed

    set_params(params)
    enum = ResultSet.new(@java_obj.execute_query)
    if block_given?
      enum.each do |row|
        yield row
      end
    else
      enum
    end
  end
  alias enumerate query

  # Adds to the batch
  # @return [NilClass]
  def add_batch(*params)
    check_closed

    set_params(params)
    @java_obj.add_batch
  end

  # Executes the batch
  # @return [Fixnum] Sum of all successful update counts
  def execute_batch
    check_closed

    @java_obj.executeBatch.select { |e| e > 0 }.inject(:+) || 0
  end

  # Clears the batch
  # @return [NilClass]
  def clear_batch
    check_closed

    @java_obj.clear_batch
  end

  # Gives the JDBC driver a hint of the number of rows to fetch from the database by a single interaction.
  # This is only a hint. It may no effect at all.
  # @return [NilClass]
  def set_fetch_size(fsz)
    check_closed

    @fetch_size = fsz
    @java_obj.set_fetch_size fsz
  end
  alias fetch_size= set_fetch_size

  # Returns the fetch size of the prepared statement. If not set, nil is returned.
  # @return [Fixnum]
  attr_reader :fetch_size
private
  def set_params(params) # :nodoc:
    params.each_with_index do | param, idx |
      set_param(idx + 1, param)
    end
  end

  def set_null idx, param
    @java_obj.setNull idx, @types ? @types[idx - 1] : Java::java.sql.Types::NULL
  end

  def initialize(*args)
    super(*args)

    begin
      @pmd   = @java_obj.getParameterMetaData
      @types = @pmd.getParameterCount.times.map { |idx|
                    # Oracle does not support getParameterType
                    @pmd.getParameterType(idx + 1) rescue Java::java.sql.Types::NULL
                  }
    rescue Exception => e
      Logger.new($stderr).warn e.to_s
      @types = nil
    end
  end
end#PreparedStatment
end#Connection
end#JDBCHelper

