# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# Abstract base class for wrappers for various database objects.
# @abstract
# @since 0.2.0
class ObjectWrapper
  # Underlying JDBCHelper::Connection object
  # @return [JDBCHelper::Connection]
  attr_reader :connection

  # Object name (or expression)
  # @return [String]
  attr_reader :name

  # Base constructor.
  # @param [JDBCHelper::Connection] conn JDBCHelper::Connection object
  # @param [String/Symbol] name Name of the object to be wrapped
  def initialize(conn, name)
    raise NotImplementedError.new(
      "JDBCHelper::ObjectWrapper is an abstract class") if self.instance_of? ObjectWrapper

    @connection = conn
    @name = name.to_s
    JDBCHelper::SQL.check @name, true
  end
end#ObjectWrapper
end#JDBCHelper

