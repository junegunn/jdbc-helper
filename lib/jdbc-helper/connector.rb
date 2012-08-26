# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
module Connector
  def ensure_close conn
    begin
      yield conn
    ensure
      conn.close rescue nil
    end
  end

  def check_params params
    if params && !params.is_a?(Hash)
      raise ArgumentError.new('extra_params must be a hash')
    end
  end

  def connect_impl type, params, extra_params, &block
    check_params extra_params

    conn = Connection.new(
      Constants::Connector::DEFAULT_PARAMETERS[type].merge(extra_params || {}).merge(params))
    block_given? ? ensure_close(conn, &block) : conn
  end
end#Connector
end#JDBCHelper

require 'jdbc-helper/connector/oracle'
require 'jdbc-helper/connector/mysql'
require 'jdbc-helper/connector/postgresql'
require 'jdbc-helper/connector/mssql'
require 'jdbc-helper/connector/cassandra'
require 'jdbc-helper/connector/filemaker'
