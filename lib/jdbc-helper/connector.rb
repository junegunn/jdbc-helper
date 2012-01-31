# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
  class Connector
  private
    def self.ensure_close conn
      begin
        yield conn
      ensure
        conn.close rescue nil
      end
    end
  end#Connector
end#JDBCHelper

require 'jdbc-helper/connector/oracle_connector'
require 'jdbc-helper/connector/mysql_connector'
require 'jdbc-helper/connector/postgres_connector'
require 'jdbc-helper/connector/sql_server_connector'

