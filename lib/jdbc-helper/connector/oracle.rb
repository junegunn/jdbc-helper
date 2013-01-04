# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# Shortcut connector for Oracle
module Oracle
  extend Connector

  # @param [String] host
  # @param [String] user
  # @param [String] password
  # @param [String] service_name
  # @return [JDBCHelper::Connection]
  def self.connect(host, user, password, service_name, extra_params = {}, &block)
    connect_impl :oracle, {
      :url      => "jdbc:oracle:thin:@#{host}/#{service_name}",
      :user     => user,
      :password => password
    }, {}, &block
  end

  # @param [String] host
  # @param [String] user
  # @param [String] password
  # @param [String] sid
  # @return [JDBCHelper::Connection]
  # @deprecated
  def self.connect_by_sid(host, user, password, sid,
           extra_params = {}, &block)
    connect_impl :oracle, {
      :url      => "jdbc:oracle:thin:@#{host}:#{sid}",
      :user     => user,
      :password => password
    }, {}, &block
  end
end#OracleConnector

# @deprecated
module OracleConnector
  extend Connector
  def self.connect(host, user, password, service_name,
           timeout = Constants::DEFAULT_LOGIN_TIMEOUT,
           extra_params = {}, &block)
    check_params extra_params
    Oracle.connect(host, user, password, service_name,
                  {:timeout => timeout}.merge(extra_params), &block)
  end

  def self.connect_by_sid(host, user, password, sid,
           timeout = Constants::DEFAULT_LOGIN_TIMEOUT,
           extra_params = {}, &block)
    check_params extra_params
    Oracle.connect_by_sid(host, user, password, sid,
                  {:timeout => timeout}.merge(extra_params), &block)
  end
end
end#JDBCHelper

