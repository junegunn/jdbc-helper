# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# Shortcut connector for Cassandra CQL3
# @since 0.7.4
class Cassandra
  extend Connector

  # @param [String] host
  # @param [String] keyspace
  # @param [Hash] extra_params
  # @return [JDBCHelper::Connection]
  def self.connect(host, keyspace, extra_params = {}, &block)
    connect_impl :cassandra, {
      :url      => ["jdbc:cassandra://#{host}", keyspace].compact.join('/')
    }, extra_params, &block
  end
end#Cassandra
end#JDBCHelper

