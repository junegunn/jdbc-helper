# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# Shortcut connector for SQLite
module SQLite
  extend Connector

  # @param [String] path
  # @param [Hash] extra_params
  # @return [JDBCHelper::Connection]
  def self.connect(path, extra_params = {}, &block)
    connect_impl :sqlite, {
      :url      => "jdbc:sqlite:#{path}",
    }, extra_params, &block
  end
end#SQLite::Connector
end#JDBCHelper

