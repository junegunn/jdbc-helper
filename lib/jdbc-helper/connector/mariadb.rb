# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# Shortcut connector for MariaDB
module MariaDB
  extend Connector

  # @param [String] host
  # @param [String] user
  # @param [String] password
  # @param [String] db
  # @param [Hash] extra_params
  # @return [JDBCHelper::Connection]
  def self.connect(host, user, password, db, extra_params = {}, &block)
    connect_impl :mariadb, {
      :url      => "jdbc:mysql://#{host}/#{db}",
      :user     => user,
      :password => password
    }, extra_params, &block
  end
end#MariaDB::Connector
end#JDBCHelper

