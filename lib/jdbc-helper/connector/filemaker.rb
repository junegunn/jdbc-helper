# encoding: UTF-8
# Larry Staton Jr. (larry@sweetpeasoftware.com)

module JDBCHelper
# Shortcut connector for FileMaker
  module FileMaker
    extend Connector

    # @param [String] host
    # @param [String] user
    # @param [String] password
    # @param [String] db
    # @param [Hash] extra_params
    # @return [JDBCHelper::Connection]
    def self.connect(host, user, password, db, extra_params = {}, &block)
      connect_impl :filemaker, {
        :url      => "jdbc:filemaker://#{host}/#{db}",
        :user     => user,
        :password => password
      }, extra_params, &block
    end

  end#FileMaker
end#JDBCHelper
