# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# Shortcut connector for MySQL
class MySQLConnector < Connector
	include Constants
	include Constants::Connector

	# @param [String] host
	# @param [String] user
	# @param [String] password
	# @param [String] db
	# @param [Fixnum] timeout
	# @param [Hash] extra_params
	# @return [JDBCHelper::Connection]
	def self.connect(host, user, password, db,
					 timeout = DEFAULT_LOGIN_TIMEOUT, 
					 extra_params = DEFAULT_PARAMETERS[:mysql], &block)

		if extra_params && extra_params.is_a?(Hash) == false
			raise ArgumentError.new('extra_params must be a hash')
		end

		conn = Connection.new(
			(extra_params || {}).merge(
				:driver   => JDBC_DRIVER[:mysql],
				:url      => "jdbc:mysql://#{host}/#{db}",
				:user     => user,
				:password => password,
				:timeout  => timeout))

		block_given? ? ensure_close(conn, &block) : conn
	end
end#MySQLConnector
end#JDBCHelper

