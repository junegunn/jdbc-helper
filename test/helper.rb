require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'test/unit'

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'jdbc-helper'

class Test::Unit::TestCase
end

module JDBCHelperTestHelper
	require 'yaml'
	def config
		@db_config ||= YAML.load File.read(File.dirname(__FILE__) + '/database.yml')
	end

	def each_connection(&block)
		config.each do | db, conn_info |
			conn = JDBCHelper::Connection.new(conn_info)
			begin
				if block.arity == 1
					yield conn
				else
					yield conn, conn_info
				end
			ensure
				conn.close
			end
		end
	end
end
