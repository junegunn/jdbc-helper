require 'rubygems'
require 'bundler'
#require 'pry'

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

	def create_test_procedure_simple conn, name
		case @type
		when :mysql
			conn.update "drop procedure #{name}" rescue nil
			conn.update("
				create procedure #{name}()
				select 1 from dual where 1 != 0")
		when :oracle
			conn.update "drop procedure #{name}" rescue nil
			conn.update "
				create or replace
				procedure #{name} as
				begin
					null;
				end;"
		else
			raise NotImplementedError.new "Procedure test not implemented for #{@type}"
		end
	end

	def create_test_procedure conn, name
		case @type
		when :mysql
			conn.update "drop procedure #{name}" rescue nil
			conn.update("
				create procedure #{name} 
				(IN i1 varchar(100), IN i2 int,
				 INOUT io1 int, INOUT io2 timestamp,
				 IN n1 int,
				 OUT o1 float, OUT o2 varchar(100))
				select io1 * i2, 0.1, i1 into io1, o1, o2 from dual where n1 is null")
		when :oracle
			conn.update "drop procedure #{name}" rescue nil
			conn.update "
				create or replace
				procedure #{name} 
				(i1 in varchar2, i2 in int default '1',
				 io1 in out int, io2 in out date,
				 n1 in int,
				 o1 out float, o2 out varchar2) as
				begin
					  select io1 * i2, 0.1, i1 into io1, o1, o2 from dual where n1 is null;
				end;"
		else
			raise NotImplementedError.new "Procedure test not implemented for #{@type}"
		end
	end

	def each_connection(&block)
		config.each do | db, conn_info |
			conn = JDBCHelper::Connection.new(conn_info.reject { |k,v| k == 'database'})
			# Just for quick and dirty testing
			@type = case conn_info['driver'] || conn_info[:driver]
					when /mysql/i
						:mysql
					when /oracle/i
						:oracle
					else
						p conn_info
						:unknown
					end

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
