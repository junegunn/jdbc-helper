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

	def create_test_procedure conn, name
		case @type
		when :mysql
			conn.update "drop procedure #{name}" rescue nil
			conn.update("
				create procedure #{name} 
				(IN i1 varchar(100), INOUT io1 int, INOUT io2 timestamp, OUT o1 float, OUT o2 varchar(100))
				select io1 * 10, 0.1, i1 into io1, o1, o2 from dual")
		when :oracle
			conn.update "drop procedure #{name}" rescue nil
			conn.update "
				create or replace
				procedure #{name} (i in varchar2, io in out int, o1 out float, o2 out varchar2) as
				begin
					  select io * 10, 0.1, i into io, o1, o2 from dual;
				end;"
		else
			raise NotImplementedError.new "Procedure test not implemented for #{@type}"
		end
	end

	# This is crazy. But Oracle.
	def assert_equal *args
		if args.first.class == Fixnum
			super(args[0].to_s, args[1].to_s)
		else
			super(*args)
		end
	end

	def each_connection(&block)
		config.each do | db, conn_info |
			conn = JDBCHelper::Connection.new(conn_info)
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
