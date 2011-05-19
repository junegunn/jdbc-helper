require 'helper'
require 'benchmark'

class TestPerformance < Test::Unit::TestCase
	include JDBCHelperTestHelper

	def setup
		@table = 'tmp_jdbc_helper'
		@range = 'aaa'..'aaz'
		@count = 1000
	end

	def teardown
		each_connection do | conn |
			drop_table conn
		end
	end

	# No assertion here.
	def test_performance
		each_connection do | conn |
			reset conn
			puts "Normal inserts: #{Benchmark.measure {
				@count.times do |i|
					conn.update "insert into #{@table} values (#{@range.map{rand @count}.join ','})"
				end
			}.real}"

			puts "Prepared inserts: #{Benchmark.measure {
				pins = conn.prepare "insert into #{@table} values (#{@range.map{'?'}.join ','})"
				@count.times do |i|
					pins.update *(@range.map {rand @count})
				end
				pins.close
			}.real}"

			conn.query("select * from #{@table}") do |row|
				# ...
			end

			puts "Accessing records using dot notation: #{Benchmark.measure {
				conn.query("select * from #{@table}") do |row|
					@range.each do |r|
						row.send r
					end
				end
			}.real}"

			puts "Accessing records using numeric indexes: #{Benchmark.measure {
				conn.query("select * from #{@table}") do |row|
					@range.each_with_index do |r,i|
						row[i]
					end
				end
			}.real}"
		end
	end

	def create_table conn
		conn.update("create table #{@table} (#{@range.map { |e| "#{e} int" }.join(', ')})")
	end

	def drop_table conn
		conn.update("drop table #{@table}") rescue nil
	end

	def reset conn
		drop_table conn
		create_table conn
	end
end

