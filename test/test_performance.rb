require 'helper'
require 'benchmark'

class TestPerformance < Test::Unit::TestCase
	include JDBCHelperTestHelper

	def setup
		@table = 'tmp_jdbc_helper'
		@range = 'aaa'..'aaz'
		@count = 10000 # Increase this for performance measurement
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

			puts "Normal inserts (batch & chuck-transactional): #{Benchmark.measure {
				(0...@count).each_slice(50) do |slice|
					conn.transaction do
						slice.each do |i|
              conn.add_batch "insert into #{@table} values (#{@range.map{rand @count}.join ','})"
						end
						conn.execute_batch
					end
				end
			}.real}"

			puts "Prepared inserts: #{Benchmark.measure {
				pins = conn.prepare "insert into #{@table} values (#{@range.map{'?'}.join ','})"
				@count.times do |i|
					pins.update *(@range.map {rand @count})
				end
				pins.close
			}.real}"

			puts "Prepared inserts (batch & chuck-transactional): #{Benchmark.measure {
				pins = conn.prepare "insert into #{@table} values (#{@range.map{'?'}.join ','})"
				(0...@count).each_slice(50) do |slice|
					conn.transaction do
						slice.each do |i|
							pins.add_batch *(@range.map {rand @count})
						end
						pins.execute_batch
					end
				end
				pins.close
			}.real}"

			puts "Inserts with hash: #{Benchmark.measure {
				table = conn.table(@table)
				@count.times do |i|
					table.insert @range.inject({}) { |hash, key| hash[key] = rand; hash }
				end
			}.real}"

			puts "Inserts with hash (batch & chunk-transactional): #{Benchmark.measure {
				table = conn.table(@table)
        btable = table.batch
				(0...@count).each_slice(50) do |slice|
					conn.transaction do
						slice.each do |i|
							btable.insert @range.inject({}) { |hash, key| hash[key] = rand; hash }
						end
						conn.execute_batch
					end
				end
			}.real}"

			assert_equal @count * 5, conn.table(@table).count

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

			puts "Chaining enumerators with query: #{Benchmark.measure {
				conn.query("select * from #{@table}").each_slice(50) do |slice|
					slice.each do |row|
						@range.each_with_index do |r,i|
							row[i]
						end
					end
				end
			}.real}"

			puts "Chaining enumerators with enumerate: #{Benchmark.measure {
				conn.enumerate("select * from #{@table}").each_slice(50) do |slice|
					slice.each do |row|
						@range.each_with_index do |r,i|
							row[i]
						end
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

