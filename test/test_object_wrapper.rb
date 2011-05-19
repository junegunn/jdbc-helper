require 'helper'

class TestObjectWrapper < Test::Unit::TestCase
	include JDBCHelperTestHelper

	def setup
		@table = "tmp_jdbc_helper"
	end

	def teardown
		each_connection do |conn|
			drop_table conn
		end
	end

	def create_table conn
		drop_table conn
		conn.update "
			create table tmp_jdbc_helper (
				id    int primary key,
				alpha int,
				beta  float,
				gamma varchar(100)
			)
		"
	end

	def drop_table conn
		begin
			conn.update "drop table #{@table}"
			return true
		rescue Exception
			return false
		end
	end

	def test_dsl
		each_connection do |conn, conn_info|
			# Object
			assert_equal JDBCHelper::ObjectWrapper, conn.some_table.class

			# Database.Object
			assert_equal JDBCHelper::ObjectWrapper, conn.some_database.some_table.class

			# Database.Object
			if conn_info.has_key? 'database'
				assert_equal JDBCHelper::ObjectWrapper, conn.send(conn_info['database']).some_table.class
			end

			# Takes no parameter
			assert_raise(NoMethodError) { conn.some_database(1, 2) }
			assert_raise(NoMethodError) { conn.some_database.some_table(3) }

			# No more than 2 depths
			assert_raise(NoMethodError) { conn.some_database.some_table.no_more }
		end
	end

	def insert table
		params = {
			:alpha => 100, 
			:beta => JDBCHelper::SQL.expr('0.1 + 0.2'), 
			:gamma => 'hello world' }

		(1..100).each do |pk|
			icnt = table.insert(params.merge(:id => pk))
			assert_equal icnt, 1
		end
	end

	def test_empty
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper

			assert table.empty?
		end
	end

	def test_insert_count
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper

			# Count
			assert_equal 0, table.count
			assert table.empty?

			# Insert
			insert table

			# Count
			assert_equal 100, table.count
			assert_equal 100, table.count(:alpha => 100)
			assert_equal 0, table.count(:beta => nil)
		end
	end

	def test_insert_ignore
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper
			params = {
				:id => 1,
				:alpha => 100, 
				:beta => JDBCHelper::SQL.expr('0.1 + 0.2'), 
				:gamma => 'hello world' }

			100.times do
				table.insert_ignore(params)
			end

			assert_equal 1, table.count
		end
	end

	def test_replace
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper
			params = {
				:id => 1,
				:beta => JDBCHelper::SQL.expr('0.1 + 0.2'), 
				:gamma => 'hello world' }

			100.times do |i|
				table.replace(params.merge(:alpha => i))
			end

			assert_equal 1, table.count
			assert_equal 99, table.select.first.alpha
		end
	end

	def test_select
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper
			insert table
			assert_equal 100, table.count

			cnt = 0
			table.select do |row|
				cnt += 1
				assert_equal 100, row.alpha
				assert_equal 'hello world', row.gamma
			end
			assert_equal 100, cnt

			cnt = 0
			table.select(:id => 11..20) do |row|
				cnt += 1
				assert_equal 100, row.alpha
				assert_equal 'hello world', row.gamma
			end
			assert_equal 10, cnt
		end
	end

	def test_delete
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper
			insert table

			# Count
			assert_equal 100, table.count

			# Delete
			assert_equal 10, table.delete(:id => (1...11))
			assert_equal 10, table.delete(:id => (11..20))
			assert_equal 1, table.delete(:id => 21)
			assert_equal 4, table.delete(:id => [22, 23, 24, 25])
			assert_equal 5, table.delete("id <= 30")

			# Could be dangerous (XXX)
			assert_equal 70, table.delete

			# Count
			assert_equal 0, table.count
		end
	end

	def test_update
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper
			insert table

			assert_equal 10, table.update(:beta => 0, :where => { :id => (1..10) })
			assert_equal 10, table.count(:beta => 0)

		end
	end

	def test_truncate_table
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper
			insert table

			table.truncate_table!
			assert table.empty?
		end
	end

	def test_drop_table
		each_connection do |conn|
			create_table conn
			table = conn.tmp_jdbc_helper
			table.drop_table!

			assert drop_table(conn) == false
		end
	end
end

