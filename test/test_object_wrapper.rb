require 'helper'

class TestObjectWrapper < Test::Unit::TestCase
	include JDBCHelperTestHelper

	def setup
		@table_name = "tmp_jdbc_helper"
	end

	def teardown
		each_connection do |conn|
			drop_table conn
		end
	end

	def create_table conn
		drop_table conn
		conn.update "
			create table #{@table_name} (
				id    int primary key,
				alpha int,
				beta  float,
				gamma varchar(100)
			)
		"
	end

	def drop_table conn
		begin
			conn.update "drop table #{@table_name}"
			return true
		rescue Exception
			return false
		end
	end

	def test_wrapper
		each_connection do |conn|
			# With symbol
			assert_kind_of     JDBCHelper::ObjectWrapper, conn.table(:some_table)
			assert_instance_of JDBCHelper::TableWrapper, conn.table(:some_table)
			assert_equal       'some_table', conn.table(:some_table).name

			# With string
			assert_kind_of     JDBCHelper::ObjectWrapper, conn.table('db.table')
			assert_instance_of JDBCHelper::TableWrapper, conn.table('db.table')
			assert_equal       'db.table', conn.table('db.table').name

			# Invalid table name
			assert_raise(ArgumentError) { conn.table('table;') }
			assert_raise(ArgumentError) { conn.table('table -- ') }
			assert_raise(ArgumentError) { conn.table("tab'le") }
			assert_raise(ArgumentError) { conn.table('tab"le') }
			assert_raise(ArgumentError) { conn.table("tab`le") }

			# Abstract class
			assert_raise(Exception) { JDBCHelper::ObjectWrapper.new(conn, 'table') }
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
			table = conn.table(@table_name)

			assert table.empty?
		end
	end

	def test_insert_count
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)

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

	# This will fail if the database doesn't support insert ignore syntax
	def test_insert_ignore
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)
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

	# This will fail if the database doesn't support replace syntax
	def test_replace
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)
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
			table = conn.table(@table_name)
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
			table = conn.table(@table_name)
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
			table = conn.table(@table_name)
			insert table

			assert_equal 10, table.update(:beta => 0, :where => { :id => (1..10) })
			assert_equal 10, table.count(:beta => 0)

		end
	end

	def test_truncate_table
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)
			insert table

			table.truncate_table!
			assert table.empty?
		end
	end

	def test_drop_table
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)
			table.drop_table!

			assert_equal false, drop_table(conn)
		end
	end
end

