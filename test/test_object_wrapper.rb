require 'helper'

class TestObjectWrapper < Test::Unit::TestCase
	include JDBCHelperTestHelper

	def setup
		@table_name = "tmp_jdbc_helper"
		@procedure_name = "tmp_jdbc_helper_test_proc"
	end

	def teardown
		each_connection do |conn|
			drop_table conn
			conn.update "drop procedure #{@procedure_name}" rescue nil
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
			assert_kind_of     JDBCHelper::ObjectWrapper, conn.function(:some_func)
			assert_instance_of JDBCHelper::FunctionWrapper, conn.function(:some_func)
			assert_kind_of     JDBCHelper::ObjectWrapper, conn.procedure(:some_proc)
			assert_instance_of JDBCHelper::ProcedureWrapper, conn.procedure(:some_proc)
			assert_equal       'some_table', conn.table(:some_table).name

			# With string
			assert_kind_of     JDBCHelper::ObjectWrapper, conn.table('table')
			assert_instance_of JDBCHelper::TableWrapper, conn.table('db.table')
			assert_kind_of     JDBCHelper::ObjectWrapper, conn.function('db.some_func')
			assert_instance_of JDBCHelper::FunctionWrapper, conn.function('some_func')
			assert_kind_of     JDBCHelper::ObjectWrapper, conn.procedure('some_proc')
			assert_instance_of JDBCHelper::ProcedureWrapper, conn.procedure('db.some_proc')
			assert_equal       'db.table', conn.table('db.table').name

			# Invalid object name
			[ '  ', 'object;', 'object -- ', "obj'ect",
				'obj"ect', 'obj`ect', 'obje(t', 'ob)ect' ].each do |inv|
				assert_raise(ArgumentError) { conn.table(inv) }
				assert_raise(ArgumentError) { conn.function(inv) }
				assert_raise(ArgumentError) { conn.table(inv.to_sym) }
				assert_raise(ArgumentError) { conn.function(inv.to_sym) }
			end

			# Abstract class
			assert_raise(Exception) { JDBCHelper::ObjectWrapper.new(conn, 'table') }
		end
	end

	def insert table, cnt = 100
		params = {
			:alpha => 100,
			:beta => JDBCHelper::SQL('0.1 + 0.2'), 
		}

		(1..cnt).each do |pk|
			icnt = table.
					default(:gamma => 'hello world').
					default(:alpha => 200).
					insert(params.merge(:id => pk))
			assert_equal 1, icnt unless table.batch?
		end
	end

	def test_empty
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)

			assert table.empty?
		end
	end

	def test_function_wrapper
		each_connection do |conn|
			assert_equal 2.to_i, conn.function(:mod).call(5, 3).to_i
			assert_equal 'yeah', conn.function(:coalesce).call(nil, nil, 'yeah', 'no')
		end
	end

	def test_procedure_wrapper
		each_connection do |conn, conn_info|
			{ 
				:proc => @procedure_name,
				:db_proc => [conn_info['database'], @procedure_name].join('.')
			}.each do |mode, prname|
				create_test_procedure_simple conn, prname

				pr = conn.procedure(prname)
				pr.call # should be ok without any arguments

				# Complex case
				create_test_procedure conn, prname
				pr.refresh

				result = pr.call 'hello', 10, [100, Fixnum], [Time.now, Time], nil, Float, String
				assert_instance_of Hash, result
				assert_equal 1000, result[3]
				assert_equal 'hello', result[7]

				result = pr.call(
					:io1 => [100, Fixnum],
					'io2' => [Time.now, Time], 
					:i2 => 10,
					:i1 => 'hello',
					:o1 => Float, 'o2' => String)
				assert_instance_of Hash, result
				assert_equal 1000, result[:io1]
				assert_equal 'hello', result['o2']

				# Test default values
				# - MySQL does not support default values
				# - Oracle JDBC does not fully implement getProcedureColumns
				#   => Cannot get default values with standard interface => Pending
				if @type != :mysql
					pend("Not tested") do
						result = pr.call(
							:io1 => [100, Fixnum],
							'io2' => [Time.now, Time], 
							#:i2 => 10,
							:i1 => 'hello',
							:o1 => Float, 'o2' => String)
						assert_instance_of Hash, result
						assert_equal 100, result[:io1]
						assert_equal 'hello', result['o2']

						result = pr.call 'hello', [100, Fixnum], [Time.now, Time], nil, Float, String
						assert_instance_of Hash, result
						assert_equal 100, result[3]
						assert_equal 'hello', result[7]
					end
				end
			end#prname
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

			# Empty?
			assert_equal false, table.empty?
			assert_equal true, table.empty?(:alpha => 999)
			assert_equal true, table.where(:alpha => 999).empty?

			# Count
			assert_equal 100, table.count
			assert_equal 100, table.count(:alpha => 100)
			assert_equal 1, table.where(:alpha => 100).count(:id => 1) # scoped
			assert_equal 0, table.where(:alpha => 200).count(:id => 1) # scoped
			assert_equal 0, table.count(:beta => nil)

			assert_equal 100, table.where(:alpha => 100).count
			assert_equal 0, table.where(:beta => nil).count
			assert_equal 40, table.where('id >= 11', 'id <= 50').count
			assert_equal 40, table.where('id >= 11').count('id <= 50')
			assert_equal 40, table.where('id >= 11').where('id <= 50').count
			assert_equal 40, table.where('id >= 11').where('id <= 50').where('1 = 1').count
			assert_equal 0, table.where(:alpha => 100).count(:beta => nil)

			assert_equal true, table.empty?(:beta => nil)
			assert_equal true, table.where(:beta => nil).empty?
		end
	end

	def test_insert_ignore
		each_connection do |conn|
			next unless @type == :mysql

			create_table conn
			table = conn.table(@table_name)
			params = {
				:id => 1,
				:alpha => 100, 
				:beta => JDBCHelper::SQL('0.1 + 0.2'), 
				:gamma => 'hello world' }

			100.times do
				table.insert_ignore(params)
			end

			assert_equal 1, table.count
		end
	end

	def test_replace
		each_connection do |conn|
			next unless @type == :mysql

			create_table conn
			table = conn.table(@table_name)
			params = {
				:id => 1,
				:beta => JDBCHelper::SQL('0.1 + 0.2'), 
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

			def check_row row
				assert_equal 100, row.alpha
				assert_equal 'hello world', row.gamma
			end

			cnt = 0
			table.select do |row|
				cnt += 1
				check_row row
			end
			assert_equal 100, cnt

			cnt = 0
			table.each do |row|
				cnt += 1
				check_row row
			end
			assert_equal 100, cnt

			cnt = 0
			table.each_slice(10) do |rows|
				cnt += rows.length
			end
			assert_equal 100, cnt

			cnt = 0
			table.select('alpha omega') do |row|
				cnt += 1
				assert_equal 100, row.omega
				assert_equal ['omega'], row.labels.map(&:downcase)
			end
			assert_equal 100, cnt

			cnt = 0
			prev_id = 100
			table.where(:id => 11..20).order('id desc') do |row|
				cnt += 1
				check_row row

				assert row.id.to_i < prev_id
				prev_id = row.id.to_i
			end
			assert_equal 10, cnt

			assert_equal "select a, b, c cc from tmp_jdbc_helper " +
					"where id >= 11 and id <= 20 order by id desc, name asc",
					table.where(:id => 11..20).
						select(:a, :b, 'c cc').
						order('id desc', 'name asc').sql

			assert_equal "select a, b, c cc from tmp_jdbc_helper " +
					"where (id != 15) and id >= 11 and id <= 20 order by id desc, name asc",
					table.where("id != 15", :id => 11..20).
						select(:a, :b, 'c cc').
						order('id desc', 'name asc').sql

			assert_raise(ArgumentError) { table.order }
			assert_raise(ArgumentError) { table.order.where }
			assert_raise(ArgumentError) { table.where.order }
			assert_raise(ArgumentError) { table.select.order }
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
			assert_equal 10, table.where("id <= 40").delete

			# Could be dangerous (XXX)
			assert_equal 60, table.delete

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
			assert_equal 2, table.where(:id => (55..56)).update(:beta => 0, :where => { :id => (51..60) })
			assert_equal 10, table.where(:id => (11..20)).update(:beta => 1)

			with_default = table.default(:beta => 0)
			assert_equal 5, with_default.where(:id => (11..15)).update
			assert_equal 5, with_default.where(:id => (16..20)).update
			with_default = table.default(:beta => 1)
			assert_equal 5, with_default.where(:id => (16..20)).update(:beta => 0) # override
			assert_equal 22, table.count(:beta => 0)
			assert_equal 100, table.update(:beta => 1)
		end
	end

	def test_batch
		each_connection do |conn|
			# Initialize test table
			create_table conn
			table = conn.table(@table_name)
			insert table

			# Duplicated calls are idempotent
			btable = table.batch
			assert_equal btable, btable.batch

			# Batch updates
			table.batch.delete
			assert_equal 100, table.count
      conn.execute_batch
			assert_equal 0, table.count

			insert table.batch, 50
			assert_equal 0, table.count
      conn.execute_batch
			assert_equal 50, table.count

			table.batch.update(:alpha => JDBCHelper::SQL('alpha * 2'))
			assert_equal 100, table.select(:alpha).to_a.first.alpha.to_i

			# Independent update inbetween
			table.delete(:id => 1..10)
			assert_equal 40, table.count

			# Finally
			conn.execute_batch

			assert_equal 200, table.select(:alpha).to_a.first.alpha.to_i
		end
	end

	def test_truncate_table
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)
			insert table

			table.truncate!
			assert table.empty?
		end
	end

	def test_drop_table
		each_connection do |conn|
			create_table conn
			table = conn.table(@table_name)
			table.drop!
			assert_equal false, drop_table(conn)

			create_table conn
			table = conn.table(@table_name)
			table.drop_table! #alias
			assert_equal false, drop_table(conn)
		end
	end

	def test_sequence
		each_connection do |conn|
			# MySQL doesn't support sequences
			next if @type == :mysql

			seq = conn.sequence(@table_name + '_seq')
			seq.reset!(100)
			assert (prev = seq.nextval) >= 100
			assert_equal prev, seq.currval
			assert_equal 1, seq.nextval - prev

			seq.reset! 1, 2
			assert seq.nextval >= 1
			assert seq.nextval <= 4
			assert seq.nextval >= 5

			seq.drop!
			seq.create!(10)
			assert seq.nextval >= 10
			seq.drop!
		end
	end

  # 0.5.1: Too many open cursors
  def test_ora_10000
		each_connection do |conn|
			create_table conn
      insert conn.table(@table_name)

      # Batch-enabled object
      conn.table(@table_name).batch

      10000.times do
        # Should not fail
        t = conn.table(@table_name)
        t.count(:id => 1)

        assert_equal false, t.batch?
        t2 = t.batch
        assert_equal false, t.batch?
        assert_equal true, t2.batch?
      end

      # OK
      assert true
    end
  end

  def test_prepared_statements
		each_connection do |conn|
			create_table conn

      # No duplicate preparations
      t = conn.table(@table_name)
      t.count(:id => 1)
      t.count('1 = 0')
      bt = t.batch

      assert_equal 2, t.prepared_statements[:count].length
      assert_equal 2, bt.prepared_statements[:count].length

      t.count(:id => 2)
      t.count('2 = 0')
      bt.count('3 = 0')
      assert_equal 4, t.prepared_statements[:count].length
      assert_equal 4, bt.prepared_statements[:count].length

      t.count(:id => 3)
      t.batch.count('4 = 0')
      assert_equal 5, t.prepared_statements[:count].length
      assert_equal 5, bt.prepared_statements[:count].length
      assert_equal 5, t.batch.prepared_statements[:count].length
      assert_equal 5, bt.batch.prepared_statements[:count].length

      t.close
      assert_equal 0, t.prepared_statements[:count].length
      assert_equal 0, bt.prepared_statements[:count].length
      assert_equal 0, t.batch.prepared_statements[:count].length
      assert_equal 0, bt.batch.prepared_statements[:count].length

      t.batch.batch.batch.count(:id => 1)
      assert_equal 1, t.prepared_statements[:count].length
      assert_equal 1, bt.prepared_statements[:count].length
      assert_equal 1, bt.batch.prepared_statements[:count].length
      assert_equal 1, t.batch.where('1 = 2').select(:a, :b).prepared_statements[:count].length

      # Should be OK
      bt.close
      t.batch.close
    end
  end
end

