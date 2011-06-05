require 'helper'

class TestConnection < Test::Unit::TestCase
	include JDBCHelperTestHelper

	def setup
	end

	def teardown
		conn.update "drop table #{TEST_TABLE}" rescue nil
		conn.update "drop procedure #{TEST_PROCEDURE}" rescue nil
	end

	TEST_TABLE = 'tmp_jdbc_helper_test'
	TEST_PROCEDURE = 'tmp_jdbc_helper_test_proc'

	def get_one_two
		"
		select 1 one, 'two' two from dual
		union all
		select 1 one, 'two' two from dual
		"
	end

	def check_one_two(rec)
		assert_equal 2, rec.length

		assert_equal 1, rec.one
		assert_equal 1, rec[0]
		assert_equal 1, rec['one']
		assert_equal 1, rec[:one]
		assert_equal ['1'], rec[0...1].map(&:to_s)
		assert_equal ['1'], rec[0, 1].map(&:to_s)

		assert_equal 'two', rec.two
		assert_equal 'two', rec[1]
		assert_equal 'two', rec['two']
		assert_equal ['two'], rec[1..-1]
		assert_equal ['two'], rec[1, 1]

		assert_equal ['1', 'two'], rec[0..1].map(&:to_s)
		assert_equal ['1', 'two'], rec[0..-1].map(&:to_s)
		assert_equal ['1', 'two'], rec[0, 2].map(&:to_s)

		assert_raise(NoMethodError) { rec.three }
		assert_raise(NameError) { rec['three'] }
		assert_raise(RangeError) { rec[3] }
	end

	def reset_test_table conn
		conn.update "drop table #{TEST_TABLE}" rescue nil
		cnt = conn.update "
			create table #{TEST_TABLE} (
				a int primary key,
				b varchar(100)
			)"
		assert_equal 0, cnt
	end

	def reset_test_table_ts conn
		conn.update "drop table #{TEST_TABLE}" rescue nil
		cnt = conn.update "
			create table #{TEST_TABLE} (
				a timestamp
			)"
		assert_equal 0, cnt
	end

	# ---------------------------------------------------------------

	def test_connect_and_close
		config.each do | db, conn_info_org |
			4.times do | i |
				conn_info = conn_info_org.reject { |k,v| k == 'database' }

				# With or without timeout parameter
				conn_info['timeout'] = 60 if i % 2 == 1

				# Can connect with hash with symbol keys?
				conn_info.keys.each do | str_key |
					conn_info[str_key.to_sym] = conn_info.delete str_key
				end if i % 2 == 0

				conn = JDBCHelper::Connection.new(conn_info)
				assert_equal(conn.closed?, false)
				conn.close
				assert_equal(conn.closed?, true)
				[ :query, :update, :add_batch, :prepare ].each do | met |
					assert_raise(RuntimeError) { conn.send met, "A" }
				end
				[ :execute_batch, :clear_batch ].each do | met |
					assert_raise(RuntimeError) { conn.send met }
				end

				# initialize with execution block
				conn = JDBCHelper::Connection.new(conn_info) do | c |
					c.query('select 1 from dual')
					assert_equal c.closed?, false
				end
				assert conn.closed?
			end
		end
	end

	def test_query_enumerate
		each_connection do | conn |
			# Query without a block => Array
			query_result = conn.query get_one_two
			assert query_result.is_a? Array
			assert_equal 2, query_result.length
			check_one_two(query_result.first)

			# Query with a block
			count = 0
			conn.query(get_one_two) do | row |
				check_one_two row
				count += 1
			end
			assert_equal 2, count

			# Enumerate
			enum = conn.enumerate(get_one_two)
			assert enum.is_a? Enumerable
			assert enum.closed? == false
			a = enum.to_a
			assert_equal 2, a.length
			check_one_two a.first
			assert enum.closed? == true
		end
	end

	def test_update_batch
		each_connection do | conn |
			reset_test_table conn
			count = 100

			iq = lambda do | i |
				"insert into #{TEST_TABLE} values (#{i}, 'A')"
			end

			# update
			assert_equal 1, conn.update(iq.call 0)
			assert_equal 1, conn.prev_stat.success_count

			# add_batch execute_batch
			reset_test_table conn

			count.times do | p |
				conn.add_batch iq.call(p)
			end
			conn.execute_batch
			assert_equal count, conn.query("select count(*) from #{TEST_TABLE}")[0][0]

			# add_batch clear_batch
			reset_test_table conn

			count.times do | p |
				conn.add_batch iq.call(p)
			end
			conn.clear_batch
			assert_equal 0, conn.query("select count(*) from #{TEST_TABLE}")[0][0]
		end
	end

	def test_prepared_query_enumerate
		each_connection do | conn |
			sel = conn.prepare get_one_two
			assert sel.closed? == false

			# Query without a block => Array
			query_result = sel.query
			assert query_result.is_a? Array
			assert_equal 2, query_result.length
			check_one_two(query_result.first)

			# Query with a block
			count = 0
			sel.query do | row |
				check_one_two row
				count += 1
			end
			assert_equal 2, count

			# Enumerate
			enum = sel.enumerate
			assert enum.is_a? Enumerable
			assert enum.closed? == false
			a = enum.to_a
			assert_equal 2, a.length
			check_one_two a.first
			assert enum.closed? == true

			sel.close
			assert sel.closed?
			[ :query, :update, :add_batch, :execute_batch, :clear_batch ].each do | met |
				assert_raise(RuntimeError) { sel.send met }
			end
		end
	end

	def test_prepared_update_batch
		each_connection do | conn |
			reset_test_table conn
			ins = conn.prepare "insert into #{TEST_TABLE} values (?, ?)"
			assert_equal 2, ins.parameter_count

			count = 100

			# update
			assert ins.closed? == false
			assert_equal 1, ins.update(0, 'A')
			assert_equal 1, conn.prev_stat.success_count
			ins.close

			# add_batch execute_batch
			reset_test_table conn
			ins = conn.prepare "insert into #{TEST_TABLE} values (?, ?)"

			count.times do | p |
				ins.add_batch(p + 1, 'A')
			end
			ins.execute_batch
			assert_equal count, conn.query("select count(*) from #{TEST_TABLE}")[0][0]
			ins.close

			# add_batch clear_batch
			reset_test_table conn
			ins = conn.prepare "insert into #{TEST_TABLE} values (?, ?)"

			count.times do | p |
				ins.add_batch(p + 1, 'A')
			end
			ins.clear_batch
			assert_equal 0, conn.query("select count(*) from #{TEST_TABLE}")[0][0]

			# close closed?
			assert ins.closed? == false
			ins.close
			assert ins.closed?
			[ :query, :update, :add_batch, :execute_batch, :clear_batch ].each do | met |
				assert_raise(RuntimeError) { ins.send met }
			end
		end
	end
	
	def test_transaction
		each_connection do | conn |
			reset_test_table conn
			count = 100

			3.times do | i |
				sum = 0
				conn.update "delete from #{TEST_TABLE}"
				conn.transaction do | tx |
					count.times.each_slice(10) do | slice |
						slice.each do | p |
							conn.add_batch("insert into #{TEST_TABLE} values (#{p}, 'xxx')")
							sum += p
						end
						conn.execute_batch
					end
					result = conn.query("select count(*), sum(a) from #{TEST_TABLE}").first

					assert_equal count, result.first
					assert_equal sum, result.last.to_i

					case i
					when 0 then tx.rollback
					when 1 then tx.commit
					else
						nil # committed implicitly
					end

					flunk 'This should not be executed' if i < 2
				end

				assert_equal (i == 0 ? 0 : count),
					conn.query("select count(*) from #{TEST_TABLE}").first.first
			end
		end
	end

	def test_setter_timestamp
		each_connection do | conn |
			# Java timestamp
			reset_test_table_ts conn
			ts = java.sql.Timestamp.new(Time.now.to_i * 1000)
			conn.prepare("insert into #{TEST_TABLE} values (?)").update(ts)
			assert_equal ts, conn.query("select * from #{TEST_TABLE}")[0][0]

			# Ruby time
			reset_test_table_ts conn
			ts = Time.now
			conn.prepare("insert into #{TEST_TABLE} values (?)").update(ts)
			got = conn.query("select * from #{TEST_TABLE}")[0][0]
			assert_equal ts.to_i * 1000, got.getTime
		end
	end

	# Conditional testing is bad, but
	# Oracle and MySQL behave differently.
	def test_callable_statement
		each_connection do | conn |
			# Creating test procedure (Defined in JDBCHelperTestHelper)
			create_test_procedure conn, TEST_PROCEDURE

			# Array parameter
			cstmt_ord = conn.prepare_call "{call #{TEST_PROCEDURE}(?, ?, ?, ?, ?, ?)}"
			result = cstmt_ord.call('hello', 10, [100, Fixnum], [Time.now, Time], Float, String)
			assert_instance_of Hash, result
			assert_equal 1000, result[3]
			assert_equal 'hello', result[6]

			# Hash parameter
			cstmt_name = conn.prepare_call(case @type
						when :oracle
							"{call #{TEST_PROCEDURE}(:i1, :i2, :io1, :io2, :o1, :o2)}"
						else
							"{call #{TEST_PROCEDURE}(?, ?, ?, ?, ?, ?)}"
						end)
			result = cstmt_name.call(
				:i1 => 'hello', :i2 => 10,
				:io1 => [100, Fixnum], 'io2' => [Time.now, Time], 
				:o1 => Float, 'o2' => String)
			assert_instance_of Hash, result
			assert_equal 1000, result[:io1]
			assert_equal 'hello', result['o2']

			# Invalid parameters
			#assert_raise(NativeException) { cstmt_ord.call 1 }
			assert_raise(ArgumentError)   { cstmt_ord.call({}, {}) }
			assert_raise(NativeException) { cstmt_name.call 1 }
			assert_raise(ArgumentError)   { cstmt_name.call({}, {}) }

			# Close
			[ cstmt_ord, cstmt_name ].each do | cstmt |
				assert_equal false, cstmt.closed?
				cstmt.close
				assert_equal true, cstmt.closed?
				assert_raise(RuntimeError) { cstmt.call }
			end

			# pend('mysql raises data truncation error') do
			if @type != :mysql
				cstmt_ord = conn.prepare_call "{call #{TEST_PROCEDURE}(?, 10, ?, ?, ?, ?)}"
				cstmt_name = conn.prepare_call(case @type
						when :oracle
							"{call #{TEST_PROCEDURE}(:i1, 10, :io1, :io2, :o1, :o2)}"
						else
							"{call #{TEST_PROCEDURE}(?, 10, ?, ?, ?, ?)}"
						end)
				# Hash parameter
				result = cstmt_name.call(
					:i1 => 'hello',# :i2 => 10,
					:io1 => [100, Fixnum], 'io2' => [Time.now, Time], 
					:o1 => Float, 'o2' => String)
				assert_instance_of Hash, result
				assert_equal 1000, result[:io1]
				assert_equal 'hello', result['o2']

				# Array parameter
				result = cstmt_ord.call('hello', [100, Fixnum], [Time.now, Time], Float, String)
				assert_instance_of Hash, result
				assert_equal 1000, result[2]
				assert_equal 'hello', result[5]
			end
		end
	end
end

