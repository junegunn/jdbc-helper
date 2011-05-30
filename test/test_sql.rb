require 'helper'
include JDBCHelper

# WARNING: tests assumes ordered hash
class TestSQL < Test::Unit::TestCase
	def setup
	end

	def teardown
	end

	def test_value
		assert_equal 1,                   SQL.value(1)
		assert_equal 1.2,                 SQL.value(1.2)
		assert_equal 9999999999999999999, SQL.value(9999999999999999999)
		assert_equal "'sysdate'",         SQL.value('sysdate')
		assert_equal "'A''s'",            SQL.value("A's")
		assert_equal "sysdate",           SQL.value(JDBCHelper::SQL('sysdate'))

	end

	def test_order
		assert_equal "", SQL.order()
		assert_equal "", SQL.order(nil)
		assert_equal "", SQL.order(nil, nil)
		assert_equal "order by a", SQL.order(:a)
		assert_equal "order by a", SQL.order('a')
		assert_equal "order by a desc", SQL.order('a desc')
		assert_equal "order by a asc", SQL.order('a asc')
		assert_equal "order by a, b asc, c desc", SQL.order(:a, 'b asc', 'c desc')

		assert_raise(ArgumentError) { SQL.order(" -- ") }
		assert_raise(ArgumentError) { SQL.order(:a, :b, "c 'd") }
	end

	def test_where
		assert_equal "where a = 1",                   SQL.where(:a => 1)
		assert_equal "where a = 1.2",                 SQL.where(:a => 1.2)
		assert_equal "where a = 9999999999999999999", SQL.where(:a => 9999999999999999999)
		assert_equal "where a >= 1 and a <= 2",       SQL.where(:a => 1..2)
		assert_equal "where a >= 1 and a < 2",        SQL.where(:a => 1...2)
		assert_equal "where a = 'A''s'",              SQL.where(:a => "A's")
		assert_equal "where a is null",               SQL.where(:a => nil)
		assert_equal "where a is not null",           SQL.where(:a => SQL.not_nil)
		assert_equal "where a is not null",           SQL.where(:a => SQL.not_null)
		assert_equal "where a = sysdate",             SQL.where(:a => JDBCHelper::SQL('sysdate'))
		assert_equal "where sysdate = sysdate",       SQL.where(JDBCHelper::SQL('sysdate') => JDBCHelper::SQL('sysdate'))
		assert_equal "where a in ('aa', 'bb', 'cc')", SQL.where(:a => %w[aa bb cc])
		assert_equal "where a = 1 and b = 'A''s'",    SQL.where(:a => 1, :b => "A's")
		assert_equal "where a = 1 or b = 1",          SQL.where("a = 1 or b = 1")
		assert_equal '', SQL.where(nil)
		assert_equal '', SQL.where(" ")

		# Non-primitive datatypes not implemented (TODO?)
		assert_raise(NotImplementedError) { SQL.where(:a => Time.now) }

		# Invalid SQL detection
		assert_raise(ArgumentError) { SQL.where(:a => JDBCHelper::SQL(" 'a--b' -- cde")) }
		assert_raise(ArgumentError) { SQL.where(:a => JDBCHelper::SQL(" 'aabbb''dd")) }
		assert_raise(ArgumentError) { SQL.where(:a => JDBCHelper::SQL(" 'aabbb''dd' /* aaa */")) }
		assert_raise(ArgumentError) { SQL.where(:a => JDBCHelper::SQL(' aabbb"""  ')) }
		assert_raise(ArgumentError) { SQL.where(:a => JDBCHelper::SQL(' aab`bb``  ')) }
	end

	def test_select
		assert_equal "select * from a.b", SQL.select('a.b')
		assert_equal "select aa, bb from a.b where a is not null",
				SQL.select('a.b', :select => %w[aa bb], :where => {:a => SQL.not_nil})
		assert_equal "select aa, bb from a.b where a is not null and b >= 1 and b <= 10 order by cc, dd", 
				SQL.select('a.b', 
						   :select => %w[aa bb], 
						   :where => {:a => SQL.not_null, :b => (1..10)},
						   :order => %w[cc dd]
						  )
	end

	def test_count
		assert_equal "select count(*) from a.b", SQL.count('a.b')
		assert_equal "select count(*) from a.b where a is not null", SQL.count('a.b', :a => SQL.not_nil)
	end

	def test_delete
		assert_equal "delete from a.b", SQL.delete('a.b')
		assert_equal "delete from a.b where a is not null", SQL.delete('a.b', :a => SQL.not_nil)
	end

	def test_update
		assert_equal "update a.b set a = 1, b = 'A''s', c = now()", 
			SQL.update('a.b', {:a => 1, :b => "A's", :c => JDBCHelper::SQL('now()')}, {})

		assert_equal "update a.b set a = 1, b = 'A''s', c = now() where a is not null", 
			SQL.update('a.b', {:a => 1, :b => "A's", :c => JDBCHelper::SQL('now()')}, { :a => SQL.not_nil })
	end

	def test_insert
		assert_equal "insert into a.b (a, b, c) values (1, 'A''s', null)",
			SQL.insert('a.b', :a => 1, :b => "A's", :c => nil)
	end
end

