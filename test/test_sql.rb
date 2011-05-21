require 'helper'
include JDBCHelper

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
		assert_equal "sysdate",           SQL.value(SQL.expr('sysdate'))

	end

	def test_order_by
		assert_equal "", SQL.order_by()
		assert_equal "", SQL.order_by(nil)
		assert_equal "", SQL.order_by(nil, nil)
		assert_equal "order by a", SQL.order_by(:a)
		assert_equal "order by a", SQL.order_by('a')
		assert_equal "order by a desc", SQL.order_by('a desc')
		assert_equal "order by a asc", SQL.order_by('a asc')
		assert_equal "order by a, b asc, c desc", SQL.order_by(:a, 'b asc', 'c desc')

		assert_raise(ArgumentError) { SQL.order_by(" -- ") }
		assert_raise(ArgumentError) { SQL.order_by(:a, :b, "c 'd") }
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
		assert_equal "where a = sysdate",             SQL.where(:a => SQL.expr('sysdate'))
		assert_equal "where sysdate = sysdate",       SQL.where(SQL.expr('sysdate') => SQL.expr('sysdate'))
		assert_equal "where a in ('aa', 'bb', 'cc')", SQL.where(:a => %w[aa bb cc])
		assert_equal "where a = 1 and b = 'A''s'",    SQL.where(:a => 1, :b => "A's")
		assert_equal "where a = 1 or b = 1",          SQL.where("a = 1 or b = 1")
		assert_equal '', SQL.where(nil)
		assert_equal '', SQL.where(" ")

		# Non-primitive datatypes not implemented (TODO?)
		assert_raise(NotImplementedError) { SQL.where(:a => Time.now) }

		# Invalid SQL detection
		assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(" 'a--b' -- cde")) }
		assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(" 'aabbb''dd")) }
		assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(" 'aabbb''dd' /* aaa */")) }
		assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(' aabbb"""  ')) }
		assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(' aab`bb``  ')) }
	end

	def test_select
		assert_equal "select * from a.b", SQL.select('a.b')
		assert_equal "select * from a.b where a is not null", SQL.select('a.b', :a => SQL.not_nil)
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
			SQL.update('a.b', :a => 1, :b => "A's", :c => SQL.expr('now()'))

		assert_equal "update a.b set a = 1, b = 'A''s', c = now() where a is not null", 
			SQL.update('a.b', :a => 1, :b => "A's", :c => SQL.expr('now()'), :where => { :a => SQL.not_nil })
	end

	def test_insert
		assert_equal "insert into a.b (a, b, c) values (1, 'A''s', null)",
			SQL.insert('a.b', :a => 1, :b => "A's", :c => nil)
	end
end

