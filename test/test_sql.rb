require 'helper'
SQL = JDBCHelper::SQL
SQLPrepared = JDBCHelper::SQLPrepared

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
    assert_equal "0.00000000000000000000009999999999999999999", 
        SQL.value(BigDecimal.new("0.00000000000000000000009999999999999999999"))
    assert_equal "'sysdate'",         SQL.value('sysdate')
    assert_equal "'A''s'",            SQL.value("A's")
    assert_equal "sysdate",           SQL.value(SQL.expr('sysdate'))

    assert_raise(NotImplementedError) { SQL.value(Time.now) }
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

  def test_complex_table_wrapper_query
    assert_equal "select a + b sum from test where a = 1 and b >= 1 and b <= 10 and c in ('a', 'b', 'c') and d = sysdate and e is not null and f > 100 and g < 100 and h like 'ABC%' and i not like 'ABC%' and j <= sysdate order by a + b desc",
      JDBCHelper::TableWrapper.new(nil, :test).where(
        :a => 1,
        :b => (1..10),
        :c => %w[a b c],
        :d => SQL.expr('sysdate'),
        :e => SQL.not_null,
        :f => SQL.gt(100),
        :g => SQL.lt(100),
        :h => SQL.like('ABC%'),
        :i => SQL.not_like('ABC%'),
        :j => SQL.le( SQL.expr('sysdate') )
      ).select(
        SQL.expr('a + b sum')
      ).order(
        SQL.expr('a + b desc')
      ).sql
  end

  def test_where
    assert_equal "where a = 1",                   SQL.where(:a => 1)
    assert_equal "where a = 12345678901234567890.1234567890123456789", 
                                                  SQL.where(:a => BigDecimal.new("12345678901234567890.1234567890123456789"))
    assert_equal "where a = 1.2",                 SQL.where(:a => 1.2)
    assert_equal "where a = 9999999999999999999", SQL.where(:a => 9999999999999999999)
    assert_equal "where a >= 1 and a <= 2",       SQL.where(:a => 1..2)
    assert_equal "where a >= 1 and a < 2",        SQL.where(:a => 1...2)
    assert_equal "where a = 'A''s'",              SQL.where(:a => "A's")
    assert_equal "where a is null",               SQL.where(:a => nil)
    assert_equal "where a is not null",           SQL.where(:a => SQL.not_nil)
    assert_equal "where a is not null",           SQL.where(:a => SQL.not_null)
    assert_equal "where a = sysdate",             SQL.where(:a => SQL.expr('sysdate'))
    assert_equal "where a = sysdate",             SQL.where(:a => SQL.expr('sysdate'))
    assert_equal "where sysdate = sysdate",       SQL.where(SQL.expr('sysdate') => SQL.expr('sysdate'))
    assert_equal "where a in ('aa', 'bb', 'cc')", SQL.where(:a => %w[aa bb cc])
    assert_equal "where a in ('aa', 'bb', 'cc', 4)", SQL.where(:a => %w[aa bb cc] + [4])
    assert_equal "where a = 1 and b = 'A''s'",    SQL.where(:a => 1, :b => "A's")
    assert_equal "where (a = 1 or b = 1)",        SQL.where("a = 1 or b = 1")
    assert_equal "where (a = 1 or b = 1)",        SQL.where(SQL.expr("a = 1 or b = 1"))
    assert_equal "where (a = 1 or b = 1) and c = 2", SQL.where("a = 1 or b = 1", :c => 2)
    assert_equal "where c = 2 and (a = 1 or b = 1)", SQL.where({:c => 2}, "a = 1 or b = 1")
    assert_equal "where c = 2 and (a = 1 or b = 1) and (e = 2) and f = 3 and (abc not like % || 'abc???' || % or def != 100 and ghi = '??') and (1 = 1)",
        SQL.where({:c => 2}, "a = 1 or b = 1", nil, "", "e = 2", nil, {:f => 3}, {},
                  ["abc not like % || ? || % or def != ? and ghi = '??'", 'abc???', 100], [], ['1 = 1'])
    assert_equal '', SQL.where(nil)
    assert_equal '', SQL.where(" ")

    # 0.7.0
    {
      :gt => '>',
      :lt => '<',
      :ge => '>=',
      :le => '<=',
      :ne => '<>'
    }.each do |x, op|
      assert_equal "where a #{op} 5", SQL.where(:a => SQL.send(x, 5))
      assert_equal "where a #{op} 'z'", SQL.where(:a => SQL.send(x, 'z'))
    end
    assert_equal "where a > sysdate", SQL.where(:a => SQL.gt(SQL.expr 'sysdate'))

    # 0.7.0: like
    assert_raise(ArgumentError) { SQL.like(123) }
    assert_raise(ArgumentError) { SQL.not_like(BigDecimal.new('1.23')) }
    assert_equal "where a like 'abc'",     SQL.where(:a => SQL.like('abc'))
    assert_equal "where a not like 'abc'", SQL.where(:a => SQL.not_like('abc'))

    # 0.7.0: with where
    assert_equal "where a > 1",        SQL.where(:a => SQL.gt(1))
    assert_equal "where a < 'z'",      SQL.where(:a => SQL.lt('z'))
    assert_equal "where a >= sysdate", SQL.where(:a => SQL.ge(SQL.expr 'sysdate'))
    assert_equal "where a <= sysdate", SQL.where(:a => SQL.le(SQL.expr 'sysdate'))
    assert_equal "where a <> 'z'",     SQL.where(:a => SQL.ne(:z))
    assert_equal "where a like 'hello''%'", SQL.where(:a => SQL.like("hello'%"))
    assert_equal "where a not like 'hello''%'", SQL.where(:a => SQL.not_like("hello'%"))

    # Non-primitive datatypes not implemented (TODO?)
    assert_raise(NotImplementedError) { SQL.where(:a => Time.now) }
    assert_raise(NotImplementedError) { SQL.where(5) }
    assert_raise(NotImplementedError) { SQL.where(Time.now) }

    # Invalid SQL detection
    assert_raise(ArgumentError) { SQL.where(" 'a--b' -- cde") }
    assert_raise(ArgumentError) { SQL.where(" 'a--b' ;") }
    assert_raise(ArgumentError) { SQL.where(" 'a--b' -- cde", :a => 1) }
    assert_raise(ArgumentError) { SQL.where(" 'a--b' -- cde", :a => 1) }
    assert_raise(ArgumentError) { SQL.where({:a => 1}, "/* a") }
    assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(" 'a--b' -- cde")) }
    assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(" 'aabbb''dd")) }
    assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(" 'aabbb''dd' /* aaa */")) }
    assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(' aabbb"""  ')) }
    assert_raise(ArgumentError) { SQL.where(:a => SQL.expr(' aab`bb``  ')) }
  end

  def test_where_prepared
    assert_equal ["where a = ?", [1]],                  SQLPrepared.where(:a => 1)
    assert_equal ["where a = ?", [1.2]],               SQLPrepared.where(:a => 1.2)
    assert_equal ["where a = ?", [9999999999999999999]], SQLPrepared.where(:a => 9999999999999999999)
    assert_equal ["where a >= ? and a <= ?", [1,2]],      SQLPrepared.where(:a => 1..2)
    assert_equal ["where a >= ? and a < ?", [1,2]],       SQLPrepared.where(:a => 1...2)
    assert_equal ["where a = ?", ["A's"]],            SQLPrepared.where(:a => "A's")
    assert_equal ["where a is null", []],             SQLPrepared.where(:a => nil)
    assert_equal ["where a is not null", []],          SQLPrepared.where(:a => SQL.not_nil)
    assert_equal ["where a is not null", []],         SQLPrepared.where(:a => SQL.not_null)
    assert_equal ["where a = sysdate", []],           SQLPrepared.where(:a => SQL.expr('sysdate'))
    assert_equal ["where a = sysdate", []],           SQLPrepared.where(:a => SQL.expr('sysdate'))
    assert_equal ["where sysdate = sysdate", []],     SQLPrepared.where(SQL.expr('sysdate') => SQL.expr('sysdate'))
    assert_equal ["where a in ('aa', 'bb', 'cc')", []], SQLPrepared.where(:a => %w[aa bb cc])
    assert_equal ["where a in ('aa', 'bb', 'cc', 4)", []], SQLPrepared.where(:a => %w[aa bb cc] + [4])
    assert_equal ["where a = ? and b = ?", [1, "A's"]],   SQLPrepared.where(:a => 1, :b => "A's")
    assert_equal ["where (a = 1 or b = 1)", []],        SQLPrepared.where("a = 1 or b = 1")
    assert_equal ["where (a = 1 or b = 1) and c = ?", [2]], SQLPrepared.where("a = 1 or b = 1", :c => 2)
    assert_equal ["where c = ? and (a = 1 or b = 1)", [2]], SQLPrepared.where({:c => 2}, "a = 1 or b = 1")
    assert_equal ["where c = ? and (a = 1 or b = 1) and (e = 2) and f = ?", [2, 3]],
        SQLPrepared.where({:c => 2}, "a = 1 or b = 1", nil, "", "e = 2", nil, {:f => 3}, {})
    assert_equal ["where c = ? and (a = 1 or b = 1) and (e = 2) and f = ? and (abc not like % || ? || % or def != ?) and (1 = 1)", [2, 3, 'abc', 100]],
        SQLPrepared.where({:c => 2}, "a = 1 or b = 1", nil, "", "e = 2", nil, {:f => 3}, {},
                  ["abc not like % || ? || % or def != ?", 'abc', 100], [], ['1 = 1'])
    assert_equal [nil, []], SQLPrepared.where(nil)
    assert_equal [nil, []], SQLPrepared.where(" ")

    # 0.7.0: with where
    assert_equal ["where a > ?", [1]],        SQLPrepared.where(:a => SQL.gt(1))
    assert_equal ["where a < ?", ['z']],      SQLPrepared.where(:a => SQL.lt('z'))
    assert_equal ["where a >= sysdate", []], SQLPrepared.where(:a => SQL.ge(SQL.expr 'sysdate'))
    assert_equal ["where a <= sysdate", []], SQLPrepared.where(:a => SQL.le(SQL.expr 'sysdate'))
    assert_equal ["where a like ?", ["hello'%"]], SQLPrepared.where(:a => SQL.like("hello'%"))
    assert_equal ["where a not like ?", ["hello'%"]], SQLPrepared.where(:a => SQL.not_like("hello'%"))
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

    assert_equal ["select count(*) from a.b", []], SQLPrepared.count('a.b')
    assert_equal ["select count(*) from a.b where a = ?", [1]], SQLPrepared.count('a.b', :a => 1)
  end

  def test_delete
    assert_equal "delete from a.b", SQL.delete('a.b')
    assert_equal "delete from a.b where a is not null", SQL.delete('a.b', :a => SQL.not_nil)

    assert_equal ["delete from a.b", []], SQLPrepared.delete('a.b')
    assert_equal ["delete from a.b where a = ?", [1]], SQLPrepared.delete('a.b', :a => 1)
  end

  def test_update
    assert_equal "update a.b set a = 1, b = 'A''s', c = now()", 
      SQL.update('a.b', {:a => 1, :b => "A's", :c => SQL.expr('now()')}, {})

    assert_equal "update a.b set a = 1, b = 'A''s', c = now() where a is not null", 
      SQL.update('a.b', {:a => 1, :b => "A's", :c => SQL.expr('now()')}, { :a => SQL.not_nil })

    assert_equal ["update a.b set a = ?, b = ?, c = now()", [1, "A's"]],
      SQLPrepared.update('a.b', {:a => 1, :b => "A's", :c => SQL.expr('now()')}, {})

    assert_equal ["update a.b set a = ?, b = ?, c = now() where a = ?", [1, "A's", 2]],
      SQLPrepared.update('a.b', {:a => 1, :b => "A's", :c => SQL.expr('now()')}, { :a => 2 })
  end

  def test_insert
    {'insert' => :insert, 'insert ignore' => :insert_ignore, 'replace' => :replace}.each do |op, met|
      assert_equal "#{op} into a.b (a, b, c, d) values (1, 'A''s', null, sysdate)",
        SQL.send(met, 'a.b', :a => 1, :b => "A's", :c => nil, :d => SQL.expr('sysdate'))

      assert_equal ["#{op} into a.b (a, b, c, d) values (?, ?, ?, sysdate)", [1, "A's", nil]],
        SQLPrepared.send(met, 'a.b', :a => 1, :b => "A's", :c => nil, :d => SQL.expr('sysdate'))
    end
  end

  def test_sql_expression_equality
    # Backward-compatibility
    assert_equal "a = b", JDBCHelper.SQL('a = b').to_s
    assert_equal JDBCHelper.SQL('a = b'), JDBCHelper.SQL('a = b')

    # type conversion across ==, but not across eql (TODO TBD)
    assert JDBCHelper.SQL('a = b') == (JDBCHelper.SQL('a = b'))
    assert JDBCHelper.SQL('a = b') == 'a = b'
    assert       JDBCHelper.SQL('a = b').eql?(JDBCHelper.SQL('a = b'))
    assert_equal JDBCHelper.SQL('a = b').hash, JDBCHelper.SQL('a = b').hash
    assert_false JDBCHelper.SQL('a = b').eql?('a = b')

    assert JDBCHelper.SQL('a = b') != JDBCHelper.SQL('a = c')

    h = {}
    h[SQL.expr 'a = b'] = 1 
    h[SQL.expr 'a = b'] = 2
    assert_equal 1, h.keys.length
    assert_equal 2, h[SQL.expr 'a = b']
  end
end

