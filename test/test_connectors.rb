require 'helper'

class TestConnectors < Test::Unit::TestCase
  include JDBCHelperTestHelper

  def setup
  end

  def teardown
  end

  def test_connectors
    config.each do | db, conn_info |
      if conn_info['driver'] =~ /mysql/
        host = conn_info['url'].match(%r{//(.*?)/})[1]
        db = conn_info['url'].match(%r{/([^/?]*?)(\?.*)?$})[1]
        conn = JDBCHelper::MySQLConnector.connect(host, conn_info['user'], conn_info['password'], db)

        assert conn.closed? == false
        conn.close
        assert conn.closed?

        @conn = nil
        ret = JDBCHelper::MySQLConnector.connect(host, conn_info['user'], conn_info['password'], db) do |conn|
          assert conn.closed? == false
          @conn = conn
          1
        end
        assert @conn.closed?
        assert_equal 1, ret
      elsif conn_info['driver'] =~ /oracle/
        host = conn_info['url'].match(%r{@(.*?)/})[1]
        svc = conn_info['url'].match(%r{/([^/?]*?)(\?.*)?$})[1]
        conn = JDBCHelper::OracleConnector.connect(host, conn_info['user'], conn_info['password'], svc)

        assert conn.closed? == false
        conn.close
        assert conn.closed?

        @conn = nil
        ret = JDBCHelper::OracleConnector.connect(host, conn_info['user'], conn_info['password'], svc) do |conn|
          assert conn.closed? == false
          @conn = conn
          1
        end
        assert @conn.closed?
        assert_equal 1, ret
      end
    end
  end
end

