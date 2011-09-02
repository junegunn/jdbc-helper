# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
module Constants
  # Default login timeout is set to 60 seconds
  DEFAULT_LOGIN_TIMEOUT = 60

  # Constants only for Connectors
  module Connector
    JDBC_DRIVER = {
      :oracle => 'oracle.jdbc.driver.OracleDriver',
      :mysql  => 'com.mysql.jdbc.Driver'
    }

    DEFAULT_PARAMETERS = {
      :mysql => {
        'zeroDateTimeBehavior' => 'convertToNull',
        'rewriteBatchedStatements' => 'true',
        'useServerPrepStmts' => 'true',
        'useCursorFetch' => 'true'
      }
    }
  end#Connector
end#Constants
end#JDBCHelper
