# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
module Constants
  # Default login timeout is set to 60 seconds
  DEFAULT_LOGIN_TIMEOUT = 60

  # Maximum nesting level for Statements
  MAX_STATEMENT_NESTING_LEVEL = 20

  # Constants only for Connectors
  module Connector
    JDBC_DRIVER = {
      :oracle => 'oracle.jdbc.driver.OracleDriver',
      :mysql  => 'com.mysql.jdbc.Driver',
      :postgres => 'org.postgresql.Driver'
    }

    DEFAULT_PARAMETERS = {
      :mysql => {
        'zeroDateTimeBehavior' => 'convertToNull',
        # Removed from 0.6.3: This can have a performance hit when batch size is large
        # 'rewriteBatchedStatements' => 'true',
        'useServerPrepStmts' => 'true',
        'useCursorFetch' => 'true'
      },
      :postgres => {
        'stringtype' => 'unspecified'
      }
    }
  end#Connector
end#Constants
end#JDBCHelper
