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
    DEFAULT_PARAMETERS = {
      :mysql => {
        :driver               => 'com.mysql.jdbc.Driver',
        :zeroDateTimeBehavior => 'convertToNull',
        # Removed from 0.6.3:
        #  This can have a performance hit when batch size is large
        # :rewriteBatchedStatements => 'true',
        :useServerPrepStmts   => 'true',
        :useCursorFetch       => 'true',
      },
      :mariadb      => {
        :driver     => 'org.mariadb.jdbc.Driver',
      },
      :oracle       => {
        :driver     => 'oracle.jdbc.driver.OracleDriver',
      },
      :postgres     => {
        :driver     => 'org.postgresql.Driver',
        :stringtype => 'unspecified',
      },
      :sqlserver    => {
        :driver     => 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
      },
      :cassandra    => {
        :driver     => 'org.apache.cassandra.cql.jdbc.CassandraDriver',
        :cqlVersion => '3.0.0',
      },
      :filemaker    => {
        :driver     => 'com.filemaker.jdbc.Driver'
      }
    }
  end#Connector
end#Constants
end#JDBCHelper
