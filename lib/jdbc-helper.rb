# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

if RUBY_PLATFORM.match(/java/).nil?
  raise LoadError, 'JRuby is required for JDBC'
end

require 'java'
require 'jdbc-helper/sql/sql'
require 'jdbc-helper/sql/sql_prepared'
require 'jdbc-helper/sql/expression'
require 'jdbc-helper/constants'
require 'jdbc-helper/connection'
require 'jdbc-helper/connector'

