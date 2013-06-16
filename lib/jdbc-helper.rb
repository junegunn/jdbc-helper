# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

require 'jdbc-helper/version'

if RUBY_PLATFORM.match(/java/).nil?
  raise LoadError, 'JRuby is required for JDBC'
end

require 'java'
require 'jdbc-helper/sql'
require 'jdbc-helper/constants'
require 'jdbc-helper/connection'
require 'jdbc-helper/connector'
require 'sql_helper'

