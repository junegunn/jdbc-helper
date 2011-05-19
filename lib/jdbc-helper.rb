# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

if RUBY_PLATFORM.match(/java/).nil?
	raise LoadError, 'JRuby is required for JDBC'
end

require 'java'

module JavaLang # :nodoc:
	include_package 'java.lang'
end

module JavaSql # :nodoc:
	include_package 'java.sql'
end

require 'jdbc-helper/sql'
require 'jdbc-helper/object_wrapper'
require 'jdbc-helper/constants'
require 'jdbc-helper/connection'
require 'jdbc-helper/connector'

