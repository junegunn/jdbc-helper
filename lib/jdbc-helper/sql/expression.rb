# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# SQL generator class methods for prepared operations.
# WARNING: Does not perform SQL.check to minimize performance overhead
# @deprecated
module SQL
  # Generate SQL snippet, prevents the string from being quoted.
  # @param [String] SQL snippet
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  # @deprecated
  def self.expr sql
    { :sql => sql }
  end

  # "is not null" expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  def self.not_nil
    { :not => nil }
  end
  class << self
    alias not_null not_nil
  end

  # Greater-than expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.gt v
    { :gt => v }
  end

  # Less-than expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.lt v
    { :lt => v }
  end

  # Less-than-or-equal-to expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.le v
    { :le => v }
  end

  # Greater-than-or-equal-to expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.ge v
    { :ge => v }
  end

  # Not-equal expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.ne v
    { :ne => v }
  end

  # Like expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.like v
    raise ArgumentError, "expected String" unless v.is_a?(String)
    { :like => v }
  end

  # "Not like" expression for where clauses
  # @deprecated
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.not_like v
    raise ArgumentError, "expected String" unless v.is_a?(String)
    { :not => { :like => v } }
  end
end#SQL
end#JDBCHelper

