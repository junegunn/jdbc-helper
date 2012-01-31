# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# SQL generator class methods for prepared operations.
# WARNING: Does not perform SQL.check to minimize performance overhead
class SQL
  # Generate SQL snippet, prevents the string from being quoted.
  # @param [String] SQL snippet
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.expr sql
    ScalarExpression.new sql
  end

  # "is not null" expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  def self.not_nil
    NotNullExpression.singleton
  end
  class << self
    alias not_null not_nil
  end

  # Greater-than expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.gt v
    ParameterizedExpression.new '>', v
  end

  # Less-than expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.lt v
    ParameterizedExpression.new '<', v
  end

  # Less-than-or-equal-to expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.le v
    ParameterizedExpression.new '<=', v
  end

  # Greater-than-or-equal-to expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.ge v
    ParameterizedExpression.new '>=', v
  end

  # Not-equal expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.ne v
    ParameterizedExpression.new '<>', v
  end

  # Like expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.like v
    raise ArgumentError.new('Like expression must be given as a String') unless v.is_a?(String)
    ParameterizedExpression.new 'like', v
  end

  # "Not like" expression for where clauses
  # @return [JDBCHelper::SQL::Expression]
  # @since 0.7.0
  def self.not_like v
    raise ArgumentError.new('Like expression must be given as a String') unless v.is_a?(String)
    ParameterizedExpression.new 'not like', v
  end

  # @since 0.7.0
  class Expression
    def initialize
      raise Exception.new("JDBCHelper::SQL::Expression is an abstract class")
    end

    def == other
      self.to_s == other.to_s
    end
    
    def eql? other
      self.class == other.class && self.to_s == other.to_s
    end

    def hash
      [self.class, self.to_s].hash
    end
  end

  # @since 0.7.0
  class ScalarExpression < Expression
    def initialize sql
      @sql = SQL.check sql.to_s
    end

    def to_s
      @sql
    end

    def to_bind
      [@to_s, []]
    end
  end

  # @since 0.7.0
  class NotNullExpression < Expression
    def self.singleton
      @@singleton ||= NotNullExpression.new
    end
    
    def initialize
    end

    def to_s
      "is not null"
    end

    def to_bind
      ["is not null", []]
    end
  end

  # @since 0.7.0
  class ParameterizedExpression < Expression
    def initialize operator, param
      @operator = operator
      @param = param
    end

    def to_s
      [@operator, SQL.value(@param)].join(' ')
    end

    def to_bind
      [[@operator, '?'].join(' '), [@param]]
    end
  end
end#SQL
end#JDBCHelper

