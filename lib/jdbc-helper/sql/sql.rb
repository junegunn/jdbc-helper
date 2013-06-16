# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

require 'bigdecimal'

module JDBCHelper
# Generate SQL snippet, prevents the string from being quoted.
# @deprecated
# @param [String] SQL snippet
# @return [JDBCHelper::SQL::Expression]
# @deprecated Use JDBCHelper::SQL.expr instead
def self.sql str
  { :sql => str }
end
class << self
  # @deprecated Use JDBCHelper::SQL.expr instead
  alias_method :SQL, :sql
end

# Class representing an SQL snippet. Also has many SQL generator class methods.
# @deprecated
module SQL
  # Formats the given data so that it can be injected into SQL
  # @deprecated
  def self.value data
    SQLHelper.quote(data)
  end

  # Generates SQL where cluase with the given conditions.
  # Parameter can be either Hash of String.
  # @deprecated
  def self.where *conds
    SQLHelper.where(*conds)
  end

  # @deprecated
  def self.where_prepared *conds
    SQLHelper.where_prepared(*conds)
  end

  # Generates SQL order by cluase with the given conditions.
  # @deprecated
  def self.order *criteria
    SQLHelper.order(*criteria)
  end

  # SQL Helpers
  # ===========

  # Generates insert SQL with hash
  # @deprecated
  def self.insert table, data_hash
    SQLHelper.insert :table => table, :data => data_hash, :prepared => false
  end

  # Generates insert ignore SQL (Non-standard syntax)
  # @deprecated
  def self.insert_ignore table, data_hash
    SQLHelper.insert_ignore :table => table, :data => data_hash, :prepared => false
  end

  # Generates replace SQL (Non-standard syntax)
  # @deprecated
  def self.replace table, data_hash
    SQLHelper.replace :table => table, :data => data_hash, :prepared => false
  end

  # Generates update SQL with hash.
  # :where element of the given hash is taken out to generate where clause.
  # @deprecated
  def self.update table, data_hash, where
    SQLHelper.update :table => table, :data => data_hash, :where => where, :prepared => false
  end

  # Generates select SQL with the given conditions
  # @deprecated
  def self.select table, opts = {}
    SQLHelper.select :table    => table,
                     :project  => opts[:select],
                     :where    => opts[:where],
                     :order    => opts[:order],
                     :prepared => false
  end

  # Generates count SQL with the given conditions
  # @deprecated
  def self.count table, conds = nil
    SQLHelper.count :table => table, :where => conds, :prepared => false
  end

  # Generates delete SQL with the given conditions
  # @deprecated
  def self.delete table, conds = nil
    SQLHelper.delete :table => table, :where => conds, :prepared => false
  end

  # FIXME: Naive protection for SQL Injection
  # TODO: check caching?
  # @deprecated
  def self.check expr, is_name = false
    return nil if expr.nil?

    tag = is_name ? 'Object name' : 'Expression'
    test = expr.gsub(/'[^']*'/, '').gsub(/`[^`]*`/, '').gsub(/"[^"]*"/, '').strip
    raise ArgumentError.new("#{tag} cannot contain (unquoted) semi-colons: #{expr}") if test.include?(';')
    raise ArgumentError.new("#{tag} cannot contain (unquoted) comments: #{expr}") if test.match(%r{--|/\*|\*/})
    raise ArgumentError.new("Unclosed quotation mark: #{expr}") if test.match(/['"`]/)
    raise ArgumentError.new("#{tag} is blank") if test.empty?

    if is_name
      raise ArgumentError.new(
        "#{tag} cannot contain (unquoted) parentheses: #{expr}") if test.match(%r{\(|\)})
    end

    return expr
  end
end#SQL
end#JDBCHelper

