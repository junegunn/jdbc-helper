# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# SQL generator class methods for prepared operations.
# WARNING: Does not perform SQL.check to minimize performance overhead
# @deprecated
module SQLPrepared
  # Generates SQL where cluase with the given conditions.
  # Parameter can be either Hash of String.
  # @deprecated
  def self.where *conds
    sql, *binds = SQLHelper.where_prepared(*conds)
    [sql, binds]
  end

  # SQL Helpers
  # ===========
  # Generates insert SQL with hash
  # @deprecated
  def self.insert table, data_hash
    sql, *binds = SQLHelper.insert :table => table, :data => data_hash, :prepared => true
    [sql, binds]
  end

  # Generates insert ignore SQL (Non-standard syntax)
  # @deprecated
  def self.insert_ignore table, data_hash
    sql, *binds = SQLHelper.insert_ignore :table => table, :data => data_hash, :prepared => true
    [sql, binds]
  end

  # Generates replace SQL (Non-standard syntax)
  # @deprecated
  def self.replace table, data_hash
    sql, *binds = SQLHelper.replace :table => table, :data => data_hash, :prepared => true
    [sql, binds]
  end

  # Generates update SQL with hash.
  # :where element of the given hash is taken out to generate where clause.
  # @deprecated
  def self.update table, data_hash, where
    sql, *binds =
      SQLHelper.update :table => table, :data => data_hash, :where => where, :prepared => true
    [sql, binds]
  end

  # Generates select SQL with the given conditions
  # @deprecated
  def self.select table, opts = {}
    sql, *binds = SQLHelper.select :table => table,
                     :project  => opts[:select],
                     :where    => opts[:where],
                     :order    => opts[:order],
                     :prepared => true
    [sql, binds]
  end

  # Generates count SQL with the given conditions
  # @deprecated
  def self.count table, conds = nil
    sql, *binds =
      SQLHelper.count :table => table, :where => conds, :prepared => true
    [sql, binds]
  end

  # Generates delete SQL with the given conditions
  # @deprecated
  def self.delete table, conds = nil
    sql, *binds = SQLHelper.delete :table => table, :where => conds, :prepared => true
    [sql, binds]
  end
end#SQL
end#JDBCHelper

