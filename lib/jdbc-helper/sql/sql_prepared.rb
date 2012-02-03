# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# SQL generator class methods for prepared operations.
# WARNING: Does not perform SQL.check to minimize performance overhead
class SQLPrepared < JDBCHelper::SQL
  # Generates SQL where cluase with the given conditions.
  # Parameter can be either Hash of String.
  def self.where *conds
    where_internal conds
  end

  # SQL Helpers
  # ===========
  # Generates insert SQL with hash
  def self.insert table, data_hash
    insert_internal 'insert', table, data_hash
  end

  # Generates insert ignore SQL (Non-standard syntax)
  def self.insert_ignore table, data_hash
    insert_internal 'insert ignore', table, data_hash
  end

  # Generates replace SQL (Non-standard syntax)
  def self.replace table, data_hash
    insert_internal 'replace', table, data_hash
  end

  # Generates update SQL with hash.
  # :where element of the given hash is taken out to generate where clause.
  def self.update table, data_hash, where
    where_clause, where_binds = where_internal where

    col_binds = []
    sql = ("update #{table} set " + data_hash.map { |k, v|
      case v
      when JDBCHelper::SQL::ScalarExpression
        "#{k} = #{v}"
      else
        col_binds << v
        "#{k} = ?"
      end
    }.join(', ') + " #{where_clause}").strip

    return sql, col_binds + where_binds
  end

  # Generates select SQL with the given conditions
  def self.select table, opts = {}
    opts = opts.reject { |k, v| v.nil? }
    w_c, w_b = where_internal(opts.fetch(:where, {}))
    sql = [
      "select #{opts.fetch(:select, ['*']).join(', ')} from #{table}", 
      w_c.to_s,
      order(opts.fetch(:order, []).join(', '))
    ].reject(&:empty?).join(' ')

    return sql, w_b
  end

  # Generates count SQL with the given conditions
  def self.count table, conds = nil
    w_c, w_b = where_internal(conds)
    sql = "select count(*) from #{table} #{w_c}".strip

    return sql, w_b
  end

  # Generates delete SQL with the given conditions
  def self.delete table, conds = nil
    w_c, w_b = where_internal(conds)
    sql =  "delete from #{table} #{w_c}".strip
    return sql, w_b
  end

  private
  def self.where_internal conds
    conds   = [conds] unless conds.is_a? Array
    binds   = []
    clauses = []
    conds.compact.each do |cond| 
      c, b = where_unit cond 
      next if c.empty?

      binds += b
      clauses << c
    end
    where_clause = clauses.join(' and ')
    if where_clause.empty?
      return nil, []
    else
      where_clause = 'where ' + where_clause
      return where_clause, binds
    end
  end

  def self.where_unit conds
    binds = []

    clause = case conds
             when String, JDBCHelper::SQL::ScalarExpression
               conds = conds.strip
               conds.empty? ? '' : "(#{conds})"
             when Hash
               conds.map { |k, v|
                 "#{k} " +
                   case v
                   when NilClass
                     "is null"
                   when Numeric
                     binds << v
                     "= ?"
                   when JDBCHelper::SQL::ScalarExpression
                     "= #{v.to_s}"
                   when JDBCHelper::SQL::NotNullExpression
                     v.to_s
                   when JDBCHelper::SQL::CriterionExpression
                     e, b = v.to_bind
                     case b.first
                     when JDBCHelper::SQL::ScalarExpression
                       v.to_s
                     else
                       binds += b
                       e
                     end
                   when Range
                     binds << v.begin << v.end
                     ">= ? and #{k} <#{'=' unless v.exclude_end?} ?"
                   when Array
                     "in (" + 
                       v.map { |e|
                       case e
                       when String
                         SQL.value e
                       else
                         e.to_s
                       end }.join(', ') + ")"
                   else
                     binds << v
                     "= ?"
                   end
               }.join(' and ')
             when Array
               if conds.empty?
                 ''
               else
                 binds += conds[1..-1] if conds.length > 1
                 "(#{conds.first})" 
               end
             else
               raise NotImplementedError.new("Parameter to where must be either Hash or String")
             end
    return clause, binds
  end

  def self.insert_internal cmd, table, data_hash
    binds  = []
    values = data_hash.values.map { |v|
      case v
      when JDBCHelper::SQL::ScalarExpression
        v
      else
        binds << v
        '?'
      end
    }
    sql = "#{cmd} into #{table} (#{data_hash.keys.join ', '}) values (#{values.join(', ')})"
    return sql, binds
  end
end#SQL
end#JDBCHelper

