# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# A wrapper object representing a database table. Allows you to perform table operations easily.
# @since 0.2.0
# @example Usage
#  # For more complex examples, refer to test/test_object_wrapper.rb
#
#  # Creates a table wrapper
#  table = conn.table('test.data')
#
#  # Counting the records in the table
#  table.count
#  table.count(:a => 10)
#  table.where(:a => 10).count
#
#  table.empty?
#  table.where(:a => 10).empty?
#
#  # Selects the table by combining select, where, and order methods
#  table.select('a apple', :b).where(:c => (1..10)).order('b desc', 'a asc') do |row|
#    puts row.apple
#  end
#
#  # Updates with conditions
#  table.update(:a => 'hello', :b => JDBCHelper::SQL('now()'), :where => { :c => 3 })
#  # Or equivalently,
#  table.where(:c => 3).update(:a => 'hello', :b => JDBCHelper::SQL('now()'))
#
#  # Insert into the table
#  table.insert(:a => 10, :b => 20, :c => JDBCHelper::SQL('10 + 20'))
#  table.insert_ignore(:a => 10, :b => 20, :c => 30)
#  table.replace(:a => 10, :b => 20, :c => 30)
#
#  # Delete with conditions
#  table.delete(:c => 3)
#  # Or equivalently,
#  table.where(:c => 3).delete
#
#  # Truncate or drop table (Cannot be undone)
#  table.truncate_table!
#  table.drop_table!
class TableWrapper < ObjectWrapper
  # Returns the name of the table
  # @return [String]
  alias to_s name

  # Retrieves the count of the table
  # @param [List of Hash/String] where Filter conditions
  # @return [Fixnum] Count of the records.
  def count *where
    sql, binds = JDBCHelper::SQLPrepared.count(name, @query_where + where)
    pstmt = prepare :count, sql
    pstmt.query(*binds)[0][0].to_i
  end

  # Sees if the table is empty
  # @param [Hash/String] Filter conditions
  # @return [boolean]
  def empty? *where
    count(*where) == 0
  end

  # Inserts a record into the table with the given hash
  # @param [Hash] data_hash Column values in Hash
  # @return [Fixnum] Number of affected records
  def insert data_hash = {}
    sql, binds = JDBCHelper::SQLPrepared.insert(name, @query_default.merge(data_hash))
    pstmt = prepare :insert, sql
    pstmt.send @update_method, *binds
  end

  # Inserts a record into the table with the given hash.
  # Skip insertion when duplicate record is found.
  # @note This is not SQL standard. Only works if the database supports insert ignore syntax.
  # @param [Hash] data_hash Column values in Hash
  # @return [Fixnum] Number of affected records
  def insert_ignore data_hash = {}
    sql, binds = JDBCHelper::SQLPrepared.insert_ignore(name, @query_default.merge(data_hash))
    pstmt = prepare :insert, sql
    pstmt.set_fetch_size @fetch_size if @fetch_size
    pstmt.send @update_method, *binds
  end

  # Replaces a record in the table with the new one with the same unique key.
  # @note This is not SQL standard. Only works if the database supports replace syntax.
  # @param [Hash] data_hash Column values in Hash
  # @return [Fixnum] Number of affected records
  def replace data_hash = {}
    sql, binds = JDBCHelper::SQLPrepared.replace(name, @query_default.merge(data_hash))
    pstmt = prepare :insert, sql
    pstmt.send @update_method, *binds
  end

  # Executes update with the given hash.
  # :where element of the hash is taken out to generate where clause of the update SQL.
  # @param [Hash] data_hash_with_where Column values in Hash.
  #   :where element of the given hash can (usually should) point to another Hash representing update filters.
  # @return [Fixnum] Number of affected records
  def update data_hash_with_where = {}
    where_ext  = data_hash_with_where.delete(:where)
    where_ext  = [where_ext] unless where_ext.is_a? Array
    sql, binds = JDBCHelper::SQLPrepared.update(name,
                    @query_default.merge(data_hash_with_where),
                    @query_where + where_ext.compact)
    pstmt = prepare :update, sql
    pstmt.send @update_method, *binds
  end

  # Deletes records matching given condtion
  # @param [List of Hash/String] where Delete filters
  # @return [Fixnum] Number of affected records
  def delete *where
    sql, binds = JDBCHelper::SQLPrepared.delete(name, @query_where + where)
    pstmt = prepare :delete, sql
    pstmt.send @update_method, *binds
  end

  # Empties the table.
  # @note This operation cannot be undone
  # @return [JDBCHelper::TableWrapper] Self.
  def truncate!
    @connection.update(JDBCHelper::SQL.check "truncate table #{name}")
    self
  end
  alias truncate_table! truncate!

  # Drops the table.
  # @note This operation cannot be undone
  # @return [JDBCHelper::TableWrapper] Self.
  def drop!
    @connection.update(JDBCHelper::SQL.check "drop table #{name}")
    self
  end
  alias drop_table! drop!

  # Select SQL wrapper
  include Enumerable

  # Returns a new TableWrapper object which can be used to execute a select
  # statement for the table selecting only the specified fields.
  # If a block is given, executes the select statement and yields each row to the block.
  # @param [*String/*Symbol] fields List of fields to select
  # @return [JDBCHelper::TableWrapper]
  # @since 0.4.0
  def select *fields, &block
    obj = self.dup
    obj.instance_variable_set :@query_select, fields unless fields.empty?
    ret obj, &block
  end

  # Returns a new TableWrapper object which can be used to execute a select
  # statement for the table with the specified filter conditions.
  # If a block is given, executes the select statement and yields each row to the block.
  # @param [List of Hash/String] conditions Filter conditions
  # @return [JDBCHelper::TableWrapper]
  # @since 0.4.0
  def where *conditions, &block
    raise ArgumentError.new("Wrong number of arguments") if conditions.empty?

    obj = self.dup
    obj.instance_variable_set :@query_where, @query_where + conditions
    ret obj, &block
  end

  # Returns a new TableWrapper object which can be used to execute a select
  # statement for the table with the given sorting criteria.
  # If a block is given, executes the select statement and yields each row to the block.
  # @param [*String/*Symbol] criteria Sorting criteria
  # @return [JDBCHelper::TableWrapper]
  # @since 0.4.0
  def order *criteria, &block
    raise ArgumentError.new("Wrong number of arguments") if criteria.empty?
    obj = self.dup
    obj.instance_variable_set :@query_order, criteria
    ret obj, &block
  end

  # Returns a new TableWrapper object with default values, which will be applied to
  # the subsequent inserts and updates.
  # @param [Hash] data_hash Default values
  # @return [JDBCHelper::TableWrapper]
  # @since 0.4.5
  def default data_hash, &block
    raise ArgumentError.new("Hash required") unless data_hash.kind_of? Hash

    obj = self.dup
    obj.instance_variable_set :@query_default, @query_default.merge(data_hash)
    ret obj, &block
  end

  # Returns a new TableWrapper object with the given fetch size.
  # If a block is given, executes the select statement and yields each row to the block.
  # @param [Fixnum] fsz Fetch size
  # @return [JDBCHelper::TableWrapper]
  # @since 0.7.7
  def fetch_size fsz, &block
    obj = self.dup
    obj.instance_variable_set :@fetch_size, fsz
    ret obj, &block
  end

  # Executes a select SQL for the table and returns an Enumerable object,
  # or yields each row if block is given.
  # @return [JDBCHelper::Connection::ResultSetEnumerator]
  # @since 0.4.0
  def each &block
    sql, binds = JDBCHelper::SQLPrepared.select(
        name,
        :select => @query_select,
        :where => @query_where,
        :order => @query_order)
    pstmt = prepare :select, sql
    pstmt.enumerate(*binds, &block)
  end

  # Returns a new TableWrapper object whose subsequent inserts, updates,
  # and deletes are added to batch for JDBC batch-execution. The actual execution
  # is deferred until JDBCHelper::Connection#execute_batch method is called.
  # Self is returned when batch is called more than once.
  # @return [JDBCHelper::TableWrapper]
  # @since 0.4.0
  def batch
    if batch?
      self
    else
      # dup makes @pstmts to be shared
      obj = self.dup
      obj.instance_variable_set :@update_method, :add_batch
      obj
    end
  end

  # Returns if the subsequent updates for this wrapper will be batched
  # @return [Boolean]
  # @since 0.4.0
  def batch?
    @update_method == :add_batch
  end

  # Clear batched operations.
  # @param [*Symbol] types Types of batched operations to clear.
  #   If not given, :insert, :update and :delete.
  # @return [nil]
  def clear_batch *types
    types = [:insert, :update, :delete] if types.empty?
    types.each do |type|
      raise ArgumentError.new("Invalid type: #{type}") unless @pstmts.has_key?(type)
      @pstmts[type].values.each(&:clear_batch)
    end
    nil
  end

  # Execute batched operations.
  # TableWrapper uses multiple PreparedStatements and each of them may have its own homogeneous batched commands.
  # It is thus not possible for TableWrapper to precisely serialize all the commands when interleaved.
  # What you can do here is to specify the types of commands (:insert, :update, and :delete) in the order of execution.
  # The default is to execute deletes first, then updates, and finally inserts.
  # You can also execute a subset of the three types.
  # @param [*Symbol] types Types of batched operations to execute in order.
  #   If not given, :delete, :insert and :update.
  # @return [nil]
  def execute_batch *types
    types = [:delete, :insert, :update] if types.empty?
    types.each do |type|
      raise ArgumentError.new("Invalid type: #{type}") unless @pstmts.has_key?(type)
      @pstmts[type].values.each(&:execute_batch)
    end
    nil
  end

  # Returns the select SQL for this wrapper object
  # @return [String] Select SQL
  # @since 0.4.0
  def sql
    JDBCHelper::SQL.select(
        name,
        :select => @query_select,
        :where => @query_where,
        :order => @query_order)
  end

  def initialize connection, table_name
    super connection, table_name
    @update_method = :update
    @query_default = {}
    @query_where = []
    @query_order = nil
    @query_select = nil
    @pstmts = {
      :select => {},
      :insert => {},
      :delete => {},
      :count => {},
      :update => {}
    }
    @fetch_size = nil
  end

  # Closes the prepared statements
  # @since 0.5.0
  def close
    @pstmts.each do |typ, hash|
      hash.each do |sql, pstmt|
        pstmt.close if pstmt
      end
      @pstmts[typ] = {}
    end
  end

  # @return [Hash] Prepared statements for this wrapper
  # @since 0.5.0
  def prepared_statements
    @pstmts
  end

  def inspect
    {
      :conn => @connection,
      :name => name,
      :sqls => @pstmts.values.map(&:keys).flatten,
      :where => @query_where,
      :default => @query_default,
      :order => @query_order,
      :batch? => batch?
    }.inspect
  end

private
  def prepare type, sql
    sql   = JDBCHelper::SQL.check(sql)
    pstmt = @pstmts[type][sql] ||= @connection.prepare(sql)
    pstmt = @pstmts[type][sql]   = @connection.prepare(sql) if pstmt.closed?
    pstmt
  end

  def ret obj, &block
    if block_given?
      obj.each(&block)
    else
      obj
    end
  end
end#TableWrapper
end#JDBCHelper

