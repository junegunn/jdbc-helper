# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# A wrapper object representing a database table. Allows you to perform table operations easily.
# @since 0.2.0
# @example Usage
#  # For more complex examples, refer to test/test_object_wrapper.rb
#
#  conn.table('test.data').count
#  conn.table('test.data').empty?
#  conn.table('test.data').select(:c => 3) do |row|
#    puts row.a
#  end
#  conn.table('test.data').update(:a => 1, :b => 2, :where => { :c => 3 })
#  conn.table('test.data').insert(:a => 10, :b => 20, :c => 30)
#  conn.table('test.data').insert_ignore(:a => 10, :b => 20, :c => 30)
#  conn.table('test.data').insert_replace(:a => 10, :b => 20, :c => 30)
#  conn.table('test.data').delete(:c => 3)
#  conn.table('test.data').truncate_table!
#  conn.table('test.data').drop_table!
class TableWrapper < ObjectWrapper
	# Returns the name of the table
	# @return [String]
	alias to_s name

	# Retrieves the count of the table
	# @return [Fixnum] Count of the records.
	def count(where = nil)
		@connection.query(JDBCHelper::SQL.count name, where)[0][0].to_i
	end

	# Sees if the table is empty
	# @return [boolean]
	def empty?
		count == 0
	end

	# Select * with optional conditions
	# @param [Hash/String] where Select filters
	# @return [Array] Array is returned if block is not given
	# @yield [JDBCHelper::Connection::Row]
	def select where = nil, &block
		@connection.query(JDBCHelper::SQL.select(name, where), &block)
	end

	# Inserts a record into the table with the given hash
	# @param [Hash] data_hash Column values in Hash
	# @return [Fixnum] Number of affected records
	def insert data_hash
		@connection.update(JDBCHelper::SQL.insert name, data_hash)
	end

	# Inserts a record into the table with the given hash.
	# Skip insertion when duplicate record is found.
	# @note This is not SQL standard. Only works if the database supports insert ignore syntax.
	# @param [Hash] data_hash Column values in Hash
	# @return [Fixnum] Number of affected records
	def insert_ignore data_hash
		@connection.update(JDBCHelper::SQL.insert_ignore name, data_hash)
	end

	# Replaces a record in the table with the new one with the same unique key.
	# @note This is not SQL standard. Only works if the database supports replace syntax.
	# @param [Hash] data_hash Column values in Hash
	# @return [Fixnum] Number of affected records
	def replace data_hash
		@connection.update(JDBCHelper::SQL.replace name, data_hash)
	end

	# Executes update with the given hash.
	# :where element of the hash is taken out to generate where clause of the update SQL.
	# @param [Hash] data_hash_with_where Column values in Hash.
	#   :where element of the given hash can (usually should) point to another Hash representing update filters.
	# @return [Fixnum] Number of affected records
	def update data_hash_with_where
		@connection.update(JDBCHelper::SQL.update name, data_hash_with_where)
	end

	# Deletes records matching given condtion
	# @param [Hash] where Delete filters
	# @return [Fixnum] Number of affected records
	def delete where = nil
		@connection.update(JDBCHelper::SQL.delete name, where)
	end

	# Empties the table.
	# @note This operation cannot be undone
	# @return [Fixnum] executeUpdate return value
	def truncate_table!
		@connection.update(JDBCHelper::SQL.check "truncate table #{name}")
	end
	
	# Drops the table.
	# @note This operation cannot be undone
	# @return [Fixnum] executeUpdate return value
	def drop_table!
		@connection.update(JDBCHelper::SQL.check "drop table #{name}")
	end
end#TableWrapper
end#JDBCHelper

