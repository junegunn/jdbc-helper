# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class ObjectWrapper
	# Underlying JDBCHelper::Connection
	attr_reader :connection

	# Object name (or expression)
	attr_reader :name

	def initialize(conn, name)
		@connection = conn
		@name = name.to_s
		JDBCHelper::SQL.check @name
	end
end#ObjectWrapper

class TableWrapper < ObjectWrapper
	# Retrieves the count of the table (or view or any equivalent underlying selectable data collection)
	# @return [Fixnum] Count of the records.
	def count(where = nil)
		@connection.query(JDBCHelper::SQL.count name, where)[0][0]
	end

	# Sees if the collection is empty
	# @return [boolean]
	def empty?
		count == 0
	end

	# Select * with optional conditions
	# @param [Hash/String] where Select filters
	# @return [Array] Array is returned if block is not given
	def select where = nil, &block
		@connection.query(JDBCHelper::SQL.select(name, where), &block)
	end

	# Inserts a record into the collection with the given hash
	# @param [Hash] data_hash Column values in Hash
	# @return [Fixnum] Number of affected records
	def insert data_hash
		@connection.update(JDBCHelper::SQL.insert name, data_hash)
	end

	# Inserts a record into the collection with the given hash.
	# Skip insertion when duplicate record is found.
	# (This is not SQL standard. Only works if the database supports insert ignore syntax)
	# @param [Hash] data_hash Column values in Hash
	# @return [Fixnum] Number of affected records
	def insert_ignore data_hash
		@connection.update(JDBCHelper::SQL.insert_ignore name, data_hash)
	end

	# Replaces a record into the collection.
	# (This is not SQL standard. Only works if the database supports replace syntax)
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

	# Empty the table. Terminates the current transaction. Cannot be undone.
	# @return [Fixnum] executeUpdate return value
	def truncate_table!
		@connection.update(JDBCHelper::SQL.check "truncate table #{name}")
	end
	
	# Drop the table. Terminates the current transaction. Cannot be undone.
	# @return [Fixnum] executeUpdate return value
	def drop_table!
		@connection.update(JDBCHelper::SQL.check "drop table #{name}")
	end
end#TableWrapper

end#JDBCHelper

