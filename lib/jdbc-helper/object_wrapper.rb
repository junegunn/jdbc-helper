# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class ObjectWrapper
	# Underlying JDBCHelper::Connection
	attr_reader :connection

	# Retrieves the count of the table (or view or any equivalent underlying selectable data collection)
	def count(where = nil)
		@connection.query(JDBCHelper::SQL.count table_name, where)[0][0]
	end

	# Sees if the collection is empty
	def empty?
		count == 0
	end

	# Select * with optional conditions
	def select where = nil, &block
		@connection.query(JDBCHelper::SQL.select(table_name, where), &block)
	end

	# Inserts a record into the collection with the given hash
	def insert data_hash
		@connection.update(JDBCHelper::SQL.insert table_name, data_hash)
	end

	# Inserts a record into the collection with the given hash.
	# Skip insertion when duplicate record is found.
	# (This is not SQL standard. Only works if the database supports insert ignore syntax)
	def insert_ignore data_hash
		@connection.update(JDBCHelper::SQL.insert_ignore table_name, data_hash)
	end

	# Replaces a record into the collection.
	# (This is not SQL standard. Only works if the database supports replace syntax)
	def replace data_hash
		@connection.update(JDBCHelper::SQL.replace table_name, data_hash)
	end

	# Executes update with the given hash.
	# :where element of the hash is taken out to generate where clause of the update SQL.
	def update data_hash_with_where
		@connection.update(JDBCHelper::SQL.update table_name, data_hash_with_where)
	end

	# Deletes records matching given condtion
	def delete where = nil
		@connection.update(JDBCHelper::SQL.delete table_name, where)
	end

	# Empty the table. Terminates the current transaction. Cannot be undone.
	def truncate_table!
		@connection.update(JDBCHelper::SQL.check "truncate table #{table_name}")
	end
	
	# Drop the table. Terminates the current transaction. Cannot be undone.
	def drop_table!
		@connection.update(JDBCHelper::SQL.check "drop table #{table_name}")
	end

	# Methods for functions
	# =====================
	# TODO TBD

	# Methods for procedures
	# ======================
	# TODO TBD

private
	def initialize(conn, stack = [])
		@connection = conn
		@stack = stack
	end

	def method_missing(symb, *args)
		# Arguments should not be given at all
		raise NoMethodError.new("undefined method `#{symb}'") if 
				args.length > 0 || @stack.length >= 2

		return ObjectWrapper.new(@conn, @stack + [symb])
	end

	def table_name
		@stack.join('.')
	end
end#JDBCHelper
end#ObjectWrapper

