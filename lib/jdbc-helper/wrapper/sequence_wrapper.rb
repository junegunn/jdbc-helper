# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# A wrapper object representing a database sequence.
# @since 0.4.2
# @example Usage
#  conn.sequence(:my_seq).create!(1)
#  conn.sequence(:my_seq).nextval
#  conn.sequence(:my_seq).currval
#  conn.sequence(:my_seq).reset!(1)
#  conn.sequence(:my_seq).drop!
class SequenceWrapper < ObjectWrapper
	# Returns the name of the sequence
	# @return [String]
	alias to_s name

	# Increments the sequence and returns the value
	# @return [Fixnum]
	def nextval
		@connection.query("select #{name}.nextval from dual")[0][0].to_i
	end

	# Returns the incremented value of the sequence
	# @return [Fixnum]
	def currval
		@connection.query("select #{name}.currval from dual")[0][0].to_i
	end

	# Recreates the sequence. Cannot be undone.
	# @param [Fixnum] value Initial value
	# @param [Fixnum] increment_by Increment size for a single nextval call
	# @return [JDBCHelper::SequenceWrapper] Self.
	def reset! value = 1, increment_by = 1
		drop! rescue nil
		create! value, increment_by
		self
	end

	# Drops the sequence. Cannot be undone.
	# @return [JDBCHelper::SequenceWrapper] Self.
	def drop!
		@connection.update("drop sequence #{name}")
		self
	end

	# Creates the sequence. Cannot be undone.
	# @param [Fixnum] value Initial value
	# @param [Fixnum] increment_by Increment size for a single nextval call
	# @return [JDBCHelper::SequenceWrapper] Self.
	def create! value = 1, increment_by = 1
		create = JDBCHelper::SQL.check(
				"create sequence #{name} start with #{value} increment by #{increment_by}")
		@connection.update(create)
		self
	end
end#SequenceWrapper
end#JDBCHelper


