# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# The result of database queries.
# Designed to be very flexible on its interface.
# You can access it like an array, a hash or an ORM object.
#
# e.g.
#
#   conn.query('SELECT a, b, c FROM t') do | row |
#       puts row.a
#       puts row[1]
#       puts row['c']
#
#       row.each do | value |
#           # ...
#       end
#
#       labels = row.labels   # equal to `row.keys`
#       values = row.to_a     # equal to `row.values`
#
#       puts row.rownum
#   end
#
class Connection
class Row
	attr_reader :labels, :values, :rownum
	alias_method :keys, :labels

	include Enumerable

	def [](idx)
		if idx.is_a? Fixnum
			raise RangeError.new("Index out of bound") if idx >= @values.length
			@values[idx]
		else
			# case-insensitive, assuming no duplicate labels
			vidx = @labels_d.index(idx.downcase) or
				raise NameError.new("Unknown column label: #{idx}")
			@values[vidx]
		end
	end

	def each(&blk)
		@values.each { | v | yield v }

		# @labels.each_with_index do | label, idx |
		# 	yield label, @values[idx]
		# end
	end

	def inspect
		strs = []
		@labels.each do | col |
			strs << "#{col}: #{self[col] || '(null)'}"
		end
		'[' + strs.join(', ') + ']'
	end

	def to_s
		@values.to_s
	end

	def to_a
		@values
	end

	def join(sep = $,)
		to_a.join(sep)
	end

	def eql?(other)
		self.hash == other.hash
	end

	def hash # :nodoc:
		@labels.zip(@values).sort.hash
	end

	alias :== :eql?

private
	def initialize(col_labels, values, rownum) # :nodoc:
		@labels = col_labels
		@labels_d = col_labels.map { | l | l.downcase }
		@values = values
		@rownum = rownum

		# @labels_d.each do | l |
		# 	(class << self; self; end).instance_eval do
		# 		define_method l do
		# 			self[l]
		# 		end
		# 	end
		# end
	end

	# Performs better than defining methods
	def method_missing(symb, *args)
		if vidx = @labels_d.index(symb.to_s.downcase)
			begin
				@values[vidx]
			rescue NameError
				raise NoMethodError.new("undefined method or attribute `#{symb}'")
			end
		elsif @values.respond_to?(symb)
			@values.send(symb, *args)
		else
			raise NoMethodError.new("undefined method or attribute `#{symb}'")
		end
	end

	# Remove dangerous default methods
	# excluding :object_id, :hash
	[:id, :tap, :gem, :display, :class, :method, :methods, :trust].
	select { | s | method_defined? s }.each do | m |
		undef_method m
	end
end#Row
end#Connection
end#JDBCHelper

