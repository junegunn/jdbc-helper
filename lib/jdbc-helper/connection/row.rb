# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
class Connection
# The result of database queries.
# Designed to be very flexible on its interface.
# You can access it like an array, a hash or an ORM object.
#
# @example
#   conn.query('SELECT a, b, c FROM t') do | row |
#       puts row.a
#       puts row[1]
#       puts row['c']
#       puts row[:a]
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
class Row
  # @return [Array] Labels of the columns
  attr_reader :labels
  # @return [Array] Values in Array
  attr_reader :values
  # @return [Fixnum] Sequential row number assigned within the scope of the query
  attr_reader :rownum
  alias_method :keys, :labels

  include Enumerable

  # @overload [](idx)
  #   @param [Fixnum/String/Symbol/Range] idx Access index
  #   @return [Object]
  # @overload [](offset, len)
  #   @param [Fixnum] offset Start offset
  #   @param [Fixnum] len Length of Array
  #   @return [Array]
  def [](*idx)
    return @values[*idx] if idx.length > 1

    idx = idx.first
    case idx
    when Fixnum
      raise RangeError.new("Index out of bound") if idx >= @values.length
      @values[idx]
    when String, Symbol
      # case-insensitive, assuming no duplicate labels
      vidx = @labels_d.index(idx.to_s.downcase) or
        raise NameError.new("Unknown column label: #{idx}")
      @values[vidx]
    else
      # See how it goes...
      @values[idx]
    end
  end

  # @yield [Object]
  def each(&blk)
    @values.each { | v | yield v }

    # @labels.each_with_index do | label, idx |
    #   yield label, @values[idx]
    # end
  end

  # @return [String]
  def inspect
    strs = []
    @labels.each do | col |
      strs << "#{col}: #{self[col] || '(null)'}"
    end
    '[' + strs.join(', ') + ']'
  end

  # @return [Array]
  def to_a
    @values
  end

  # @return [String]
  def join(sep = $,)
    to_a.join(sep)
  end

  # @return [Boolean]
  def eql?(other)
    self.hash == other.hash
  end

  def hash # :nodoc:
    @labels.zip(@values).sort.hash
  end

  alias :== :eql?

private
  def initialize(col_labels, col_labels_d, values, rownum) # :nodoc:
    @labels   = col_labels
    @labels_d = col_labels_d
    @values   = values
    @rownum   = rownum

    # @labels_d.each do | l |
    #   (class << self; self; end).instance_eval do
    #     define_method l do
    #       self[l]
    #     end
    #   end
    # end
  end

  # Performs better than defining methods
  def method_missing(symb, *args)
    if vidx = @labels_d.index(symb.to_s.downcase)
      @values[vidx]
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

