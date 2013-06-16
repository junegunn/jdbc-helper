# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

require 'bigdecimal'

module JDBCHelper
class Connection
# Class for enumerating query results.
# Automatically closed after used. When not used, you must close it explicitly by calling "close".
class ResultSetEnumerator
  include Enumerable

  def each
    return if closed?

    count = -1
    begin
      while @rset.next
        idx = 0
        # Oracle returns numbers in NUMERIC type, which can be of any precision.
        # So, we retrieve the numbers in String type not to lose their precision.
        # This can be quite annoying when you're just working with integers,
        # so I tried the following code to automatically convert integer string into integer
        # when it's obvious. However, the performance drop is untolerable.
        # Thus, commented out.
        #
        # if v && @cols_meta[i-1] == java.sql.Types::NUMERIC && v !~ /[\.e]/i
        #   v.to_i
        # else
        #   v
        # end
        yield Connection::Row.new(
            @col_labels,
            @col_labels_d,
            @getters.map { |gt|
              case gt
              when :getBigNum
                v = @rset.getBigDecimal idx+=1
                @rset.was_null ? nil : v.toPlainString.to_i
              when :getBigDecimal
                v = @rset.getBigDecimal idx+=1
                @rset.was_null ? nil : BigDecimal.new(v.toPlainString)
              else
                v = @rset.send gt, idx+=1
                @rset.was_null ? nil : v
              end
            }, count += 1)
      end
    ensure
      close
    end
  end

  def close
    return if closed?

    @rset.close
    @close_callback.call if @close_callback
    @closed = true
  end

  def closed?
    @closed
  end

private
  def initialize(rset, &close_callback) # :nodoc:
    unless rset.respond_to? :get_meta_data
      rset.close if rset
      @closed = true
      return
    end

    @close_callback = close_callback
    @rset           = rset
    @rsmd           = @rset.get_meta_data
    @num_col        = @rsmd.get_column_count
    @getters        = []
    @col_labels     = []
    @col_labels_d   = []
    (1..@num_col).each do | i |
      type = @rsmd.get_column_type(i)

      @getters <<
        case type
        when java.sql.Types::NUMERIC, java.sql.Types::DECIMAL
          precision = @rsmd.get_precision(i)
          scale = @rsmd.get_scale(i)

          if precision > 0 && scale >= 0
            # Numbers with fractional parts
            if scale > 0
              :getBigDecimal
            # Numbers without fractional parts
            else
              if precision <= 9
                :getInt
              elsif precision <= 18
                :getLong
              else
                :getBigNum
              end
            end
          # No guarantee
          else
            :getBigDecimal
          end
        else
          JDBCHelper::Connection::GETTER_MAP.fetch(type, :get_string)
        end

      label = @rsmd.get_column_label(i)
      @col_labels << label
      @col_labels_d << label.downcase

    end

    @closed = false
  end
end#ResultSetEnumerator
end#Connection
end#JDBCHelper

