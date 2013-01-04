# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
# A wrapper object representing a database procedure.
# @since 0.3.0
# @example Usage
#  conn.procedure(:my_procedure).call(1, ["a", String], Fixnum)
class ProcedureWrapper < ObjectWrapper
  # Returns the name of the procedure
  # @return [String]
  alias to_s name

  # Executes the procedure and returns the values of INOUT & OUT parameters in Hash
  # @return [Hash]
  def call(*args)
    params = build_params args
    cstmt = @connection.prepare_call "{call #{name}(#{Array.new(@cols.length){'?'}.join ', '})}"
    begin
      process_result( args, cstmt.call(*params) )
    ensure
      cstmt.close
    end
  end

  # Reloads procedure metadata. Metadata is cached for performance.
  # However, if you have modified the procedure, you need to reload the
  # metadata with this method.
  # @return [JDBCHelper::ProcedureWrapper]
  def refresh
    @cols = @defaults = nil
    self
  end

private
  def metadata_lookup
    return if @cols

    procedure, package, schema = name.split('.').reverse
    procedure_u, package_u, schema_u = name.upcase.split('.').reverse

    # Alas, metadata lookup can be case-sensitive. e.g. Oracle
    dbmd = @connection.java_obj.get_meta_data
    lookups =
      if schema
        [ lambda { dbmd.getProcedureColumns(package, schema, procedure, nil) },
          lambda { dbmd.getProcedureColumns(package_u, schema_u, procedure_u, nil) } ]
      else
        # schema or catalog? we don't know.
        # - too inefficient for parameter-less procedures
        [ lambda { dbmd.getProcedureColumns(nil, package, procedure, nil) },
          lambda { dbmd.getProcedureColumns(nil, package_u, procedure_u, nil) },
          lambda { dbmd.getProcedureColumns(package, nil, procedure, nil) },
          lambda { dbmd.getProcedureColumns(package_u, nil, procedure_u, nil) } ]
      end

    cols = []
    @defaults = {}
    lookups.each do |ld|
      rset = ld.call
      default_support = rset.get_meta_data.get_column_count >= 14
      while rset.next
        next if rset.getString("COLUMN_TYPE") == java.sql.DatabaseMetaData.procedureColumnReturn

        cols << rset.getString("COLUMN_NAME").upcase
        # TODO/NOT TESTED
        # - MySQL doesn't support default values for procedure parameters
        # - Oracle supports default values, however it does not provide
        #   standard interface to query default values. COLUMN_DEF always returns null.
        #   http://download.oracle.com/docs/cd/E11882_01/appdev.112/e13995/oracle/jdbc/OracleDatabaseMetaData.html#getProcedureColumns_java_lang_String__java_lang_String__java_lang_String__java_lang_String_
        if default_support && (default = rset.getString("COLUMN_DEF"))
          @defaults[cols.length - 1] << default
        end
      end
      unless cols.empty?
        @cols = cols
        break
      end
    end
    @cols ||= []
  end

  def build_params args
    metadata_lookup

    params = []
    # Array
    unless args.first.kind_of?(Hash)
      if args.length < @cols.length
        # Fill the Array with default values
        @cols.length.times do |idx|
          if @defaults[idx]
            params << @defaults[idx]
          else
            params << args[idx]
          end
        end
      else
        params = args
      end

    # Hash
    else
      raise ArgumentError.new("More than one Hash given") if args.length > 1

      # Set to default,
      @defaults.each do |idx, v|
        params[idx] = v
      end

      # then override
      args.first.each do |k, v|
        idx = @cols.index k.to_s.upcase
        params[idx] = v
      end
    end

    return params
  end

  def process_result args, result
    input = args.first
    return result unless input.kind_of? Hash

    final = {}
    result.each do |idx, ret|
      key = input.keys.find { |key| key.to_s.upcase == @cols[idx - 1] }
      final[key] = ret
    end
    final
  end
end#ProcedureWrapper
end#JDBCHelper

