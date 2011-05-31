# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper

# Generate SQL snippet, prevents the string from being quoted.
# @param [String] SQL snippet
# @return [JDBCHelper::SQL]
def self.SQL str
	JDBCHelper::SQL.new str
end

# Class representing an SQL snippet. Also has many SQL generator class methods.
class SQL
	# Returns NotNilClass singleton object
	# @return [JDBCHelper::SQL::NotNilClass]
	def self.not_nil
		NotNilClass.singleton
	end
	class << self
		alias not_null not_nil
	end

	# Formats the given data so that it can be injected into SQL
	def self.value data
		case data
		when NilClass
			'null'
		when Fixnum, Bignum, Float
			data
		when JDBCHelper::SQL
			data.to_s
		when String
			"'#{esc data}'"
		else
			raise NotImplementedError.new("Unsupported datatype: #{data.class}")
		end
	end

	# Generates SQL where cluase with the given conditions.
	# Parameter can be either Hash of String.
	def self.where *conds
		where_clause = where_internal conds
		where_clause.empty? ? where_clause : check(where_clause)
	end

	# Generates SQL order by cluase with the given conditions.
	def self.order *criteria
		str = criteria.map(&:to_s).reject(&:empty?).join(', ')
		str.empty? ? str : check('order by ' + str)
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
		where_clause = where_internal where
		updates = data_hash.map { |k, v| "#{k} = #{value v}" }.join(', ')
		check "update #{table} set #{updates} #{where_clause}".strip
	end

	# Generates select SQL with the given conditions
	def self.select table, opts = {}
		opts = opts.reject { |k, v| v.nil? }
		check [
			"select #{opts.fetch(:select, ['*']).join(', ')} from #{table}", 
			where_internal(opts.fetch(:where, {})),
			order(opts.fetch(:order, []).join(', '))
		].reject(&:empty?).join(' ')
	end

	# Generates count SQL with the given conditions
	def self.count table, conds = nil
		check "select count(*) from #{table} #{where_internal conds}".strip
	end

	# Generates delete SQL with the given conditions
	def self.delete table, conds = nil
		check "delete from #{table} #{where_internal conds}".strip
	end

	# FIXME: Naive protection for SQL Injection
	# TODO: check caching?
	def self.check expr, is_name = false
		return nil if expr.nil?

		tag = is_name ? 'Object name' : 'Expression'
		test = expr.gsub(/'[^']*'/, '').gsub(/`[^`]*`/, '').gsub(/"[^"]*"/, '').strip
		raise ArgumentError.new("#{tag} cannot contain (unquoted) semi-colons: #{expr}") if test.include?(';')
		raise ArgumentError.new("#{tag} cannot contain (unquoted) comments: #{expr}") if test.match(%r{--|/\*|\*/})
		raise ArgumentError.new("Unclosed quotation mark: #{expr}") if test.match(/['"`]/)
		raise ArgumentError.new("#{tag} is blank") if test.empty?

		if is_name
			raise ArgumentError.new(
				"#{tag} cannot contain (unquoted) parentheses: #{expr}") if test.match(%r{\(|\)})
		end

		return expr
	end

	def to_s
		@expr
	end

private
	def self.esc str
		str.gsub("'", "''")
	end

	# No check
	def self.where_internal conds
		conds = [conds] unless conds.is_a? Array
		where_clause = conds.compact.map { |cond| where_unit cond }.reject(&:empty?).join(' and ')
		where_clause.empty? ? '' : 'where ' + where_clause
	end

	def self.where_unit conds
		case conds
		when String
			conds = conds.strip
			conds.empty? ? '' : "(#{conds})"
		when Hash
			conds.map { |k, v|
				"#{k} " +
					case v
					when NilClass
						"is null"
					when NotNilClass
						"is not null"
					when Fixnum, Bignum, Float, JDBCHelper::SQL
						"= #{v}"
					when Range
						">= #{v.first} and #{k} <#{'=' unless v.exclude_end?} #{v.last}"
					when Array
						"in (" + 
						v.map { |e|
							case e
							when String
								"'#{esc e}'"
							else
								e
							end }.join(', ') + ")"
					when String
						"= '#{esc v}'"
					else
						raise NotImplementedError.new("Unsupported class: #{v.class}")
					end
			}.join(' and ')
		else
			raise NotImplementedError.new("Parameter to where must be either Hash or String")
		end
	end

	def self.insert_internal cmd, table, data_hash
		cols = data_hash.keys
		check "#{cmd} into #{table} (#{cols.join ', '}) values (#{cols.map{|c|value data_hash[c]}.join ', '})"
	end

	def initialize str
		@expr = JDBCHelper::SQL.check str
	end
	
	# Class to represent "(IS) NOT NULL" expression in SQL
	class NotNilClass
		# Returns the singleton object of NotNilClass
		# @return [NotNilClass]
		def self.singleton
			@@singleton ||= NotNilClass.new
		end
	end
end#SQL
end#JDBCHelper

