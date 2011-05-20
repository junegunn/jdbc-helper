# encoding: UTF-8
# Junegunn Choi (junegunn.c@gmail.com)

module JDBCHelper
module SQL
	# Prevents a string from being quoted
	def self.expr str
		Expr.new str
	end
	
	# Returns NotNilClass singleton object
	def self.not_nil
		NotNilClass.singleton
	end

	# Formats the given data so that it can be injected into SQL
	def self.value data
		case data
		when NilClass
			'null'
		when Fixnum, Bignum, Float
			data
		when JDBCHelper::SQL::Expr
			data.to_s
		when String
			"'#{esc data}'"
		else
			raise NotImplementedError.new("Unsupported datatype: #{data.class}")
		end
	end

	# Generates SQL where cluase with the given conditions.
	# Parameter can be either Hash of String.
	def self.where conds
		where_clause = where_internal conds
		where_clause.empty? ? where_clause : check(where_internal conds)
	end

	# Generates SQL order by cluase with the given conditions.
	def self.order_by criteria
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
	def self.update table, data_hash
		where_clause = where_internal(data_hash.delete :where)
		updates = data_hash.map { |k, v| "#{k} = #{value v}" }.join(', ')
		check "update #{table} set #{updates} #{where_clause}".strip
	end

	# Generates select * SQL with the given conditions
	def self.select table, conds = nil
		check "select * from #{table} #{where_internal conds}".strip
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

private
	def self.esc str
		str.gsub("'", "''")
	end

	# No check
	def self.where_internal conds
		return '' if conds.nil?

		where = 
			case conds
			when String
				conds.strip
			when Hash
				conds.map { |k, v|
					"#{k} " +
						case v
						when NilClass
							"is null"
						when NotNilClass
							"is not null"
						when Fixnum, Bignum, Float, JDBCHelper::SQL::Expr
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
		where.empty? ? '' : 'where ' + where
	end

	def self.insert_internal cmd, table, data_hash
		cols = data_hash.keys
		check "#{cmd} into #{table} (#{cols.join ', '}) values (#{cols.map{|c|value data_hash[c]}.join ', '})"
	end

	class Expr
		def initialize str
			@expr = str
		end
		
		def to_s
			@expr
		end
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

