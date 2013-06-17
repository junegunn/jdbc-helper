# -*- encoding: utf-8 -*-
require File.expand_path('../lib/jdbc-helper/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Junegunn Choi"]
  gem.email         = ["junegunn.c@gmail.com"]
  gem.description   = %q{A JDBC helper for JRuby/Database developers.}
  gem.summary       = %q{A JDBC helper for JRuby/Database developers.}
  gem.homepage      = "https://github.com/junegunn/jdbc-helper"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "jdbc-helper"
  gem.require_paths = ["lib"]
  gem.version       = JDBCHelper::VERSION

  gem.add_runtime_dependency 'insensitive_hash', '>= 0.2.4'
  gem.add_runtime_dependency 'sql_helper', '~> 0.1.1'
  gem.add_development_dependency "bundler"
  gem.add_development_dependency "simplecov"
  gem.add_development_dependency "test-unit"
end
