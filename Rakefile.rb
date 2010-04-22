require 'rake'
require 'rake/testtask'
require 'rake/clean'

CLOBBER.include('cassandra_mapper-*.gem')

Rake::TestTask.new do |t|
  t.pattern = 'test/**/*_test.rb'
  t.libs = ['test', 'lib']
end

