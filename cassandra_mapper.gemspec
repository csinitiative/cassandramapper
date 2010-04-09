Gem::Specification.new do |s|
  s.name    = 'cassandra_mapper'
  s.version = '0.0.1'
  s.email   = 'ethan@endpoint.com'
  s.author = 'Ethan Rowe'

  s.description = %q{Provides class-building functionality and ORM-like behaviors for working with Cassandra, based on the SimpleMapper project.}
  s.summary     = %q{Build classes for working with Cassandra data structures, but in a manner less specifically like an ORM and more idiomatic to Cassandra's column family schema}

  s.add_dependency('rake')
  s.add_dependency('shoulda')
  s.add_dependency('mocha')
  s.add_dependency('cassandra')
  s.add_dependency('simple_mapper')

  s.files = Dir['lib/**/*'] + Dir['/test/**/*']
  s.require_path = 'lib'
end
