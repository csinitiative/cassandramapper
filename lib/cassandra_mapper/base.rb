require 'simple_mapper'
class CassandraMapper::Base
  include SimpleMapper::Attributes

  require 'cassandra_mapper/identity'
  include CassandraMapper::Identity

  require 'cassandra_mapper/persistence'
  include CassandraMapper::Persistence

  require 'cassandra_mapper/connection'
  include CassandraMapper::Connection
end
