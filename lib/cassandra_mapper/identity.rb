module CassandraMapper::Identity
  module ClassMethods
    def key(attribute = nil)
      @cassandra_mapper_key = attribute if attribute
      @cassandra_mapper_key
    end
  end

  def self.included(klass)
    klass.extend ClassMethods
  end

  def key
    read_attribute(self.class.key)
  end
end
