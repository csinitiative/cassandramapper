module CassandraMapper::Identity
  module ClassMethods
    def key(attribute = nil)
      @cassandra_mapper_key = attribute if attribute
      @cassandra_mapper_key ||= default_key_name
    end

    def default_key_name
      :key
    end
  end

  def self.included(klass)
    klass.extend ClassMethods
  end

  def key
    read_attribute(self.class.key)
  end

  def new_record=(flag)
    @cassandra_mapper_new_record = (flag && true) || false
  end

  def new_record?
    @cassandra_mapper_new_record = true unless defined? @cassandra_mapper_new_record
    @cassandra_mapper_new_record
  end
end
