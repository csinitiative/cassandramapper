module CassandraMapper::Persistence
  def save(with_validation = true)
    uniq_key = self.key
    raise CassandraMapper::UndefinedKeyException if uniq_key.nil?
    connection.insert(self.class.column_family, uniq_key, to_simple(:defined => true))
    self
  end

  module ClassMethods
    def find(*keys)
      single = false
      case keys.first
        when Array
          keys = keys.first
        when nil
          raise CassandraMapper::InvalidArgumentException
        else
          single = true if keys.length == 1
      end
      result = connection.multi_get(column_family, keys).values.collect {|hash| new(hash)}
      raise CassandraMapper::RecordNotFoundException unless result.size == keys.size
      single ? result.first : result
    end
  end

  def self.included(klass)
    klass.extend ClassMethods
  end
end
