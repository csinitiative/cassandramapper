module CassandraMapper::Persistence
  def save(with_validation = true)
    uniq_key = self.key
    raise CassandraMapper::UndefinedKeyException if uniq_key.nil?
    options = {}
    if new_record?
      options[:defined] = true
    else
      return false unless changed_attributes.length > 0
      options[:changed] = true
    end
    connection.insert(self.class.column_family, uniq_key, to_simple(options))
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
      result = connection.multi_get(column_family, keys).values.collect do |hash|
        obj = new(hash)
        obj.new_record = false
        obj
      end
      raise CassandraMapper::RecordNotFoundException unless result.size == keys.size
      single ? result.first : result
    end
  end

  def self.included(klass)
    klass.extend ClassMethods
  end
end
