module CassandraMapper::Persistence
  def save(with_validation = true)
    uniq_key = self.key
    raise CassandraMapper::UndefinedKeyException if uniq_key.nil?
    connection.insert(self.class.column_family, uniq_key, to_simple(:defined => true))
    self
  end
end
