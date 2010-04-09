module CassandraMapper::Persistence
  def save(with_validation = true)
    connection.insert(self.class.column_family, key, to_simple(:defined => true))
  end
end
