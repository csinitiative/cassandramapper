module CassandraMapper::Connection
  def connection
    self.class.connection
  end
end
