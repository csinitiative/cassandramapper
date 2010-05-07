module CassandraMapper::Connection
  def connection=(conn)
    @connection = conn
  end

  def connection
    @connection || self.class.connection
  end
end
