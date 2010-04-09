module CassandraMapper
  class Exception < ::Exception
  end
  class InvalidArgumentException < Exception
  end
  class RecordNotFoundException < Exception
  end
  class UndefinedKeyException < Exception
  end
end
