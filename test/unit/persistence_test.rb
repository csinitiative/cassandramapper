require 'test_helper'

class PersistenceTest < Test::Unit::TestCase
  context 'CassandraMapper::Base' do
    setup do
      @column_family = 'TestColumnFamily'
      @class = Class.new(CassandraMapper::Base) do
        [:a, :b, :c].each {|name| maps name}
        def key; self.a; end
      end
      @connection = stub('cassandra_client')
      @class.stubs(:connection).returns(@connection)
      @class.stubs(:column_family).returns(@column_family)
    end

    context 'when saving an instance' do
      setup do
        @values = {:a => 'Aa', :b => 'Bb', :c => 'Cc'}
        @instance = @class.new(@values)
      end

      should 'pass defined attributes to thrift for a new object' do
        @class.connection.expects(:insert).with(@column_family, @values[:a], @values).returns(true)
        assert @instance.save
      end

      should 'not pass undefined attributes to thrift for a new object' do
        @values.delete :b
        @instance.b = nil
        @class.connection.expects(:insert).with(@column_family, @values[:a], @values).returns(true)

        assert @instance.save
      end

      should 'throw an UndefinedKey exception if key attribute is empty' do
        @instance.a = nil
        assert_raise(CassandraMapper::UndefinedKeyException) { @instance.save }
      end
    end
  end
end

