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
        # The nil result from Cassandra/Thrift is somewhat uninspiring.
        @class.connection.expects(:insert).with(@column_family, @values[:a], @values).returns(nil)
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

    context 'retrieving via :find' do
      setup do
        @instances = [:A, :B, :C].inject({}) do |hash, name|
          val = name.to_s
          hash[name.to_s] = @class.new(:a => val, :b => 'b' + val, :c => 'c' + val)
          hash
        end
      end

      should 'use multi_get to return a single item for a single found key' do
        key = @instances.keys.first.to_s
        @connection.expects(:multi_get).with(@column_family, [key]).returns({key => @instances[key].to_simple})
        assert_equal @instances[key].to_simple, @class.find(key).to_simple
      end

      should 'mark :new_record? as false on single item returned' do
        key = @instances.keys.first.to_s
        @connection.stubs(:multi_get).with(@column_family, [key]).returns({key => @instances[key].to_simple})
        assert_equal false, @class.find(key).new_record?
      end

      should 'use multi_get to return a single item in an array for a single-item key list' do
        key = @instances.keys.first.to_s
        @connection.expects(:multi_get).with(@column_family, [key]).returns({key => @instances[key].to_simple})
        assert_equal [@instances[key].to_simple], @class.find([key]).collect {|x| x.to_simple}
      end

      should 'mark :new_record? as false on the single item returned' do
        key = @instances.keys.first.to_s
        @connection.stubs(:multi_get).with(@column_family, [key]).returns({key => @instances[key].to_simple})
        assert_equal [false], @class.find([key]).collect {|x| x.new_record?}
      end

      should 'use multi_get to return an array of items for a multi-key list' do
        keys = @instances.keys.sort
        client_result = @instances.inject({}) {|h, pair| h[pair[0]] = pair[1].to_simple; h}
        @connection.expects(:multi_get).with(@column_family, keys).returns(client_result)
        result = (@class.find(keys) || []).sort_by {|x| x.key}
        assert_equal @instances.values_at(*keys).collect {|x| x.to_simple}, result.collect {|y| y.to_simple}
      end

      should 'mark :new_record? false on all items returned' do
        keys = @instances.keys
        client_result = @instances.inject({}) {|h, pair| h[pair[0]] = pair[1].to_simple; h}
        @connection.stubs(:multi_get).with(@column_family, keys).returns(client_result)
        assert_equal(keys.collect {|x| false}, @class.find(keys).collect {|y| y.new_record?})
      end

      should 'throw a RecordNotFound exception if a key cannot be found' do
        source_keys = @instances.keys
        keys = source_keys.clone
        keys.pop
        client_result = @instances.values_at(*keys).inject({}) {|h, i| h[i.key] = i.to_simple; h}
        @connection.expects(:multi_get).with(@column_family, source_keys).returns(client_result)
        assert_raises(CassandraMapper::RecordNotFoundException) { @class.find(source_keys) }
      end

      should 'throw an InvalidArgument exception if the key list is empty' do
        assert_raises(CassandraMapper::InvalidArgumentException) { @class.find() }
      end
    end
  end
end

