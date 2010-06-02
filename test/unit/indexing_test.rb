require 'test_helper'
class IndexingTest < Test::Unit::TestCase
  context 'A CassandraMapper::Index' do
    setup do
      @class = CassandraMapper::Index
    end

    context 'with defaults' do
      setup do
        @instance = @class.new
      end

      should 'have a nil :indexed_class attribute' do
        assert_equal nil, @instance.indexed_class
      end

      should 'have a nil :source attribute' do
        assert_equal nil, @instance.source
      end

      should 'have a nil :column_family attribute' do
        assert_equal nil, @instance.column_family
      end

      should 'have :key for :indexed_identifier' do
        assert_equal :key, @instance.indexed_identifier
      end

      should 'have nil for :name attribute' do
        assert_equal nil, @instance.name
      end
    end

    context 'with constructor options' do
      setup do
        @options = {:column_family      => :SomeIndex,
                    :indexed_class      => Class.new,
                    :source             => :to_be_indexed,
                    :indexed_identifier => :index_column,
                    :name               => :some_index_name}
        @instance = @class.new(@options)
      end

      should 'get :indexed_class attribute' do
        assert_equal @options[:indexed_class], @instance.indexed_class
      end

      should 'get :source attribute' do
        assert_equal @options[:source], @instance.source
      end

      should 'get :column_family attribute' do
        assert_equal @options[:column_family], @instance.column_family
      end

      should 'get :indexed_identifier attribute' do
        assert_equal @options[:indexed_identifier], @instance.indexed_identifier
      end

      should 'get :name attribute' do
        assert_equal @options[:name], @instance.name
      end
    end

    should 'retrieve :source_for(instance) from instance :source' do
      instance = @class.new(:source => :some_source)
      object = mock('object to be indexed')
      object.expects(:some_source).with.returns('my source')
      assert_equal 'my source', instance.source_for(object)
    end

    should 'retrieve :indexed_identifier_for(instance) from instance :indexed_identifier' do
      instance = @class.new(:indexed_identifier => :some_identifier)
      object = mock('object to be indexed')
      object.expects(:some_identifier).with.returns('my identifier')
      assert_equal 'my identifier', instance.indexed_identifier_for(object)
    end

    context 'indexing method' do
      setup do
        @name = :this_index_name
        @column_family = stub('column_family')
        @source = :index_source
        @indexed_class = Class.new(CassandraMapper::Base)
        @target_object = @indexed_class.new
        @indexed_identifier = :index_identifier
        @instance = @class.new(:name               => @name,
                               :indexed_class      => @indexed_class,
                               :column_family      => @column_family,
                               :source             => @source,
                               :indexed_identifier => @indexed_identifier)
        @client = stub('cassandra_client')
        @target_object.stubs(:connection).returns(@client)
        @target_object.stubs(:key).with.returns(@object_key = 'some_object_identifier')
        @target_object.stubs(@indexed_identifier).with.returns(@object_identifier = 'some_object_sortable_identifier')
        @target_object_state = stub('target_object_index_state')
        @target_object.stubs(@name).returns(@target_object_state)
      end

      context ':create' do
        should 'insert the target source as key to the column family, with key as value' do
          @instance.expects(:source_for).with(@target_object).returns(source_value = 'some_source_value')
          @client.expects(:insert).with(@column_family, source_value, {@object_identifier => @object_key})
          @target_object_state.expects(:source_value=).with(source_value)
          @target_object_state.expects(:identifier_value=).with(@object_identifier)
          @instance.create(@target_object)
        end

        should 'do nothing if the current index source is nil' do
          @instance.expects(:source_for).with(@target_object).returns(nil)
          @client.expects(:insert).never
          @instance.create(@target_object)
        end
      end

      context ':remove' do
        should "remove the target object's old identifier value from the row at the old source" do
          # use sequences to work around naming conflicts
          @target_object_state.stubs(:source_value).with.returns(source_value = 'some_old_source_value')
          @target_object_state.stubs(:identifier_value).with.returns(identifier_value = 'some_old_identifier_value')
          @target_object_state.expects(:source_value=).with(nil)
          @target_object_state.expects(:identifier_value=).with(nil)
          @client.expects(:remove).with(@column_family, source_value, identifier_value)
          @instance.remove(@target_object)
        end

        should 'do nothing if the old identifier value is nil' do
          @target_object_state.stubs(:source_value).with.returns('some_old_source_value')
          @target_object_state.expects(:identifier_value).with.returns(nil)
          @target_object_state.expects(:source_value=).never
          @target_object_state.expects(:identifier_value=).never
          @client.expects(:remove).never
          @instance.remove(@target_object)
        end

        should 'do nothing if the old source value is nil' do
          @target_object_state.expects(:source_value).with.returns(nil)
          @target_object_state.stubs(:identifier_value).with.returns('some_old_identifier_value')
          @target_object_state.expects(:source_value=).never
          @target_object_state.expects(:identifier_value=).never
          @client.expects(:remove).never
          @instance.remove(@target_object)
        end
      end

      context ':update' do
        should 'perform a :create and :remove if the source values differ' do
          @target_object_state.stubs(:source_value).with.returns('some_old_source_value')
          @instance.expects(:source_for).with(@target_object).returns('some_source_value').at_least_once
          @target_object_state.stubs(:identifier_value).with.returns(@object_identifier)
          seq = sequence('operations')
          @instance.expects(:remove).with(@target_object).in_sequence(seq)
          @instance.expects(:create).with(@target_object).in_sequence(seq)
          @instance.update(@target_object)
        end

        should 'perform a :create and :remove if identifier values differ' do
          @target_object_state.stubs(:source_value).with.returns('some_source_value')
          @instance.stubs(:source_for).with(@target_object).returns('some_source_value')
          @target_object_state.stubs(:identifier_value).with.returns('some_old_identifier_value')
          @instance.expects(:indexed_identifier_for).with(@target_object).returns('some new identifier value').at_least_once
          seq = sequence('operations')
          @instance.expects(:remove).with(@target_object).in_sequence(seq)
          @instance.expects(:create).with(@target_object).in_sequence(seq)
          @instance.update(@target_object)
        end

        should 'do nothing if the values are unchanged' do
          @target_object_state.expects(:source_value).with.returns('some_source_value')
          @instance.expects(:source_for).with(@target_object).returns('some_source_value').at_least_once
          @target_object_state.expects(:identifier_value).with.returns(@object_identifier)
          @instance.expects(:indexed_identifier_for).with(@target_object).returns(@object_identifier).at_least_once
          @instance.expects(:remove).never
          @instance.expects(:create).never
          @instance.update(@target_object)
        end
      end
    end
  end

  context 'A Cassandra::Base-derived class' do
    setup do
      @class = Class.new(CassandraMapper::Base)
    end
    
    context 'has_index method' do
      should 'create a class accessor for the named index' do
        assert_equal true, ! @class.respond_to?(:this_index)
        @class.has_index :this_index
        assert_equal true, @class.respond_to?(:this_index)
      end

      should 'set the :name attribute on the resulting index object' do
        @class.has_index :this_index
        assert_equal :this_index, @class.this_index.name
      end

      should 'determine class of index from the :class option' do
        subclass = Class.new(CassandraMapper::Index)
        @class.has_index :this_index, :class => subclass
        assert_equal true, subclass === @class.this_index
      end

      should 'default class of index to CassandraMapper::Index' do
        @class.has_index :this_index
        assert_equal true, CassandraMapper::Index === @class.this_index
      end

      should 'pass all options, with :indexed_class mapped to indexed class, to the constructor' do
        options = {:a => 'a', :b => 'b', :c => 'c'}
        subclass = Class.new(CassandraMapper::Index)
        subclass.expects(:new).with(options.merge(:indexed_class => @class,
                                                  :name => :this_index)).returns(CassandraMapper::Index.new(options.merge(:indexed_class => @class)))
        @class.has_index :this_index, options.merge(:class => subclass)
      end

      context 'given a block' do
        should 'evaluate the block in the context of the new index object' do
          received = []
          @class.has_index :this_index do
            received << self
          end
          assert_equal [@class.this_index], received
        end
      end
    end

    context 'instance' do
      setup do
        @instance = @class.new
      end

      context 'with class that has_index :foo' do
        setup do
          @class.has_index :foo
          @class.maps :some_attr
          @class.maps :id
          @class.key :id
        end

        should 'have an attribute :foo of type CassandraMapper::Index::State' do
          assert_equal CassandraMapper::Index::State, @instance.foo.class
        end

        should 'have nil index state values for :foo state object' do
          assert_equal nil, @instance.foo.source_value
          assert_equal nil, @instance.foo.identifier_value
        end

        should 'receive proper state_value and identifier_value values after :loaded' do
          @class.foo.source = :some_attr
          @class.foo.indexed_identifier = :key
          @instance.stubs(:some_attr).returns(source = 'some_source_value')
          @instance.stubs(:key).returns(key = 'some_key')
          @instance.loaded!
          assert_equal source, @instance.foo.source_value
          assert_equal key, @instance.foo.identifier_value
        end

        should 'create the index upon saving a new instance' do
          @instance.stubs(:new_record?).returns(true)
          @instance.some_attr = 'blah'
          @instance.id = 'foo'
          @class.foo.expects(:create).with(@instance)
          @instance.connection.expects(:insert)
          @instance.save
        end

        should 'update the index upon saving an existing instance' do
          @instance.stubs(:new_record?).returns(false)
          @instance.some_attr = 'blah'
          @instance.id = 'foo'
          @class.foo.expects(:update).with(@instance)
          @instance.connection.expects(:insert)
          @instance.save
        end
      end
    end
  end
end
