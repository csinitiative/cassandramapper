require 'test_helper'

class PersistenceTest < Test::Unit::TestCase
  context 'CassandraMapper::Base class' do
    setup do
      @class = Class.new(CassandraMapper::Base)
    end

    should 'provide a column_family class setter' do
      assert_equal nil, @class.column_family
      assert_equal 'SomeColumnFamily', @class.column_family('SomeColumnFamily')
      assert_equal 'SomeColumnFamily', @class.column_family
    end

    should 'provide a connection class attribute' do
      assert_equal nil, @class.connection
      connection = stub('connection')
      @class.connection = connection
      assert_equal connection, @class.connection
    end
  end

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

    context 'connection' do
      setup do
        @instance = @class.new
      end

      should 'use class connection at instance level by default' do
        assert_equal @connection, @instance.connection
      end

      should 'use instance connection if set' do
        instance_connection = stub('instance_cassandra_client')
        @instance.connection = instance_connection
        assert_equal instance_connection, @instance.connection
      end
    end

    context 'when saving' do
      setup do
        @values = {'a' => 'Aa', 'b' => 'Bb', 'c' => 'Cc'}
        @instance = @class.new(@values)
        @instance_connection = stub('instance_cassandra_client')
        @instance.stubs(:connection).returns(@instance_connection)
      end

      context 'to cassandra' do
        context 'a new instance' do
          setup do
            @instance.stubs(:new_record?).returns(true)
          end

          should 'pass defined attributes to thrift' do
            # The nil result from Cassandra/Thrift is somewhat uninspiring.
            @instance_connection.expects(:insert).with(@column_family, @values['a'], @values).returns(nil)
            assert @instance.save
          end

          should 'not pass undefined attributes to thrift' do
            @values.delete 'b'
            @instance.b = nil
            @instance_connection.expects(:insert).with(@column_family, @values['a'], @values).returns(nil)

            assert @instance.save
          end

          should 'throw an UndefinedKey exception if key attribute is empty' do
            @instance.a = nil
            assert_raise(CassandraMapper::UndefinedKeyException) { @instance.save }
          end

          should 'set :new_record? to false after the insert' do
            operations = sequence('save operations')
            @instance_connection.expects(:insert).once.in_sequence(operations)
            @instance.expects(:new_record=).with(false).once.in_sequence(operations).returns(false)
            assert @instance.save
          end
        end

        context 'an existing record instance' do
          setup do
            @instance.stubs(:new_record?).returns(false)
          end

          should 'be a no-op if no attributes were changed' do
            @instance_connection.expects(:insert).never
            assert_equal false, @instance.save
          end

          should 'pass only the key/values for attributes that changed to thrift' do
            key = @values['a']
            @instance.b = 'B foo'
            @instance.c = 'C foo'
            expected = {'b' => @instance.b, 'c' => @instance.c}
            @instance_connection.expects(:insert).with(@column_family, key, expected).returns(nil)
            assert_equal @instance, @instance.save
          end
        end
      end
      context 'to a mutation' do
        setup do
          @column_family = :ColumnFamily
          @class.stubs(:column_family).returns(@column_family)
          @supercolumn_family = :SuperColumnFamily
          @supercolumn_class = Class.new(CassandraMapper::Base) do
            [:a, :b, :c].each do |supercol|
              maps supercol do
                [:x, :y, :z].each do |col|
                  maps col
                end
              end
            end
            def key; a ? a.x : nil; end
          end
          @supercolumn_class.stubs(:column_family).returns(@supercolumn_family)
          @supercolumn_instance = @supercolumn_class.new(:a => {}, :b => {}, :c => {})
          @supercolumn_instance.stubs(:connection).returns(@instance_connection)
          @timestamp = Time.stamp
          Time.stubs(:stamp).returns(@timestamp)
        end

        should 'throw an UndefinedKeyException if key is undefined' do
          @instance.stubs(:key).returns(nil)
          assert_raise(CassandraMapper::UndefinedKeyException) { @instance.to_mutation }
        end

        context 'a new instance' do
          setup do
            @instance.stubs(:new_record?).returns(true)
            @supercolumn_instance.stubs(:new_record?).returns(true)
          end

          should 'only provide mutations for defined simple columns' do
            @instance.c = nil
            result = @instance.to_mutation
            mutations = result[@values['a']][@column_family.to_s]
            mutations.sort! do |a, b|
              a.column_or_supercolumn.column.name <=> b.column_or_supercolumn.column.name
            end
            assert_equal(
              {
                @values['a'] => {
                  @column_family.to_s => ['a', 'b'].collect {|attrib|
                    CassandraThrift::Mutation.new(
                      :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
                        :column => CassandraThrift::Column.new(
                          :name      => attrib,
                          :value     => @values[attrib],
                          :timestamp => @timestamp
                        )
                      )
                    )
                  }
                }
              },
              result
            )
          end

          should 'only provide mutations for defined super/sub columns' do
            @supercolumn_instance.a.x = 'a-x'
            @supercolumn_instance.a.y = 'a-y'
            @supercolumn_instance.b.z = 'b-z'
            result = @supercolumn_instance.to_mutation
            mutations = result['a-x'][@supercolumn_family.to_s]
            mutations.sort! do |a, b|
              a.column_or_supercolumn.super_column.name <=> b.column_or_supercolumn.super_column.name
            end
            mutations.each do |mutation|
              mutation.column_or_supercolumn.super_column.columns.sort! do |a, b|
                a.name <=> b.name
              end
            end
            assert_equal(
              {
                'a-x' => {@supercolumn_family.to_s => [[:a, :x, :y], [:b, :z]].collect { |args|
                  supercol = args.shift.to_s
                  CassandraThrift::Mutation.new(
                    :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
                      :super_column => CassandraThrift::SuperColumn.new(
                        :name       => supercol,
                        :columns    => args.collect {|col|
                          CassandraThrift::Column.new(
                            :timestamp => @timestamp,
                            :name      => col.to_s,
                            :value     => supercol + '-' + col.to_s
                          )
                        }
                      )
                    )
                  )
                }
              }},
              result
            )
          end
        end

        context 'an existing record instance' do
          setup do
            @instance.stubs(:new_record?).returns(false)
            @supercolumn_instance = @supercolumn_class.new(
              @supercolumn_values = {
                'a' => {'x' => 'a-x', 'y' => 'a-y', 'z' => 'a-z'},
                'b' => {'x' => 'b-x', 'y' => 'b-y', 'z' => 'b-z'},
                'c' => {'x' => 'c-x', 'y' => 'c-y', 'z' => 'c-z'}
              }
            )
            @supercolumn_instance.stubs(:new_record?).returns(false)
          end

          should 'only output mutations for attributes that changed' do
            @instance.b = 'foo'
            assert_equal(
              {
                @values['a'] => {
                  @column_family.to_s => [
                    CassandraThrift::Mutation.new(
                      :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
                        :column => CassandraThrift::Column.new(
                          :name      => 'b',
                          :value     => 'foo',
                          :timestamp => @timestamp
                        )
                      )
                    )
                  ]
                }
              },
              @instance.to_mutation
            )
          end

          should 'output deletions for attributes that were set to nil' do
            @instance.b = nil
            @instance.c = nil
            result = @instance.to_mutation
            result[@values['a']][@column_family.to_s].first.deletion.predicate.column_names.sort!
            assert_equal(
              {
                @values['a'] => {
                  @column_family.to_s => [
                    CassandraThrift::Mutation.new(
                      :deletion => CassandraThrift::Deletion.new(
                        :super_column => nil,
                        :timestamp    => @timestamp,
                        :predicate    => CassandraThrift::SlicePredicate.new(
                          :column_names => ['b', 'c']
                        )
                      )
                    )
                  ]
                }
              },
              result
            )
          end

# to-do: simplemapper doesn't have change tracking quite dialed in for nested
# structures just yet.
#          should 'only provide mutations for supercolumn/subcol attributes that changed' do
#            @supercolumn_instance.b.y = 'foo'
#            assert_equal(
#              {
#                @supercolumn_values['a']['x'] => {
#                  @supercolumn_family.to_s => [
#                    CassandraThrift::Mutation.new(
#                      :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
#                        :super_column => CassandraThrift::SuperColumn.new(
#                          :name    => 'b',
#                          :columns => [
#                            CassandraThrift::Column.new(
#                              :name      => 'x',
#                              :value     => 'foo',
#                              :timestamp => @timestamp
#                            )
#                          ]
#                        )
#                      )
#                    )
#                  ]
#                }
#              },
#              @supercolumn_instance.to_mutation
#            )
#          end
#
#          should 'provide deletions for supercolumn/column attributes that were set to nil' do
#          end
        end
      end
    end

    context 'retrieving via :find' do
      setup do
        @instances = [:A, :B, :C].inject({}) do |hash, name|
          val = name.to_s
          hash[name.to_s] = @class.new('a' => val, 'b' => 'b' + val, 'c' => 'c' + val)
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

      context 'with keys that cannot be found' do
        setup do
          @source_keys = @instances.keys
          @keys = @source_keys.clone
          @keys.pop
          @client_result = @instances.values_at(*@keys).inject({}) {|h, i| h[i.key] = i.to_simple; h}
          @connection.expects(:multi_get).with(@column_family, @source_keys).returns(@client_result)
        end

        should 'throw a RecordNotFound exception' do
          assert_raises(CassandraMapper::RecordNotFoundException) { @class.find(@source_keys) }
        end

        should 'return subset of objects found if :allow_missing option is true' do
          result = (@class.find(@source_keys, :allow_missing => true) || []).sort_by {|x| x.key}.collect {|o| o.to_simple}
          assert_equal @instances.values_at(*@keys).collect {|y| y.to_simple},
                       result
        end
      end

      should 'throw an InvalidArgument exception if the key list is empty' do
        assert_raises(CassandraMapper::InvalidArgumentException) { @class.find() }
      end
    end

    context 'removing via class.delete' do
      should 'invoke :remove on underlying client for the given id' do
        @connection.expects(:remove).with(@column_family, key = 'foo')
        # explicity verify: no object is instantiated for this
        @class.expects(:new).never
        @class.delete(key)
      end

      should 'invoke :remove on underlying client for all ids' do
        # this is suboptimal, but that's what the client offers for now
        keys = ['a', 'b', 'c', 'd']
        keys.each {|key| @connection.expects(:remove).with(@column_family, key)}
        # again, be sure no object is instantiated for this
        @class.expects(:new).never
        @class.delete(keys)
      end
    end

    context 'removing via instance.delete' do
      setup do
        @object_new = @class.new
        @object_new.stubs(:new_record?).returns(true)
        @key_new = 'new row'
        @object_new.stubs(:key).with.returns(@key_new)
        @object_old = @class.new
        @object_old.stubs(:new_record?).returns(false)
        @key_old = 'old row'
        @object_old.stubs(:key).with.returns(@key_old)
        # intercept freeze calls to prevent mocha issues
        @object_new.stubs(:freeze)
        @object_old.stubs(:freeze)
      end

      context 'via instance.delete' do
        should 'mark the instance as destroyed' do
          @class.stubs(:delete)
          @object_new.delete
          @object_old.delete
          assert_equal true, @object_new.destroyed?
          assert_equal true, @object_old.destroyed?
        end

        should 'freeze the instance' do
          @class.stubs(:delete)
          # This would be better with a traditional assertion to check :frozen?,
          # but Mocha has teardown problems that breaks the entire test suite on
          # Ruby 1.8.7 when de-stubbing objects that are now marked as frozen.
          # So we have to use expectations instead.
          @object_new.expects(:freeze).with.once
          @object_old.expects(:freeze).with.once
          @object_new.delete
          @object_old.delete
        end

        should 'invoke class.delete on existing rows only' do
          seq = sequence('expectations')
          @class.expects(:delete).with(@key_new).never.in_sequence(seq)
          @class.expects(:delete).with(@key_old).once.in_sequence(seq)
          @object_new.delete
          @object_old.delete
        end
      end

      context 'via instance.destroy' do
        should 'invoke class.delete on existing rows only' do
          seq = sequence('expectations')
          @class.expects(:delete).with(@key_new).never.in_sequence(seq)
          @class.expects(:delete).with(@key_old).once.in_sequence(seq)
          @object_new.destroy
          @object_old.destroy
        end

        should 'freeze the instance' do
          @class.stubs(:delete)
          # Again, working around Mocha teardown issues, using expectations rather
          # than assertions.
          @object_new.expects(:freeze).with.once
          @object_old.expects(:freeze).with.once
          @object_new.destroy
          @object_old.destroy
        end

        should 'mark the instance as destroyed' do
          @class.stubs(:delete)
          @object_new.destroy
          @object_old.destroy
          assert_equal true, @object_new.destroyed?
          assert_equal true, @object_old.destroyed?
        end
      end
    end

    context 'removing via class.destroy' do
      should ':find the object based on given id and :destroy it' do
        @instance = mock('row')
        seq = sequence('order of events')
        @class.expects(:find).with(some_id = 'terrible key').in_sequence(seq).returns(@instance)
        @instance.expects(:destroy).in_sequence(seq)
        @class.destroy(some_id)
      end

      should ':find all objects from given ids and :destroy them in turn' do
        ids = ['a', 'b', 'c', 'd', 'e']
        instances = ids.collect {|id| x = mock(id); x.expects(:destroy); x}
        @class.expects(:find).with(ids).returns(instances)
        @class.destroy(ids)
      end
    end
  end
end

