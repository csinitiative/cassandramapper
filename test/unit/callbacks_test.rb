require 'test_helper'

class CallbacksTest < Test::Unit::TestCase
  context 'A CassandraModel::Base-derived instance' do
    setup do
      @class = Class.new(CassandraMapper::Base) do
        before_save   :do_before_save
        after_save    :do_after_save
        before_create :do_before_create
        after_create  :do_after_create
        before_update :do_before_update
        after_update  :do_after_update
        before_destroy :do_before_destroy
        after_destroy  :do_after_destroy
      end
      @instance = @class.new
      @connection = stub('connection')
      @instance.stubs(:connection).returns(@connection)
      @sequence = sequence('invocation_sequence')
      @instance.stubs(:to_simple).returns({})
      @instance.stubs(:key).returns(:foo)
    end

    context 'first save' do
      setup do
        @instance.stubs(:new_record?).returns(true)
      end

      should 'invoke the before_save, before_create, after_create, after_save callbacks' do
        @instance.expects(:do_before_save).in_sequence(@sequence).returns(true)
        @instance.expects(:do_before_create).in_sequence(@sequence).returns(true)
        @connection.expects(:insert).in_sequence(@sequence).returns(@instance)
        @instance.expects(:do_after_create).in_sequence(@sequence).returns(true)
        @instance.expects(:do_after_save).in_sequence(@sequence).returns(true)
        @instance.save
      end
    end

    context 'update' do
      setup do
        @instance.stubs(:new_record?).returns(false)
        @instance.stubs(:changed_attributes).returns([:blah])
      end

      should 'invoke the before_save, before_update, after_update, after_save callbacks' do
        @instance.expects(:do_before_save).in_sequence(@sequence).returns(true)
        @instance.expects(:do_before_update).in_sequence(@sequence).returns(true)
        @connection.expects(:insert).in_sequence(@sequence).returns(@instance)
        @instance.expects(:do_after_update).in_sequence(@sequence).returns(true)
        @instance.expects(:do_after_save).in_sequence(@sequence).returns(true)
        @instance.save
      end
    end

    context 'after being retrieved via :find' do
      setup do
        @key = 'some_key'
        @class.maps :some_attr
        @source_values = {'some_attr' => 'some_value'}
        @connection = stub('connection', :multi_get => {@key => @source_values})
        @class.stubs(:connection).returns(@connection)
        @class.module_eval do
          after_load :do_after_load

          def do_after_load
            self.class.after_load_invoked('some_attr' => some_attr)
          end
        end
      end

      should 'invoke the after_load callback' do
        @class.expects(:after_load_invoked).with(@source_values)
        @class.find(@key)
      end
    end

    context 'destroy' do
      should 'invoke the before_destroy and after_destroy callbacks on an existing row' do
        @instance.stubs(:new_record?).returns(false)
        @instance.expects(:do_before_destroy).once.in_sequence(@sequence).returns(true)
        @class.expects(:delete).with(@instance.key).once.in_sequence(@sequence)
        @instance.expects(:do_after_destroy).once.in_sequence(@sequence).returns(true)
        @instance.destroy
      end

      should 'not invoke callbacks on a new row' do
        @instance.stubs(:new_record?).returns(true)
        @instance.expects(:do_before_destroy).never.in_sequence(@sequence)
        @class.expects(:delete).with(@instance.key).never.in_sequence(@sequence)
        @instance.expects(:do_after_destroy).never.in_sequence(@sequence)
        @instance.destroy
      end
    end
  end
end
