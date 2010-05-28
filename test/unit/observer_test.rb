require 'test_helper'
class ObserverTest < Test::Unit::TestCase
  context 'A CassandraMapper::Base-derived class' do
    setup do
      @class = Class.new(CassandraMapper::Base)
      @class = CassandraMapper::Base
      @class.maps :key
    end

    context 'with a registered observer class' do
      setup do
        @observer_class = Class.new(CassandraMapper::Observer)
        @observer_class.observe @class
        @observer = @observer_class.instance
        @instance = @class.new
        @instance.key = 'foo'
        @instance.connection = stub('connection', :insert => @instance)
      end

      should 'invoke :after_load on observer' do
        @observer.expects(:after_load).with(@instance).returns(true)
        @instance.loaded!
      end

      should 'invoke :before_save, :before_create, :after_create, :after_save on observer' do
        seq = sequence('callbacks')
        @observer.expects(:before_save).with(@instance).in_sequence(seq).returns(true)
        @observer.expects(:before_create).with(@instance).in_sequence(seq).returns(true)
        @observer.expects(:after_create).with(@instance).in_sequence(seq).returns(true)
        @observer.expects(:after_save).with(@instance).in_sequence(seq).returns(true)
        @instance.stubs(:new_record?).returns(true)
        @instance.save
      end

      should 'invoke :before_save, :before_update, :after_update, :after_save on observer' do
        seq = sequence('callbacks')
        @observer.expects(:before_save).with(@instance).in_sequence(seq).returns(true)
        @observer.expects(:before_update).with(@instance).in_sequence(seq).returns(true)
        @observer.expects(:after_update).with(@instance).in_sequence(seq).returns(true)
        @observer.expects(:after_save).with(@instance).in_sequence(seq).returns(true)
        @instance.stubs(:new_record?).returns(false)
        @instance.save
      end
    end
  end
end
