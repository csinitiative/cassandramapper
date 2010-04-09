require 'test_helper'

class IdentityTest < Test::Unit::TestCase
  context 'CassandraWrapper::Base' do
    setup do
      @class = Class.new(CassandraMapper::Base)
      @values = {:a => 'Aa', :b => 'Bb', :c => 'Cc'}
      @values.keys.each {|key| @class.maps key}
      @instance = @class.new(@values)
    end

    context 'key attribute declaration' do
      should 'be possible with the :key class method' do
        @class.key :a
        assert_equal :a, @class.key
      end

      should 'determine the :key value on an instance' do
        @class.key :a
        assert_equal @values[:a], @instance.key
        @class.key :b
        assert_equal @values[:b], @instance.key
      end

      should 'be overriddable with an instance method' do
        @class.key :a
        @class.module_eval do
          def key
            "#{a}-#{b}"
          end
        end
        assert_equal "#{@values[:a]}-#{@values[:b]}", @instance.key 
      end

      should 'result in default of :key if left undeclared' do
        assert_equal :key, @class.key
      end
    end
  end
end
