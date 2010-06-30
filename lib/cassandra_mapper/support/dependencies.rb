require 'cassandra_mapper/core_ext/module/attribute_accessors'

module CassandraMapper #:nodoc:
  module Support
    module Dependencies #:nodoc:
      extend self

      # An array of qualified constant names that have been loaded. Adding a name to
      # this array will cause it to be unloaded the next time Dependencies are cleared.
      mattr_accessor :autoloaded_constants
      self.autoloaded_constants = []

      # Determine if the given constant has been automatically loaded.
      def autoloaded?(desc)
        # No name => anonymous module.
        return false if desc.is_a?(Module) && desc.anonymous?
        name = to_constant_name desc
        return false unless qualified_const_defined? name
        return autoloaded_constants.include?(name)
      end

      # Convert the provided const desc to a qualified constant name (as a string).
      # A module, class, symbol, or string may be provided.
      def to_constant_name(desc) #:nodoc:
        name = case desc
          when String then desc.sub(/^::/, '')
          when Symbol then desc.to_s
          when Module
            desc.name.presence ||
              raise(ArgumentError, "Anonymous modules have no name to be referenced by")
          else raise TypeError, "Not a valid constant descriptor: #{desc.inspect}"
        end
      end

      # Is the provided constant path defined?
      def qualified_const_defined?(path)
        names = path.sub(/^::/, '').to_s.split('::')

        names.inject(Object) do |mod, name|
          return false unless local_const_defined?(mod, name)
          mod.const_get name
        end
      end

      if Module.method(:const_defined?).arity == 1
        # Does this module define this constant?
        # Wrapper to accommodate changing Module#const_defined? in Ruby 1.9
        def local_const_defined?(mod, const)
          mod.const_defined?(const)
        end
      else
        def local_const_defined?(mod, const) #:nodoc:
          mod.const_defined?(const, false)
        end
      end

    end
  end
end
