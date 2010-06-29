require 'cassandra_mapper/support/observing'
module CassandraMapper
  module Observable
    CALLBACKS = [
      :after_load,
      :before_create,
      :after_create,
      :before_update,
      :after_update,
      :before_save,
      :after_save,
      :before_destroy,
      :after_destroy,
    ]

    CALLBACKS.each do |cb|
      name = cb.to_s
      module_eval <<-cbnotify
        def _notify_observer_#{name}; notify_observers(:#{name}); true; end
      cbnotify
    end

    def self.included(klass)
      klass.module_eval do
        include ActiveModel::Observing
        CALLBACKS.each do |callback|
          name = callback.to_s
          send(callback, :"_notify_observer_#{name}")
        end
      end
    end
  end

  class Observer < CassandraMapper::Support::Observer
  end
end
