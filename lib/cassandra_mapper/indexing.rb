module CassandraMapper
  # Provides indexing behavior for CassandraMapper::Base objects.
  # Rather than maintaining indexes in Cassandra yourself, use the higher-level
  # functionality provided by CassandraMapper::Indexing, and CassandraMapper will
  # manage the underlying index state for you.
  #
  # An index needs a standard column family into which index data is placed.
  # A given searchable value (an indexed value) becomes a row key in the column family.
  # The columns and values in the row provide the keys in your indexed column family
  # that have the indexed value.
  #
  # Suppose we have column family _A_ with rows:
  #     'foo': {
  #         'key'  : 'foo',
  #         'value': 'a',
  #     };
  #     'bar': {
  #         'key'  : 'bar',
  #         'value': 'b',
  #     };
  #     'fu': {
  #         'key'  : 'fu',
  #         'value': 'a',
  #     }
  #
  # Suppose further that in column family _B_ we want to index on _A_'s _value_ column.
  # We would therefore expect _B_ to have rows:
  #     'a': {
  #         'foo': 'foo',
  #         'fu' : 'fu',
  #     };
  #     'b': {
  #         'bar': 'bar',
  #     };
  #
  # Cassandra automatically sorts columns within a row, based on the configuration for
  # the column family in question.  Therefore, while the redundant data for column
  # keys and values shown above seems somewhat awkward, the column keys can be designed
  # to give smarter sorting of results; for instance, were each row to have a _created_at_
  # timestamp string, we could index on _value_ as before but sort by _created_at_.
  #
  # So, with _A_ values:
  #     'foo': {
  #         'key'       : 'foo',
  #         'value'     : 'a',
  #         'created_at': '20100601 093000',
  #     };
  #     'bar': {
  #         'key'       : 'bar',
  #         'value'     : 'b',
  #         'created_at': '20100529 172500',
  #     };
  #     'fu': {
  #         'key'       : 'fu',
  #         'value'     : 'a',
  #         'created_at': '20100602 121500',
  #     };
  #
  # We could index on _value_ with results sorted in ascending order of _created_at_
  # with _B_ rows:
  #     'a': {
  #         '20100601 093000 foo': 'foo',
  #         '20100602 121500 fu': 'fu',
  #     };
  #     'b': {
  #         '20100529 172500 bar': 'bar',
  #     };
  #
  # The end result is that rows in _A_ could be looked up via _data_ values using
  # the desired _data_ value as the key of _B_ for finding identifiers.  Those results
  # can be structure (via column name) to ensure that keys come back in the desired order
  # (in this case, by _created_at_ order).
  #
  # The column family that stores the index can be used for one index or multiple
  # indexes, depending on your use case.
  module Indexing
    module ClassMethods
      # Build an index object and install it into the calling class.
      # * The _index_ argument should be a symbol, which will be the name of the index
      #   and the name of the accessor method for that index at both the class level
      #   and the instance level.
      # * The _options_ hash is passed through to the CassandraMapper::Index constructor,
      #   with some minor mapping logic.  See the CassandraMapper::Index documentation for
      #   most options.  Some options specific to this method:
      #   * _class_: the class object to use for the index object; use this if you want to
      #     provide your own custom index behavior.  Defaults to Cassandra::Index.  This
      #     option determines the class to be instantiated and is not passed along to the
      #     constructor.
      #   * _indexed_class_: always gets set to the receiver, even if you set it explicitly.
      #     This ensures that the index binds to the class against which _has_index_ was
      #     called.
      #   * _name_: always gets set to the _index_ argument provided to the _has_index_ call,
      #     even if you set it explicitly in _options_.
      # * If a _&block_ is provided, it will be evaluated in the context of the newly-created
      #   index object; this makes it easy to build indexes that have specialized logic for
      #   formatting sortable identifiers, etc.
      #
      # The index is installed as the _index_ attribute of the class object, so all index
      # operations can be accomplished from there.  Additionally, the _index_ name is used as
      # an instance attribute, in which an instance's state relative to the index is tracked.
      # Therefore, choose an _index_ value that you're happy having on both class and instances.
      #
      # The index object is activated after installation, so its observer goes into effect
      # immediately.
      #
      # Given the example class and index described at CassandraMapper::Index, the same
      # strategy could be achieved less verbosely with:
      #     class ToBeIndexed < CassandraMapper::Base
      #       column_family :ToBeIndexed
      #       maps :key, :type => :simple_uuid
      #
      #       maps :data
      #       maps :created_at, :type => :timestamp, :default => :from_type
      #
      #       def timestamped_key
      #         "#{created_at.to_s}_#{key}"
      #       end
      #
      #       has_index :data_index, :source             => :data,
      #                              :indexed_identifier => :timestamped_key,
      #                              :column_family      => :Indexes
      #     end
      #
      # The +has_index+ invocation takes care of the details for creating the :data_index
      # class and instance attributes, the CassandraMapper::Index instance, its installation
      # and activation, etc.
      #
      # Finally, if the timestamped key only pertains to this index (as is the case in this
      # example), we could arguably reduce clutter in the main model class and keep the key
      # generation encapsulated in the index by using the block-style invocation.
      #     class ToBeIndexed < CassandraMapper::Base
      #       column_family :ToBeIndexed
      #       maps :key, :type => :simple_uuid
      #
      #       maps :data
      #       maps :created_at, :type => :timestamp, :default => :from_type
      #
      #       has_index :data_index, :source => :data, :column_family => :Indexes do
      #         def indexed_identifier_for(instance)
      #           "#{instance.created_at.to_s}_#{instance.key}"
      #         end
      #       end
      #     end
      #
      def has_index(index, options={}, &block)
        klass = options.delete(:class) || CassandraMapper::Index
        object = klass.new(options.merge(:indexed_class => self, :name => index))
        object.instance_eval &block if block_given?
        install_index(index, object)
      end

      def install_index(name, index)
        name_string = name.to_s
        instance_variable_set(:"@#{name_string}", index)
        instance_eval "def #{name_string}; @#{name_string}; end"
        module_eval "def #{name_string}; @#{name_string} ||= CassandraMapper::Index::State.new; end"
        index.activate!
        index
      end
    end

    def self.included(klass)
      klass.extend(ClassMethods)
    end
  end

  # The fundamental implementation of an index in Cassandra.  Once installed into the
  # class to be indexed, the CassandraMapper::Index maintains index values for all
  # instances of the indexed class as those instances are written out t the database.
  #
  # For any given instance of an indexed class, CassandraMapper::Index will update
  # the index information based on the following criteria:
  # * The class being indexed should be provided through _indexed_class_.  The index uses
  #   an observer under the hood to track state changes per instance, and therefore requires
  #   the _indexed_class_ to be provided to hook into the observer/callback machinery.  Additionally,
  #   the index needs to know the class to instantiate when reading objects out of the index.
  # * The column family to contain the indexing data is specified with the _column_family_
  #   attribute.  CassandraMapper::Index handles writes/removes to that column family directly;
  #   there is no need for a CassandraMapper::Base model fronting the column family.
  # * The actual indexed value is determined by invoking the method specified in the
  #   index's _source_ attribute on the object written to the database.  If a class
  #   should have an index on its +:foo+ attribute, then the index object should have
  #   _source_ set to +:foo+.  This determines the row key for the index.
  # * Entries can be sorted within the index, provided an identifier is available per
  #   object that is sensibly sortable.  The _indexed_identifier_ attribute specifies the
  #   method to call to provide that sortable identifier, which will correspond to the column
  #   named used within the index row for the given object.  The _indexed_identifier_ defaults
  #   to +:key+, and does not need to be changed unless you have some criteria for sorting
  #   entries within the index.  Like _source_, the _indexed_identifier_ identifies a method
  #   on the object being saved, not a method on the index object itself.
  # * The _name_ identifies the name of the index.  This ultimately must match up to the
  #   name of an attribute on objects being indexed that holds the instance index state information,
  #   in an instance of CassandraMapper::Index::State.  Without this, index operations will
  #   fail because indexing of an object requires tracking state changes from one save to the
  #   next (to determine at save time in the case of an update whether the index needs to be
  #   changed and consequently requires a delete and a write).
  #
  # Say we have the following model class:
  #     class ToBeIndexed < CassandraMapper::Base
  #       column_family :ToBeIndexed
  #       maps :key, :type => :simple_uuid
  #
  #       # We'll be indexing this attribute.
  #       maps :data
  #
  #       # and within the index, we'll sort by create date from this attribute.
  #       maps :created_at, :type => :timestamp, :default => :from_type
  #
  #       # we'll need this to match up with the :name attribute, as described above.
  #       def data_index
  #         @data_index ||= CassandraMapper::Index::State.new
  #       end
  #
  #       # we'll use this to generate the sortable identifiers; it'll output
  #       # a string like "2010-06-02T09:45:21-04:00_47118d04-6e4e-11df-911a-e141fbb809ab".
  #       # It should be unique to each indexed object, as it includes the object's key.
  #       # But it is structured so it is effectively sortable according to create timestamp.
  #       def timestamped_key
  #         "#{created_at.to_s}_#{key}"
  #       end
  #     end
  #
  # We can index this class using the +Indexes+ column family to hold index data.
  #     index = CassandraMapper::Index.new(:indexed_identifier => :timestamped_key,
  #                                        :source             => :data,
  #                                        :name               => :data_index,
  #                                        :indexed_class      => :to_be_indexed,
  #                                        :column_family      => :Indexes)
  #     # activate it to install the observer and start indexing.
  #     index.activate!
  #
  # Then supposing we ran this code:
  #     # supposing key 47118d04-6e4e-11df-911a-e141fbb809ab is generated
  #     ToBeIndexed.new(:data => 'this data').save
  #     sleep 1
  #     # say that key 5a7e65fa-6e4f-11df-9554-d05c3d9715f7 is generated
  #     ToBeIndexed.new(:data => 'that data').save
  #     sleep 1
  #     # and finally say key gets 68985128-6e4f-11df-8e08-093a2b8b1253
  #     ToBeIndexed.new(:data => 'this data').save
  #
  # The resulting index structure in the +Indexes+ column family would look like:
  #     'this data': {
  #         '2010-06-02T10:01:00-04:00_47118d04-6e4e-11df-911a-e141fbb809ab': '47118d04-6e4e-11df-911a-e141fbb809ab',
  #         '2010-06-02T10:01:02-04:00_68985128-6e4f-11df-8e08-093a2b8b1253': '68985128-6e4f-11df-8e08-093a2b8b1253'
  #     },
  #     'that data': {
  #         '2010-06-02T10:01:01-04:00_5a7e65fa-6e4f-11df-9554-d05c3d9715f7': '5a7e65fa-6e4f-11df-9554-d05c3d9715f7'
  #     }
  #
  # Thus, the +Indexes+ column family could be used to retrieve +ToBeIndexed+ instances that
  # have particular values for +:data+, and retrieve those instances sorted by create timestamp
  # (thanks to the sortable column names).
  #
  # Ultimately, the structure that goes to the index column family for an instance of an indexed
  # class would look like this (relative to the index attributes and the instance being indexed):
  #     :source : {
  #         :indexed_identifier : :key
  #     }
  class Index
    ATTRS = [:source, :indexed_class, :column_family, :name, :indexed_identifier]
    attr_accessor *ATTRS

    DEFAULTS = {:indexed_identifier => :key}

    def initialize(options={})
      opts = DEFAULTS.merge(options)
      ATTRS.each do |attrib|
        value = opts[attrib]
        send(:"#{attrib.to_s}=", value) if not value.nil?
      end
    end

    # Returns the CassandraMapper::Index::State instance pertaining to the receiver
    # on _instance_, determined by the receiver's _name_ attribute.
    #
    # The _instance_ is expected to implement that interface, ensuring that an accessor
    # with name matching index's _name_ returns an object conforming to the state object
    # interface.
    def state_for(instance)
      instance.send(name)
    end

    # Returns the "source" value (the index row key) for _instance_ based on the method
    # specified in the receiver's _source_ attribute.
    #
    # This could be overridden to have more sophisticated index row key generation techniques
    # applied for a particular index.
    def source_for(instance)
      instance.send(source)
    end

    # Returns the "indexed identifier" (the sort-friendly column name) for _instance_ based
    # on the method specified in the receiver's _indexed_identifier_ attribute.
    #
    # This could be overridden to have more sophisticated sort logic within an index for
    # a particular index object.
    def indexed_identifier_for(instance)
      instance.send(indexed_identifier)
    end

    # If the value to be indexed is non-nil, performs an insert into the appropriate
    # column family of the index structure for the _instance_ provided.  Also updates
    # the state information at the index's _name_ on _instance_ to reflect the latest
    # source and indexed identifier values.
    #
    # This is typically managed under the hood by observer callbacks during the _instance_
    # lifecycle, but you could invoke it directly if you need to force certain index values
    # to be present.
    def create(instance)
      index_key = source_for(instance)
      if not index_key.nil?
        column = indexed_identifier_for(instance)
        instance.connection.insert(column_family, index_key, {column => instance.key})
        state = state_for(instance)
        state.source_value = index_key
        state.identifier_value = column
      end
      instance
    end

    # Given non-nil values in the _instance_'s index state for the index's _name_,
    # performs a +:remove+ against the appropriate column family to remove that old
    # state from the index.  Also clears the index state object for the _instance_.
    #
    # Like :create, this is intended to be managed automatically during the _instance_
    # lifecycle, but you could invoke it directly if necessary.  In this case, take care
    # to note that the remove acts against the index state object at _name_ on _instance_,
    # *not* against the current source/identifier values.
    def remove(instance)
      state = state_for(instance)
      unless state.source_value.nil? or state.identifier_value.nil?
        instance.connection.remove(column_family, state.source_value, state.identifier_value)
        state.source_value = nil
        state.identifier_value = nil
      end
      instance
    end

    # If the source or indexed identifier values are found to have changed on _instance_
    # (current values compared to the state preserved in the index state object at the index's
    # _name_ on _instance_), performs a +:remove+ followed by a +:create+ to keep the index
    # up to date.
    def update(instance)
      state = state_for(instance)
      if state.source_value != source_for(instance) or state.identifier_value != indexed_identifier_for(instance)
        remove(instance)
        create(instance)
      end
      instance
    end

    # Creates the necessary observer for the class to be indexed and thus activates the callbacks
    # for index management.
    def activate!
      @observer = Class.new(Observer)
      @observer.activate!(self)
    end

    class Observer < CassandraMapper::Observer
      class << self
        attr_accessor :index

        def activate!(index_object)
          observe index_object.indexed_class
          self.index = index_object
          instance
        end
      end

      def index
        self.class.index
      end

      def after_load(instance)
        state = index.state_for(instance)
        state.source_value     = instance.send(index.source)
        state.identifier_value = instance.send(index.indexed_identifier)
        instance
      end

      def after_create(instance)
        index.create(instance)
        instance
      end

      def after_update(instance)
        index.update(instance)
        instance
      end
    end

    class State
      attr_accessor :source_value, :identifier_value
    end
  end
end
