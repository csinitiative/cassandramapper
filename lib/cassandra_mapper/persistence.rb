require 'active_model'
module CassandraMapper::Persistence
  def _determine_transform_options
    options = {:string_keys => true}
    is_update = false
    if new_record?
      options[:defined] = true
    else
      return false unless changed_attributes.length > 0
      options[:changed] = true
      is_update = true
    end
    [options, is_update]
  end

  def save(with_validation = true)
    _run_save_callbacks do
      uniq_key = _check_key
      options, is_update = _determine_transform_options
      return false unless options
      if is_update
        update(uniq_key, options)
      else
        create(uniq_key, options)
      end
    end
  end

  def loaded!
    self.new_record = false
    _run_load_callbacks
    self
  end

  def create(uniq_key, options)
    _run_create_callbacks do
      write!(uniq_key, options)
      self.new_record = false
      self
    end
  end

  def update(uniq_key, options)
    _run_update_callbacks do
      write!(uniq_key, options.merge({ :with_delete => true }))
      self
    end
  end

  def destroy
    unless new_record?
      _run_destroy_callbacks do
        self.class.delete(_check_key)
      end
    end
    @destroyed = true
    freeze
    self
  end

  def write!(uniq_key, options)
    with_delete = options.delete(:with_delete)
    structure = to_simple(options)
    if with_delete
      deletes = self.class.prune_deletes_from_simple_structure(structure)
      cassandra_args = []
      if ! (structure.empty? or deletes.empty?) or deletes.size >= 2
        cassandra_args << {:timestamp => Time.stamp}
      end
      base_args = [self.class.column_family, uniq_key]
      connection.insert(*base_args, structure, *cassandra_args) unless structure.empty?
      deletes.each {|del_args| connection.remove(*base_args, *del_args, *cassandra_args)}
    else
      connection.insert(self.class.column_family, uniq_key, to_simple(options))
    end
  end

  def to_mutation(with_validation = true, options = {})
    uniq_key = _check_key.to_s
    timestamp = options.delete(:timestamp) || Time.stamp
    general_opts, is_update = _determine_transform_options
    return false unless general_opts
    options.merge!(general_opts)
    {
      uniq_key => {
        self.class.column_family.to_s => self.class.to_mutation(to_simple(options), timestamp)
      }
    }
  end

  def _check_key
    uniq_key = self.key
    raise CassandraMapper::UndefinedKeyException if uniq_key.nil?
    uniq_key
  end

  def delete
    self.class.delete(_check_key) unless new_record?
    @destroyed = true
    freeze
    self
  end

  def destroyed?
    (@destroyed && true) || false
  end

  module ClassMethods
    # Given a single key or list of keys, returns all mapped objects found
    # for thoese keys.
    #
    # If the row for a specified key is missing, a CassndraMapper::RecordNotFoundException
    # exception is raised.  This can be overridden by specifying the +:allow_missing+
    # option (:allow_missing => true)
    #
    # Keys and options may be specified in a variety of ways:
    # * Flat list
    #     SomeClass.find(key1, key2, key3, options)
    # * Separate lists
    #     SomeClass.find([key1, key2, key3], options)
    #
    # And of course, _options_ can always be left out.
    def find(*args)
      single = false
      case args.first
        when Array
          keys = args.first
        when nil
          raise CassandraMapper::InvalidArgumentException
        else
          keys = args
          single = true if keys.length == 1
      end
      case args.last
        when Hash
          options = args.pop
        else
          options = {}
      end
      
      result = connection.multi_get(column_family, keys).values.inject([]) do |arr, hash|
        if not hash.empty?
          obj = new(hash)
          obj.new_record = false
          arr << obj
          obj.loaded!
        end
        arr
      end
      raise CassandraMapper::RecordNotFoundException unless result.size == keys.size or options[:allow_missing]
      single ? result.first : result
    end

    # Given a _key_ of a single row key or array of row keys,
    # removes the rows from the Cassandra column family associated with
    # the receiving class.
    #
    # As the underlying Cassandra client returns no information about the
    # result of the operation, the result is always +nil+.
    #
    # The delete operation is purely database-side; no objects are instantiated
    # and therefore no object callbacks take place for the deleted rows.
    # This makes delete risky for classes that manage associations, indexes, etc.
    def delete(key)
      keys = Array === key ? key : [key]
      keys.each do |key|
        connection.remove(column_family, key)
      end
      nil
    end

    # Similar to +delete+; given a _key_ of a single row key or array of row keys,
    # removes the rows from the Cassandra column family associated with the receiving
    # class.
    #
    # However, unlike +delete+, +destroy+ loads up the object per row key in turn
    # and invokes +destroy+ on each object.  This allows for callbacks and observers
    # to be executed, which is essential for index maintenance, association maintenance,
    # etc.
    def destroy(key)
      objs = find(key)
      case objs
        when Array
          objs.each {|obj| obj.destroy}
        else
          objs.destroy
      end
      nil
    end

    def connection
      @cassandra_mapper_connection
    end

    def connection=(connection)
      @cassandra_mapper_connection = connection
    end

    def column_family(family = nil)
      @cassandra_mapper_column_family = family if ! family.nil?
      @cassandra_mapper_column_family
    end

    def self.extended(klass)
      klass.module_eval do
        extend ActiveModel::Callbacks
        define_model_callbacks :save, :create, :update, :destroy
        define_model_callbacks :load, :only => :after
      end
    end

    def to_mutation(simple_structure, timestamp)
      mutator.from_simple(simple_structure, timestamp)
    end

    def mutator
      unless @mutator
        mutator_class = simple_mapper.attributes.first[1].mapper ? SuperMutator : SimpleMutator
        @mutator = mutator_class.new
      end
      @mutator
    end

    class SimpleMutator
      def from_simple(structure, timestamp)
        deletion = nil
        deletion_columns = nil
        structure.inject([]) do |list, pair|
          key,val = pair
          if val.nil?
            unless deletion
              deletion = CassandraThrift::Mutation.new(
                :deletion => CassandraThrift::Deletion.new(
                  :super_column => nil,
                  :timestamp    => timestamp,
                  :predicate    => CassandraThrift::SlicePredicate.new(
                    :column_names => (deletion_columns = [])
                  )
                )
              )
              list << deletion
            end
            deletion_columns << key
          else
            list << CassandraThrift::Mutation.new(
              :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
                :column => CassandraThrift::Column.new(
                  :name      => key,
                  :value     => val,
                  :timestamp => timestamp
                )
              )
            )
          end
          list
        end
      end
    end

    class SuperMutator
      def from_simple(structure, timestamp)
        structure.inject([]) do |list, pair|
          supercol_key, val = pair
          if val and ! val.empty?
            list << CassandraThrift::Mutation.new(
              :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
                :super_column => CassandraThrift::SuperColumn.new(
                  :name    => supercol_key,
                  :columns => val.collect {|column, value|
                    CassandraThrift::Column.new(
                      :name      => column,
                      :value     => value,
                      :timestamp => timestamp
                    )
                  }
                )
              )
            )
          end
          list
        end
      end
    end

    def prune_deletes(source, deletes, context)
      source.each do |k, v|
        if v.nil?
          deletes << context + [k]
          source.delete(k)
        elsif v.respond_to?(:each)
          prune_deletes(v, deletes, context + [k])
          source.delete(k) if v.empty?
        end
      end
    end

    def prune_deletes_from_simple_structure(structure)
      deletes = []
      prune_deletes(structure, deletes, [])
      deletes
    end
  end

  def self.included(klass)
    klass.extend ClassMethods
  end
end
