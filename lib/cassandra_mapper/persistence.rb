module CassandraMapper::Persistence
  def _determine_transform_options
    options = {:string_keys => true}
    if new_record?
      options[:defined] = true
    else
      return false unless changed_attributes.length > 0
      options[:changed] = true
    end
    options
  end

  def save(with_validation = true)
    uniq_key = _check_key
    return false unless options = _determine_transform_options
    connection.insert(self.class.column_family, uniq_key, to_simple(options))
    self
  end

  def to_mutation(with_validation = true, options = {})
    uniq_key = _check_key.to_s
    timestamp = options.delete(:timestamp) || Time.stamp
    return false unless general_opts = _determine_transform_options
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

  module ClassMethods
    def find(*keys)
      single = false
      case keys.first
        when Array
          keys = keys.first
        when nil
          raise CassandraMapper::InvalidArgumentException
        else
          single = true if keys.length == 1
      end
      result = connection.multi_get(column_family, keys).values.collect do |hash|
        obj = new(hash)
        obj.new_record = false
        obj
      end
      raise CassandraMapper::RecordNotFoundException unless result.size == keys.size
      single ? result.first : result
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
  end

  def self.included(klass)
    klass.extend ClassMethods
  end
end
