module Rack
  class MiniProfiler
    class DynamoStore < AbstractStore

      # Sub-class thread so we have a named thread (useful for debugging in Thread.list).
      class DynamoCleanupThread < Thread
      end

#      class DynamoCache
#        def initialize(path, prefix)
#          @path = path
#          @prefix = prefix || 'MiniProfilerDynomoStore'
#        end
#
#        #todo remove
#        def [](key)
#        end
#
#        #todo remove
#        def []=(key,val)
#        end
#
#        private
#        if RUBY_PLATFORM =~ /mswin(?!ce)|mingw|cygwin|bccwin/
#          def path(key)
#            @path + '/' + @prefix  + '_' + key.gsub(/:/, '_')
#          end
#        else
#          def path(key)
#            @path + '/' + @prefix  + '_' + key
#          end
#        end
#      end

      EXPIRES_IN_SECONDS = 60 * 60 * 24

      def initialize(args = nil)
        require 'aws-sdk' unless defined? Aws
        args ||= {}
        @mp_item_table = args[:table_name] || 'MPItems'
        @mp_view_table = args[:table_name] || 'MPViews'
        @prefix = args[:prefix] || 'MiniProfilerDynomoStore'
        @dynamo = args[:client]
        create_tables
        @expires_in_seconds = args[:expires_in] || EXPIRES_IN_SECONDS
      end

      def table_exists? table_name
        @dynamo.describe_table({table_name: table_name}).successful?
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        #ignore table doesn't exist
      end

      def create_tables
        create_table @mp_item_table
        create_table @mp_view_table
      end

      def create_table table_name
        return true if table_exists? table_name

        @dynamo.create_table({
          attribute_definitions: [ # required
            {
              attribute_name: 'id', # required
              attribute_type: 'S', # required, accepts S, N, B
            }
          ],
          table_name: table_name, # required
          key_schema: [ # required
            {
              attribute_name: 'id', # required
              key_type: 'HASH', # required, accepts HASH, RANGE
            }
          ],
          provisioned_throughput: { # required
            read_capacity_units: 5, # required
            write_capacity_units: 5, # required
          }
        })
      end

      def save(page_struct)
        @dynamo.put_item(
          table_name: @mp_item_table,
          item: {
            'id' => prefixed(page_struct[:id]),
            'content' => Marshal::dump(page_struct).force_encoding('ISO-8859-1').encode('UTF-8')
          }
        )
      end

      def load(id)
        result =  @dynamo.get_item({
          table_name: @mp_item_table,
          key: {
            'id' => prefixed(id)
          }
        }).item

        Marshal::load(result['content'].encode('ISO-8859-1')) if result
      end

      def set_unviewed(user, id)
        #TODO need to be doing updates rather than adds/deletes I think
        puts "putting #{prefixed(user)}-v - content: #{id}"
        #old_result = @dynamodb.update_item(
        #      :update_expression => update_exp,
        #      :condition_expression => condition_exp,
        #      :expression_attribute_values => exp_attribute_values,
        #      :table_name => "ProductCatalog",
        #      :key => { :Id => key_id },
        #      :return_values => "ALL_OLD",
        #    ).data.attributes
        #
        #puts @dynamo.get_item({table_name: @mp_view_table, key: {id: "#{prefixed(user)}-v"}}).item['id']
        @dynamo.update_item({
          table_name: @mp_view_table,
          condition_expression: nil,
          update_expression: "ADD entry :entry",
          expression_attribute_values: {":entry" => Set.new([id])},
          key: {id: "#{prefixed(user)}-v"}
          #item: {
          #  'id' => "#{prefixed(user)}-v",
          #  'content' => id
          #}
        })
      end

      def set_viewed(user, id)
        @dynamo.update_item({
          table_name: @mp_view_table,
          condition_expression: nil,
          update_expression: "DELETE entry :entry",
          expression_attribute_values: {":entry" => Set.new([id])},
          key: {id: "#{prefixed(user)}-v"}
        })
      rescue => e
        puts e
        puts e.backtrace()
        puts e.message
        raise e
      end

      def get_unviewed_ids(user)
        @dynamo.get_item({table_name: @mp_view_table, key: {id: "#{prefixed(user)}-v"}}).item['entry'].to_a
        #puts @dynamo.get_item({table_name: @mp_view_table, key: {id: "#{prefixed(user)}-v"}}).item['content']
      end

      def cleanup_cache
        files = Dir.entries(@path)
        @timer_struct_lock.synchronize {
          files.each do |f|
            f = @path + '/' + f
            ::File.delete f if ::File.basename(f) =~ /^mp_timers/ and (Time.now - ::File.mtime(f)) > @expires_in_seconds
          end
        }
        @user_view_lock.synchronize {
          files.each do |f|
            f = @path + '/' + f
            ::File.delete f if ::File.basename(f) =~ /^mp_views/ and (Time.now - ::File.mtime(f)) > @expires_in_seconds
          end
        }
      end

      private

      def prefixed(object)
        "#{@prefix}-#{object}"
      end
    end
  end
end
