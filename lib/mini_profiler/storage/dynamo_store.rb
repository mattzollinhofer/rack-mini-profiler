module Rack
  class MiniProfiler
    class DynamoStore < AbstractStore

      # Sub-class thread so we have a named thread (useful for debugging in Thread.list).
      class DynamoCleanupThread < Thread
      end

      class DynamoCache
        def initialize(path, prefix)
          @path = path
          @prefix = prefix
        end

        #todo remove
        def [](key)
        end

        #todo remove
        def []=(key,val)
        end

        private
        if RUBY_PLATFORM =~ /mswin(?!ce)|mingw|cygwin|bccwin/
          def path(key)
            @path + '/' + @prefix  + '_' + key.gsub(/:/, '_')
          end
        else
          def path(key)
            @path + '/' + @prefix  + '_' + key
          end
        end
      end

      EXPIRES_IN_SECONDS = 60 * 60 * 24

      def initialize(args = nil)
        require 'aws-sdk' unless defined? Aws
        args ||= {}
        @table_name = args[:table_name] || 'MiniProfiler'
        @client = args[:client]
        create_table unless table_exists?
        @expires_in_seconds = args[:expires_in] || EXPIRES_IN_SECONDS
      end

      def table_exists?
        @client.describe_table({table_name: @table_name}).successful?
      rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        #ignore table doesn't exist
      end

      def create_table
        @client.create_table({
          attribute_definitions: [ # required
            {
              attribute_name: 'id', # required
              attribute_type: 'S', # required, accepts S, N, B
            }
          ],
          table_name: @table_name, # required
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
        @client.put_item(
          table_name: @table_name,
          item: {
            'id' => page_struct[:id],
            'data' => Marshal::dump(page_struct).force_encoding('ISO-8859-1').encode('UTF-8')
          }
        )
      end

      def load(id)
        result =  @client.get_item({
          table_name: @table_name,
          key: {
            'id' => id
          }
        }).item

        Marshal::load(result['data'].encode('ISO-8859-1')) if result
      end

      def set_unviewed(user, id)
        @user_view_lock.synchronize {
          current = @user_view_cache[user]
          current = [] unless Array === current
          current << id
          @user_view_cache[user] = current.uniq
        }
      end

      def set_viewed(user, id)
        @user_view_lock.synchronize {
          @user_view_cache[user] ||= []
          current = @user_view_cache[user]
          current = [] unless Array === current
          current.delete(id)
          @user_view_cache[user] = current.uniq
        }
      end

      def get_unviewed_ids(user)
        @user_view_lock.synchronize {
          @user_view_cache[user]
        }
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

    end
  end
end
