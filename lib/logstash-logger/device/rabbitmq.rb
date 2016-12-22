require 'bunny'

module LogStashLogger
  module Device
    class Rabbitmq < Connectable
      DEFAULT_EXCHANGE = ''
      DEFAULT_EXCHANGE_TYPE = 'direct' # fanout, topic, direct
      DEFAULT_QUEUE = 'logstash'
      DEFAULT_DURABLE = true
      DEFAULT_AUTO_DELETE = false

      attr_accessor :key

      def initialize(opts)
        super
        @exchange_name = opts.delete(:exchange) || DEFAULT_EXCHANGE
        @exchange_type = (opts.delete(:exchange_type) || DEFAULT_EXCHANGE_TYPE).to_sym
        @queue_name = opts.delete(:queue) || DEFAULT_QUEUE
        @durable = opts.delete(:durable) || DEFAULT_DURABLE
        @auto_delete = opts.delete(:auto_delete) || DEFAULT_AUTO_DELETE
        @publish_options = {}
        @buffer_group = nil # @exchange_name
        @rabbitmq_options = opts
        @conn ||= ::Bunny.new(@rabbitmq_options)
      end

      def connect
        @io = @conn.start
      end

      def write_batch(messages, group = nil)
        with_connection do
          messages.inject(exchange) {|ex, msg| ex.send(:publish, msg, @publish_options) }
        end
      end

      def write_one(message)
        puts message
        write_batch([message])
      end

      private

      def exchange
        @exchange ||= case @exchange_type
                      when :direct
                        @publish_options = { routing_key: @key }
                        ex = channel.direct(@exchange_name, exchange_options)
                        queue.bind(ex)
                        ex
                      when :fanout
                        channel.fanout(@exchange_name, exchange_options)
                        queue.bind(ex)
                        ex
                      when :topic
                        @publish_options = { routing_key: @key }
                        channel.topic(@exchange_name, exchange_options)
                        queue.bind(ex)
                        ex
                      else
                        @publish_options = { routing_key: @key }
                        channel.default_exchange
                        queue.bind(ex)
                        ex
                      end
      end

      def queue
        @queue ||= channel.queue(@queue_name, queue_options)
      end

      def exchange_options
        { durable: @durable, auto_delete: @auto_delete}
      end

      def queue_options
        { durable: @durable, auto_delete: @auto_delete}
      end

      def channel
        with_connection do
          @channel ||= @io.create_channel
        end
      end
    end
  end
end
