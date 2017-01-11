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
          messages.each {|message| exchange.publish(message, @publish_options) }
        end
      end

      def write_one(message)
        with_connection do
          exchange.publish(message, @publish_options)
        end
      end

      private

      def exchange
        @exchange ||= case @exchange_type
                      when :direct
                        @publish_options = { routing_key: @queue_name }
                        channel.direct(@exchange_name, exchange_options)
                      when :fanout
                        channel.fanout(@exchange_name, exchange_options)
                      when :topic
                        @publish_options = { routing_key: @queue_name }
                        channel.topic(@exchange_name, exchange_options)
                      else
                        @publish_options = { routing_key: @queue_name }
                        channel.default_exchange
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
          return @channel if @channel && @channel.open?
          @channel = @io.create_channel
        end
      end
    end
  end
end
