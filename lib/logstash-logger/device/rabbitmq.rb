require 'bunny'

module LogStashLogger
  module Device
    class Rabbitmq < Connectable
      DEFAULT_EXCHANGE = ''
      DEFAULT_EXCHANGE_TYPE = 'direct' # fanout, topic, direct
      DEFAULT_ROUTING_KEY = 'logstash'
      DEFAULT_DURABLE = true
      DEFAULT_AUTO_DELETE = false

      attr_accessor :key

      def initialize(opts)
        super
        @exchange_name = opts.delete(:exchange) || DEFAULT_EXCHANGE
        @exchange_type = (opts.delete(:exchange_type) || DEFAULT_EXCHANGE_TYPE).to_sym
        @routing_key = opts.delete(:routing_key) || DEFAULT_ROUTING_KEY
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
          messages.each {|message| exchange.publish(message, publish_options) }
        end
      end

      def write_one(message)
        with_connection do
          exchange.publish(message, publish_options)
        end
      end

      private

      def exchange
        @exchange ||= case @exchange_type
                      when :direct
                        channel.direct(@exchange_name, exchange_options)
                      when :fanout
                        channel.fanout(@exchange_name, exchange_options)
                      when :topic
                        channel.topic(@exchange_name, exchange_options)
                      else
                        channel.default_exchange
                      end
      end

      def exchange_options
        { durable: @durable, auto_delete: @auto_delete}
      end

      def publish_options
        {}.tap do |h|
          h[:routing_key] = @routing_key if @routing_key
        end
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
