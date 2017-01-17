require 'bunny'

module LogStashLogger
  module Device
    class Rabbitmq < Connectable
      DEFAULT_EXCHANGE = ''
      DEFAULT_EXCHANGE_TYPE = 'direct' # fanout, topic, direct
      DEFAULT_ROUTING_KEY = 'logstash'
      DEFAULT_DURABLE = true
      DEFAULT_AUTO_DELETE = false

      def initialize(opts)
        super
        @exchange_name = opts.delete(:exchange) || DEFAULT_EXCHANGE
        @exchange_type = (opts.delete(:exchange_type) || DEFAULT_EXCHANGE_TYPE).to_sym
        @routing_key = opts.delete(:routing_key) || DEFAULT_ROUTING_KEY
        @durable = opts.delete(:durable) || DEFAULT_DURABLE
        @auto_delete = opts.delete(:auto_delete) || DEFAULT_AUTO_DELETE
        @publish_options = {}
        @buffer_group = @routing_key
        @rabbitmq_options = opts
        @conn ||= ::Bunny.new(@rabbitmq_options)
      end

      def connect
        @pid = Process.pid
        @conn.start
        @io = @conn.create_channel
        reset_buffer
      end

      def close!
        reset_buffer
        super
        @pid = nil
        @exchange = nil
        @conn.close
      end

      def connected?
        if @pid && (pid = Process.pid) != @pid
          log_warning "Fork detected: parent pid #{@pid} != current pid #{pid}"
          return false
        end
        super
      end

      def buffer_receive(*)
        if flush_timer_thread && !flush_timer_thread.alive?
          log_warning "Dead flush timer thread in process #{Process.pid}: restarting"
          @flush_timer_thread = nil
          reset_buffer
        end
        super
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
        @io
      end
    end
  end
end
