require 'logstash-logger/device/base'

module LogStashLogger
  module Device
    DEFAULT_TYPE = :udp

    autoload :Base, 'logstash-logger/device/base'
    autoload :Connectable, 'logstash-logger/device/connectable'
    autoload :Socket, 'logstash-logger/device/socket'
    autoload :UDP, 'logstash-logger/device/udp'
    autoload :TCP, 'logstash-logger/device/tcp'
    autoload :Unix, 'logstash-logger/device/unix'
    autoload :Redis, 'logstash-logger/device/redis'
    autoload :Kafka, 'logstash-logger/device/kafka'
    autoload :Rabbitmq, 'logstash-logger/device/rabbitmq'
    autoload :File, 'logstash-logger/device/file'
    autoload :IO, 'logstash-logger/device/io'
    autoload :Stdout, 'logstash-logger/device/stdout'
    autoload :Stderr, 'logstash-logger/device/stderr'
    autoload :Balancer, 'logstash-logger/device/balancer'
    autoload :MultiDelegator, 'logstash-logger/device/multi_delegator'

    def self.new(opts)
      opts = opts.dup
      build_device(opts)
    end

    def self.build_device(opts)
      if parsed_uri_opts = parse_uri_config(opts)
        opts.delete(:uri)
        opts.merge!(parsed_uri_opts)
      end

      type = opts.delete(:type) || DEFAULT_TYPE

      device_klass_for(type).new(opts)
    end

    def self.parse_uri_config(opts)
      if uri = opts[:uri]
        require 'uri'
        parsed = ::URI.parse(uri)
        query_params = {}
        if parsed.query
          parsed_query = URI.decode_www_form(parsed.query)
          # symbolize keys
          query_params = parsed_query.each_with_object({}) {|(k,v),h| h[k.to_sym] = v}
        end
        uri_params = case parsed.scheme.to_sym
                     when :amqp, :amqps
                       require 'amq/uri'
                       AMQ::URI.parse(uri).tap do |amq_uri|
                         amq_uri[:type] = amq_uri.delete(:scheme)
                         amq_uri[:password] = amq_uri.delete(:pass)
                       end
                     else
                       {
                         type: parsed.scheme,
                         host: parsed.host,
                         port: parsed.port,
                         path: parsed.path,
                         user: parsed.user,
                         password: parsed.password
                       }
                     end
        query_params.merge(uri_params).reject { |_, value| value.nil? }
      end
    end

    def self.device_klass_for(type)
      case type.to_sym
      when :udp then UDP
      when :tcp then TCP
      when :unix then Unix
      when :file then File
      when :redis then Redis
      when :kafka then Kafka
      when :amqp, :amqps then Rabbitmq
      when :io then IO
      when :stdout then Stdout
      when :stderr then Stderr
      when :multi_delegator then MultiDelegator
      when :balancer then Balancer
      else fail ArgumentError, 'Invalid device type'
      end
    end
  end
end
