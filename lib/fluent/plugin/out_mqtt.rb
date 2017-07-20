require 'mqtt'
require 'msgpack'
require 'fluent/plugin/output'

module Fluent::Plugin
  class OutMqtt < Output
    Fluent::Plugin.register_output('mqtt', self)

    helpers :compat_parameters, :inject, :formatter

    DEFAULT_BUFFER_TYPE = "memory"

    config_set_default :include_tag_key, false
    config_set_default :include_time_key, true

    config_param :port, :integer, :default => 1883
    config_param :bind, :string, :default => '127.0.0.1'
    config_param :topic, :string, :default => 'td-agent'
    config_param :format, :string, :default => 'none'
    config_param :client_id, :string, :default => nil
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil
    config_param :ssl, :bool, :default => nil
    config_param :ca, :string, :default => nil
    config_param :key, :string, :default => nil
    config_param :cert, :string, :default => nil
    config_param :retain, :bool, :default => true

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
      config_set_default :chunk_keys, ['tag']
    end

    config_section :inject do
      config_set_default :time_key, "time"
      config_set_default :time_type, "string"
      config_set_default :time_format, "%Y-%m-%dT%H:%M:%S%z"
    end

    def initialize
      super

      @clients = {}
      @connection_options = {}
      @collection_options = {:capped => false}
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer, :inject, :formatter)
      super
      @bind ||= conf['bind']
      @topic ||= conf['topic']
      @port ||= conf['port']
      @formatter = formatter_create
      if conf.has_key?('buffer_chunk_limit')
        #check buffer_size
        conf['buffer_chunk_limit'] = available_buffer_chunk_limit(conf)
      end
    end

    def start

      log.debug "start mqtt #{@bind}"
      opts = {host: @bind,
              port: @port}
      opts[:client_id] = @client_id if @client_id
      opts[:username] =  @username if @username
      opts[:password] = @password if @password
      opts[:ssl] = @ssl if @ssl
      opts[:ca_file] = @ca if @ca
      opts[:cert_file] = @cert if @cert
      opts[:key_file] = @key if @key
      @connect = MQTT::Client.connect(opts)
      super
    end

    def shutdown
      @connect.disconnect
      super
    end

    def format(tag, time, record)
      [time, record].to_msgpack
    end

    def formatted_to_msgpack_binary
      true
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
      tag = chunk.metadata.tag
      chunk.msgpack_each { |time, record|
        record = inject_values_to_record(tag, time, record)
        log.debug "write #{@topic} #{@formatter.format(tag,time,record)}"
        @connect.publish(@topic, @formatter.format(tag,time,record), retain=@retain)
      }
    end

    private
    # Following limits are heuristic. BSON is sometimes bigger than MessagePack and JSON.
    LIMIT_MQTT = 2 * 1024  # 2048kb

    def available_buffer_chunk_limit(conf)
      if conf['buffer_chunk_limit'] > LIMIT_MQTT
        log.warn ":buffer_chunk_limit(#{conf['buffer_chunk_limit']}) is large. Reset :buffer_chunk_limit with #{LIMIT_MQTT}"
        LIMIT_MQTT
      else
        conf['buffer_chunk_limit']
      end
    end
  end
end
