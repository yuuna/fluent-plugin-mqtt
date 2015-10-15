require 'fluent/mixin/config_placeholders'
require 'json'

module Fluent
  class MqttOutput < Output
    Plugin.register_output('mqtt', self)

    include Fluent::SetTagKeyMixin
    config_set_default :include_tag_key, false

    include Fluent::SetTimeKeyMixin
    config_set_default :include_time_key, true

    config_param :port, :integer, :default => 1883
    config_param :host, :string, :default => '127.0.0.1'
    config_param :format, :string, :default => 'none'
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil
    config_param :ssl, :bool, :default => nil
    config_param :ca, :string, :default => nil
    config_param :key, :string, :default => nil
    config_param :cert, :string, :default => nil

    require 'mqtt'

    def configure(conf)
      super
      @bind ||= conf['bind']
      @topic ||= conf['topic']
      @port ||= conf['port']
    end

    def start
      $log.debug "start mqtt #{@bind}"
      opts = {host: @host,
              port: @port,
              username: @username,
              password: @password}
      opts[:ssl] = @ssl if @ssl
      opts[:ca_file] = @ca if @ca
      opts[:crt_file] = @crt if @crt
      opts[:key_file] = @key if @key
      @connect = MQTT::Client.connect(opts)
    end

    def emit(tag, es, chain)
      es.each {|time,record|
        @connect.publish(tag, JSON.generate(record))
      }
      $log.flush

      chain.next
    end

    def shutdown
      @thread.kill
      @connect.disconnect
    end
  end
end
