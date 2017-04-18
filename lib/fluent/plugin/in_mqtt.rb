require 'fluent/plugin/input'
require 'fluent/plugin/parser'

module Fluent::Plugin
  class MqttInput < Input
    Fluent::Plugin.register_input('mqtt', self)

    include Fluent::SetTagKeyMixin
    config_set_default :include_tag_key, false

    include Fluent::SetTimeKeyMixin
    config_set_default :include_time_key, true

    # Define `router` method of v0.12 to support v0.10 or earlier
    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end


    config_param :port, :integer, :default => 1883
    config_param :bind, :string, :default => '127.0.0.1'
    config_param :topic, :string, :default => '#'
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

      configure_parser(conf)
    end

    def configure_parser(conf)
      @parser = Fluent::Plugin.new_parser(@format)
      @parser.configure(conf)
    end

    # Return [time (if not available return now), message]
    def parse(message)
      @parser.parse(message) {|time, record|
        return (time || Fluent::Engine.now), record
      }
    end

    def start
      super
      $log.debug "start mqtt #{@bind}"
      opts = {host: @bind,
              port: @port}
      opts[:username] =  @username if @username
      opts[:password] = @password if @password
      opts[:ssl] = @ssl if @ssl
      opts[:ca_file] = @ca if @ca
      opts[:cert_file] = @cert if @cert
      opts[:key_file] = @key if @key
      @connect = MQTT::Client.connect(opts)
      @connect.subscribe(@topic)

      @thread = Thread.new do
        @connect.get do |topic,message|
          topic.gsub!("/","\.")
          $log.debug "#{topic}: #{message}"
          begin
            time, record = self.parse(message)
          rescue Exception => e
            $log.error e
          end
          emit topic, record, time
        end
      end
    end


    def emit topic, message, time = Fluent::Engine.now
      if message.class == Array
        message.each do |data|
          $log.debug "#{topic}: #{data}"
          router.emit(topic , time , data)
        end
      else
        router.emit(topic , time , message)
      end
    end

    def shutdown
      @thread.kill
      @connect.disconnect
      super
    end
  end
end
