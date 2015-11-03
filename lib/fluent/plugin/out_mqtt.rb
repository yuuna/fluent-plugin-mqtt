module Fluent
  class MqttOutput < BufferedOutput
    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('mqtt', self)

    # config_param defines a parameter. You can refer a parameter via @path instance variable
    # Without :default, a parameter is required.
    config_param :port, :integer, :default => 1883
    config_param :bind, :string, :default => '127.0.0.1'
    #config_param :topic, :string, :default => '#'
    config_param :username, :string, :default => nil
    config_param :password, :string, :default => nil
    config_param :topic_rewrite_pattern, :string, :default => '^([\w\/]+)$'
    config_param :topic_rewrite_replacement, :string, :default => '\1/rewritten'


    # This method is called before starting.
    # 'conf' is a Hash that includes configuration parameters.
    # If the configuration is invalid, raise Fluent::ConfigError.
    def configure(conf)
      super

      # You can also refer raw parameter via conf[name].
      @bind ||= conf['bind']
      #@topic ||= conf['topic']
      @port ||= conf['port']
      @username ||= conf['username']
      @password ||= conf['password']
      @topic_rewrite_pattern ||= conf['topic_rewrite_pattern']
      @topic_rewrite_replacement ||= conf['topic_rewrite_replacement']
    end

    # This method is called when starting.
    # Open sockets or files here.
    def start
      super

      $log.debug "start mqtt host: #{@bind}, port: #{@port}, username: #{@username}, password: #{@password}"
      @connect = MQTT::Client.connect(host: @bind, port: @port,
                                      username: @username, password: @password)
    end

    # This method is called when shutting down.
    # Shutdown the thread and close sockets or files here.
    def shutdown
      super

      @connect.disconnect
    end

    # This method is called when an event reaches to Fluentd.
    # Convert the event to a raw string.
    def format(tag, time, record)
      [tag, time, record].to_json + "\n"
      ## Alternatively, use msgpack to serialize the object.
      # [tag, time, record].to_msgpack
    end

    # This method is called every flush interval. Write the buffer chunk
    # to files or databases here.
    # 'chunk' is a buffer chunk that includes multiple formatted
    # events. You can use 'data = chunk.read' to get all events and
    # 'chunk.open {|io| ... }' to get IO objects.
    #
    # NOTE! This method is called by internal thread, not Fluentd's main thread. So IO wait doesn't affect other plugins.
    def write(chunk)
      data = chunk.read
      #print data
      json = json_parse(data)
      #print json[0]
      @connect.publish(json[0].gsub(Regexp.new(@topic_rewrite_pattern), @topic_rewrite_replacement), json[2].to_json)
    end

    ## Optionally, you can use chunk.msgpack_each to deserialize objects.
    #def write(chunk)
    #  chunk.msgpack_each {|(tag,time,record)|
    #  }
    #end
    
    def json_parse message
      begin
        y = Yajl::Parser.new
        y.parse(message)
      rescue
        $log.error "JSON parse error", :error => $!.to_s, :error_class => $!.class.to_s
        $log.warn_backtrace $!.backtrace         
      end
    end
  end
end
