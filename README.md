# Fluent::Plugin::Mqtt

Fluent plugin for MQTT protocol

## Installation

Add this line to your application's Gemfile:

    gem 'fluent-plugin-mqtt'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install fluent-plugin-mqtt

## Usage

fluent-plugin-mqtt provides Input and Output Plugins for MQTT.

Input Plugin can be used via source directive in the configuration.

```

<source>
  type mqtt
  bind 127.0.0.1
  port 1883
</source>

```

The default MQTT topic is "#". Configurable options are the following:

- bind: IP address of MQTT broker
- port: Port number of MQTT broker
- topic: Topic name to be subscribed
- username: User name for authentication
- password: Password for authentication
- format: Input parser can be chosen, e.g. json, xml

Output Plugin can be used via match directive.

```

<match topic.**>
  type mqtt
  bind 127.0.0.1
  port 1883
</match>

```

The options are basically the same as Input Plugin. The difference is the following.

- time_key: An attribute name used for timestamp field. Default is 'timestamp'.
- time_format: Output format of timestamp field. Default is ISO8601. You can specify your own format by using TimeParser.
- topic_rewrite_pattern: Regexp pattern to extract replacement words from received topic or tag name
- topic_rewrite_replacement: Topic name used for the publish using extracted pattern

The topic name or tag name, e.g. "topic", received from an event can not be published without modification because if MQTT input plugin is used as a source, the same message will become an input repeatedly. In order to support data conversion with this plugin using parser plugin, simple topic rewriting is supported. Since topic is rewritten using #gsub method, 'pattern' and 'replacement' are the same as #gsub arguments.

```

<match topic.**>
  type mqtt
  bind 127.0.0.1
  port 1883
  topic_rewrite_pattern '^([\w\/]+)$'
  topic_rewrite_replacement '\1/rewritten'
</match>

```

## Contributing

1. Fork it ( http://github.com/yuuna/fluent-plugin-mqtt/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
