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

This client works as ONLY MQTT client.
MQTT topic is set "#".

```

<source>
  type mqtt
  bind 127.0.0.1
  port 1883
</source>

```

## Contributing

1. Fork it ( http://github.com/yuuna/fluent-plugin-mqtt/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
