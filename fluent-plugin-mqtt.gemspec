# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-mqtt"
  spec.version       = "0.0.4"
  spec.authors       = ["Yuuna Kurita"]
  spec.email         = ["yuuna.m@gmail.com"]
  spec.summary       = %q{fluentd input plugin for mqtt server}
  spec.description   = %q{fluentd input plugin for mqtt server}
  spec.homepage      = "http://github.com/yuuna/fluent-plugin-mqtt"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "mqtt"
  spec.add_development_dependency "fluentd"

end
