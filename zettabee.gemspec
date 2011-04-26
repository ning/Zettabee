# -*- encoding: utf-8 -*-
require File.expand_path("../lib/zettabee/version", __FILE__)

Gem::Specification.new do |s|
  s.name        = "zettabee"
  s.version     = ZettaBee::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = "Gerardo López-Fernádez"
  s.email       = 'gerir@ning.com'
  s.homepage    = ''
  s.summary     = "Remote Asynchronous ZFS Mirroring"
  s.description = "Remote Asynchronous ZFS Mirroring"

  s.required_rubygems_version = ">= 1.3.5"

  s.files        = [ 'lib/zettabee.rb', 'lib/zettabee/cli.rb', 'lib/zettabee/version.rb', 'lib/zettabee/nsca.rb', 'bin/zettabee' ]
  s.executables  = [ 'zettabee' ]
  s.require_path = 'lib'
end