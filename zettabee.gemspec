# -*- encoding: utf-8 -*-
require File.expand_path("../lib/zettabee/version", __FILE__)

Gem::Specification.new do |s|
  s.name                      = "zettabee"
  s.version                   = ZettaBee::VERSION
  s.platform                  = Gem::Platform::RUBY
  s.authors                   = "Gerardo López-Fernádez"
  s.email                     = 'gerir@ning.com'
  s.homepage                  = 'https://github.com/ning/Zettabee'
  s.summary                   = "Remote Asynchronous ZFS Mirroring"
  s.description               = "Zettabee performs incremental, block-level, asynchronous replication of remote ZFS file systems through the use of zfs send and zfs recv"
  s.license                   = "Apache License, Version 2.0"
#  s.required_ruby_version     = '= 1.8.7'
  s.required_rubygems_version = ">= 1.3.5"

  s.add_dependency('log4r', '>= 1.1.9')
  s.add_dependency('net-ssh', '>= 2.1.3')
  s.add_dependency('open4', '>= 1.0.1')
  s.add_dependency('zmq', '>= 2.1.0.1')

  s.files        = [ 'lib/zettabee.rb', 'lib/zettabee/cli.rb', 'lib/zettabee/version.rb', 'lib/zettabee/nsca.rb', 'lib/zettabee/zfs.rb', 'bin/zettabee', 'bin/__zettabeem' ]
  s.executables  = [ 'zettabee', '__zettabeem' ]
  s.require_path = 'lib'
end
