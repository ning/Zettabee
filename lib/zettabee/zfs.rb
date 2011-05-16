require 'rubygems'
require 'net/ssh'
require 'open4'
include Open4
require 'date'
require 'log4r'
require 'fileutils'
require 'digest/md5'

module ZettaBee

  module ZFS

    class Dataset

      class Error < StandardError; end
      class ZFSError < Error; end

      attr_reader :name, :pool, :filesystem, :type, :host, :snapshot_name

      def initialize(name,host=nil)
        @host = host
        @name = name
        @pool = name.split('/')[0]
        @filesystem = name.split('/')[1..-1]
        @session = nil
        @type = @name.include?('@') ? :snapshot : :filesystem
        @snapshot_name = @type == :snapshot ? name.split('@')[-1] : nil
      end

      def parent
        File.dirname(@name)
      end

      def get(property)
        out = nil
        err = nil
        value = nil
        pstatus = popen4("zfs get -H -o value #{property} #{@name}") do |pid, pstdin, pstdout, pstderr|
          o = pstdout.readlines[0]
          out = o.strip unless o.nil?
          e = pstderr.readlines[0]
          err = e.strip unless e.nil?
          value = out unless out == '-'
        end
        raise ZFSError, "#{err}" unless pstatus.exitstatus == 0
        value
      end

      def set(property,value)
        err = nil
        pstatus = popen4("zfs set #{property}=#{value} #{@name}") do |pid, pstdin, pstdout, pstderr|
          e = pstderr.readlines[0]
          err = e.strip unless e.nil?
        end
        raise ZFSError, "#{err}" unless pstatus.exitstatus == 0
      end

      def destroy
        err = nil
        pstatus = popen4("zfs destroy #{@name}") do |pid, pstdin, pstdout, pstderr|
          e = pstderr.readlines[0]
          err = e.strip unless e.nil?
        end
        raise ZFSError, "#{err}" unless pstatus.exitstatus == 0
      end

      def snapshot
        err = nil
        pstatus = popen4("zfs snapshot #{@name}") do |pid, pstdin, pstdout, pstderr|
          e = pstderr.readlines[0]
          err = e.strip unless e.nil?
        end
        raise ZFSError, "#{err}" unless pstatus.exitstatus == 0
      end

      def exists?
        s = false
        begin
          self.get("creation")
          s = true
        rescue ZFSError => e
          raise unless e.message.include?("dataset does not exist")
        end
        s
      end
    end
  end
end
