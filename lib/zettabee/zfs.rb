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
      class SSHError < Error; end

      attr_reader :name, :pool, :filesystem, :type, :host, :snapshot_name

      def initialize(name,host,options={})
        @host = host
        @name = name
        @pool = name.split('/')[0]
        @filesystem = name.split('/')[1..-1].join('/')
        @log = options[:log]
        @session = nil
        @type = @name.include?('@') ? :snapshot : :filesystem
        @snapshot_name = @type == :snapshot ? name.split('@')[-1] : nil
      end

      def to_s
        "#{host}:#{@name}"
      end

      def parent
        File.dirname(@name)
      end

      def get(property,session=nil)
        zfscommand = "zfs get -H -o value #{property.to_s} #{@name}"
        value,err = zfs(zfscommand,session)
        value
      end

      def set(property,value,session=nil)
        zfscommand = "zfs set #{property.to_s}=#{value.to_s} #{@name}"
        zfs(zfscommand,session)
      end

      def destroy(session=nil)
        zfscommand = "zfs destroy #{@name}"
        zfs(zfscommand,session)
      end

      def snapshot(session=nil)
        zfscommand = "zfs snapshot #{@name}"
        zfs(zfscommand,session)
      end

      def send(args)
        session = args[:session]
        pipe = args[:pipe]
        skt = args[:zmqsocket]
        options = args[:options]
        ec = nil
        err = nil
        sessionchannel = session.open_channel do |channel|
          zfscommand = "zfs send #{options} #{@name}"
          zfscommand += " | #{pipe}" unless pipe.nil?
          @log.debug "  starting SSH session channel to #{session.host}"
          channel.exec zfscommand do |ch,chs|
            @log.debug "   channel.exec(#{zfscommand}"
            if chs
              channel.on_request "exit-status" do |ch, data|
               ec = data.read_long
              end
              channel.on_data do |ch, data|
                skt.send data
              end
              channel.on_extended_data do |ch, type, data|
                skt.send data
                err = data
              end
              channel.on_close do |ch|
                @log.debug " channel closed"
              end
            else
              raise SSHError, "unable to open channel to #{session.host} to exec #{zfscommand}"
            end
          end
        end
        sessionchannel.wait
        raise ZFSError, "failed to zfs send #{@name}: #{err}" unless ec == 0
      end

      def zfs(command,session=nil)
        out = nil
        err = nil
        if session.nil? then
          pstatus = popen4(command) do |pid, pstdin, pstdout, pstderr|
#            @log.debug " launching #{command}"
            o = pstdout.readlines[0]
            out = o.strip unless o.nil?
            e = pstderr.readlines[0]
            err = e.strip unless e.nil?
          end
          raise ZFSError, "#{err}" unless pstatus.exitstatus == 0
        else
          ec = nil
          sessionchannel = session.open_channel do |channel|
            @log.debug "  starting SSH session channel to #{session.host}"
            channel.exec command do |ch,chs|
              if chs
                @log.debug "    channel.exec #{command}"
                channel.on_request "exit-status" do |ch, data|
                  ec = data.read_long
                end
                channel.on_extended_data do |ch,type,data|
                  err = data
                end
                channel.on_data do |ch,data|
                  out = data
                end
                channel.on_close do |ch|
                  @log.debug "  channel closed"
                end
              else
                raise SSHError, "could not open SSH channel"
              end
            end
          end
          sessionchannel.wait
          raise ZFSError, "#{err}" unless ec == 0
        end
        return out,err
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
