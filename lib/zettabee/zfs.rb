require 'rubygems'
require 'net/ssh'
require 'open4'
include Open4
require 'date'
require 'log4r'
require 'fileutils'
require 'digest/md5'
require 'pp'

module ZettaBee

  module ZFS

    class Dataset

      class Error < StandardError; end
      class ZFSError < Error; end
      class SSHError < Error; end

      attr_reader :name, :pool, :filesystem, :type, :host, :snapshot_name
      attr_accessor :session

      def initialize(name,host,options={})
        @host = host
        @name = name
        @pool = name.split('/')[0]
        @filesystem = name.split('/')[1..-1].join('/')
        @log = options[:log]
        @session = nil
        @type = @name.include?('@') ? :snapshot : :filesystem
        @snapshot_name = @type == :snapshot ? name.split('@')[-1] : nil

        @zpool_version = nil
        @zfs_version = nil
      end

      def to_s
        "#{host}:#{@name}"
      end

      def parent
        File.dirname(@name)
      end

      def zpool_version
        return @zpool_version unless @zpool_version.nil?
        puts @session
        puts zfs("zpool get version #{@pool} | tail -1 | awk '{print $3}'",@session)[0][0]
        @zpool_version = Integer(zfs("zpool get version #{@pool} | tail -1 | awk '{print $3}'",@session)[0][0])
      end

      def zfs_version
        return @zfs_version unless @zfs_version.nil?
        begin
          @zfs_version = Integer(get(:version,@session)[0])
        rescue ZFSError
          @zfs_version = Integer(zfs("zfs get -H -o value version #{@pool}",@session)[0][0])
        end
      end

      def get(property,session=nil)
        zfscommand = "zfs get -H -o value #{property.to_s} #{@name}"
        value,err = zfs(zfscommand,session)
        value[0]
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

      def list_snapshots(session=nil)
        zfscommand = "zfs list -r -t snapshot -H -o name,creation #{@name}"
        value,err = zfs(zfscommand,session)
        value
      end

      def send(args)
        session = args[:session]
        pipe = args[:pipe]
        skt = args[:zmqsocket]
        options = args[:options]
        ec = nil
        err = nil
        sessionchannel = session.open_channel do |channel|
          zfscommand = "zfs send #{options} #{@name} 2>/tmp/#{@snapshot_name}.zfssend.err"
          zfscommand += " | #{pipe}" unless pipe.nil?
          @log.debug "  starting SSH session channel to #{session.host}"
          channel.exec zfscommand do |ch,chs|
            @log.debug "   channel.exec(#{zfscommand}"
            if chs
              channel.on_request "exit-status" do |ch, data|
               ec = data.read_long
               @log.debug "    on_request #{ec}"
              end
              channel.on_data do |ch, data|
                skt.send data
                @log.debug "    on_data #{data.strip}"
              end
              channel.on_extended_data do |ch, type, data|
                skt.send data
                @log.debug "    on_extended_data \n#{data.strip}"
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
        out = []
        err = []
        if session.nil? then
          pstatus = popen4(command) do |pid, pstdin, pstdout, pstderr|
            out = pstdout.readlines.map { |l| l.strip }
            err = pstderr.readlines.map { |l| l.strip }
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
                  o = data.strip
                  out = [o]
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
