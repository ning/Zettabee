require 'rubygems'
require 'net/ssh'
require 'open4'
include Open4
require 'optparse'
require 'date'
require 'ostruct'
require 'rdoc/usage'
require 'date'
require 'zmq'
require 'log4r'
require 'yaml'

module ZettaBee

  class ZettaBee

    @debug = false
    @nagios = false
    @verbose = false
    @cfgfile = nil
    class << self; attr_accessor :debug, :nagios, :verbose, :cfgfile; end

    attr_reader :shost, :szfs, :dhost, :dzfs, :transport, :port, :sshport, :sshkey

    ZFIX = "zettabee"

    LASTSNAP_ZFSP = "#{ZFIX}:lastsnap"
    SOURCEFS_ZFSP = "#{ZFIX}:sourcefs"
    DESTINFS_ZFSP = "#{ZFIX}:destinfs"

    def ZettaBee.readconfig()
      begin
        zfsrs = {}
        File.open(ZettaBee.cfgfile,"r").readlines.each do |cfgline|
          cfgline.chomp!
          next if cfgline[0] == 35    # need to change in 1.9
          c = cfgline.split(/\s+/)
          shost,szfs = c[0].split(':')
          dhost,dzfs = c[1].split(':')
          cfgoptions = {}
          c[2].split(',').each do |o|
            cfgoptions[o.split('=')[0]] = o.split('=')[1]
          end
          abort "duplicate destination in configuration file: #{dzfs}" if zfsrs.has_key?(dzfs)
          zfsrs[dzfs] = ZettaBee.new(shost,szfs,dhost,dzfs,cfgoptions)
        end
        zfsrs
      rescue Errno::ENOENT => e
        $stderr.write "error: #{e.message}\n"
        exit 1
      end
    end

    def initialize(shost,szfs,dhost,dzfs,cfgoptions=none)
      @shost = shost
      @szfs = szfs
      @dhost = dhost
      @dzfs = dzfs
      @transport = cfgoptions['transport']
      @port = cfgoptions['port']
      @sshport = cfgoptions['sshport']
      @sshkey = cfgoptions['sshkey']
      @logfile = "/local/var/log/#{ZFIX}/#{@port}.log"
      @zmqsock = "/local/var/run/#{ZFIX}/#{@port}.zmq"

      @log = Log4r::Logger.new(@port)
    end

    def execute(action)
      case action
        when :status then print "#{status}\n"
        when :runstatus then runstatus
        when :initialize then run(:initialize)
        when :update  then run(:update)
        when :setup then setup
        else $stderr.write "error: unknown action #{action}\n"
      end
    end

    def status
      # need to detect when state is inconsistent: for instance, if the zfsrem property exists but no
      # snapshots exist, and the like
      lsnap = lsnapsync
      lsnap_creation = getzfsproperty("creation","#{@dzfs}@#{lsnap}")

      if lsnap_creation then
        is_running? ? remstatus = "Running" : remstatus = "Idle"
        remstate = "Synchronized"

        hours,minutes,seconds,frac = Date.day_fraction_to_time(DateTime.now() - DateTime.parse(lsnap_creation))
        Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d  %s:%d  %s",@shost.ljust(8),@szfs.ljust(38),@dhost.rjust(8),@dzfs.ljust(36),remstate.ljust(14),hours,minutes,seconds,lsnap.rjust(24),@port,remstatus)
      else
        is_running? ? remstatus = "Initalizing" : remstatus = "Uninitialized"
        remstate = "Uninitialized"
        Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d  %s:%d  %s",@shost.ljust(8),@szfs.ljust(38),@dhost.rjust(8),@dzfs.ljust(36),remstate.ljust(14),0,0,0,"-".rjust(24),@port,remstatus)
      end
    end

    def is_running?
      File.exists?("/local/var/run/#{ZFIX}/#{@port}") ? true : false
    end

    def runstatus(interval=0)
      interrupted = false
      trap("INT") { interrupted = true }
      if is_running? then
        ctx = ZMQ::Context.new
        skt = ctx.socket(ZMQ::SUB)
        skt.connect statuszocket
        skt.setsockopt(ZMQ::SUBSCRIBE, '')
        STDOUT.sync = true
        begin
          loop do
            if interrupted
              skt.close
              ctx.close
              exit 0
            else
              $stdout.write skt.recv
            end
          end
        rescue RuntimeError
              $stdout.write "\n"
              skt.close
              ctx.close
              exit 0
        end
        STDOUT.sync = false
      else
        $stderr.write "#{@dhost}:#{dzfs} currently not running\n"
      end
    end

    def setup
      # create run directory
      # create log directory
      # create configuration directory and empty configuration file
    end

    def getzfsproperty(zfsproperty,zfsfs=@dzfs)
      value = nil
      begin
        pstatus = popen4("zfs get -H -o value #{zfsproperty} #{zfsfs}") do |pid, pstdin, pstdout, pstderr|
          out = pstdout.read.strip
          value = out unless out == '-'
        end
      end
      pstatus.exitstatus == 0 ? value : nil
    end

    def setzfsproperty(zfsproperty,value,zfsfs=@dzfs)
      begin
        pstatus = popen4("zfs set #{zfsproperty}=#{value} #{zfsfs}") do |pid, pstdin, pstdout, pstderr|
        end
      end
    end

    def lsnapsync=(snapshot)
      begin
        setzfsproperty(LASTSNAP_ZFSP,snapshot)
      end
    end

    def lsnapsync
      getzfsproperty(LASTSNAP_ZFSP)
    end

    def statuszocket
      "ipc://#{@zmqsock}"
    end

    def log4level
      if ZettaBee.debug
        Log4r::DEBUG
      else
        Log4r::INFO
      end
    end





























    def run(mode)

      ctx = ZMQ::Context.new()
      skt = ctx.socket(ZMQ::PUB)
      skt.bind statuszocket
      
      @log.add Log4r::FileOutputter.new("logfile", :filename => @logfile, :trunc => false, :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] #{ZFIX}:%c [%p] %l %m"), :level => log4level)
      @log.add Log4r::StdoutOutputter.new('console', :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] #{ZFIX}:%c [%p] %l %m"), :level => Log4r::DEBUG) if ZettaBee.verbose

      nextsnapshot = "#{ZFIX}.#{Time.new.strftime('%Y%m%d%H%m%S%Z')}"
      lastsnapshot = lsnapsync

      @log.info "#{@shost}:#{@szfs}@#{nextsnapshot} #{mode.to_s.upcase} #{@dhost}:#{@dzfs} START"

      case mode
        when :initialize then
          abort "error: cannot initialize: #{@dzfs} already exists" if getzfsproperty("creation",@dzfs)
          # check destination parent!
          zfssend_opts = ""
          zfsrecv_opts = "-o #{SOURCEFS_ZFSP}='#{@shost}:#{@szfs}' -o #{DESTINFS_ZFSP}='#{@dhost}:#{@dzfs}'"
        when :update then
          abort "error: cannot update: #{@dzfs} does not exists" unless getzfsproperty("creation",@dzfs)
          abort "error: cannot update: cannot determine #{@dzfs}:#{LSNAPSYNC} property" unless lastsnapshot
          zfssend_opts = "-i #{lastsnapshot}"
        else
          abort "invalid mode"
      end
      @log.debug " mode: #{mode.to_s}; zfs send options: '#{zfssend_opts}'"

      Net::SSH.start(@shost,'root',:port => @sshport,:keys => [ @sshkey ]) do |session|

        ec = nil
        eo = nil

        @log.debug " starting SSH session to #{@shost}"

        sessionchannel = session.open_channel do |channel|
          @log.debug " starting SSH session channel to #{@shost}"
          channel.exec "zfs snapshot #{@szfs}@#{nextsnapshot}" do |ch,chs|
            @log.debug "  channel.exec(zfs snapshot #{@szfs}@#{nextsnapshot})"
            if chs
              @log.debug(" creating #{@shost}:#{@szfs}@#{nextsnapshot} snapshot")
              channel.on_request "exit-status" do |ch, data|
                ec = data.read_long
                @log.debug "   exit-status received: #{ec}"
              end
              channel.on_extended_data do |ch,data|
                eo = data
              end
              channel.on_close do |ch|
                @log.debug "   channel closed"
              end
            else
              abort "could not open channel to create remote snapshot #{@shost}:#{@szfs}@#{nextsnapshot}"
            end
          end
        end
        sessionchannel.wait

        abort "failed to create remote snapshot #{@shost}:#{@szfs}@#{nextsnapshot}: #{eo}" unless ec == 0
        @log.debug " successfully created remote snapshot #{@shost}:#{@szfs}@#{nextsnapshot}"
        ec = nil
        eo = nil

#        begin

           pid, stdin, stdout, stderr = popen4("mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}")
           @log.debug " launched 'mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}' [pid #{pid}]"

           sessionchannel = session.open_channel do |channel|
             @log.debug " starting SSH session channel to #{@shost}"
             channel.exec "zfs send #{zfssend_opts} #{@szfs}@#{nextsnapshot} | mbuffer -s 128k -m 500M -R 50M -O #{@dhost}:#{@port}" do |ch,chs|
               @log.debug "  channel.exec(zfs send #{zfssend_opts} #{@szfs}@#{nextsnapshot} | mbuffer -s 128k -m 500M -R 50M -O #{@dhost}:#{@port})"
               if chs
                 @log.debug " sending #{@szfs}@#{nextsnapshot}"
                 channel.on_request "exit-status" do |ch, data|
                   ec = data.read_long
                 end
                 channel.on_data do |ch, data|
                   skt.send data
                 end
                 channel.on_extended_data do |ch, type, data|
                   skt.send data
                 end
                 channel.on_close do |ch|
                 end
               else
                  Process.kill(:TERM,pid)
                  skt.close
                  ctx.close
                  abort "failed to open channel to zfs send #{@shost}:#{@szfs}@#{nextsnapshot}"
               end
             end
           end
           sessionchannel.wait

           abort "failed to zfs send #{@shost}:#{@szfs}@#{nextsnapshot}: #{stderr.readlines()}" unless ec == 0
           self.lsnapsync = nextsnapshot

           if mode == :update

             sessionchannel = session.open_channel do |channel|
               @log.debug " starting SSH session channel to #{@shost}"
               channel.exec "zfs destroy #{@szfs}@#{lastsnapshot}" do |ch,chs|
                 if chs
                   @log.debug(" destroying #{@shost}:#{@szfs}@#{lastsnapshot} snapshot")
                   channel.on_request "exit-status" do |ch, data|
                     ec = data.read_long
                   end
                   channel.on_extended_data do |ch,data|
                     eo = data
                   end
                 else
                   abort "could not open channel to destroy remote snapshot #{@shost}:#{@szfs}@#{lastsnapshot}"
                 end
               end
             end
             sessionchannel.wait

             sleep(30) # this sleep is intended to let zfs recv wrap up its work _after_ the transfer is done

             pstatus = popen4("zfs destroy #{@dzfs}@#{lastsnapshot}") do |pid, pstdin, pstdout, pstderr|
              out = pstdout.read.strip
              err = pstderr.read.strip
             end
             @log.debug " destroying #{@dhost}:#{@dzfs}@#{lastsnapshot} snapshot"
           end

           skt.close
           ctx.close

#        end

        @log.info "#{@shost}:#{@szfs}@#{nextsnapshot} #{@dhost}:#{@dzfs} END"

      end

    end

  end
end
