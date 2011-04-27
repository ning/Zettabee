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
require 'fileutils'

module ZettaBee

  class ZettaBee

    @debug = false
    @nagios = false
    @verbose = false
    @cfgfile = nil
    class << self; attr_accessor :debug, :nagios, :verbose, :cfgfile; end

    attr_reader :shost, :szfs, :dhost, :dzfs, :transport, :port, :sshport, :sshkey, :clag, :wlag, :nagios_svc_description

    ZFIX = "zettabee"

    LASTSNAP_ZFSP = "#{ZFIX}:lastsnap"
    SOURCEFS_ZFSP = "#{ZFIX}:sourcefs"
    DESTINFS_ZFSP = "#{ZFIX}:destinfs"
    CREATION_ZFSP = "creation"

    STATE = { :synchronized => "Synchronized", :uninitialized => "Uninitialized", :inconsistent => "Inconsistent!" }
    STATUS = { :idle => "Idle", :running => "Running", :initializing => "Initializing" }

    class Error < StandardError; end
    class SSHError < Error; end
    class ZFSError < Error; end
    class ConfigurationError < Error; end
    class LockError < Error; end
    class StateError < Error; end

    class Info < StandardError; end
    class IsRunningInfo < Info; end

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
          raise ConfigurationError, "duplicate destination in configuration file: #{dzfs}" if zfsrs.has_key?(dzfs)
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
      @clag = cfgoptions['clag'].to_i
      @wlag = cfgoptions['wlag'].to_i
      @nagios_svc_description = "service/#{ZFIX}:#{@dhost}:#{@port}"
      @logfile = "/local/var/log/#{ZFIX}/#{@port}.log"
      @zmqsock = "/local/var/run/#{ZFIX}/#{@port}.zmq"
      @lckfile = "/local/var/run/#{ZFIX}/#{@port}.lck"

      @log = Log4r::Logger.new(@port)
    end

    def execute(action)
      case action
        when :setup then setup
        when :status then output_status
        when :runstatus then runstatus
        when :initialize then run(:initialize)
        when :update  then run(:update)
        when :setup then setup
        when :unlock then unlock
        else $stderr.write "error: unknown action #{action}\n"
      end
    end

    def output_status
      h,m,s= lag(:hms)
      l = lag()
      lbang = " "
      lbang = "L" if is_locked?
      unless l.nil?
        lbang = '+' if l > @wlag
        lbang = '!' if l > @clag
      end
      print Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d%s  %s:%d  %s\n",@shost.ljust(8),@szfs.ljust(45),@dhost.rjust(8),@dzfs.ljust(36),state.ljust(14),h,m,s,lbang,lastsnapshot.rjust(26),@port,status)
    end

    def lock
      begin
        FileUtils.mkdir(@lckfile)
        true
      rescue Errno::EEXIST => e
        if is_running? then
          raise IsRunningInfo
        else
          raise LockError, "#{@dzfs}:#{@port} lock file exists, but no transfer appears to be running"
        end
      end
    end

    def unlock
      if is_running? then
        raise IsRunningInfo
      else
        begin
          FileUtils.rmdir(@lckfile)
        rescue Errno::ENOENT
          true
        end
      end
    end
    
    def is_locked?
      File.exists?(@lckfile) ? true : false
    end

    def is_running?
      File.exists?(@zmqsock) ? true : false
    end

    def is_initialized?
      lastsnapshot = getzfsproperty(@dzfs,LASTSNAP_ZFSP)
      lastsnapshot_creation = getzfsproperty("#{@dzfs}@#{lastsnapshot}",CREATION_ZFSP)

      lastsnapshot_creation ? true : false 
    end
    
    def lastsnapshot
      l = getzfsproperty(@dzfs,LASTSNAP_ZFSP)
      l.nil? ? '-' : l
    end

    def lag(mode=nil)
      h,m,s = 0,0,0
      seconds = 0
      if is_initialized? then
        lastsnapshot = getzfsproperty(@dzfs,LASTSNAP_ZFSP)
        lastsnapshot_creation = getzfsproperty("#{@dzfs}@#{lastsnapshot}",CREATION_ZFSP)
        dt_delta = DateTime.now - DateTime.parse(lastsnapshot_creation)
        h,m,s,f = DateTime.day_fraction_to_time(dt_delta)
        seconds = h * 60 * 60 + m * 60 + s
      end
      if mode.nil? then
        return seconds.to_i
      elsif mode == :hms then
        return h,m,s
      elsif mode == :string then
        return Kernel.sprintf("%d:%02d:%02d",h,m,s)
      end
    end

    def state
      if is_initialized? then
        STATE[:synchronized]
      else
        if getzfsproperty(@dzfs,CREATION_ZFSP)
          STATE[:inconsistent]
        else
          STATE[:uninitialized]
        end
      end
    end

    def status
      s = '-'
      if is_initialized? then
        is_running? ? s = STATUS[:running] : s = STATUS[:idle]
      else
        is_running? ? s = STATUS[:initializing] : s = STATUS[:idle]
      end
      s
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

    def setup(rundir="/local/var/run/#{ZFIX}",logdir="/local/var/log/#{ZFIX}",cfgdir="/local/etc/#{ZFIX}")
      FileUtils.mkdir_p(rundir)
      FileUtils.mkdir_p(logdir)
      FileUtils.mkdir_p(cfgdir)
    end

    def zfsproperty(action,zfsfs,property,value=nil,session=nil)
      zfspropval = nil
      zfscommand = case action
        when :get then "zfs get -H -o value #{property} #{zfsfs}"
        when :set then "zfs set #{property}=#{value} #{zfsfs}"
        else nil
      end

      begin
        if session.nil?
          pstatus = popen4(zfscommand) do |pid, pstdin, pstdout, pstderr|
            out = pstdout.read.strip
            zfspropval = out unless out == '-'
          end
          pstatus.exitstatus == 0 ? zfspropval : nil
        else
          ec = nil
          sessionchannel = session.open_channel do |channel|
            channel.exec zfscommand do |ch,chs|
              if chs
                channel.on_request "exit-status" do |ch, data|
                  ec = data.read_long
                end
                channel.on_extended_data do |ch,data|
                  zfspropval = data
                end
              else
                raise SSHError, "could not open SSH channel"
              end
            end
          end
          sessionchannel.wait
          ec == 0 ? zfspropval : nil
        end
      end
    end

    def setzfsproperty(zfsfs,key,value,session=nil)
      zfsproperty(:set,zfsfs,key,value,session)
    end

    def getzfsproperty(zfsfs,key,session=nil)
      zfsproperty(:get,zfsfs,key,session)
    end

    def lsnapsync=(snapshot)
      begin
        setzfsproperty(@dzfs,LASTSNAP_ZFSP,snapshot)
      end
    end

    def lsnapsync
      getzfsproperty(@dzfs,LASTSNAP_ZFSP)
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

      lock

      ctx = ZMQ::Context.new()
      skt = ctx.socket(ZMQ::PUB)
      skt.bind statuszocket
      
      @log.add Log4r::FileOutputter.new("logfile", :filename => @logfile, :trunc => false, :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] #{ZFIX}:%c [%p] %l %m"), :level => log4level)
      @log.add Log4r::StdoutOutputter.new('console', :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] #{ZFIX}:%c [%p] %l %m"), :level => Log4r::DEBUG) if ZettaBee.verbose

      nextsnapshot = "#{ZFIX}.#{Time.new.strftime('%Y%m%d%H%M%S%Z')}"
      lastsnapshot = getzfsproperty(@dzfs,LASTSNAP_ZFSP)

      @log.info "#{@shost}:#{@szfs}@#{nextsnapshot} #{mode.to_s.upcase} #{@dhost}:#{@dzfs} START"

      case mode
        when :initialize then
          raise StateError, "cannot initialize: #{@dzfs} already exists" if getzfsproperty(@dzfs,CREATION_ZFSP)
          # check destination parent!
          zfssend_opts = ""
          zfsrecv_opts = "-o #{SOURCEFS_ZFSP}='#{@shost}:#{@szfs}' -o #{DESTINFS_ZFSP}='#{@dhost}:#{@dzfs}'"
        when :update then
          raise StateError, "cannot update: #{@dzfs} does not exists" unless getzfsproperty(@dzfs,CREATION_ZFSP)
          raise StateError, "cannot update: #{lastsnapshot} snapshot does not exists" unless getzfsproperty("#{@dzfs}@#{lastsnapshot}",CREATION_ZFSP)
          raise StateError, "error: cannot update: cannot determine #{@dzfs}:#{LASTSNAP_ZFSP} property" unless lastsnapshot
          zfssend_opts = "-i #{lastsnapshot}"
        else
          raise Error, "invalid mode"
      end
      @log.debug " mode: #{mode.to_s}; zfs send options: '#{zfssend_opts}'; zfs recv options: '#{zfsrecv_opts}'"

      Net::SSH.start(@shost,'root',:port => @sshport,:keys => [ @sshkey ]) do |session|

        ec = nil
        eo = nil

        @log.debug " starting SSH session to #{@shost}"

        sessionchannel = session.open_channel do |channel|
          @log.debug " starting SSH session channel to #{@shost}"
          channel.exec "zfs snapshot #{@szfs}@#{nextsnapshot}" do |ch,chs|
            @log.debug "  channel.exec(zfs snapshot #{@szfs}@#{nextsnapshot})"
            if chs
              @log.debug("  creating #{@shost}:#{@szfs}@#{nextsnapshot} snapshot")
              channel.on_request "exit-status" do |ch, data|
                ec = data.read_long
                @log.debug "   exit-status received: #{ec}"
              end
              channel.on_extended_data do |ch,data|
                eo = data
              end
              channel.on_close do |ch|
                @log.debug "  channel closed"
              end
            else
              raise SSHError, "unable to open SSH channel to #{@shost} to create snapshot #{nextsnapshot}"
            end
          end
        end
        sessionchannel.wait

        raise ZFSError, "unable to create remote snapshot #{@shost}:#{@szfs}@#{nextsnapshot}: #{eo}" unless ec == 0
        @log.debug " successfully created remote snapshot #{@shost}:#{@szfs}@#{nextsnapshot}"
        ec = nil
        eo = nil

#        begin

          pid, stdin, stdout, stderr = popen4("mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}")
          @log.debug " launched 'mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}' [pid #{pid}]"

          sleep(30) # this sleep is intended to let zfs recv get ready

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
                raise SSHError, "unable to open channel to zfs send #{@shost}:#{@szfs}@#{nextsnapshot}"
              end
            end
          end
          sessionchannel.wait

          raise ZFSError, "failed to zfs send #{@shost}:#{@szfs}@#{nextsnapshot}: #{stderr.readlines()}" unless ec == 0
          setzfsproperty(@dzfs,LASTSNAP_ZFSP,nextsnapshot)
          setzfsproperty(@szfs,LASTSNAP_ZFSP,nextsnapshot,session)

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
                  raise SSHError, "unable to open SSH channel to #{@shost} to destroy snapshot #{lastsnapshot}"
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

      unlock

    end

  end
end
