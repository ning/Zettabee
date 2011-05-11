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
require 'digest/md5'

module ZettaBee

  class Set

    include Enumerable

    attr_accessor :pairs

    @debug = false
    @nagios = false
    @verbose = false
    @cfgfile = nil
    class << self; attr_accessor :debug, :nagios, :verbose, :cfgfile; end

    class Error < StandardError; end
    class ConfigurationError < Error; end

    def initialize(cfgfile)
      @pairs_by_port = {}
      @pairs_by_destination = {}
      @pairs = []

      begin
        File.open(cfgfile,"r").readlines.each do |cfgline|
          cfgline.chomp!
          next if cfgline[0] == 35    # need to change in 1.9
          c = cfgline.split(/\s+/)
          shost,szfs = c[0].split(':')
          dhost,dzfs = c[1].split(':')
          cfgoptions = {}
          c[2].split(',').each do |o|
            cfgoptions[o.split('=')[0].to_sym] = o.split('=')[1]
          end
          cfgoptions[:log] =  Log4r::Logger.new(cfgoptions[:port])
          cfgoptions[:log].add Log4r::StdoutOutputter.new('console', :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] zettabee:%c [%p] %l %m"), :level => Log4r::DEBUG) if Set.verbose
          pair = Pair.new(shost,szfs,dhost,dzfs,cfgoptions)

          raise ConfigurationError, "duplicate destination in configuration file: #{dzfs}:#{cfgoptions[:port]}" if @pairs_by_destination.has_key?(dzfs)
          raise ConfigurationError, "duplicate port in configuration file: #{dzfs}:#{cfgoptions[:port]}" if @pairs_by_port.has_key?(cfgoptions[:port])

          @pairs_by_port[cfgoptions[:port]] = pair
          @pairs_by_destination[dzfs] = pair
          @pairs.push(pair)
        end
      rescue Errno::ENOENT => e
        $stderr.write "error: #{e.message}\n"
        exit 1
      end
    end

    def each &block
      @pairs.each { |pair| block.call(pair) }
    end

    def pair_by_port(port)
      @pairs_by_port.has_key?(port) ? @pairs_by_port[port] : nil
    end

    def pair_by_destination(destination)
      @pairs_by_destination.has_key?(destination) ? @pairs_by_port[destination] : nil
    end

  end

  class Pair

    attr_reader :shost, :szfs, :dhost, :dzfs, :transport, :port, :sshport, :sshkey, :clag, :wlag, :nagios_svc_description, :logfile

    ZFIX = "zettabee"

    STATE =   { :synchronized => "Synchronized",
                :uninitialized => "Uninitialized",
                :inconsistent => "Inconsistent!"
    }
    STATUS =  { :idle => "Idle",
                :running => "Running",
                :initializing => "Initializing"
    }

    class Error < StandardError; end
    class SSHError < Error; end
    class ZFSError < Error; end
    class ConfigurationError < Error; end
    class LockError < Error; end
    class StateError < Error; end
    class ActionError < Error; end

    class Info < StandardError; end
    class IsRunningInfo < Info; end

    def initialize(shost,szfs,dhost,dzfs,cfgoptions={})
      @shost = shost
      @szfs = szfs
      @dhost = dhost
      @dzfs = dzfs
      @dzfsparent = File.dirname(dzfs)
      @transport = cfgoptions[:transport]
      @port = cfgoptions[:port]
      @sshport = cfgoptions[:sshport]
      @sshkey = cfgoptions[:sshkey]
      @clag = cfgoptions[:clag].to_i
      @wlag = cfgoptions[:wlag].to_i
      @logfile = "/local/var/log/#{ZFIX}/#{@port}.log"
      @log = cfgoptions[:log]
      Set.debug ? log4level = Log4r::DEBUG : log4level = Log4r::INFO
      @log.add Log4r::FileOutputter.new("logfile", :filename => @logfile, :trunc => false, :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] #{ZFIX}:%c [%p] %l %m"), :level => log4level)
      @runstart = 0
      @nagios_svc_description = "service/#{ZFIX}:#{@dhost}:#{@port}"

      @zmqsock = "/local/var/run/#{ZFIX}/#{@port}.zmq"
      @lckfile = "/local/var/run/#{ZFIX}/#{@port}.lck"
      @fingerprint = Digest::MD5.hexdigest("#{@shost}:#{szfs}::#{@dhost}:#{@dzfs}")

      @zfsproperties = {  :source       => "#{ZFIX}:#{@fingerprint}:source",
                          :destination  => "#{ZFIX}:#{@fingerprint}:destination",
                          :lastsnap     => "#{ZFIX}:#{@fingerprint}:lastsnap",
                          :fingerprint  => "#{ZFIX}:fingerprint",
                          :creation     => "creation"
      }
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
        else raise ActionError, "unknown action #{action}"
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
      rh,rm,rs = runtime(:hms)
      zfs_exists? ? lss = lastsnapshot : lss = '-'
      print Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d%s  %s:%d  %s (%d:%02d:%02d)\n",@shost.ljust(8),@szfs.ljust(45),@dhost.rjust(8),@dzfs.ljust(36),state.ljust(14),h,m,s,lbang,lss.rjust(26),@port,status,rh,rm,rs)
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
    
    def lastsnapshot
      l = getzfsproperty(@dzfs,@zfsproperties[:lastsnap])
      l.nil? ? '-' : l
    end

    def lag(mode=nil)
      h,m,s = 0,0,0
      seconds = 0
      if zfs_exists? and is_synchronized? then
        lastsnapshot = getzfsproperty(@dzfs,@zfsproperties[:lastsnap])
        lastsnapshot_creation = getzfsproperty("#{@dzfs}@#{lastsnapshot}",@zfsproperties[:creation])
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

    def runtime(mode=nil)
      h,m,s = 0,0,0
      seconds = 0
      if is_running? then
        dt_delta = DateTime.now - DateTime.parse(File.ctime(@zmqsock).to_s)
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

    def is_locked?
      File.exists?(@lckfile)
    end

    def is_running?
      File.exists?(@zmqsock)
    end

    def is_consistent?
      consistency = false
      fingerprint = getzfsproperty(@dzfs,@zfsproperties[:fingerprint])

      if @fingerprint ==  getzfsproperty(@dzfs,@zfsproperties[:fingerprint]) then
        lastsnapshot = getzfsproperty(@dzfs,@zfsproperties[:lastsnap])
        consistency = true if getzfsproperty("#{@dzfs}@#{lastsnapshot}",@zfsproperties[:creation])
      end

      consistency
    end

    def zfs_exists?
      s = false

      begin
        getzfsproperty(@dzfs,@zfsproperties[:creation])
        s = true
      rescue ZFSError => e
        raise unless e.message.include?("dataset does not exist")
      end

      s
    end

    def is_synchronized?

      s = false

      begin
        fingerprint = getzfsproperty(@dzfs,@zfsproperties[:fingerprint])
        if @fingerprint == fingerprint then
          lastsnapshot = getzfsproperty(@dzfs,@zfsproperties[:lastsnap])
          lastsnapshot_creation = getzfsproperty("#{@dzfs}@#{lastsnapshot}",@zfsproperties[:creation])
          lastsnapshot_creation ? s = true : s = false
        end
      rescue ZFSError => e
        raise unless e.message.include?("dataset does not exist")
      end

      s
    end

    def state
      if zfs_exists? and is_synchronized? then
        STATE[:synchronized]
      else
        zfs_exists? ? STATE[:inconsistent] : STATE[:uninitialized]
      end
    end

    def status
      s = '-'
      if zfs_exists? and is_synchronized? then
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
          out = nil
          err = nil
          pstatus = popen4(zfscommand) do |pid, pstdin, pstdout, pstderr|
            o = pstdout.readlines[0]
            out = o.strip unless o.nil?
            e = pstderr.readlines[0]
            err = e.strip unless e.nil?
            zfspropval = out unless out == '-'
          end
          if pstatus.exitstatus != 0 then
            raise ZFSError, "#{err}"
          end
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
        end
      end
      zfspropval
    end

    def setzfsproperty(zfsfs,key,value,session=nil)
      zfsproperty(:set,zfsfs,key,value,session)
    end

    def getzfsproperty(zfsfs,key,session=nil)
      zfsproperty(:get,zfsfs,key,session)
    end

    def statuszocket
      "ipc://#{@zmqsock}"
    end









    def create_zfs_snapshot(zfs,snapshot,session=nil)
      _credes_zfs_snapshot(:create,zfs,snapshot,session)
    end

    def destroy_zfs_snapshot(zfs,snapshot,session=nil)
      _credes_zfs_snapshot(:destroy,zfs,snapshot,session)
    end

    def _credes_zfs_snapshot(credes,zfs,snapshot,session=nil)
      command = "false"
      case credes
        when :create  then command = "zfs snapshot #{zfs}@#{snapshot}"
        when :destroy then command = "zfs destroy #{zfs}@#{snapshot}"
      end
      ec = nil
      eo = nil
      if session.nil? then
        @log.debug "executing #{command}"
        pid, stdin, stdout, stderr = popen4("#{command}")
        ignored, status = Process::waitpid2 pid
        raise ZFSError, stderr.read.strip unless status.exitstatus == 0
      else
        sessionchannel = session.open_channel do |channel|
        @log.debug "  starting SSH session channel to #{session.host}"
          channel.exec command do |ch,chs|
            @log.debug "   channel.exec(zfs snapshot #{zfs}@#{snapshot})"
            if chs
              channel.on_request "exit-status" do |ch, data|
                ec = data.read_long
                @log.debug "    exit-status received: #{ec}"
              end
              channel.on_extended_data do |ch,data|
                eo = data
              end
              channel.on_close do |ch|
                @log.debug "  channel closed"
              end
            else
              raise SSHError, "unable to open SSH channel to #{session.host} to #{credes.to_s} snapshot #{snapshot}"
            end
          end
        end
        sessionchannel.wait
        raise ZFSError, "unable to create remote snapshot #{session.host}:#{zfs}@#{snapshot}: #{eo}" unless ec == 0
      end
    end

    def zfs_send(zfs,snapshot,zfssend_opts,dhost,port,session,skt)
      ec = nil
      sessionchannel = session.open_channel do |channel|
        @log.debug "  starting SSH session channel to #{session.host}"
        channel.exec "zfs send #{zfssend_opts} #{zfs}@#{snapshot} | mbuffer -s 128k -m 500M -R 50M -O #{dhost}:#{port}" do |ch,chs|
          @log.debug "   channel.exec(zfs send #{zfssend_opts} #{zfs}@#{snapshot} | mbuffer -s 128k -m 500M -R 50M -O #{dhost}:#{port})"
          if chs
            channel.on_request "exit-status" do |ch, data|
             ec = data.read_long
             @log.debug "   exit-status received: #{ec}"
            end
            channel.on_data do |ch, data|
              skt.send data
            end
            channel.on_extended_data do |ch, type, data|
              skt.send data
            end
            channel.on_close do |ch|
              @log.debug " channel closed"
            end
          else
            raise SSHError, "unable to open channel to zfs send #{session.host}:#{zfs}@#{snapshot}"
          end
        end
      end
      sessionchannel.wait
      raise ZFSError, "failed to zfs send #{session.host}:#{zfs}@#{snapshot}}" unless ec == 0
    end



    def run(mode)

      lock

      ctx = ZMQ::Context.new()
      skt = ctx.socket(ZMQ::PUB)
      skt.bind statuszocket

      @runstart = DateTime.parse(File.ctime(@zmqsock).to_s)

      nextsnapshot = "#{ZFIX}.#{@fingerprint}.#{Time.new.strftime('%Y%m%d%H%M%S%Z')}"
      lastsnapshot = getzfsproperty(@dzfs,@zfsproperties[:lastsnap])

      @log.info "#{@shost}:#{@szfs}@#{nextsnapshot} #{mode.to_s.upcase} #{@dhost}:#{@dzfs} START [#{@fingerprint}]"

      case mode
        when :initialize then
          raise StateError, "cannot initialize: #{@dzfs} already exists" if getzfsproperty(@dzfs,@zfsproperties[:creation])
          # check destination parent!
          zfssend_opts = ""
          zfsrecv_opts = "-o #{@zfsproperties[:source]}='#{@shost}:#{@szfs}' -o #{@zfsproperties[:destination]}='#{@dhost}:#{@dzfs}'"
        when :update then
          raise StateError, "cannot update: #{@dzfs} does not exists" unless getzfsproperty(@dzfs,@zfsproperties[:creation])
          raise StateError, "cannot update: #{lastsnapshot} snapshot does not exists" unless getzfsproperty("#{@dzfs}@#{lastsnapshot}",@zfsproperties[:creation])
          raise StateError, "error: cannot update: cannot determine #{@dzfs}:#{@zfsproperties[:lastsnap]} property" unless lastsnapshot
          zfssend_opts = "-i #{lastsnapshot}"
        else
          raise Error, "invalid mode #{mode}"
      end
      @log.debug " mode: #{mode.to_s}; zfs send options: '#{zfssend_opts}'; zfs recv options: '#{zfsrecv_opts}'"

      Net::SSH.start(@shost,'root',:port => @sshport,:keys => [ @sshkey ]) do |session|
        @log.debug " starting SSH session to #{@shost}"

        setzfsproperty(@szfs,@zfsproperties[:source],"#{@shost}:#{@szfs}",session) if mode == :initialize
        setzfsproperty(@szfs,@zfsproperties[:destination],"#{@dhost}:#{@dzfs}",session) if mode == :initialize

        create_zfs_snapshot(@szfs,nextsnapshot,session)

        pid, stdin, stdout, stderr = popen4("mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}")
        @log.debug "  launched 'mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}' [pid #{pid}]"

        sleep(30) # this sleep is intended to let zfs recv get ready

        zfs_send(@szfs,nextsnapshot,zfssend_opts,@dhost,@port,session,skt)
        ignored, status = Process::waitpid2 pid

        raise ZFSError, stderr.read.strip unless status.exitstatus == 0

        setzfsproperty(@dzfs,@zfsproperties[:fingerprint],@fingerprint) if mode == :initialize
        setzfsproperty(@dzfs,@zfsproperties[:lastsnap],nextsnapshot)
        setzfsproperty(@szfs,@zfsproperties[:lastsnap],nextsnapshot,session)

        if mode == :update
          destroy_zfs_snapshot(@szfs,lastsnapshot,session)
          destroy_zfs_snapshot(@dzfs,lastsnapshot)
        end

        skt.close
        ctx.close
        @log.info "#{@shost}:#{@szfs}@#{nextsnapshot} #{mode.to_s.upcase} #{@dhost}:#{@dzfs} END"
      end
      unlock
    end
  end
end