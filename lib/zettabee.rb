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
require 'zettabee/zfs'

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
      
      @source = ZFS::Dataset.new(szfs,shost, :log => @log)
      @destination = ZFS::Dataset.new(dzfs,dhost, :log => @log)
      @source_lastsnap = nil
      @destination_lastsnap = nil

      begin
        lsnp = @destination.get(@zfsproperties[:lastsnap])
        @source_lastsnap = ZFS::Dataset.new("#{@source.name}@#{lsnp}",@source.host, :log => @log)
        @destination_lastsnap = ZFS::Dataset.new("#{@destination.name}@#{lsnp}",@destination.host, :log => @log)
      rescue ZFS::Dataset::ZFSError => e
        raise unless e.message.include?("dataset does not exist")
      end

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
      @destination.exists? ? lss = lastsnapshot : lss = '-'
#      print Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d%s  %s:%d  %s (%d:%02d:%02d)\n",@shost.ljust(8),@szfs.ljust(45),@dhost.rjust(8),@dzfs.ljust(36),state.ljust(14),h,m,s,lbang,lss.rjust(26),@port,status,rh,rm,rs)
      print Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d%s  %s (%d:%02d:%02d)\n",@shost.ljust(8),@szfs.ljust(45),@dhost.rjust(8),@dzfs.ljust(36),state.ljust(14),h,m,s,lbang,status,rh,rm,rs)
    end

    def lock
      begin
        FileUtils.mkdir(@lckfile)
        true
      rescue Errno::EEXIST => e
        if is_running? then
          raise IsRunningInfo
        else
          raise LockError, "#{@destination.name}:#{@port} lock file exists, but no transfer appears to be running"
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
      l = @destination.get(@zfsproperties[:lastsnap])
      l.nil? ? '-' : l
    end

    def lag(mode=nil)
      h,m,s = 0,0,0
      seconds = 0
      if is_synchronized? then
        lastsnapshot_creation = @destination_lastsnap.get(@zfsproperties[:creation])
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
      fingerprint = @destination.get(@zfsproperties[:fingerprint])

      if @fingerprint ==  fingerprint then
        consistency = true if @destination_lastsnap.get(@zfsproperties[:creation])
      end

      consistency
    end

    def is_synchronized?

      s = false

      begin
        if @destination.exists? then
          if @destination_lastsnap.exists? then
            if @fingerprint == @destination.get(@zfsproperties[:fingerprint])
              s = true
            else
              raise StateError, "mismatched fingerprint"
            end
          else
            raise StateError, "missing last snapshot"
          end
        end
      rescue ZFSError
        raise # unless e.message.include?("dataset does not exist")
      end

      s
    end

    def state
      begin
        if is_synchronized? then
          STATE[:synchronized]
        else
          STATE[:uninitialized]
        end
      rescue StateError => e
        @log.error "#{e.message}"
        STATE[:inconsistent]
      end
    end

    def status
      s = '-'
      if is_synchronized? then
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

    def statuszocket
      "ipc://#{@zmqsock}"
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

      nextsnapshot = ZFS::Dataset.new("#{@source.name}@#{ZFIX}.#{@fingerprint}.#{Time.new.strftime('%Y%m%d%H%M%S%Z')}",@source.host, :log => @log)

      @log.info "#{nextsnapshot} #{mode.to_s.upcase} #{@destination} START [#{@fingerprint}]"

      case mode
        when :initialize then
          raise StateError, "cannot initialize a synchronized pair" if is_synchronized?
          zfssend_opts = ""
          zfsrecv_opts = "-o #{@zfsproperties[:source]}='#{@shost}:#{@szfs}' -o #{@zfsproperties[:destination]}='#{@dhost}:#{@destination.name}'"
        when :update then
          raise StateError, "must initialize a pair before updating" unless is_synchronized?
          zfssend_opts = "-i #{@source_lastsnap.snapshot_name}"
        else
          raise Error, "invalid mode #{mode}"
      end
      @log.debug " mode: #{mode.to_s}; zfs send options: '#{zfssend_opts}'; zfs recv options: '#{zfsrecv_opts}'"

      Net::SSH.start(@shost,'root',:port => @sshport,:keys => [ @sshkey ]) do |session|
        @log.debug " starting SSH session to #{@shost}"

        @source.set(@zfsproperties[:source],"#{@source.host}:#{@source.name}",session) if mode == :initialize
        @source.set(@zfsproperties[:destination],"#{@destination.host}:#{@destination.name}",session) if mode == :initialize

        nextsnapshot.snapshot(session)

        pid, stdin, stdout, stderr = popen4("mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}")
        @log.debug "  launched 'mbuffer -s 128k -m 500M -q -I #{@port} | zfs receive -o readonly=on #{zfsrecv_opts} #{@dzfs}' [pid #{pid}]"

        sleep(15) # this sleep is intended to let zfs recv get ready

        zfs_send(@szfs,nextsnapshot.snapshot_name,zfssend_opts,@dhost,@port,session,skt)
        ignored, status = Process::waitpid2 pid

        raise ZFSError, stderr.read.strip unless status.exitstatus == 0

        @destination.set(@zfsproperties[:fingerprint],@fingerprint) if mode == :initialize
        @destination.set(@zfsproperties[:lastsnap],nextsnapshot.snapshot_name)
        @source.set(@zfsproperties[:lastsnap],nextsnapshot.snapshot_name,session)

        if mode == :update
          @source_lastsnap.destroy(session)
          @source_lastsnap = ZFS::Dataset.new("#{@source.name}@#{nextsnapshot.snapshot_name}",@source.host, :log => @log)
          @destination_lastsnap.destroy
          @destination_lastsnap = ZFS::Dataset.new("#{@destination.name}@#{nextsnapshot.snapshot_name}",@destination.host, :log => @log)
        end

        skt.close
        ctx.close
        @log.info "#{@shost}:#{@szfs}@#{nextsnapshot.snapshot_name} #{mode.to_s.upcase} #{@dhost}:#{@dzfs} END"
      end
      unlock
    end
  end
end