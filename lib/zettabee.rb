require 'rubygems'
require 'net/ssh'
require 'open4'
include Open4
require 'optparse'
require 'date'
require 'ostruct'
require 'date'
require 'zmq'
require 'log4r'
require 'yaml'
require 'fileutils'
require 'digest/md5'
require 'zettabee/zfs'
require 'date'
require 'yaml'

module ZettaBee

  class Set

    include Enumerable

    attr_accessor :pairs

    class Error < StandardError; end
    class ConfigurationError < Error; end

    def initialize(options_cli)
      @pairs_by_port = {}
      @pairs_by_destination = {}
      @pairs_by_fingerprint = {}
      @pairs = []
      @options = {
        :config => "/local/etc/zettabee/zettabee.cfg",
        :zonfig => "/local/etc/zettabee/zettabee.zfg",
        :nagios => false,
        :debug => false,
        :fullstatus => false,
        :verbose => false
      }


      begin
        options_cfg = YAML::load_file (options_cli[:config] || @options[:config])
        @options.merge! options_cfg unless options_cfg.nil? or options_cfg == false
        @options.merge! options_cli
      rescue Errno::ENOENT
        raise ConfigurationError, "could not read main configuration file #{options_cli[:config]}" if options_cli[:config]
        @options[:config] = nil
      end

      begin
        File.open(@options[:zonfig],"r").readlines.each do |zfgline|
          zfgline.chomp!
          next if zfgline[0] == 35    # need to change in 1.9
          z = zfgline.split(/\s+/)
          shost,szfs = z[0].split(':')
          dhost,dzfs = z[1].split(':')
          zfgoptions = {}
          z[2].split(',').each do |o|
            zfgoptions[o.split('=')[0].to_sym] = o.split('=')[1]
          end
          pair = Pair.new(shost,szfs,dhost,dzfs,zfgoptions,@options)

          raise ConfigurationError, "duplicate destination in configuration file: #{dzfs}:#{zfgoptions[:port]}" if @pairs_by_destination.has_key?(dzfs)
          raise ConfigurationError, "duplicate port in configuration file: #{dzfs}:#{zfgoptions[:port]}" if @pairs_by_port.has_key?(zfgoptions[:port])
          raise ConfigurationError, "duplicate source::destination #{dzfs}}" if @pairs_by_fingerprint.has_key?(pair.fingerprint)

          @pairs_by_port[zfgoptions[:port]] = pair
          @pairs_by_destination[dzfs] = pair
          @pairs_by_fingerprint[pair.fingerprint] = pair
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

    attr_reader :source, :destination, :transport, :port, :sshport, :sshkey, :clag, :wlag, :mbs, :nagios_svc_description, :logfile, :fingerprint

    ZFIX = "zettabee"

    STATE =   { :synchronized   => "Synchronized",
                :uninitialized  => "Uninitialized",
                :inconsistent   => "Inconsistent!"
    }
    STATUS =  { :idle           => "Idle",
                :running        => "Running",
                :initializing   => "Initializing"
    }

    class Error < StandardError; end
    class SSHError < Error; end
    class ZFSError < Error; end
    class ConfigurationError < Error; end
    class LockError < Error; end
    class StateError < Error; end
    class ActionError < Error; end
    class ZettabeeError < Error; end

    class Info < StandardError; end
    class IsRunningInfo < Info; end

    def initialize(shost,szfs,dhost,dzfs,cfgoptions={},options)

      @transport = cfgoptions[:transport]
      @port = cfgoptions[:port]
      @sshport = cfgoptions[:sshport]
      @sshkey = cfgoptions[:sshkey]
      @clag = cfgoptions[:clag].to_i
      @wlag = cfgoptions[:wlag].to_i
      @mbs = nil

      # ---------------------------------------------------------------------------

      @fingerprint_s = "#{shost}:#{szfs}::#{dhost}:#{dzfs}"
      @fingerprint = Digest::MD5.hexdigest(@fingerprint_s)

      @zfsproperties = {  :source       => "#{ZFIX}:#{@fingerprint}:source",
                          :destination  => "#{ZFIX}:#{@fingerprint}:destination",
                          :lastsnap     => "#{ZFIX}:#{@fingerprint}:lastsnap",
                          :fingerprint  => "#{ZFIX}:fingerprint"
      }

      @zmqsock = "/local/var/run/#{ZFIX}/#{@fingerprint}.zmq"
      @lckfile = "/local/var/run/#{ZFIX}/#{@fingerprint}.lck"
      @logfile = "/local/var/log/#{ZFIX}/#{@fingerprint}.log"
      @tmpdir = "/local/var/tmp/#{ZFIX}"
      @log = Log4r::Logger.new(@fingerprint)

      @source = ZFS::Dataset.new(szfs,shost, :log => @log)
      @destination = ZFS::Dataset.new(dzfs,dhost, :log => @log)

      @source_lastsnap = nil
      @destination_lastsnap = nil

      @options = options

      begin
        lsnp = @destination.get(@zfsproperties[:lastsnap])
        @source_lastsnap = ZFS::Dataset.new("#{@source.name}@#{lsnp}",@source.host, :log => @log)
        @destination_lastsnap = ZFS::Dataset.new("#{@destination.name}@#{lsnp}",@destination.host, :log => @log)
      rescue ZFS::Dataset::ZFSError => e
        raise unless e.message.include?("dataset does not exist")
      end

      @log.add Log4r::StdoutOutputter.new('console', :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] zettabee:%c [%p] %l %m"), :level => Log4r::DEBUG) if @options[:verbose]
      @options[:debug] ? log4level = Log4r::DEBUG : log4level = Log4r::INFO
      @log.add Log4r::FileOutputter.new("logfile", :filename => @logfile, :trunc => false, :formatter => Log4r::PatternFormatter.new(:pattern => "[%d] #{ZFIX}:%c [%p] %l %m"), :level => log4level)
      @nagios_svc_description = "service/#{ZFIX}:#{@fingerprint}"

      @runstart = 0

    end

    def execute(action)
      case action
        when :setup then setup
        when :status then output_status
        when :runstatus then runstatus
        when :fingerprint then output_fingerprint
        when :logfile then output_logfile
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
      if @options[:fullstatus] then
        print Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d%s  %s:%d  %s (%d:%02d:%02d)\n",@source.host.ljust(8),@source.name.ljust(45),@destination.host.rjust(8),@destination.name.ljust(36),state.ljust(14),h,m,s,lbang,lss.rjust(26),@port,status,rh,rm,rs)
      else
        print Kernel.sprintf("%s:%s  %s:%s  %s  %3d:%02d:%02d%s  %s (%d:%02d:%02d)\n",@source.host.ljust(8),@source.name.ljust(45),@destination.host.rjust(8),@destination.name.ljust(36),state.ljust(14),h,m,s,lbang,status,rh,rm,rs)
      end
    end

    def output_fingerprint
      print Kernel.sprintf("%s:%s  %s:%s  %s\n",@source.host.ljust(8),@source.name.ljust(45),@destination.host.rjust(8),@destination.name.ljust(36),@fingerprint)
    end

    def output_logfile
      puts @logfile
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
        lastsnapshot_creation = @destination_lastsnap.get(:creation)
        dt_delta = ((DateTime.now.to_time - DateTime.parse(lastsnapshot_creation).to_time).abs).round
        h,m,s = dt_delta / 60 / 60, ( dt_delta / 60 ) % 60, dt_delta % 60
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
        dt_delta = ((DateTime.now.to_time - DateTime.parse(File.ctime(@zmqsock).to_s).to_time).abs).round
        h,m,s = dt_delta / 60 / 60, ( dt_delta / 60 ) % 60, dt_delta % 60
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

      if @fingerprint == fingerprint then
        consistency = true if @destination_lastsnap.get(:creation)
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
          elsif is_running?
            s = false
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
        $stderr.write "#{@destination.host}:#{@destination.name} currently not running\n"
      end
    end

    def runstatus_thr(interval=0)
      if is_running? then
        puts "ok, im runing"
        ctx = ZMQ::Context.new
        skt = ctx.socket(ZMQ::SUB)
        skt.connect statuszocket
        skt.setsockopt(ZMQ::SUBSCRIBE, '')
        skt.setsockopt(ZMQ::NOBLOCK, 1 )
        puts "about to enter loop"
        loop do
          skt.recv
        end
      else
        raise Error, "currently not running"
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
          zfsrecv_opts = "-F"
          zfsrecv_opts += " -o readonly=on -o #{@zfsproperties[:source]}=#{@source.host}:#{@source.name} -o #{@zfsproperties[:destination]}=#{@destination.host}:#{@destination.name}" if @destination.zpool_version >= 31
        when :update then
          raise StateError, "must initialize a pair before updating" unless is_synchronized?
          zfssend_opts = "-i #{@source_lastsnap.snapshot_name}"
          zfsrecv_opts = "-F"
          zfsrecv_opts += " -o readonly=on" if @destination.zpool_version >= 31
        else
          raise Error, "invalid mode #{mode}"
      end
      @log.debug " mode: #{mode.to_s};"
      @log.debug "   zfs send options: #{zfssend_opts}"
      @log.debug "   zfs recv options: #{zfsrecv_opts}"

      Net::SSH.start(@source.host,'root',:port => @sshport,:keys => [ @sshkey ]) do |session|

        @source.session = session

        STDOUT.sync = true

        raise ZettabeeError, "destination zpool version must be greater or equal to source zpool version" unless @destination.zpool_version >= @source.zpool_version

        if mode == :initialize
          @source.set(@zfsproperties[:source],"#{@source.host}:#{@source.name}",session) if mode == :initialize
          @source.set(@zfsproperties[:destination],"#{@destination.host}:#{@destination.name}",session) if mode == :initialize
          zfsrecv_opts += " -o quota=#{@source.get('quota',session)}" if @destination.zpool_version >= 31
        end

        nextsnapshot.snapshot(session)

        mbuffer_send_log = "/tmp/#{@fingerprint}.mbuffer.log"
        zfs_send_err = "/tmp/#{@source_lastsnap.snapshot_name}.zfssend.err"
        FileUtils.mkdir_p("#{@tmpdir}/#{@fingerprint}")
        mbuffer_recv_log = "#{@tmpdir}/#{@fingerprint}/mbuffer.log"
        mbuffer_recv_err = "#{@tmpdir}/#{@fingerprint}/mbuffer.err"


        zfsrecv_cmd = "mbuffer -s 128k -m 500M -q -I #{@port} -l #{mbuffer_recv_log} 2>#{mbuffer_recv_err} | zfs receive #{zfsrecv_opts} #{@destination.name}"
        pid, stdin, stdout, stderr = popen4(zfsrecv_cmd)
        @log.debug "  #{zfsrecv_cmd}' [pid #{pid}]"

        zettabeem_cmd = "#{File.expand_path(File.dirname(__FILE__))}/../bin/__zettabeem #{statuszocket}"
        mpid, mstdin, mstdout, mstderr = popen4(zettabeem_cmd)
        mstdin.close
        @log.debug "  launched '#{zettabeem_cmd}'"

        sleep(15) # this sleep is intended to let zfs recv get ready

        begin
         nextsnapshot.send(:options => zfssend_opts,:session => session,:pipe => "mbuffer -l #{mbuffer_send_log} -s 128k -m 500M -R 50M -O #{@destination.host}:#{@port} >/dev/null",:zfs_send_err => zfs_send_err, :zmqsocket => skt)
        rescue ZFSError
          raise ZFSError stderr.read.strip
        end

        ignored, status = Process::waitpid2 pid

        raise ZFSError, stderr.read.strip unless status.exitstatus == 0

        unless @destination.zpool_version >= 31
          @destination.set(:readonly,"on")
          @destination.set(@zfsproperties[:source],"#{@source.host}:#{@source.name}")
          @destination.set(@zfsproperties[:destination], "#{@destination.host}:#{@destination.name}")
          @destination.set(:quota,@source.get('quota',session))
        end
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
        mignored, mstatus = Process::waitpid2 mpid
        if mstatus.exitstatus == 0 then
          # summary:  0.0 KiByte in  0.0 sec - average of  0.0 KiB/s
          @mbs = mstdout.read.strip.sub("summary: ","").sub("- average of","@")
        else
          @mbs = mstderr.read.strip
        end

        session.exec("rm -f #{mbuffer_send_log} #{zfs_send_err}")
        FileUtils.rm_f([mbuffer_recv_log,mbuffer_recv_err])
        @log.info "#{@source.host}:#{nextsnapshot.name} #{mode.to_s.upcase} #{@destination.host}:#{@destination.name} END"
      end
      unlock
    end
  end
end