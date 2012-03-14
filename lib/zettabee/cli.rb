require 'rubygems'
require 'optparse'
require 'ostruct'
require 'date'
require 'log4r'
require 'yaml'

module ZettaBee

  class CLI

    ME = "zettabee"

    NAGIOS_OK = 0
    NAGIOS_WARNING = 1
    NAGIOS_CRITICAL = 2
    NAGIOS_UNKNOWN = 3

    attr_reader :options

    def initialize(arguments)
      @arguments = arguments
      @options = OpenStruct.new

      # defaults
      @options.cfgfile = "/local/etc/zettabee/zettabee.cfg"
      @options.debug = false
      @options.nagios = false
      @options.verbose = false
      @action = nil
      @destination = nil
      @pairs = {}

    end

    def run
      if parsed_options? && arguments_valid?
        process_arguments
        process_command
      else
        output_help
        exit 127
      end
    end

    protected

      def parsed_options?
        opts = OptionParser.new
        opts.on('-V', '--version')                                                          { output_version ; exit 0 }
        opts.on('-h', '--help')                                                             { output_help ; exit 0}
        opts.on('-d', '--debug', "Debug mode" )                                             { @options.debug = true }
        opts.on('-v', '--verbose', "Verbose Mode")                                          { @options.verbose = true }
        opts.on('-N', '--nagios NAGIOSHOST', String, "Nagios Host for NSCA")                { |nagioshost| @options.nagios = nagioshost }
        opts.on('-c', '--config CONFIG', String, "Configuration file location")             { |cfgfile| @options.cfgfile = cfgfile }
        opts.on('-F', '--full-status', "Show full status")                                  { @options.fullstatus = true }

        opts.parse!(@arguments) rescue return false

        process_options
        true

      end

      def arguments_valid?
        if @arguments.length < 1 or @arguments.length > 2
          $stderr.puts "#{ME}: error: invalid number of arguments: #{@arguments}"
          return false
        end
        true
      end

      def process_options
        Set.nagios = @options.nagios if @options.nagios
        Set.debug = @options.debug if @options.debug
        Set.cfgfile = @options.cfgfile if @options.cfgfile
        Set.verbose = @options.verbose if @options.verbose
        Set.fullstatus = @options.fullstatus if @options.fullstatus
      end

      def process_arguments
        case @arguments.length
          when 2 then
            @action = @arguments[0]
            @destination = @arguments[1].split(':')[-1]
          when 1 then
            @action = @arguments[0]
            @destination = nil
          else
            return false
        end

        # verify action is valid
      end

      def process_command

        begin
          @zettabees = Set.new(@options.cfgfile)
          execpairs = []
        rescue Set::ConfigurationError => e
          $stderr.write "#{ME}: error: #{e.message}\n"
          exit 1
        end
        
        if @destination then
          # a destination can be fully specified (a/b/c) or using the last component (c)
          if @destination.split('/').length > 1
            execpairs.push(@zettabees.pair_by_destination[@destination])
          elsif @destination.split('/').length == 1
            @zettabees.each do |pair|
              if pair.destination.name.split('/')[-1] == @destination
                execpairs.push(pair)
              end
            end
          end
          abort "error: invalid destination: #{@destination}" unless execpairs.length == 1
        else
          if ["status","fingerprint"].include?(@action) then
            @zettabees.each { |pair| execpairs.push(pair) }
          else
            abort "error: only status, fingerprint actions can be run against all destinations"
          end
        end

        execpairs.each do |pair|
          sn = NSCA.new(@options.nagios,pair.destination.host,pair.nagios_svc_description)
          sn_rt = NAGIOS_OK
          sn_svc_out = "#{@action.to_s.upcase} #{pair.destination}: #{Time.now.asctime}: "

          begin
            pair.execute(@action.to_sym)
            sn_svc_out += "#{pair.mbs}: OK"
          rescue Pair::IsRunningInfo
            if @options.nagios then
              if pair.state == Pair::STATE[:inconsistent] then # really need is_consistent? method
                sn_rt = NAGIOS_CRITICAL
                sn_svc_out += "state is #{Pair::STATE[:inconsistent]}"
              elsif pair.lag >= pair.clag then
                sn_rt = NAGIOS_CRITICAL
                sn_svc_out += "lag is CRITICAL"
              elsif pair.lag >= pair.wlag then
                sn_rt = NAGIOS_WARNING
                sn_svc_out += "lag is WARNING"
              else
                sn_svc_out += "#{pair.status}: OK"
              end
            else
              pair.execute(:status)
            end
          rescue => e
            $stderr.write "#{ME}: error: #{@action.to_s.upcase} #{pair.destination}: #{e.message}\n"
            sn_rt = NAGIOS_UNKNOWN
            sn_svc_out += "#{e.message}"
          end

          begin
            sn.send_nsca(sn_rt,sn_svc_out) if @options.nagios
          rescue NSCA::SendNSCAError => e
            $stderr.write "#{ME}: error: send_nsca failed: #{e.message}\n"
          end
        end

      end

      def output_version
        $stderr.write "#{VERSION}\n"
      end

      def output_help
        $stderr.write "#{ME} [<options>] <action> [<destination>]\n"
        $stderr.write "\n"
        $stderr.write " <action>   setup                     : initial #{ME} setup\n"
        $stderr.write "            status [<destination>]    : display status for all destinations or <destination>\n"
        $stderr.write "            runstatus <destination>   : show running status for <destination>\n"
        $stderr.write "            initialize <destination>  : perform first sync for <destination>\n"
        $stderr.write "            update <destination>      : update sync for <destination>\n"
        $stderr.write "            \n"
        $stderr.write " <options>  -d, --debug               : debug (to logfile)\n"
        $stderr.write "            -v, --verbose             : verbose (to console)\n"
        $stderr.write "            -n, --nagios <nagioshost> : send NSCA result to <nagioshost>\n"
        $stderr.write "            -c, --config <configfile> : read configuration from <configfile>\n"
        $stderr.write "            \n"
        $stderr.write " <destination> is [<host>:]<filesystem>\n"
      end

      def output_options(exit_status)

        puts "Options:\n"

        @options.marshal_dump.each do |name, val|
          puts "  #{name} = #{val}"
        end
        exit exit_status
      end
  end
end