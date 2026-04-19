require "open3"

module Turbofan
  # Thin wrapper around Open3.capture3 with consistent error handling.
  # Replaces the mix of Kernel#system, backticks, and bare Open3 calls
  # previously scattered across the deploy and CLI layers so that:
  #
  #   * stderr is always captured (not discarded via 2>/dev/null)
  #   * non-zero exits raise a structured Error with command + streams
  #   * the exit-status-or-boolean ambiguity of `system()` is eliminated
  #
  # Callers that explicitly want to tolerate failure pass allow_failure: true
  # and receive the status object without a raise.
  module Subprocess
    class Error < StandardError
      attr_reader :command, :exit_code, :stdout, :stderr

      def initialize(command:, exit_code:, stdout:, stderr:)
        @command = command
        @exit_code = exit_code
        @stdout = stdout
        @stderr = stderr
        stderr_excerpt = stderr.to_s.strip
        super("Command failed (exit #{exit_code}): #{command.join(' ')}" +
              (stderr_excerpt.empty? ? "" : "\nstderr: #{stderr_excerpt}"))
      end
    end

    # Run a command and capture stdout/stderr/status.
    #
    # Returns:
    #   [stdout_string, stderr_string, Process::Status]
    #
    # Raises:
    #   Turbofan::Subprocess::Error on non-zero exit (unless allow_failure: true)
    #   Errno::ENOENT for missing commands (unchanged — same as Open3 direct use)
    def self.capture(*cmd, allow_failure: false, stdin_data: nil, env: nil)
      open3_args = []
      open3_args << env if env
      open3_args.concat(cmd)
      open3_kwargs = {}
      open3_kwargs[:stdin_data] = stdin_data if stdin_data

      stdout, stderr, status = Open3.capture3(*open3_args, **open3_kwargs)

      if !allow_failure && !status.success?
        raise Error.new(command: cmd, exit_code: status.exitstatus, stdout: stdout, stderr: stderr)
      end

      [stdout, stderr, status]
    end
  end
end
