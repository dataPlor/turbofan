module Turbofan
  class CLI < Thor
    module Deploy
      module Preflight
        def self.buildkit_available?
          _, _, status = Turbofan::Subprocess.capture("docker", "buildx", "version", allow_failure: true)
          status.success?
        rescue Errno::ENOENT
          false
        end

        def self.aws_credentials_valid?
          Aws::STS::Client.new.get_caller_identity
          true
        rescue Aws::STS::Errors::ServiceError
          false
        end

        def self.git_clean?
          # If git isn't installed (Errno::ENOENT), we intentionally let the
          # error propagate -- unlike buildkit_available? which tolerates
          # missing docker. Protected-stage deploys rely on this check to
          # gate pushes; returning a default here would mask a misconfigured
          # environment instead of failing loudly.
          #
          # Guard against the non-repo case: `git status` in a non-git
          # directory exits non-zero with an empty stdout. Without this
          # check, the empty-stdout assertion would fail-open and report
          # "clean" for a directory that isn't a repo at all.
          stdout, stderr, status = Turbofan::Subprocess.capture("git", "status", "--porcelain", allow_failure: true)
          raise "git status failed: #{stderr.strip}" unless status.success?
          stdout.strip.empty?
        end

        def self.warn_running_executions(sfn_client, state_machine_arn)
          running = sfn_client.list_executions(state_machine_arn: state_machine_arn, status_filter: "RUNNING")
          if running.executions.any?
            puts "WARNING: #{running.executions.size} execution(s) currently running."
            puts "Running executions will continue with previous job definitions."
          end
        end
      end
    end
  end
end
