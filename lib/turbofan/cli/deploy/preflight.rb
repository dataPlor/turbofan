module Turbofan
  class CLI < Thor
    module Deploy
      module Preflight
        def self.buildkit_available?
          system("docker", "buildx", "version", out: File::NULL, err: File::NULL) == true
        end

        def self.aws_credentials_valid?
          Aws::STS::Client.new.get_caller_identity
          true
        rescue Aws::STS::Errors::ServiceError
          false
        end

        def self.git_clean?
          `git status --porcelain`.strip.empty?
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
