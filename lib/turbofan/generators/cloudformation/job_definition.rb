module Turbofan
  module Generators
    class CloudFormation
      module JobDefinition
        def self.generate(prefix:, step_name:, step_class:, job_role_ref:, execution_role_ref:, log_group_ref:, duckdb:, tags:, size_name: nil, size_config: nil, image_tag: nil, external_image: nil, consumable_resource_refs: [])
          cpu = size_config ? size_config[:cpu] : step_class.turbofan_default_cpu
          ram = size_config ? size_config[:ram] : step_class.turbofan_default_ram

          image = external_image || image_uri(prefix, step_name, image_tag: image_tag)

          resource_reqs = []
          resource_reqs << {"Type" => "VCPU", "Value" => cpu.to_s} if cpu
          resource_reqs << {"Type" => "MEMORY", "Value" => (ram * 1024).to_s} if ram

          container = {
            "Image" => image,
            "JobRoleArn" => job_role_ref,
            "ExecutionRoleArn" => execution_role_ref,
            "ResourceRequirements" => resource_reqs,
            "LogConfiguration" => {
              "LogDriver" => "awslogs",
              "Options" => {
                "awslogs-group" => log_group_ref,
                "awslogs-stream-prefix" => "batch"
              }
            }
          }

          if duckdb
            container["Volumes"] = [
              {"Name" => "nvme", "Host" => {"SourcePath" => "/mnt/nvme"}}
            ]
            container["MountPoints"] = [
              {"SourceVolume" => "nvme", "ContainerPath" => "/mnt/nvme", "ReadOnly" => false}
            ]
          end

          suffix = size_name ? "-#{size_name}" : ""
          resource_suffix = size_name ? Naming.pascal_case(size_name) : ""

          properties = {
            "JobDefinitionName" => "#{prefix}-jobdef-#{step_name}#{suffix}",
            "Type" => "container",
            "PlatformCapabilities" => ["EC2"],
            "PropagateTags" => true,
            "Tags" => CloudFormation.tags_hash(tags + [
              {"Key" => "turbofan:retry-budget", "Value" => INFRASTRUCTURE_RETRIES.to_s}
            ]),
            "ContainerProperties" => container,
            "RetryStrategy" => retry_strategy(step_class),
            "Timeout" => {"AttemptDurationSeconds" => step_class.turbofan_timeout}
          }

          if consumable_resource_refs.any?
            properties["ConsumableResourceProperties"] = {
              "ConsumableResourceList" => consumable_resource_refs.map { |ref|
                {"ConsumableResource" => ref, "Quantity" => 1}
              }
            }
          end

          resource = {"Type" => "AWS::Batch::JobDefinition", "Properties" => properties}

          {"JobDef#{Naming.pascal_case(step_name)}#{resource_suffix}" => resource}
        end

        # Batch retry attempts for infrastructure failures (spot termination,
        # OOM, placement failures). Application failures EXIT immediately via
        # the catch-all rule, so these attempts are purely for infrastructure.
        # Set high enough to absorb spot kills without exhausting the budget.
        INFRASTRUCTURE_RETRIES = 10

        def self.retry_strategy(step_class)
          {
            "Attempts" => INFRASTRUCTURE_RETRIES,
            "EvaluateOnExit" => [
              {"OnStatusReason" => "Host EC2*", "Action" => "RETRY"},
              {"OnExitCode" => "137", "Action" => "RETRY"},
              {"OnExitCode" => "143", "Action" => "RETRY"},
              {"OnStatusReason" => "Task failed to start*", "Action" => "RETRY"},
              {"OnReason" => "*", "Action" => "EXIT"}
            ]
          }
        end
        private_class_method :retry_strategy

        def self.image_uri(prefix, step_name, image_tag: nil)
          tag = image_tag || "latest"
          {"Fn::Sub" => "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/#{prefix}-ecr-#{step_name}:#{tag}"}
        end
        private_class_method :image_uri
      end
    end
  end
end
