# frozen_string_literal: true

module Turbofan
  module Generators
    class CloudFormation
      module JobDefinition
        def self.generate(prefix:, step_name:, step_class:, job_role_ref:, execution_role_ref:, log_group_ref:, duckdb:, tags:, size_name: nil, size_config: nil, image_tag: nil, external_image: nil, consumable_resource_refs: [])
          cpu = size_config ? size_config[:cpu] : step_class.turbofan.default_cpu
          ram = size_config ? size_config[:ram] : step_class.turbofan.default_ram

          image = external_image || image_uri(prefix, step_name, image_tag: image_tag)

          resource_reqs = []
          resource_reqs << {"Type" => "VCPU", "Value" => cpu.to_s} if cpu
          resource_reqs << {"Type" => "MEMORY", "Value" => (ram * 1024).to_i.to_s} if ram

          # Static env vars baked into the JobDefinition — values don't
          # vary per execution, so they belong here rather than in the
          # ASL container override. ASL overrides can still add dynamic
          # values (TURBOFAN_EXECUTION_ID, TURBOFAN_STEP_NAME, etc.) on
          # top of these.
          jobdef_env = []
          if ram
            # MB representation of the allocated RAM. Consumed by Python
            # step containers' StepMetrics.emit_success to compute
            # MemoryUtilization (the Python wrapper has no access to the
            # Ruby Step class's `ram N` declaration). Single source of
            # truth: the same `ram` value that sets MEMORY resource req.
            jobdef_env << {
              "Name" => "TURBOFAN_ALLOCATED_RAM_MB",
              "Value" => (ram * 1024).to_i.to_s
            }
          end

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
              {"Name" => "nvme", "Host" => {"SourcePath" => Turbofan::ComputeEnvironment::NVME_MOUNT_PATH}}
            ]
            container["MountPoints"] = [
              {"SourceVolume" => "nvme", "ContainerPath" => Turbofan::ComputeEnvironment::NVME_MOUNT_PATH, "ReadOnly" => false}
            ]
            # NVMe path is only meaningful when the CE userdata script
            # has actually mounted /mnt/nvme AND the JobDefinition
            # mounts it into the container. The Python wrapper's
            # _setup_storage reads this env to derive its scratch dir.
            jobdef_env << {
              "Name" => "TURBOFAN_NVME_MOUNT_PATH",
              "Value" => Turbofan::ComputeEnvironment::NVME_MOUNT_PATH
            }
          end

          container["Environment"] = jobdef_env unless jobdef_env.empty?

          suffix = size_name ? "-#{size_name}" : ""
          resource_suffix = size_name ? Naming.pascal_case(size_name) : ""

          retry_cfg = retry_strategy(step_class)
          timeout_val = step_class.turbofan.timeout

          # Hash retry+timeout config into the name so CloudFormation triggers
          # REPLACEMENT when they change. Without this, CFN applies a "No
          # interruption" update which Batch silently ignores (immutable revisions).
          config_hash = Digest::SHA256.hexdigest("#{retry_cfg}#{timeout_val}")[0, 6]

          job_tags = tags.dup
          job_tags << {"Key" => "turbofan:size", "Value" => size_name.to_s} if size_name

          properties = {
            "JobDefinitionName" => "#{prefix}-jobdef-#{step_name}#{suffix}-#{config_hash}",
            "Type" => "container",
            "PlatformCapabilities" => ["EC2"],
            "PropagateTags" => true,
            "Tags" => CloudFormation.tags_hash(job_tags),
            "ContainerProperties" => container,
            "RetryStrategy" => retry_strategy(step_class)
          }

          if step_class.turbofan.timeout
            properties["Timeout"] = {"AttemptDurationSeconds" => step_class.turbofan.timeout}
          end

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

        # Batch retry attempts. Only spot reclaim ("Host EC2*" status
        # reason) is retried; application failures and all other Batch
        # failures exit immediately. Without this narrowing, app bugs
        # (exit 1) burn the entire retry budget while looking healthy.
        INFRASTRUCTURE_RETRIES = 3

        def self.retry_strategy(step_class)
          {
            "Attempts" => INFRASTRUCTURE_RETRIES,
            "EvaluateOnExit" => [
              {"OnExitCode" => "0", "Action" => "EXIT"},
              {"OnStatusReason" => "Host EC2*", "Action" => "RETRY"},
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
