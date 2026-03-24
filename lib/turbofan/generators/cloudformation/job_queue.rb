module Turbofan
  module Generators
    class CloudFormation
      module JobQueue
        def self.generate(prefix:, step_name:, compute_environment_ref:, tags:, size_name: nil)
          suffix = size_name ? "-#{size_name}" : ""
          resource_suffix = size_name ? Naming.pascal_case(size_name) : ""

          resource = {
            "Type" => "AWS::Batch::JobQueue",
            "Properties" => {
              "JobQueueName" => "#{prefix}-queue-#{step_name}#{suffix}",
              "ComputeEnvironmentOrder" => [
                {"Order" => 1, "ComputeEnvironment" => compute_environment_ref}
              ],
              "Priority" => 1,
              "State" => "ENABLED",
              "Tags" => CloudFormation.tags_hash(tags)
            }
          }

          {"JobQueue#{Naming.pascal_case(step_name)}#{resource_suffix}" => resource}
        end
      end
    end
  end
end
