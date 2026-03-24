module Turbofan
  module Generators
    class CloudFormation
      module Ecr
        def self.generate(prefix:, step_name:, tags:)
          resource = {
            "Type" => "AWS::ECR::Repository",
            "Properties" => {
              "RepositoryName" => "#{prefix}-ecr-#{step_name}",
              "ImageScanningConfiguration" => {"ScanOnPush" => true},
              "LifecyclePolicy" => {
                "LifecyclePolicyText" => {
                  "rules" => [
                    {
                      "rulePriority" => 1,
                      "description" => "Keep last 30 tagged images",
                      "selection" => {
                        "tagStatus" => "tagged",
                        "tagPrefixList" => ["sha-"],
                        "countType" => "imageCountMoreThan",
                        "countNumber" => 30
                      },
                      "action" => {"type" => "expire"}
                    },
                    {
                      "rulePriority" => 2,
                      "description" => "Expire untagged images after 7 days",
                      "selection" => {
                        "tagStatus" => "untagged",
                        "countType" => "sinceImagePushed",
                        "countUnit" => "days",
                        "countNumber" => 7
                      },
                      "action" => {"type" => "expire"}
                    }
                  ]
                }.to_json
              },
              "Tags" => tags
            }
          }

          {"ECR#{Naming.pascal_case(step_name)}" => resource}
        end
      end
    end
  end
end
