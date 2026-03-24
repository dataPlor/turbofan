module Turbofan
  module Generators
    class CloudFormation
      module Logs
        def self.generate(prefix:, step_name:, tags:)
          resource = {
            "Type" => "AWS::Logs::LogGroup",
            "Properties" => {
              "LogGroupName" => "#{prefix}-logs-#{step_name}",
              "RetentionInDays" => 7,
              "Tags" => tags
            }
          }

          {"LogGroup#{Naming.pascal_case(step_name)}" => resource}
        end
      end
    end
  end
end
