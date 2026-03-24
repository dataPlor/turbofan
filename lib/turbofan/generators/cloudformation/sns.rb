module Turbofan
  module Generators
    class CloudFormation
      module Sns
        def self.generate(prefix:, tags:)
          {
            "NotificationTopic" => {
              "Type" => "AWS::SNS::Topic",
              "Properties" => {
                "TopicName" => "#{prefix}-notifications",
                "Tags" => tags
              }
            }
          }
        end
      end
    end
  end
end
