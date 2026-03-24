require "json"

module Turbofan
  class CLI < Thor
    module Resources
      def self.deploy(stage:)
        require "aws-sdk-cloudformation"

        resources = Turbofan::Resource.discover.select { |r| !r.turbofan_consumable.nil? }

        if resources.empty?
          puts "No resources defined."
          return
        end

        cf_client = Aws::CloudFormation::Client.new
        stack_name = "turbofan-resources-#{stage}"
        template_body = generate_template(resources, stage)

        Turbofan::Deploy::StackManager.deploy(
          cf_client,
          stack_name: stack_name,
          template_body: template_body
        )
      end

      def self.list
        resources = Turbofan::Resource.discover

        if resources.empty?
          puts "No resources defined."
          return
        end

        resources.each do |resource_class|
          key = resource_class.turbofan_key
          consumable = resource_class.turbofan_consumable
          type = resource_class.respond_to?(:turbofan_resource_type) ? resource_class.turbofan_resource_type : nil

          parts = [key.to_s, "consumable: #{consumable}"]
          parts << "type: #{type}" if type
          puts parts.join("  ")
        end
      end

      def self.generate_template(resources, stage)
        cfn_resources = {}

        resources.each do |resource_class|
          key = resource_class.turbofan_key
          logical_id = "ConsumableResource#{Turbofan::Naming.pascal_case(key)}"

          cfn_resources[logical_id] = {
            "Type" => "AWS::Batch::ConsumableResource",
            "Properties" => {
              "ConsumableResourceName" => "turbofan-#{key}-#{stage}",
              "TotalQuantity" => resource_class.turbofan_consumable,
              "ResourceType" => "REPLENISHABLE",
              "Tags" => {
                "turbofan:managed" => "true",
                "turbofan:resource" => key.to_s,
                "turbofan:stage" => stage
              }
            }
          }
        end

        outputs = {}
        resources.each do |resource_class|
          key = resource_class.turbofan_key
          logical_id = "ConsumableResource#{Turbofan::Naming.pascal_case(key)}"
          outputs["#{logical_id}Arn"] = {
            "Value" => {"Fn::GetAtt" => [logical_id, "ConsumableResourceArn"]},
            "Export" => {"Name" => resource_class.export_name(stage)}
          }
        end

        JSON.generate(
          "AWSTemplateFormatVersion" => "2010-09-09",
          "Description" => "Turbofan consumable resources (#{stage})",
          "Resources" => cfn_resources,
          "Outputs" => outputs
        )
      end

      private_class_method :generate_template
    end
  end
end
