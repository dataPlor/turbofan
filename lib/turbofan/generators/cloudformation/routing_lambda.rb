require "digest"
require "zlib"
require "stringio"

module Turbofan
  module Generators
    class CloudFormation
      module RoutingLambda
        # Generic handler wrapper that loads the user's router.rb and calls route(item).
        # The router.rb is bundled into the zip at deploy time.
        HANDLER = <<~'RUBY'
          require 'json'
          require 'aws-sdk-s3'
          require_relative 'turbofan_router'
          require_relative 'router'

          S3 = Aws::S3::Client.new

          def s3_key(*parts)
            prefix = ENV['TURBOFAN_BUCKET_PREFIX']
            key = parts.join('/')
            prefix && !prefix.empty? ? "#{prefix}/#{key}" : key
          end

          def read_items(event, bucket, execution_id)
            if event.key?('prev_step')
              prev_step = event['prev_step']
              key = s3_key(execution_id, prev_step, 'output.json')
              response = S3.get_object(bucket: bucket, key: key)
              data = JSON.parse(response.body.read)
              data.is_a?(Hash) && data.key?('items') ? data['items'] : [data]
            elsif event.key?('trigger')
              trigger = event['trigger']
              if trigger.is_a?(Hash) && trigger.key?('items_s3_uri')
                uri = trigger['items_s3_uri']
                key = uri.sub("s3://#{bucket}/", '')
                response = S3.get_object(bucket: bucket, key: key)
                data = JSON.parse(response.body.read)
                data['items']
              else
                raise "Routing Lambda: trigger must have items_s3_uri"
              end
            else
              raise "Routing Lambda: expected 'prev_step' or 'trigger' in event"
            end
          end

          def handler(event:, context:)
            bucket = ENV['TURBOFAN_BUCKET']
            execution_id = event['execution_id'] || context.aws_request_id
            step_name = event['step_name']
            router_class_name = event['router_class']

            items = read_items(event, bucket, execution_id)

            router = Object.const_get(router_class_name).new
            tagged_items = items.map do |item|
              size = router.route(item)
              item.merge('__turbofan_size' => size.to_s)
            end

            # Write tagged items to S3
            output_key = s3_key(execution_id, step_name, 'routed_input.json')
            S3.put_object(
              bucket: bucket,
              key: output_key,
              body: JSON.generate({ 'items' => tagged_items }),
              content_type: 'application/json'
            )

            { 'items_s3_uri' => "s3://#{bucket}/#{output_key}" }
          end
        RUBY

        # Inline the Turbofan::Router module so user routers can `include Turbofan::Router`
        ROUTER_MODULE = <<~'RUBY'
          module Turbofan
            module Router
              class InvalidSizeError < StandardError; end

              def self.included(base)
                base.extend(ClassMethods)
                base.instance_variable_set(:@turbofan_sizes, [])
              end

              module ClassMethods
                attr_reader :turbofan_sizes

                def sizes(*names)
                  @turbofan_sizes = names
                end
              end

              def route(input)
                raise NotImplementedError, "#{self.class} must implement #route"
              end

              def group_inputs(inputs)
                declared = self.class.turbofan_sizes
                groups = declared.each_with_object({}) { |s, h| h[s] = [] }

                inputs.each do |input|
                  size = route(input)
                  unless declared.include?(size)
                    raise InvalidSizeError, "route returned #{size.inspect}, must be one of #{declared.inspect}"
                  end
                  groups[size] << input
                end

                groups
              end
            end
          end
        RUBY

        LAMBDA_RUNTIME = "ruby3.3"

        def self.handler_s3_key(bucket_prefix, step_name, code_hash)
          "#{bucket_prefix}/routing-lambda/#{step_name}-#{code_hash}.zip"
        end

        # Build a zip containing:
        #   index.rb          - generic handler
        #   turbofan_router.rb - Turbofan::Router module
        #   router.rb          - user's router class
        #   vendor/bundle/**   - bundled gems (if Gemfile exists)
        def self.build_zip(router_source:)
          files = {
            "index.rb" => HANDLER,
            "turbofan_router.rb" => ROUTER_MODULE,
            "router.rb" => router_source
          }

          build_zip_from_files(files)
        end

        def self.build_zip_from_files(files)
          buf = StringIO.new
          buf.set_encoding(Encoding::BINARY)

          entries = []
          files.each do |name, content|
            content = content.b
            name_b = name.b
            crc = Zlib.crc32(content)
            size = content.bytesize
            offset = buf.pos

            # Local file header
            buf.write([0x04034b50, 20, 0, 0, 0, 0].pack("Vvvvvv"))
            buf.write([crc, size, size, name_b.bytesize, 0].pack("VVVvv"))
            buf.write(name_b)
            buf.write(content)

            entries << {name: name_b, crc: crc, size: size, offset: offset}
          end

          # Central directory
          cd_offset = buf.pos
          entries.each do |e|
            buf.write([0x02014b50, 20, 20, 0, 0, 0, 0].pack("Vvvvvvv"))
            buf.write([e[:crc], e[:size], e[:size], e[:name].bytesize, 0, 0, 0, 0, 0, e[:offset]].pack("VVVvvvvvVV"))
            buf.write(e[:name])
          end
          cd_size = buf.pos - cd_offset

          # End of central directory
          buf.write([0x06054b50, 0, 0, entries.size, entries.size].pack("Vvvvv"))
          buf.write([cd_size, cd_offset, 0].pack("VVv"))

          buf.string
        end
        private_class_method :build_zip_from_files

        def self.generate(prefix:, step_name:, bucket_prefix:, tags:, code_hash:)
          resources = {}
          resources.merge!(lambda_role(prefix, step_name, tags))
          resources.merge!(lambda_function(prefix, step_name, bucket_prefix, tags, code_hash))
          resources
        end

        def self.lambda_function(prefix, step_name, bucket_prefix, tags, code_hash)
          resource_name = "RoutingLambda#{Naming.pascal_case(step_name)}"
          {
            resource_name => {
              "Type" => "AWS::Lambda::Function",
              "Properties" => {
                "FunctionName" => "#{prefix}-routing-#{step_name}",
                "Runtime" => LAMBDA_RUNTIME,
                "Handler" => "index.handler",
                "Timeout" => 300,
                "MemorySize" => 512,
                "Role" => {"Fn::GetAtt" => ["RoutingLambdaRole#{Naming.pascal_case(step_name)}", "Arn"]},
                "Code" => {
                  "S3Bucket" => Turbofan.config.bucket,
                  "S3Key" => handler_s3_key(bucket_prefix, step_name, code_hash)
                },
                "Environment" => {
                  "Variables" => {
                    "TURBOFAN_BUCKET" => Turbofan.config.bucket,
                    "TURBOFAN_BUCKET_PREFIX" => bucket_prefix
                  }
                },
                "Tags" => tags
              }
            }
          }
        end
        private_class_method :lambda_function

        def self.lambda_role(prefix, step_name, tags)
          resource_name = "RoutingLambdaRole#{Naming.pascal_case(step_name)}"
          {
            resource_name => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => "#{prefix}-routing-#{step_name}-role",
                "Tags" => tags,
                "AssumeRolePolicyDocument" => {
                  "Version" => "2012-10-17",
                  "Statement" => [
                    {
                      "Effect" => "Allow",
                      "Principal" => {"Service" => "lambda.amazonaws.com"},
                      "Action" => "sts:AssumeRole"
                    }
                  ]
                },
                "ManagedPolicyArns" => [
                  "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                ],
                "Policies" => [
                  {
                    "PolicyName" => "S3Access",
                    "PolicyDocument" => {
                      "Version" => "2012-10-17",
                      "Statement" => [
                        {
                          "Effect" => "Allow",
                          "Action" => ["s3:GetObject", "s3:PutObject"],
                          "Resource" => "arn:aws:s3:::#{Turbofan.config.bucket}/*"
                        }
                      ]
                    }
                  }
                ]
              }
            }
          }
        end
        private_class_method :lambda_role

        def self.lambda_artifacts(bucket_prefix:, step_name:, router_source:)
          code_hash = Digest::SHA256.hexdigest(HANDLER + router_source)[0, 12]
          [{
            bucket: Turbofan.config.bucket,
            key: handler_s3_key(bucket_prefix, step_name, code_hash),
            body: build_zip(router_source: router_source)
          }]
        end
      end
    end
  end
end
