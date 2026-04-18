require "digest"
require "zlib"
require "stringio"

module Turbofan
  module Generators
    class CloudFormation
      module ChunkingLambda
        # Handler code bundled into the Lambda zip as `index.rb`. Kept as a real
        # Ruby file so syntax/lint tools can check it; read at gem-load time.
        HANDLER = File.read(File.expand_path("chunking_handler.rb", __dir__))

        # Turbofan::Router source bundled into the zip as `turbofan_router.rb`
        # for the per-step routed variant. Read from the gem's canonical Router
        # module so there's no drift between the user-facing module and the
        # Lambda-bundled copy.
        ROUTER_MODULE = File.read(File.expand_path("../../router.rb", __dir__))

        LAMBDA_RUNTIME = "ruby3.3"

        # Returns the S3 key where the handler zip should be uploaded.
        # Includes the code hash so CloudFormation detects code changes
        # (S3Key change forces Lambda to re-download the zip).
        def self.handler_s3_key(bucket_prefix)
          code_hash = Digest::SHA256.hexdigest(HANDLER)[0, 12]
          "#{bucket_prefix}/chunking-lambda/handler-#{code_hash}.zip"
        end

        # Builds a minimal zip file containing index.rb with the handler code
        def self.handler_zip
          content = HANDLER.b
          name = "index.rb".b
          crc = Zlib.crc32(content)
          size = content.bytesize

          buf = StringIO.new
          buf.set_encoding(Encoding::BINARY)

          # Local file header
          buf.write([0x04034b50, 20, 0, 0, 0, 0].pack("Vvvvvv"))
          buf.write([crc, size, size, name.bytesize, 0].pack("VVVvv"))
          buf.write(name)
          buf.write(content)

          # Central directory entry
          cd_offset = buf.pos
          buf.write([0x02014b50, 20, 20, 0, 0, 0, 0].pack("Vvvvvvv"))
          buf.write([crc, size, size, name.bytesize, 0, 0, 0, 0, 0, 0].pack("VVVvvvvvVV"))
          buf.write(name)
          cd_size = buf.pos - cd_offset

          # End of central directory
          buf.write([0x06054b50, 0, 0, 1, 1].pack("Vvvvv"))
          buf.write([cd_size, cd_offset, 0].pack("VVv"))

          buf.string
        end

        def self.generate(prefix:, bucket_prefix:, tags:)
          resources = {}
          resources.merge!(lambda_role(prefix, tags))
          resources.merge!(lambda_function(prefix, bucket_prefix, tags))
          resources
        end

        def self.lambda_function(prefix, bucket_prefix, tags)
          {
            "ChunkingLambda" => {
              "Type" => "AWS::Lambda::Function",
              "Properties" => {
                "FunctionName" => "#{prefix}-chunking",
                "Runtime" => LAMBDA_RUNTIME,
                "Handler" => "index.handler",
                "Timeout" => 300,
                "MemorySize" => 1024,
                "Role" => {"Fn::GetAtt" => ["ChunkingLambdaRole", "Arn"]},
                "Code" => {
                  "S3Bucket" => Turbofan.config.bucket,
                  "S3Key" => handler_s3_key(bucket_prefix)
                },
                "Environment" => {
                  "Variables" => {
                    "TURBOFAN_BUCKET" => Turbofan.config.bucket,
                    "TURBOFAN_BUCKET_PREFIX" => bucket_prefix,
                    "TURBOFAN_CODE_HASH" => Digest::SHA256.hexdigest(HANDLER)[0, 12]
                  }
                },
                "Tags" => tags
              }
            }
          }
        end
        private_class_method :lambda_function

        def self.lambda_role(prefix, tags)
          {
            "ChunkingLambdaRole" => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => Naming.iam_role_name("#{prefix}-chunking-lambda-role"),
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

        # Per-step routed variant — bundles the user's router.rb into the zip
        # so one Lambda invocation both routes items and chunks them.

        def self.handler_s3_key_per_step(bucket_prefix, step_name, code_hash)
          "#{bucket_prefix}/chunking-lambda/#{step_name}-#{code_hash}.zip"
        end

        def self.generate_per_step(prefix:, step_name:, bucket_prefix:, tags:, code_hash:)
          lambda_role_per_step(prefix, step_name, tags)
            .merge(lambda_function_per_step(prefix, step_name, bucket_prefix, tags, code_hash))
        end

        def self.lambda_artifacts_per_step(bucket_prefix:, step_name:, router_source:)
          code_hash = Digest::SHA256.hexdigest(HANDLER + ROUTER_MODULE + router_source)[0, 12]
          [{
            bucket: Turbofan.config.bucket,
            key: handler_s3_key_per_step(bucket_prefix, step_name, code_hash),
            body: build_zip_with_router(router_source: router_source)
          }]
        end

        def self.lambda_function_per_step(prefix, step_name, bucket_prefix, tags, code_hash)
          resource_name = "ChunkingLambda#{Naming.pascal_case(step_name)}"
          {
            resource_name => {
              "Type" => "AWS::Lambda::Function",
              "Properties" => {
                "FunctionName" => "#{prefix}-chunking-#{step_name}",
                "Runtime" => LAMBDA_RUNTIME,
                "Handler" => "index.handler",
                "Timeout" => 300,
                "MemorySize" => 1024,
                "Role" => {"Fn::GetAtt" => ["ChunkingLambdaRole#{Naming.pascal_case(step_name)}", "Arn"]},
                "Code" => {
                  "S3Bucket" => Turbofan.config.bucket,
                  "S3Key" => handler_s3_key_per_step(bucket_prefix, step_name, code_hash)
                },
                "Environment" => {
                  "Variables" => {
                    "TURBOFAN_BUCKET" => Turbofan.config.bucket,
                    "TURBOFAN_BUCKET_PREFIX" => bucket_prefix,
                    "TURBOFAN_CODE_HASH" => code_hash
                  }
                },
                "Tags" => tags
              }
            }
          }
        end
        private_class_method :lambda_function_per_step

        def self.lambda_role_per_step(prefix, step_name, tags)
          resource_name = "ChunkingLambdaRole#{Naming.pascal_case(step_name)}"
          {
            resource_name => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => Naming.iam_role_name("#{prefix}-chunking-#{step_name}-role"),
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
        private_class_method :lambda_role_per_step

        def self.build_zip_with_router(router_source:)
          build_zip_from_files(
            "index.rb" => HANDLER,
            "turbofan_router.rb" => ROUTER_MODULE,
            "router.rb" => router_source
          )
        end
        private_class_method :build_zip_with_router

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
      end
    end
  end
end
