require "digest"
require "zlib"
require "stringio"

module Turbofan
  module Generators
    class CloudFormation
      module Lambdas
        # Shared packaging + CFN resource construction for framework Lambdas.
        #
        # Each Lambda module (ChunkingLambda, ToleranceLambda, ...) owns its
        # HANDLER source and module-specific config. It calls Packager with a
        # LambdaConfig struct to produce the CFN resource hashes and the
        # bundled zip artifact. Packager holds no state — pure functions.
        module Packager
          LAMBDA_RUNTIME = "ruby3.3"
          CODE_HASH_LENGTH = 12

          # Value object carrying everything Packager needs to emit CFN
          # resources for a single Lambda. Required keys must be set;
          # extra_policies defaults to [].
          LambdaConfig = Struct.new(
            :logical_id,         # CFN logical ID of the Lambda function (e.g., "ChunkingLambda")
            :role_logical_id,    # CFN logical ID of the IAM role (e.g., "ChunkingLambdaRole")
            :function_name,      # AWS Lambda FunctionName
            :role_name,          # IAM role name (already passed through Naming.iam_role_name)
            :s3_key,             # S3 key for the zip artifact
            :memory_size,        # MB
            :timeout,            # seconds
            :code_hash,          # hex string stored as TURBOFAN_CODE_HASH env var
            :bucket_prefix,      # TURBOFAN_BUCKET_PREFIX env var
            :tags,               # Array of {"Key" => ..., "Value" => ...}
            :extra_policies,     # Array of additional CFN policy hashes (default [])
            keyword_init: true
          ) do
            def initialize(**kwargs)
              kwargs[:extra_policies] ||= []
              super
              validate!
            end

            private

            def validate!
              %i[logical_id role_logical_id function_name role_name s3_key memory_size timeout code_hash bucket_prefix tags].each do |field|
                raise ArgumentError, "LambdaConfig #{field.inspect} cannot be nil" if self[field].nil?
              end
              raise ArgumentError, "LambdaConfig tags must be an Array" unless tags.is_a?(Array)
              raise ArgumentError, "LambdaConfig extra_policies must be an Array" unless extra_policies.is_a?(Array)
            end
          end

          # Compute a stable 12-char hex code hash across all source strings.
          # Order matters — callers must pass the same strings in the same
          # order across generate-time and artifact-upload-time.
          def self.code_hash(*sources)
            raise ArgumentError, "code_hash requires at least one source" if sources.empty?
            Digest::SHA256.hexdigest(sources.join)[0, CODE_HASH_LENGTH]
          end

          # Build the S3 object key for a Lambda zip.
          # Subdir convention: "chunking-lambda", "tolerance-lambda", etc.
          # Basename defaults to "handler" (shared Lambdas). Per-step variants
          # pass the step name as basename so each step's zip gets its own key.
          def self.handler_s3_key(bucket_prefix:, subdir:, code_hash:, basename: "handler")
            "#{bucket_prefix}/#{subdir}/#{basename}-#{code_hash}.zip"
          end

          # Build a zip containing a single index.rb file with the given source.
          def self.build_handler_zip(handler_source)
            build_zip_from_files("index.rb" => handler_source)
          end

          # Build a zip from a {filename => content} hash. Raises if empty or
          # if any content is not a String.
          def self.build_zip_from_files(files)
            raise ArgumentError, "build_zip_from_files requires at least one file" if files.empty?
            files.each do |name, content|
              raise ArgumentError, "file #{name.inspect} content must be a String" unless content.is_a?(String)
            end

            buf = StringIO.new
            buf.set_encoding(Encoding::BINARY)

            entries = []
            files.each do |name, content|
              content_b = content.b
              name_b = name.b
              crc = Zlib.crc32(content_b)
              size = content_b.bytesize
              offset = buf.pos

              # Local file header
              buf.write([0x04034b50, 20, 0, 0, 0, 0].pack("Vvvvvv"))
              buf.write([crc, size, size, name_b.bytesize, 0].pack("VVVvv"))
              buf.write(name_b)
              buf.write(content_b)

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

          # Build the CFN AWS::Lambda::Function resource hash.
          def self.lambda_function(config)
            {
              config.logical_id => {
                "Type" => "AWS::Lambda::Function",
                "Properties" => {
                  "FunctionName" => config.function_name,
                  "Runtime" => LAMBDA_RUNTIME,
                  "Handler" => "index.handler",
                  "Timeout" => config.timeout,
                  "MemorySize" => config.memory_size,
                  "Role" => {"Fn::GetAtt" => [config.role_logical_id, "Arn"]},
                  "Code" => {
                    "S3Bucket" => Turbofan.config.bucket,
                    "S3Key" => config.s3_key
                  },
                  "Environment" => {
                    "Variables" => {
                      "TURBOFAN_BUCKET" => Turbofan.config.bucket,
                      "TURBOFAN_BUCKET_PREFIX" => config.bucket_prefix,
                      "TURBOFAN_CODE_HASH" => config.code_hash
                    }
                  },
                  "Tags" => config.tags
                }
              }
            }
          end

          # Build the CFN AWS::IAM::Role resource hash with a base S3Access
          # policy plus any extra policies from config.
          def self.lambda_role(config)
            {
              config.role_logical_id => {
                "Type" => "AWS::IAM::Role",
                "Properties" => {
                  "RoleName" => config.role_name,
                  "Tags" => config.tags,
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
                  "Policies" => [s3_access_policy, *config.extra_policies]
                }
              }
            }
          end

          def self.s3_access_policy
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
          end
          private_class_method :s3_access_policy
        end
      end
    end
  end
end
