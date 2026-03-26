module Turbofan
  module Generators
    class CloudFormation
      module Iam
        def self.generate(prefix:, steps:, tags:, pipeline_name:, resources: {}, has_fan_out: false, has_tolerated_fan_out: false)
          secret_arns = collect_secret_arns(steps, resources)
          iam_resources = {}
          iam_resources.merge!(job_role(prefix, steps, tags, pipeline_name, secret_arns))
          iam_resources.merge!(execution_role(prefix, steps, tags, secret_arns))
          iam_resources.merge!(sfn_role(prefix, tags, has_fan_out: has_fan_out, has_tolerated_fan_out: has_tolerated_fan_out))
          iam_resources
        end

        def self.job_role(prefix, steps, tags, pipeline_name, secret_arns)
          s3_statements = collect_s3_statements(prefix, steps)
          log_group_arns = steps.map { |sname, _| "arn:aws:logs:*:*:log-group:#{prefix}-logs-#{sname}:*" }

          policies = [
            {
              "PolicyName" => "S3Access",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => s3_statements
              }
            },
            {
              "PolicyName" => "CloudWatchMetrics",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [
                  {
                    "Effect" => "Allow",
                    "Action" => ["cloudwatch:PutMetricData"],
                    "Resource" => "*",
                    "Condition" => {"StringEquals" => {"cloudwatch:namespace" => "Turbofan/#{pipeline_name}"}}
                  }
                ]
              }
            },
            {
              "PolicyName" => "CloudWatchLogs",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [
                  {
                    "Effect" => "Allow",
                    "Action" => ["logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource" => log_group_arns
                  }
                ]
              }
            }
          ]

          if secret_arns.any?
            policies << {
              "PolicyName" => "SecretsAccess",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [
                  {
                    "Effect" => "Allow",
                    "Action" => ["secretsmanager:GetSecretValue"],
                    "Resource" => secret_arns
                  }
                ]
              }
            }
          end

          {
            "JobRole" => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => "#{prefix}-job-role",
                "Tags" => tags,
                "AssumeRolePolicyDocument" => {
                  "Version" => "2012-10-17",
                  "Statement" => [
                    {
                      "Effect" => "Allow",
                      "Principal" => {"Service" => "ecs-tasks.amazonaws.com"},
                      "Action" => "sts:AssumeRole"
                    }
                  ]
                },
                "Policies" => policies
              }
            }
          }
        end
        private_class_method :job_role

        def self.execution_role(prefix, steps, tags, secret_arns)
          ecr_arns = steps.reject { |_, sclass| sclass.turbofan_external? }
            .map { |sname, _| "arn:aws:ecr:*:*:repository/#{prefix}-ecr-#{sname}" }

          secrets_statement = if secret_arns.any?
            [
              {
                "Effect" => "Allow",
                "Action" => ["secretsmanager:GetSecretValue"],
                "Resource" => secret_arns
              }
            ]
          else
            []
          end

          {
            "ExecutionRole" => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => "#{prefix}-execution-role",
                "Tags" => tags,
                "AssumeRolePolicyDocument" => {
                  "Version" => "2012-10-17",
                  "Statement" => [
                    {
                      "Effect" => "Allow",
                      "Principal" => {"Service" => "ecs-tasks.amazonaws.com"},
                      "Action" => "sts:AssumeRole"
                    }
                  ]
                },
                "ManagedPolicyArns" => [
                  "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
                ],
                "Policies" => [
                  {
                    "PolicyName" => "ECRAccess",
                    "PolicyDocument" => {
                      "Version" => "2012-10-17",
                      "Statement" => [
                        {
                          "Effect" => "Allow",
                          "Action" => [
                            "ecr:GetDownloadUrlForLayer",
                            "ecr:BatchGetImage",
                            "ecr:BatchCheckLayerAvailability"
                          ],
                          "Resource" => ecr_arns
                        }
                      ] + secrets_statement
                    }
                  }
                ]
              }
            }
          }
        end
        private_class_method :execution_role

        def self.sfn_role(prefix, tags, has_fan_out: false, has_tolerated_fan_out: false)
          policies = [
            {
              "PolicyName" => "BatchAccess",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [
                  {
                    "Effect" => "Allow",
                    "Action" => ["batch:SubmitJob", "batch:DescribeJobs", "batch:TerminateJob"],
                    "Resource" => "*"
                  },
                  {
                    "Effect" => "Allow",
                    "Action" => ["events:PutTargets", "events:PutRule", "events:DescribeRule"],
                    "Resource" => "*"
                  }
                ]
              }
            },
            {
              "PolicyName" => "CloudWatchLogs",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [
                  {
                    "Effect" => "Allow",
                    "Action" => [
                      "logs:CreateLogDelivery", "logs:GetLogDelivery",
                      "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
                      "logs:ListLogDeliveries", "logs:PutResourcePolicy",
                      "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
                    ],
                    "Resource" => "*"
                  }
                ]
              }
            },
            {
              "PolicyName" => "SNSPublish",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [{
                  "Effect" => "Allow",
                  "Action" => ["sns:Publish"],
                  "Resource" => {"Ref" => "NotificationTopic"}
                }]
              }
            }
          ]

          if has_fan_out || has_tolerated_fan_out
            lambda_resources = []
            lambda_resources << {"Fn::GetAtt" => ["ChunkingLambda", "Arn"]} if has_fan_out
            lambda_resources << {"Fn::GetAtt" => ["ToleranceLambda", "Arn"]} if has_tolerated_fan_out
            policies << {
              "PolicyName" => "LambdaInvoke",
              "PolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [
                  {
                    "Effect" => "Allow",
                    "Action" => "lambda:InvokeFunction",
                    "Resource" => lambda_resources.size == 1 ? lambda_resources.first : lambda_resources
                  }
                ]
              }
            }
          end

          {
            "SfnRole" => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => "#{prefix}-sfn-role",
                "Tags" => tags,
                "AssumeRolePolicyDocument" => {
                  "Version" => "2012-10-17",
                  "Statement" => [
                    {
                      "Effect" => "Allow",
                      "Principal" => {"Service" => "states.amazonaws.com"},
                      "Action" => "sts:AssumeRole"
                    }
                  ]
                },
                "Policies" => policies
              }
            }
          }
        end
        private_class_method :sfn_role

        def self.collect_s3_statements(prefix, steps)
          bucket_arn = "arn:aws:s3:::#{Turbofan.config.bucket}"
          bucket_objects = "arn:aws:s3:::#{Turbofan.config.bucket}/*"

          statements = [
            {
              "Effect" => "Allow",
              "Action" => ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
              "Resource" => [bucket_arn, bucket_objects]
            }
          ]

          # Read-only S3 URIs (from uses/reads_from)
          read_arns = steps.flat_map { |_, sclass|
            sclass.uses_s3.flat_map { |dep| s3_uri_to_arns(dep[:uri]) }
          }.uniq
          if read_arns.any?
            statements << {
              "Effect" => "Allow",
              "Action" => ["s3:GetObject", "s3:ListBucket"],
              "Resource" => read_arns
            }
          end

          # Read-write S3 URIs (from writes_to)
          write_arns = steps.flat_map { |_, sclass|
            sclass.writes_to_s3.flat_map { |dep| s3_uri_to_arns(dep[:uri]) }
          }.uniq
          if write_arns.any?
            statements << {
              "Effect" => "Allow",
              "Action" => ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
              "Resource" => write_arns
            }
          end

          statements
        end
        private_class_method :collect_s3_statements

        def self.s3_uri_to_arns(uri)
          Turbofan::S3Uri.new(uri).to_arns
        end
        private_class_method :s3_uri_to_arns

        def self.collect_secret_arns(steps, resources)
          arns = Set.new
          steps.each_value do |sclass|
            sclass.turbofan_secrets.each { |s| arns << s[:from] }
            sclass.turbofan_resource_keys.each do |k|
              r = resources[k]
              arns << r.turbofan_secret if r&.respond_to?(:turbofan_secret) && r.turbofan_secret
            end
          end
          arns.map { |arn|
            if arn.start_with?("arn:")
              arn.end_with?("*") ? arn : "#{arn}*"
            else
              "arn:aws:secretsmanager:*:*:secret:#{arn}*"
            end
          }
        end
        private_class_method :collect_secret_arns
      end
    end
  end
end
