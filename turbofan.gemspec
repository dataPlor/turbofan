require_relative "lib/turbofan/version"

Gem::Specification.new do |spec|
  spec.name = "turbofan"
  spec.version = Turbofan::VERSION
  spec.authors = ["dataplor"]
  spec.summary = "Opinionated framework for AWS Batch data processing pipelines"
  spec.description = <<~DESC
    Turbofan is a Ruby DSL and runtime for building AWS Batch data processing
    pipelines. Pipelines and steps are defined as Ruby classes; deployment
    generates CloudFormation + Step Functions ASL. Includes a runtime harness
    that runs inside Batch containers with retry, metrics, fan-out, and SIGTERM
    cooperative shutdown semantics.
  DESC
  spec.homepage = "https://github.com/dataplor/turbofan"
  spec.license = "MIT"

  spec.metadata = {
    "source_code_uri" => "https://github.com/dataplor/turbofan",
    "changelog_uri" => "https://github.com/dataplor/turbofan/blob/main/CHANGELOG.md",
    "bug_tracker_uri" => "https://github.com/dataplor/turbofan/issues",
    "documentation_uri" => "https://github.com/dataplor/turbofan/blob/main/README.md",
    "rubygems_mfa_required" => "true"
  }

  spec.required_ruby_version = ">= 3.2"

  spec.add_dependency "thor", "~> 1.3"
  spec.add_dependency "aws-sdk-cloudformation", "~> 1"
  spec.add_dependency "aws-sdk-batch", "~> 1"
  spec.add_dependency "aws-sdk-s3", "~> 1"
  spec.add_dependency "aws-sdk-ec2", "~> 1"
  spec.add_dependency "aws-sdk-ecr", "~> 1"
  spec.add_dependency "aws-sdk-states", "~> 1"
  spec.add_dependency "aws-sdk-sts", "~> 1"
  spec.add_dependency "aws-sdk-ecs", "~> 1"
  spec.add_dependency "aws-sdk-cloudwatch", "~> 1"
  spec.add_dependency "aws-sdk-cloudwatchlogs", "~> 1"
  spec.add_dependency "aws-sdk-secretsmanager", "~> 1"
  spec.add_dependency "oj", "~> 3"
  spec.add_dependency "json_schemer", "~> 2"
  spec.add_dependency "zeitwerk", "~> 2.6"

  spec.files = Dir["lib/**/*", "exe/*"] + %w[README.md CHANGELOG.md LICENSE]
  spec.bindir = "exe"
  spec.executables = ["turbofan"]
end
