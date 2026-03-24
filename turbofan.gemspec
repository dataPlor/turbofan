require_relative "lib/turbofan/version"

Gem::Specification.new do |spec|
  spec.name = "turbofan"
  spec.version = Turbofan::VERSION
  spec.authors = ["dataplor"]
  spec.summary = "Opinionated framework for AWS Batch data processing pipelines"
  spec.homepage = "https://github.com/dataplor/turbofan"
  spec.license = "MIT"

  spec.metadata = {
    "source_code_uri" => "https://github.com/dataplor/turbofan",
    "changelog_uri" => "https://github.com/dataplor/turbofan/blob/main/CHANGELOG.md"
  }

  spec.required_ruby_version = ">= 3.2"

  spec.add_dependency "thor", "~> 1.3"
  spec.add_dependency "aws-sdk-cloudformation", "~> 1"
  spec.add_dependency "aws-sdk-batch", "~> 1"
  spec.add_dependency "aws-sdk-s3", "~> 1"
  spec.add_dependency "aws-sdk-ecr", "~> 1"
  spec.add_dependency "aws-sdk-states", "~> 1"
  spec.add_dependency "aws-sdk-sts", "~> 1"
  spec.add_dependency "aws-sdk-cloudwatch", "~> 1"
  spec.add_dependency "aws-sdk-cloudwatchlogs", "~> 1"
  spec.add_dependency "aws-sdk-secretsmanager", "~> 1"
  spec.add_dependency "oj", "~> 3"
  spec.add_dependency "json_schemer", "~> 2"

  spec.files = Dir["lib/**/*", "bin/*"]
  spec.bindir = "bin"
  spec.executables = ["turbofan"]
end
