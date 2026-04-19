# frozen_string_literal: true

require "digest"

module Turbofan
  module Naming
    def self.pascal_case(name)
      name.to_s.split("_").map(&:capitalize).join
    end

    def self.stack_name(pipeline_name, stage)
      "turbofan-#{pipeline_name.to_s.tr("_", "-")}-#{stage}"
    end

    def self.bucket_prefix(pipeline_name, stage)
      "#{pipeline_name}-#{stage}"
    end

    IAM_ROLE_NAME_LIMIT = 64

    # IAM role names have a 64-character limit. When the generated name
    # exceeds that, truncate and append a short hash for uniqueness.
    def self.iam_role_name(name)
      return name if name.length <= IAM_ROLE_NAME_LIMIT
      hash = Digest::SHA256.hexdigest(name)[0, 6]
      "#{name[0, IAM_ROLE_NAME_LIMIT - 7]}-#{hash}"
    end
  end
end
