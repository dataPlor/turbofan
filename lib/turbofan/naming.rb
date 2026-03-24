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
  end
end
