module Turbofan
  module Check
    module RouterCheck
      def self.run(steps:, routers:)
        errors = []

        routers.each do |step_name, router_class|
          step_class = steps[step_name]
          next unless step_class

          step_sizes = step_class.turbofan_sizes.keys
          router_sizes = router_class.turbofan_sizes
          extra = router_sizes - step_sizes

          next if extra.empty?

          errors << "Router for :#{step_name} declares sizes #{extra.map { |s| ":#{s}" }} not found on step"
        end

        Result.new(passed: errors.empty?, errors: errors, warnings: [], report: nil)
      end
    end
  end
end
