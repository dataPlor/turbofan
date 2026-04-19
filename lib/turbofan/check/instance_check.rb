# frozen_string_literal: true

module Turbofan
  module Check
    module InstanceCheck
      NARROW_POOL_THRESHOLD = 4

      def self.run(steps:)
        report = {}
        warnings = []

        steps.each do |step_name, step_class|
          duckdb = step_class.turbofan_needs_duckdb?

          if step_class.turbofan_sizes.empty?
            report[step_name] = build_single_report(step_class, duckdb)
            check_narrow_pool(report[step_name][:instance_types], step_name, warnings)
          else
            sizes_report = {}
            step_class.turbofan_sizes.each do |size_name, derived|
              size_cpu = derived[:cpu]
              size_ram = derived[:ram]
              size_cpu ||= [size_ram / 2, 1].max if size_ram
              size_ram ||= size_cpu * 2 if size_cpu

              selector_result = InstanceSelector.select(
                cpu: size_cpu,
                ram: size_ram,
                duckdb: duckdb
              )
              sizes_report[size_name] = {
                instance_types: selector_result.instance_types,
                waste: build_waste_hash(selector_result),
                spot_availability: selector_result.spot_availability
              }
              check_narrow_pool(selector_result.instance_types, step_name, warnings)
            end
            report[step_name] = {sizes: sizes_report}
          end
        end

        Result.new(passed: true, errors: [], warnings: warnings, report: report)
      end

      def self.build_single_report(step_class, duckdb)
        cpu = step_class.turbofan_default_cpu
        ram = step_class.turbofan_default_ram
        return {instance_types: [], waste: {}, spot_availability: nil, note: "no cpu/ram set"} unless cpu || ram

        # Default missing dimension based on what was specified
        cpu ||= [(ram / 8.0).ceil, 1].max
        ram ||= cpu * 2

        selector_result = InstanceSelector.select(
          cpu: cpu,
          ram: ram,
          duckdb: duckdb
        )
        {
          instance_types: selector_result.instance_types,
          waste: build_waste_hash(selector_result),
          spot_availability: selector_result.spot_availability
        }
      end
      private_class_method :build_single_report

      def self.build_waste_hash(selector_result)
        selector_result.details.each_with_object({}) do |detail, hash|
          hash[detail[:type]] = detail[:waste]
        end
      end
      private_class_method :build_waste_hash

      def self.check_narrow_pool(instance_types, step_name, warnings)
        return if instance_types.size >= NARROW_POOL_THRESHOLD

        warnings << "Step :#{step_name} has a narrow instance pool (#{instance_types.size} types)"
      end
      private_class_method :check_narrow_pool
    end
  end
end
