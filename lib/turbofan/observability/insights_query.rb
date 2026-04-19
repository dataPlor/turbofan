# frozen_string_literal: true

module Turbofan
  module Observability
    class InsightsQuery
      # Pattern for safe filter values: word chars, hyphens, dots, colons, slashes.
      SAFE_VALUE_PATTERN = /\A[\w\-.:\/]+\z/

      attr_reader :log_group

      def initialize(log_group:, filters: [])
        @log_group = log_group
        @filters = filters.dup.freeze
      end

      def execution(id)
        validate_filter_value!(id, "execution")
        self.class.new(log_group: @log_group, filters: @filters + [%(filter execution_id = "#{id}")])
      end

      def step(name)
        validate_filter_value!(name, "step")
        self.class.new(log_group: @log_group, filters: @filters + [%(filter step = "#{name}")])
      end

      def item(index)
        validate_filter_value!(index.to_s, "item")
        self.class.new(log_group: @log_group, filters: @filters + ["filter array_index = #{index}"])
      end

      # NOTE: This method accepts raw CloudWatch Insights filter syntax.
      # The caller is responsible for ensuring the expression is safe.
      # Do not pass unsanitized end-user input directly to this method.
      def expression(expr)
        self.class.new(log_group: @log_group, filters: @filters + ["filter #{expr}"])
      end

      def build
        parts = ["fields @timestamp, @message"]
        parts.concat(@filters)
        parts << "sort @timestamp desc"
        parts << "limit 1000"
        parts.join("\n| ")
      end

      private

      def validate_filter_value!(value, field_name)
        unless value.to_s.match?(SAFE_VALUE_PATTERN)
          raise ArgumentError, "Invalid #{field_name} filter value: #{value.inspect}. " \
            "Only word characters, hyphens, dots, and colons are allowed."
        end
      end
    end
  end
end
