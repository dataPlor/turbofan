# frozen_string_literal: true

module Turbofan
  module Router
    class InvalidSizeError < Turbofan::ValidationError; end

    def self.included(base)
      base.extend(ClassMethods)
      base.instance_variable_set(:@turbofan_sizes, [])
      Turbofan::Discovery.reset_cache!
    end

    module ClassMethods
      attr_reader :turbofan_sizes

      def sizes(*names)
        @turbofan_sizes = names
      end
    end

    def route(input)
      raise NotImplementedError, "#{self.class} must implement #route"
    end

    def group_inputs(inputs)
      declared = self.class.turbofan_sizes
      groups = declared.each_with_object({}) { |s, h| h[s] = [] }

      inputs.each do |input|
        size = route(input)
        unless declared.include?(size)
          raise InvalidSizeError, "route returned #{size.inspect}, must be one of #{declared.inspect}"
        end
        groups[size] << input
      end

      groups
    end
  end
end
