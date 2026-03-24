module Turbofan
  module Resource
    def self.included(base)
      base.instance_variable_set(:@turbofan_key, nil)
      base.instance_variable_set(:@turbofan_consumable, nil)
      base.instance_variable_set(:@turbofan_secret, nil)
      base.extend(ClassMethods)
    end

    module ClassMethods
      attr_reader :turbofan_key, :turbofan_consumable, :turbofan_secret

      def key(value)
        @turbofan_key = value.to_sym
      end

      def consumable(value)
        @turbofan_consumable = value
      end

      def secret(value)
        @turbofan_secret = value
      end

      def export_name(stage)
        "turbofan-resources-#{stage}-#{turbofan_key.to_s.tr("_", "-")}"
      end
    end

    def self.discover
      ObjectSpace.each_object(Class).select { |c|
        next false unless begin
          c.include?(self)
        rescue NoMethodError
          false
        end
        class_name = Turbofan::GET_CLASS_NAME.bind_call(c)
        next false unless class_name
        live = begin
          Object.const_get(class_name)
        rescue NameError
          nil
        end
        live == c
      }
    end
  end
end
