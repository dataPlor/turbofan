module Turbofan
  module Discovery
    def self.subclasses_of(mod)
      ObjectSpace.each_object(Class).select { |c|
        class_name = GET_CLASS_NAME.bind_call(c)
        next false unless class_name
        is_subclass = begin
          c < mod
        rescue NoMethodError
          false
        end
        next false unless is_subclass
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
