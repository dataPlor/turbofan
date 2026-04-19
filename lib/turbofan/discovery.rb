module Turbofan
  module Discovery
    # Captured here because classes can override #name (e.g. anonymous
    # subclasses assigned to a const, classes overriding via singleton
    # method) and we need the original Module#name to compute stable
    # registry keys. Kept private: callers use class_name_of(mod).
    CLASS_NAME = Module.instance_method(:name)
    private_constant :CLASS_NAME

    # Returns the module/class's original name (pre-override), or nil for
    # truly anonymous modules. Use this instead of `mod.name` anywhere the
    # caller cares about subclass/resource identity.
    def self.class_name_of(mod)
      CLASS_NAME.bind_call(mod)
    end

    def self.subclasses_of(mod)
      ObjectSpace.each_object(Class).select { |c|
        class_name = CLASS_NAME.bind_call(c)
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
