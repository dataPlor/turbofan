# frozen_string_literal: true

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

    # Per-module cache of ObjectSpace sweep results. ObjectSpace walks
    # are O(N) in the live-object count — hot path during deploy (each
    # of pipeline_loader, generators, check calls this 1+ times), cold
    # on first access. Keyed on the module's object_id (cheap, stable
    # per-process). Invalidation is explicit via `reset_cache!`; the
    # only event that can *add* a subclass is `Kernel.load` inside
    # PipelineLoader, which calls `reset_cache!` before returning.
    @cache_mutex = Mutex.new
    @cache = {}

    def self.subclasses_of(mod)
      cached = @cache_mutex.synchronize { @cache[mod.object_id] }
      return cached if cached

      computed = compute_subclasses_of(mod)
      @cache_mutex.synchronize { @cache[mod.object_id] ||= computed }
      computed
    end

    # Clear the subclass cache. Must be called after `Kernel.load` or
    # any runtime that defines new user classes (PipelineLoader does
    # this automatically). Tests that define anonymous subclasses
    # inside `Class.new` receivers don't need to reset — those are
    # reachable via their caller's local var, not via `const_get`, so
    # they're already filtered out of the cache.
    def self.reset_cache!
      @cache_mutex.synchronize { @cache.clear }
    end

    def self.compute_subclasses_of(mod)
      matches = ObjectSpace.each_object(Class).select { |c|
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
      # Sort by fully-qualified class name for deterministic iteration
      # order. ObjectSpace.each_object is GC-order dependent, which would
      # otherwise produce non-reproducible CloudFormation diffs and ASL
      # state ordering across runs/platforms. Flagged by Xavier Noria.
      matches.sort_by { |c| CLASS_NAME.bind_call(c) }
    end
    private_class_method :compute_subclasses_of
  end
end
