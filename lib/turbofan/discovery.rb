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
      cached = @cache_mutex.synchronize { @cache[mod] }
      return cached if cached

      computed = compute_subclasses_of(mod)
      @cache_mutex.synchronize do
        # Proper double-checked locking: re-read under the lock so a
        # racing caller's already-stored result wins and we don't
        # replace-then-return a different array.
        @cache[mod] ||= computed
      end
    end

    # Clear the subclass cache. Called automatically from each root
    # module's `included` hook (Step/Pipeline/Resource/Router/
    # ComputeEnvironment) and from PipelineLoader after `Kernel.load`.
    # Test helpers may also call it — otherwise users should not need
    # to touch it.
    def self.reset_cache!
      @cache_mutex.synchronize do
        # Gate on non-empty to avoid thrashing the mutex on boot — every
        # user `include Turbofan::Step` during Zeitwerk eager_load fires
        # this hook, and the cache is empty at that point anyway.
        @cache.clear unless @cache.empty?
      end
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
