# frozen_string_literal: true

require "spec_helper"

# Regression tests for the Step/Pipeline subclass-of-subclass hazard.
#
# Before the `inherited` hook was added, class B < A (where A already
# included Turbofan::Step) left B's @turbofan_* ivars unset, because
# `include` is a no-op when the module is already in the ancestors
# chain. Any DSL macro that mutated an ivar (`@turbofan_uses << dep`)
# would NoMethodError on nil.
#
# Equally important: the fix must not cause subclass mutations to leak
# into the parent class. That happens when `init_state` hands the
# subclass the parent's Array/Hash references instead of .dup'd copies.
RSpec.describe "Turbofan::Step subclass inheritance" do
  it "initializes @turbofan_* ivars on a subclass so DSL macros don't NoMethodError" do
    parent = Class.new { include Turbofan::Step }
    stub_const("SubclassParent", parent)

    child = Class.new(parent)
    stub_const("SubclassChild", child)

    expect { child.uses :postgres }.not_to raise_error
    expect(child.turbofan.uses).to eq([{type: :resource, key: :postgres}])
  end

  it "does not leak subclass DSL mutations back into the parent class" do
    parent = Class.new { include Turbofan::Step }
    stub_const("LeakParent", parent)
    parent.uses :alpha

    child = Class.new(parent)
    stub_const("LeakChild", child)
    child.uses :beta

    expect(parent.turbofan.uses.map { |d| d[:key] }).to eq([:alpha])
    expect(child.turbofan.uses.map { |d| d[:key] }).to eq([:beta])
  end

  it "preserves super in inherited so downstream hooks fire" do
    # ActiveSupport et al. install their own `inherited` hooks; we must
    # call super to cooperate. Simulate a parent that counts subclasses.
    parent = Class.new do
      include Turbofan::Step
      class << self
        attr_accessor :subclass_count
      end
      self.subclass_count = 0

      def self.inherited(subclass)
        super
        self.subclass_count += 1
      end
    end
    stub_const("HookParent", parent)

    Class.new(parent)
    Class.new(parent)

    expect(parent.subclass_count).to eq(2)
  end

  it "gives the subclass its own Hash for @turbofan_sizes (no shared-state aliasing)" do
    parent = Class.new { include Turbofan::Step }
    stub_const("SizesParent", parent)
    parent.size(:medium, cpu: 4)

    child = Class.new(parent)
    stub_const("SizesChild", child)
    child.size(:large, cpu: 16)

    expect(parent.turbofan.sizes.keys).to eq([:medium])
    expect(child.turbofan.sizes.keys).to eq([:large])
  end
end

RSpec.describe "Turbofan::Pipeline subclass inheritance" do
  it "initializes @turbofan_* ivars on a Pipeline subclass" do
    parent = Class.new { include Turbofan::Pipeline }
    stub_const("PipeSubParent", parent)
    parent.pipeline_name "parent_pipe"

    child = Class.new(parent)
    stub_const("PipeSubChild", child)

    expect { child.pipeline_name("child_pipe") }.not_to raise_error
    expect(child.turbofan_name).to eq("child_pipe")
  end

  it "does not leak Pipeline subclass tag mutations into the parent" do
    parent = Class.new { include Turbofan::Pipeline }
    stub_const("PipeLeakParent", parent)
    parent.tags(owner: "parent-team")

    child = Class.new(parent)
    stub_const("PipeLeakChild", child)
    child.tags(owner: "child-team")

    expect(parent.turbofan_tags).to eq({"owner" => "parent-team"})
    expect(child.turbofan_tags).to eq({"owner" => "child-team"})
  end
end
