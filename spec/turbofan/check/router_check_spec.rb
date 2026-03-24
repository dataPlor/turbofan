require "spec_helper"

RSpec.describe Turbofan::Check::RouterCheck do
  describe ".run" do
    context "when router sizes match step sizes" do
      let(:step_class) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          size :s, cpu: 1
          size :m, cpu: 2
          size :l, cpu: 4
        end
      end

      let(:router_class) do
        Class.new do
          include Turbofan::Router

          sizes :s, :m, :l

          def route(input)
            :s
          end
        end
      end

      it "passes when router sizes match step sizes" do
        result = described_class.run(steps: {process: step_class}, routers: {process: router_class})
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(steps: {process: step_class}, routers: {process: router_class})
        expect(result.errors).to be_empty
      end
    end

    context "when router sizes do not match step sizes" do
      let(:step_class) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          size :s, cpu: 1
          size :m, cpu: 2
        end
      end

      let(:router_class) do
        Class.new do
          include Turbofan::Router

          sizes :s, :m, :l

          def route(input)
            :s
          end
        end
      end

      it "fails when router declares sizes not on the step" do
        result = described_class.run(steps: {process: step_class}, routers: {process: router_class})
        expect(result.passed?).to be false
      end

      it "reports which sizes are mismatched" do
        result = described_class.run(steps: {process: step_class}, routers: {process: router_class})
        expect(result.errors.any? { |e| e.include?(":l") }).to be true
      end
    end

    context "without a router" do
      let(:step_class) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          cpu 2
        end
      end

      it "passes when a step has no router (router is optional)" do
        result = described_class.run(steps: {process: step_class}, routers: {})
        expect(result.passed?).to be true
      end
    end

    context "when router has sizes not declared on step" do
      let(:step_class) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          size :s, cpu: 1
          size :l, cpu: 4
        end
      end

      let(:router_class) do
        Class.new do
          include Turbofan::Router

          sizes :s, :m, :l, :xl

          def route(input)
            :s
          end
        end
      end

      it "fails when router has sizes not in step" do
        result = described_class.run(steps: {process: step_class}, routers: {process: router_class})
        expect(result.passed?).to be false
      end

      it "reports all unmatched sizes" do
        result = described_class.run(steps: {process: step_class}, routers: {process: router_class})
        errors_text = result.errors.join(" ")
        expect(errors_text).to include(":m")
        expect(errors_text).to include(":xl")
      end
    end

    context "with multiple steps and routers" do
      let(:step_a) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          size :s, cpu: 1
          size :l, cpu: 4
        end
      end

      let(:step_b) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          size :small, ram: 8
          size :large, ram: 32
        end
      end

      let(:router_a) do
        Class.new do
          include Turbofan::Router

          sizes :s, :l

          def route(input)
            :s
          end
        end
      end

      let(:router_b) do
        Class.new do
          include Turbofan::Router

          sizes :small, :large

          def route(input)
            :small
          end
        end
      end

      it "passes when all routers match their steps" do
        result = described_class.run(
          steps: {process_a: step_a, process_b: step_b},
          routers: {process_a: router_a, process_b: router_b}
        )
        expect(result.passed?).to be true
      end
    end
  end
end
