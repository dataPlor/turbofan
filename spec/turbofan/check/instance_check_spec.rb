require "spec_helper"

RSpec.describe Turbofan::Check::InstanceCheck do
  describe ".run" do
    context "with a single-size step" do
      let(:step_class) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 2
          uses :duckdb
        end
      end

      let(:result) { described_class.run(steps: {process: step_class}) }

      it "reports selected instance types per step" do
        report = result.report[:process]
        expect(report[:instance_types]).to be_an(Array)
        expect(report[:instance_types]).not_to be_empty
      end

      it "reports waste percentage per step" do
        report = result.report[:process]
        expect(report[:waste]).to be_a(Hash)
        report[:waste].each_value do |waste_pct|
          expect(waste_pct).to be >= 0.0
          expect(waste_pct).to be < 1.0
        end
      end

      it "reports spot availability assessment" do
        report = result.report[:process]
        expect(%i[good moderate risky]).to include(report[:spot_availability]) # rubocop:disable RSpec/ExpectActual
      end

      it "passes the check" do
        expect(result.passed?).to be true
      end
    end

    context "when instance pool is narrow" do
      let(:step_class) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 48
          uses :duckdb
        end
      end

      let(:result) { described_class.run(steps: {big_step: step_class}) }

      it "has fewer than 4 instance types for 48 vCPU + DuckDB" do
        report = result.report[:big_step]
        expect(report[:instance_types].size).to be < 4
      end

      it "warns about narrow instance pool" do
        expect(result.warnings.any? { |w| w.match?(/narrow|pool/i) }).to be true
      end
    end

    context "with a multi-size step" do
      let(:step_class) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          size :s, cpu: 1
          size :m, cpu: 2
          size :l, cpu: 4
          uses :duckdb
        end
      end

      let(:result) { described_class.run(steps: {process: step_class}) }

      it "reports instance types for each size" do
        report = result.report[:process]
        expect(report[:sizes]).to be_a(Hash)
        expect(report[:sizes].keys).to contain_exactly(:s, :m, :l)
      end

      it "reports waste per size" do
        report = result.report[:process]
        report[:sizes].each do |_size, details|
          expect(details[:waste]).to be_a(Hash)
        end
      end

      it "reports spot availability per size" do
        report = result.report[:process]
        report[:sizes].each do |_size, details|
          expect(%i[good moderate risky]).to include(details[:spot_availability]) # rubocop:disable RSpec/ExpectActual
        end
      end
    end

    context "with multiple steps" do
      let(:step_a) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 2
          uses :duckdb
        end
      end

      let(:step_b) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          ram 16
        end
      end

      let(:result) { described_class.run(steps: {extract: step_a, transform: step_b}) }

      it "reports instance selection for each step" do
        expect(result.report).to have_key(:extract)
        expect(result.report).to have_key(:transform)
      end

      it "selects different instance families for different step families" do
        extract_types = result.report[:extract][:instance_types]
        transform_types = result.report[:transform][:instance_types]

        expect(extract_types).to all(match(/^c\d/))
        expect(transform_types).to all(match(/^r\d/))
      end
    end
  end
end
