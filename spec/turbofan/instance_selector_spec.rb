# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::InstanceSelector do
  describe ".select" do
    context "with c-family duckdb step (cpu: 2)" do
      subject(:result) { described_class.select(cpu: 2, ram: 4, duckdb: true) }

      it "returns only gd (NVMe) instance types" do
        expect(result.instance_types).to all(match(/gd/))
      end

      it "returns c-family instances" do
        expect(result.instance_types).to all(match(/^c\d/))
      end

      it "includes multiple generations for Spot diversity" do
        generations = result.instance_types.map { |t| t[/c(\d)/, 1] }.uniq
        expect(generations.size).to be > 1
      end

      it "returns instances where waste is below 10% threshold" do
        result.details.each do |detail|
          expect(detail[:waste]).to be < 0.10
        end
      end

      it "calculates waste against CPU (binding dimension for c-family)" do
        result.details.each do |detail|
          expected_waste = (detail[:vcpus] % 2).to_f / detail[:vcpus]
          expect(detail[:waste]).to be_within(0.001).of(expected_waste)
        end
      end

      it "calculates jobs_per_instance correctly" do
        result.details.each do |detail|
          expected_jobs = [detail[:vcpus] / 2, detail[:ram_gb] / 4].min
          expect(detail[:jobs_per_instance]).to eq(expected_jobs)
        end
      end

      it "returns a spot availability assessment" do
        expect(%i[good moderate risky]).to include(result.spot_availability) # rubocop:disable RSpec/ExpectActual
      end
    end

    context "with c-family non-duckdb step (cpu: 2)" do
      subject(:result) { described_class.select(cpu: 2, ram: 4, duckdb: false) }

      it "returns only non-gd (no NVMe) instance types" do
        result.instance_types.each do |type|
          expect(type).not_to match(/gd/)
        end
      end

      it "returns c-family instances" do
        expect(result.instance_types).to all(match(/^c\d/))
      end
    end

    context "with c-family waste formula using cpu: 3" do
      subject(:result) { described_class.select(cpu: 3, ram: 6, duckdb: true) }

      it "excludes instances with >= 10% waste" do
        result.details.each do |detail|
          expect(detail[:waste]).to be < 0.10
        end
      end

      it "calculates modulo-based waste correctly for 4xlarge (16 vCPU)" do
        # c8gd.4xlarge: 16 vCPU, 16 % 3 = 1, waste = 1/16 = 0.0625
        detail = result.details.find { |d| d[:type] == "c8gd.4xlarge" }
        expect(detail).not_to be_nil
        expect(detail[:waste]).to be_within(0.001).of(0.0625)
        expect(detail[:jobs_per_instance]).to eq(5)
      end

      it "calculates modulo-based waste correctly for 12xlarge (48 vCPU)" do
        # c8gd.12xlarge: 48 vCPU, 48 % 3 = 0, waste = 0
        detail = result.details.find { |d| d[:type] == "c8gd.12xlarge" }
        expect(detail).not_to be_nil
        expect(detail[:waste]).to eq(0.0)
        expect(detail[:jobs_per_instance]).to eq(16)
      end

      it "excludes instances that are too small for even one job" do
        # c8gd.medium has 1 vCPU, cannot fit cpu: 3
        types = result.instance_types
        expect(types).not_to include("c8gd.medium")
        expect(types).not_to include("c8gd.large")
      end
    end

    context "with an r-family step where RAM is binding" do
      subject(:result) { described_class.select(cpu: 2, ram: 16, duckdb: false) }

      it "measures waste against RAM, not CPU" do
        result.details.each do |detail|
          expected_waste = (detail[:ram_gb] % 16).to_f / detail[:ram_gb]
          expect(detail[:waste]).to be_within(0.001).of(expected_waste)
        end
      end

      it "returns r-family instances" do
        expect(result.instance_types).to all(match(/^r\d/))
      end

      it "returns non-gd instances (no duckdb)" do
        result.instance_types.each do |type|
          expect(type).not_to match(/gd/)
        end
      end
    end

    context "with an m-family step" do
      subject(:result) { described_class.select(cpu: 4, ram: 16, duckdb: false) }

      it "returns m-family instances" do
        expect(result.instance_types).to all(match(/^m\d/))
      end

      it "returns instances with acceptable waste" do
        result.details.each do |detail|
          expect(detail[:waste]).to be < 0.10
        end
      end
    end

    context "with spot availability assessment results" do
      it "reports :good for large instance pools" do
        # cpu: 2 on c-family should have many qualifying instance types
        result = described_class.select(cpu: 2, ram: 4, duckdb: true)
        expect(result.spot_availability).to eq(:good)
      end

      it "reports :risky or :moderate for narrow instance pools" do
        # Very large job size that restricts to few instances
        result = described_class.select(cpu: 48, ram: 96, duckdb: true)
        expect(%i[risky moderate]).to include(result.spot_availability) # rubocop:disable RSpec/ExpectActual
      end
    end

    context "with cpu set to 1" do
      subject(:result) { described_class.select(cpu: 1, ram: 2, duckdb: true) }

      it "has zero waste for all instance types (1 divides everything)" do
        result.details.each do |detail|
          expect(detail[:waste]).to eq(0.0)
        end
      end
    end

    context "with the result structure" do
      subject(:result) { described_class.select(cpu: 2, ram: 4, duckdb: true) }

      it "returns instance_types as an array of strings" do
        expect(result.instance_types).to be_an(Array)
        expect(result.instance_types).to all(be_a(String))
      end

      it "returns details with type, vcpus, ram_gb, waste, jobs_per_instance" do
        result.details.each do |detail| # rubocop:disable RSpec/IteratedExpectation
          expect(detail).to have_key(:type)
          expect(detail).to have_key(:vcpus)
          expect(detail).to have_key(:ram_gb)
          expect(detail).to have_key(:waste)
          expect(detail).to have_key(:jobs_per_instance)
        end
      end

      it "returns spot_availability as a symbol" do
        expect(result.spot_availability).to be_a(Symbol)
      end
    end
  end
end
