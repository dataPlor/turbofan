require "spec_helper"

RSpec.describe Turbofan::Runtime::StepMetrics do
  describe ".peak_memory_mb" do
    it "reads from /proc/self/status when available" do
      skip "Not on Linux" unless File.exist?("/proc/self/status")

      result = described_class.send(:peak_memory_mb)
      expect(result).to be_a(Float)
      expect(result).to be >= 0
    end

    it "falls back to ps when /proc/self/status doesn't exist" do
      allow(File).to receive(:exist?).and_call_original
      allow(File).to receive(:exist?).with("/proc/self/status").and_return(false)

      result = described_class.send(:peak_memory_mb)
      expect(result).to be_a(Float)
      expect(result).to be >= 0
    end

    it "returns 0.0 when all methods fail" do
      allow(File).to receive(:exist?).and_call_original
      allow(File).to receive(:exist?).with("/proc/self/status").and_return(false)
      allow(described_class).to receive(:`).and_raise(StandardError, "ps failed")

      result = described_class.send(:peak_memory_mb)
      expect(result).to eq(0.0)
    end
  end

  describe ".cpu_utilization" do
    it "calculates CPU utilization as (cpu_time / wall_time) * 100" do
      utilization = described_class.send(:cpu_utilization, 10.0)
      expect(utilization).to be_a(Float)
      expect(utilization).to be >= 0
    end

    it "returns 0 when wall time is zero" do
      expect(described_class.send(:cpu_utilization, 0.0)).to eq(0.0)
    end

    it "returns 0 when wall time is negative" do
      expect(described_class.send(:cpu_utilization, -1.0)).to eq(0.0)
    end
  end

  describe ".memory_utilization" do
    it "calculates memory utilization as (peak_mb / allocated_mb) * 100" do
      # 512 MB peak / 4 GB (4096 MB) allocated = 12.5%
      utilization = described_class.send(:memory_utilization, 512.0, 4)
      expect(utilization).to eq(12.5)
    end

    it "returns 0 when allocated RAM is zero" do
      expect(described_class.send(:memory_utilization, 512.0, 0)).to eq(0.0)
    end
  end
end
