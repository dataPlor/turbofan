require "spec_helper"

RSpec.describe Turbofan::Extensions do
  describe ".version" do
    it "prepends v to the configured duckdb_version" do
      expect(described_class.version).to eq("v1.4.3")
    end

    it "respects a changed duckdb_version" do
      Turbofan.config.duckdb_version = "1.5.0"
      expect(described_class.version).to eq("v1.5.0")
    end
  end

  describe ".repo_url" do
    it "returns core repo URL for core extensions" do
      url = described_class.repo_url(:spatial)
      expect(url).to eq("https://extensions.duckdb.org/v1.4.3/linux_arm64/spatial.duckdb_extension.gz")
    end

    it "returns core repo URL for httpfs" do
      url = described_class.repo_url(:httpfs)
      expect(url).to start_with("https://extensions.duckdb.org/")
    end

    it "returns community repo URL for h3" do
      url = described_class.repo_url(:h3)
      expect(url).to eq("https://community-extensions.duckdb.org/v1.4.3/linux_arm64/h3.duckdb_extension.gz")
    end

    it "returns core repo URL for vss" do
      url = described_class.repo_url(:vss)
      expect(url).to start_with("https://extensions.duckdb.org/")
    end

    it "returns community repo URL for delta" do
      url = described_class.repo_url(:delta)
      expect(url).to start_with("https://community-extensions.duckdb.org/")
    end

    it "returns core repo URL for postgres_scanner" do
      url = described_class.repo_url(:postgres_scanner)
      expect(url).to start_with("https://extensions.duckdb.org/")
    end
  end

  describe ".install_path" do
    it "returns the DuckDB extension directory for root user" do
      expect(described_class.install_path).to eq("/root/.duckdb/extensions/v1.4.3/linux_arm64")
    end

    it "reflects changed duckdb_version" do
      Turbofan.config.duckdb_version = "2.0.0"
      expect(described_class.install_path).to eq("/root/.duckdb/extensions/v2.0.0/linux_arm64")
    end
  end

  describe "PLATFORM" do
    it "is linux_arm64" do
      expect(described_class::PLATFORM).to eq("linux_arm64")
    end
  end

  describe "COMMUNITY" do
    it "includes h3 and delta" do
      expect(described_class::COMMUNITY).to contain_exactly(:h3, :delta)
    end
  end
end
