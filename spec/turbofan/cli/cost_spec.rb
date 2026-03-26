require "spec_helper"

RSpec.describe "turbofan cost" do # rubocop:disable RSpec/DescribeClass
  describe ".call" do
    it "prints missing config message when cur_s3_uri is not set" do
      Turbofan.config.cur_s3_uri = nil

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: "my_pipeline", stage: "production")
      end

      expect(output).to include("No CUR data configured")
      expect(output).to include("cur_s3_uri")
      expect(output).to include("s3://your-billing-bucket")
    end

    it "accepts days and period keyword arguments" do
      Turbofan.config.cur_s3_uri = nil

      expect {
        Turbofan::CLI::Cost.call(pipeline_name: "p", stage: "s", days: 7, period: "hour")
      }.not_to raise_error
    end
  end

  describe ".period_trunc" do
    it "returns hour truncation" do
      result = Turbofan::CLI::Cost.send(:period_trunc, "hour")
      expect(result).to include("hour")
    end

    it "returns day truncation by default" do
      result = Turbofan::CLI::Cost.send(:period_trunc, "day")
      expect(result).to include("day")
    end

    it "returns week truncation" do
      result = Turbofan::CLI::Cost.send(:period_trunc, "week")
      expect(result).to include("week")
    end

    it "returns month truncation" do
      result = Turbofan::CLI::Cost.send(:period_trunc, "month")
      expect(result).to include("month")
    end

    it "defaults to day for unknown period" do
      result = Turbofan::CLI::Cost.send(:period_trunc, "banana")
      expect(result).to include("day")
    end
  end

  describe ".billing_periods" do
    it "returns all months between start and end" do
      start_date = Date.new(2026, 1, 15)
      end_date = Date.new(2026, 3, 10)
      periods = Turbofan::CLI::Cost.send(:billing_periods, start_date, end_date)
      expect(periods).to eq(["2026-01", "2026-02", "2026-03"])
    end

    it "returns single month when start and end are same month" do
      start_date = Date.new(2026, 2, 1)
      end_date = Date.new(2026, 2, 28)
      periods = Turbofan::CLI::Cost.send(:billing_periods, start_date, end_date)
      expect(periods).to eq(["2026-02"])
    end
  end

  describe ".parquet_glob_sql" do
    it "returns a wildcard glob for all partitions" do
      result = Turbofan::CLI::Cost.send(:parquet_glob_sql, "s3://bucket/data", Date.new(2026, 1, 1), Date.new(2026, 3, 1))
      expect(result).to include("s3://bucket/data/**/*.parquet")
    end
  end

  describe "CUR 2.0 tag format" do
    it "uses user_ prefix for turbofan tags in queries" do
      # CUR 2.0 prepends user_ to user-defined tags and replaces : with _
      # turbofan:managed → user_turbofan_managed
      source = File.read(File.join(__dir__, "../../../lib/turbofan/cli/cost.rb"))
      expect(source).to include("user_turbofan_managed")
      expect(source).to include("user_turbofan_pipeline")
      expect(source).to include("user_turbofan_stage")
      expect(source).to include("user_turbofan_step")
      expect(source).not_to include("resource_tags['turbofan:")
    end
  end
end
