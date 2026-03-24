require "spec_helper"

RSpec.describe "turbofan cost" do # rubocop:disable RSpec/DescribeClass
  let(:duckdb_connection) { double("DuckDB::Connection", close: nil) }
  let(:duckdb_database) { double("DuckDB::Database", connect: duckdb_connection) }
  let(:pipeline_name) { "daily_chores" }
  let(:stage) { "production" }
  let(:now) { Time.new(2026, 2, 17, 12, 0, 0, "+00:00") }

  let(:execution_rows) do
    [
      {"pipeline" => "daily-chores", "execution" => "exec-abc123", "cost" => 42.15, "min_time" => "2026-02-17 06:00:00", "max_time" => "2026-02-17 09:12:00"},
      {"pipeline" => "daily-chores", "execution" => "exec-def456", "cost" => 38.90, "min_time" => "2026-02-16 06:00:00", "max_time" => "2026-02-16 08:58:00"}
    ]
  end

  let(:step_rows) do
    [
      {"step" => "validate_places", "cost" => 312.40},
      {"step" => "chain_identify", "cost" => 120.15},
      {"step" => "extract_places", "cost" => 71.30}
    ]
  end

  let(:total_cost) { 503.85 }

  before do
    allow(Time).to receive(:now).and_return(now)
  end

  describe ".call" do
    it "accepts pipeline_name and stage keyword arguments" do
      expect(Turbofan::CLI::Cost).to respond_to(:call)
    end

    it "queries CUR data grouped by pipeline and execution" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)

      queries = []
      allow(duckdb_connection).to receive(:query) do |sql, *args|
        queries << sql
        case queries.size
        when 1 then result
        when 2 then step_result
        when 3 then total_result
        else double("result")
        end
      end

      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      select_queries = queries.select { |sql| sql.include?("resource_tags_turbofan_pipeline") }
      expect(select_queries).not_to be_empty
      select_queries.each do |sql| # rubocop:disable RSpec/IteratedExpectation
        expect(sql).to include("resource_tags_turbofan_execution")
        expect(sql).to include("resource_tags_turbofan_managed")
      end
    end

    it "passes pipeline name as a query parameter" do
      result = double("result", each: [].each, to_a: [])
      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result)

      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(duckdb_connection).to have_received(:query).with(
        a_string_matching(/\$1/), "daily-chores", anything
      ).at_least(:once)
    end
  end

  describe "output formatting" do
    before do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result, step_result, total_result)
    end

    it "displays pipeline name and stage in the header" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("daily-chores").or include("daily_chores")
      expect(output).to include("production")
    end

    it "displays the billing period" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("2026-02")
    end

    it "displays recent executions with cost" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("exec-abc123")
      expect(output).to include("42.15")
      expect(output).to include("exec-def456")
      expect(output).to include("38.90")
    end

    it "displays per-execution timing" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("exec-abc123")
      expect(output).to match(/3h\s*12m|3:12|192\s*min/)
    end

    it "displays per-step cost breakdown" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("validate_places")
      expect(output).to include("312.40")
      expect(output).to include("chain_identify")
      expect(output).to include("120.15")
      expect(output).to include("extract_places")
      expect(output).to include("71.30")
    end

    it "displays cost percentages per step" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("62%").or include("62.0%")
    end

    it "displays the total cost for the month" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("503.85")
    end

    it "includes a Recent Executions section" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("Recent Executions")
    end

    it "includes a By Step section" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to match(/By Step/)
    end
  end

  describe "parquet export" do
    before do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result, step_result, total_result)
    end

    it "saves cost data to a parquet file with timestamp" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to match(/cost-\d{4}-\d{2}-\d{2}T\d{6}\.parquet/)
    end

    it "includes the save path in the output" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("Saved to")
    end

    it "exports data via DuckDB COPY statement" do
      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(duckdb_connection).to have_received(:query).with(a_string_matching(/COPY|parquet/i)).at_least(:once)
    end
  end

  describe "graceful handling when no CUR data exists" do
    before do
      empty_result = double("result", each: [].each, to_a: [])
      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(empty_result)
    end

    it "does not crash when no CUR data exists" do
      expect {
        capture_stdout do
          Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
        end
      }.not_to raise_error
    end

    it "displays a message indicating no cost data found" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to match(/no cost data|no CUR data|no data/i)
    end

    it "does not save a parquet file when there is no data" do
      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).not_to include("Saved to")
    end
  end

  describe "DuckDB error handling" do
    it "handles missing parquet files gracefully" do
      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_raise(DuckDB::Error, "No files found matching 'cur/*.parquet'")

      expect {
        capture_stdout do
          Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
        end
      }.not_to raise_error
    end
  end

  describe "CLI registration" do
    it "is registered as a subcommand on Turbofan::CLI" do
      expect(Turbofan::CLI.instance_methods + Turbofan::CLI.private_instance_methods).to include(:cost)
    end
  end

  describe "execution ordering" do
    it "displays executions in date-descending order (most recent first)" do
      rows = [
        {"pipeline" => "daily-chores", "execution" => "exec-newest", "cost" => 10.00, "min_time" => "2026-02-17 06:00:00", "max_time" => "2026-02-17 07:00:00"},
        {"pipeline" => "daily-chores", "execution" => "exec-middle", "cost" => 20.00, "min_time" => "2026-02-15 06:00:00", "max_time" => "2026-02-15 07:00:00"},
        {"pipeline" => "daily-chores", "execution" => "exec-oldest", "cost" => 30.00, "min_time" => "2026-02-13 06:00:00", "max_time" => "2026-02-13 07:00:00"}
      ]
      result = double("result", each: rows.each, to_a: rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      newest_pos = output.index("exec-newest")
      middle_pos = output.index("exec-middle")
      oldest_pos = output.index("exec-oldest")

      expect(newest_pos).to be < middle_pos
      expect(middle_pos).to be < oldest_pos
    end
  end

  describe "step percentage accuracy" do
    it "step percentages sum to 100% when all steps are present" do
      rows = [
        {"step" => "step_a", "cost" => 50.0},
        {"step" => "step_b", "cost" => 30.0},
        {"step" => "step_c", "cost" => 20.0}
      ]
      total = 100.0
      exec_result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: rows.each, to_a: rows)
      total_result = double("result", each: [{"cost" => total}].each, to_a: [{"cost" => total}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(exec_result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("50%")
      expect(output).to include("30%")
      expect(output).to include("20%")
    end

    it "displays 0% for a step with zero cost" do
      rows = [
        {"step" => "expensive_step", "cost" => 100.0},
        {"step" => "free_step", "cost" => 0.0}
      ]
      total = 100.0
      exec_result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: rows.each, to_a: rows)
      total_result = double("result", each: [{"cost" => total}].each, to_a: [{"cost" => total}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(exec_result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("free_step")
      expect(output).to include("0%")
    end
  end

  describe "zero-cost executions" do
    it "handles executions with zero cost gracefully" do
      rows = [
        {"pipeline" => "daily-chores", "execution" => "exec-free", "cost" => 0.0, "min_time" => "2026-02-17 06:00:00", "max_time" => "2026-02-17 06:05:00"}
      ]
      zero_steps = [{"step" => "noop_step", "cost" => 0.0}]
      exec_result = double("result", each: rows.each, to_a: rows)
      step_result = double("result", each: zero_steps.each, to_a: zero_steps)
      total_result = double("result", each: [{"cost" => 0.0}].each, to_a: [{"cost" => 0.0}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(exec_result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("exec-free")
      expect(output).to include("0.00")
    end
  end

  describe "large cost formatting" do
    it "formats very large costs without thousands separators" do
      rows = [
        {"pipeline" => "daily-chores", "execution" => "exec-big", "cost" => 12345.67, "min_time" => "2026-02-17 06:00:00", "max_time" => "2026-02-17 07:00:00"}
      ]
      large_steps = [{"step" => "costly_step", "cost" => 12345.67}]
      exec_result = double("result", each: rows.each, to_a: rows)
      step_result = double("result", each: large_steps.each, to_a: large_steps)
      total_result = double("result", each: [{"cost" => 12345.67}].each, to_a: [{"cost" => 12345.67}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(exec_result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("12345.67")
      expect(output).not_to include("12,345.67")
    end
  end

  describe "parquet export filename format" do
    it "uses exact timestamp pattern YYYY-MM-DDTHHMMSS in filename" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("cost-2026-02-17T120000.parquet")
    end
  end

  describe "DuckDB connection handling" do
    it "opens a DuckDB database and obtains a connection" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result, step_result, total_result, export_result)

      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(DuckDB::Database).to have_received(:open)
      expect(duckdb_database).to have_received(:connect)
    end
  end

  describe "CUR bucket path" do
    it "reads from cur/*.parquet glob path" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)

      queries = []
      allow(duckdb_connection).to receive(:query) do |sql, *_args|
        queries << sql
        case queries.size
        when 1 then result
        when 2 then step_result
        when 3 then total_result
        else double("result")
        end
      end

      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(queries.any? { |sql| sql.include?("cur/*.parquet") }).to be true
    end
  end

  describe "export_parquet parameterization" do
    it "does not interpolate pipeline_name into the export SQL string" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)

      queries = []
      allow(duckdb_connection).to receive(:query) do |sql, *args|
        queries << {sql: sql, args: args}
        case queries.size
        when 1 then result
        when 2 then step_result
        when 3 then total_result
        else double("result")
        end
      end

      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      # The CREATE VIEW query (4th) should not contain interpolated values
      view_query = queries.find { |q| q[:sql].include?("CREATE") }
      expect(view_query[:sql]).not_to include("daily-chores")
      expect(view_query[:sql]).not_to include("2026-02")
    end

    it "passes pipeline_name and billing_period as parameters to the export query" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)

      queries = []
      allow(duckdb_connection).to receive(:query) do |sql, *args|
        queries << {sql: sql, args: args}
        case queries.size
        when 1 then result
        when 2 then step_result
        when 3 then total_result
        else double("result")
        end
      end

      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      # The CREATE VIEW query uses parameters ($1/$2)
      view_query = queries.find { |q| q[:sql].include?("CREATE") }
      expect(view_query[:args]).not_to be_empty
    end
  end

  describe "executions empty but steps present" do
    it "still displays step breakdown when only execution data is empty" do
      empty_result = double("result", each: [].each, to_a: [])
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(empty_result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("By Step")
      expect(output).to include("validate_places")
      expect(output).to include("312.40")
      expect(output).not_to match(/no cost data/i)
    end
  end

  describe "SQL injection safety" do
    it "uses parameterized queries instead of string interpolation" do
      result = double("result", each: [].each, to_a: [])
      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result)

      capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: "'; DROP TABLE--", stage: stage)
      end

      # Parameterized SELECT queries pass pipeline name as arg, not in SQL string
      expect(duckdb_connection).to have_received(:query).with(
        a_string_matching(/\$1/), anything, anything
      ).at_least(:once)
    end

    it "does not crash when pipeline name contains special characters" do
      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_raise(DuckDB::Error, "syntax error")

      expect {
        capture_stdout do
          Turbofan::CLI::Cost.call(pipeline_name: "'; DROP TABLE--", stage: stage)
        end
      }.not_to raise_error
    end

    it "displays no-data message when special characters cause query failure" do
      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_raise(DuckDB::Error, "syntax error")

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: "'; DROP TABLE--", stage: stage)
      end

      expect(output).to match(/no cost data/i)
    end
  end

  describe "underscore to dash conversion" do
    it "converts underscores to dashes in pipeline name for display" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: "my_cool_pipeline", stage: stage)
      end

      expect(output).to include("my-cool-pipeline")
    end
  end

  describe "duration formatting edge cases" do
    it "displays minutes-only duration for sub-hour executions" do
      rows = [
        {"pipeline" => "daily-chores", "execution" => "exec-short", "cost" => 5.00, "min_time" => "2026-02-17 06:00:00", "max_time" => "2026-02-17 06:45:00"}
      ]
      exec_result = double("result", each: rows.each, to_a: rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(exec_result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("45m")
      expect(output).not_to match(/0h/)
    end

    it "displays 0m for zero-duration executions" do
      rows = [
        {"pipeline" => "daily-chores", "execution" => "exec-instant", "cost" => 1.00, "min_time" => "2026-02-17 06:00:00", "max_time" => "2026-02-17 06:00:00"}
      ]
      exec_result = double("result", each: rows.each, to_a: rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(exec_result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("0m")
    end
  end

  describe "total cost display" do
    it "includes the month name in the total line" do
      result = double("result", each: execution_rows.each, to_a: execution_rows)
      step_result = double("result", each: step_rows.each, to_a: step_rows)
      total_result = double("result", each: [{"cost" => total_cost}].each, to_a: [{"cost" => total_cost}])
      export_result = double("result")

      allow(DuckDB::Database).to receive(:open).and_return(duckdb_database)
      allow(duckdb_connection).to receive(:query).and_return(result, step_result, total_result, export_result)

      output = capture_stdout do
        Turbofan::CLI::Cost.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to include("Feb 2026")
    end
  end
end
