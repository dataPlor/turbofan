require "time"

begin
  require "duckdb"
rescue LoadError
  # DuckDB gem not available — define a minimal shim so .open is public
  # (Kernel#open is private and RSpec partial doubles cannot intercept it).
  unless defined?(DuckDB::Database)
    module DuckDB
      class Database
        def self.open = raise(LoadError, "duckdb gem not installed")
      end

      Error = Class.new(StandardError) unless defined?(DuckDB::Error)
    end
  end
end

module Turbofan
  class CLI < Thor
    module Cost
      def self.call(pipeline_name:, stage:)
        dash_name = pipeline_name.tr("_", "-")
        now = Time.now
        billing_period = now.strftime("%Y-%m")

        db = DuckDB::Database.open
        conn = db.connect

        exec_rows = query_executions(conn, dash_name, billing_period)
        step_rows = query_steps(conn, dash_name, billing_period)
        total_rows = query_total(conn, dash_name, billing_period)

        if exec_rows.empty? && step_rows.empty?
          $stdout.puts "No cost data found for #{dash_name} (#{stage})."
          return
        end

        print_header(dash_name, stage, now, billing_period)
        print_executions(exec_rows)
        print_steps(step_rows, total_rows)
        print_total(total_rows, now)
        export_parquet(conn, dash_name, billing_period, now)
      rescue DuckDB::Error => e
        warn e.message
        $stdout.puts "No cost data found for #{dash_name} (#{stage})."
      ensure
        conn&.close if conn.respond_to?(:close)
      end

      def self.query_executions(conn, pipeline_name, billing_period)
        conn.query(<<~SQL, pipeline_name, billing_period).to_a
          SELECT
            resource_tags_turbofan_pipeline AS pipeline,
            resource_tags_turbofan_execution AS execution,
            SUM(line_item_unblended_cost) AS cost,
            MIN(line_item_usage_start_date) AS min_time,
            MAX(line_item_usage_end_date) AS max_time
          FROM 'cur/*.parquet'
          WHERE resource_tags_turbofan_managed = 'true'
            AND resource_tags_turbofan_pipeline = $1
            AND billing_period = $2
          GROUP BY resource_tags_turbofan_pipeline, resource_tags_turbofan_execution
          ORDER BY min_time DESC
          LIMIT 10
        SQL
      end
      private_class_method :query_executions

      def self.query_steps(conn, pipeline_name, billing_period)
        conn.query(<<~SQL, pipeline_name, billing_period).to_a
          SELECT
            resource_tags_turbofan_step AS step,
            SUM(line_item_unblended_cost) AS cost
          FROM 'cur/*.parquet'
          WHERE resource_tags_turbofan_managed = 'true'
            AND resource_tags_turbofan_pipeline = $1
            AND resource_tags_turbofan_execution IS NOT NULL
            AND billing_period = $2
          GROUP BY resource_tags_turbofan_step
          ORDER BY cost DESC
        SQL
      end
      private_class_method :query_steps

      def self.query_total(conn, pipeline_name, billing_period)
        conn.query(<<~SQL, pipeline_name, billing_period).to_a
          SELECT
            SUM(line_item_unblended_cost) AS cost
          FROM 'cur/*.parquet'
          WHERE resource_tags_turbofan_managed = 'true'
            AND resource_tags_turbofan_pipeline = $1
            AND resource_tags_turbofan_step IS NOT NULL
            AND resource_tags_turbofan_execution IS NOT NULL
            AND billing_period = $2
        SQL
      end
      private_class_method :query_total

      def self.print_header(pipeline_name, stage, now, billing_period)
        period_start = "#{billing_period}-01"
        period_end = now.strftime("%Y-%m-%d")
        $stdout.puts "Pipeline: #{pipeline_name} (#{stage})"
        $stdout.puts "Period: #{period_start} to #{period_end}"
        $stdout.puts ""
      end
      private_class_method :print_header

      def self.print_executions(rows)
        $stdout.puts "Recent Executions:"
        rows.each do |row|
          execution = row["execution"]
          cost = format("%.2f", row["cost"])
          min_time = row["min_time"].to_s
          max_time = row["max_time"].to_s
          duration = format_duration(min_time, max_time)
          date_part = min_time[0, 16]
          $stdout.puts "  #{date_part}  #{execution}  $#{cost}  (#{duration})"
        end
        $stdout.puts ""
      end
      private_class_method :print_executions

      def self.print_steps(rows, total_rows)
        total_cost = total_rows.first&.fetch("cost", 0).to_f
        $stdout.puts "By Step (current month):"
        rows.each do |row|
          step_name = row["step"]
          cost = row["cost"].to_f
          cost_str = format("%.2f", cost)
          pct = (total_cost > 0) ? (cost / total_cost * 100).round : 0
          $stdout.puts "  #{step_name}    $#{cost_str}  (#{pct}%)"
        end
        $stdout.puts ""
      end
      private_class_method :print_steps

      def self.print_total(total_rows, now)
        total_cost = total_rows.first&.fetch("cost", 0).to_f
        month_name = now.strftime("%b %Y")
        $stdout.puts "Total (#{month_name}):     $#{format("%.2f", total_cost)}"
      end
      private_class_method :print_total

      def self.export_parquet(conn, pipeline_name, billing_period, now)
        filename = "cost-#{now.strftime("%Y-%m-%dT%H%M%S")}.parquet"
        conn.query(<<~SQL, pipeline_name, billing_period)
          CREATE OR REPLACE TEMP VIEW cost_export AS
          SELECT
            resource_tags_turbofan_pipeline AS pipeline,
            resource_tags_turbofan_step AS step,
            resource_tags_turbofan_execution AS execution,
            SUM(line_item_unblended_cost) AS cost
          FROM 'cur/*.parquet'
          WHERE resource_tags_turbofan_managed = 'true'
            AND resource_tags_turbofan_pipeline = $1
            AND billing_period = $2
          GROUP BY ALL
          ORDER BY cost DESC
        SQL
        # COPY TO does not support parameterized filenames; sanitize instead
        safe_filename = filename.gsub(/[^a-zA-Z0-9._\-\/]/, "_")
        conn.query("COPY cost_export TO '#{safe_filename}' (FORMAT PARQUET)")
        $stdout.puts "Saved to #{filename}"
      end
      private_class_method :export_parquet

      def self.format_duration(min_time_str, max_time_str)
        return "0m" if min_time_str.empty? || max_time_str.empty?
        min_t = Time.parse(min_time_str)
        max_t = Time.parse(max_time_str)
        seconds = (max_t - min_t).to_i
        hours = seconds / 3600
        minutes = (seconds % 3600) / 60
        if hours > 0
          "#{hours}h #{minutes.to_s.rjust(2, "0")}m"
        else
          "#{minutes}m"
        end
      rescue ArgumentError, TypeError
        "unknown"
      end
      private_class_method :format_duration
    end
  end
end
