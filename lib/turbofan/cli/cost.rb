require "time"
require "date"

begin
  require "duckdb"
rescue LoadError
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
      def self.call(pipeline_name:, stage:, days: 60, period: "day")
        cur_uri = Turbofan.config.cur_s3_uri
        unless cur_uri
          print_missing_config
          return
        end

        dash_name = pipeline_name.tr("_", "-")
        end_date = Date.today
        start_date = end_date - days

        db = DuckDB::Database.open
        conn = db.connect
        conn.query("INSTALL httpfs; LOAD httpfs;")
        conn.query("CREATE SECRET (TYPE s3, PROVIDER credential_chain);")

        parquet_glob = parquet_glob_sql(conn, cur_uri, start_date, end_date)
        trunc = period_trunc(period)

        step_rows = conn.query(<<~SQL).to_a
          SELECT
            resource_tags['user_turbofan_step'] AS step,
            #{trunc} AS period_start,
            SUM(line_item_unblended_cost) AS cost
          FROM read_parquet(#{parquet_glob}, hive_partitioning=true)
          WHERE resource_tags['user_turbofan_managed'] = 'true'
            AND resource_tags['user_turbofan_pipeline'] = '#{dash_name}'
            AND resource_tags['user_turbofan_stage'] = '#{stage}'
            AND line_item_usage_start_date >= '#{start_date}'
            AND line_item_usage_start_date < '#{end_date + 1}'
            AND line_item_unblended_cost > 0
          GROUP BY step, period_start
          ORDER BY period_start DESC, cost DESC
        SQL

        total_rows = conn.query(<<~SQL).to_a
          SELECT
            #{trunc} AS period_start,
            SUM(line_item_unblended_cost) AS cost
          FROM read_parquet(#{parquet_glob}, hive_partitioning=true)
          WHERE resource_tags['user_turbofan_managed'] = 'true'
            AND resource_tags['user_turbofan_pipeline'] = '#{dash_name}'
            AND resource_tags['user_turbofan_stage'] = '#{stage}'
            AND line_item_usage_start_date >= '#{start_date}'
            AND line_item_usage_start_date < '#{end_date + 1}'
            AND line_item_unblended_cost > 0
          GROUP BY period_start
          ORDER BY period_start DESC
        SQL

        if step_rows.empty?
          $stdout.puts "No cost data found for #{dash_name} (#{stage})."
          return
        end

        print_results(dash_name, stage, start_date, end_date, period, step_rows, total_rows)
      rescue DuckDB::Error => e
        warn "[Turbofan] DuckDB error: #{e.message}"
        $stdout.puts "No cost data found for #{dash_name} (#{stage})."
      ensure
        conn&.close if conn.respond_to?(:close)
      end

      def self.period_trunc(period)
        case period
        when "hour"  then "date_trunc('hour', line_item_usage_start_date)"
        when "day"   then "date_trunc('day', line_item_usage_start_date)"
        when "week"  then "date_trunc('week', line_item_usage_start_date)"
        when "month" then "date_trunc('month', line_item_usage_start_date)"
        else "date_trunc('day', line_item_usage_start_date)"
        end
      end
      private_class_method :period_trunc

      def self.billing_periods(start_date, end_date)
        periods = []
        current = Date.new(start_date.year, start_date.month, 1)
        last = Date.new(end_date.year, end_date.month, 1)
        while current <= last
          periods << current.strftime("%Y-%m")
          current = current.next_month
        end
        periods
      end
      private_class_method :billing_periods

      def self.parquet_glob_sql(conn, cur_uri, start_date, end_date)
        # Check which billing periods actually exist on S3 to avoid
        # DuckDB failing on missing partitions.
        globs = billing_periods(start_date, end_date).filter_map do |bp|
          glob = "#{cur_uri}/BILLING_PERIOD=#{bp}/*.parquet"
          begin
            conn.query("SELECT 1 FROM read_parquet('#{glob}') LIMIT 0")
            glob
          rescue DuckDB::Error
            nil # billing period doesn't exist yet
          end
        end
        raise DuckDB::Error, "No CUR data found for the requested period" if globs.empty?
        "[#{globs.map { |g| "'#{g}'" }.join(", ")}]"
      end
      private_class_method :parquet_glob_sql

      def self.print_missing_config
        $stdout.puts <<~MSG
          No CUR data configured. Set the S3 URI for your AWS Cost and Usage Report:

            Turbofan.configure do |c|
              c.cur_s3_uri = "s3://your-billing-bucket/cur-exports/your-export/data"
            end

          The URI should point to the directory containing BILLING_PERIOD=YYYY-MM/ partitions.
          CUR 2.0 must be enabled with resource-level data and cost allocation tags.
          See: https://docs.aws.amazon.com/cur/latest/userguide/dataexports-create-standard.html
        MSG
      end
      private_class_method :print_missing_config

      def self.print_results(pipeline, stage, start_date, end_date, period, step_rows, total_rows)
        $stdout.puts "Pipeline: #{pipeline} (#{stage})"
        $stdout.puts "Period: #{start_date} to #{end_date} (#{period}ly)"
        $stdout.puts ""

        grand_total = total_rows.sum { |r| r["cost"].to_f }

        steps = step_rows.group_by { |r| r["step"] }
        $stdout.puts "By Step:"
        steps.sort_by { |_, rows| -rows.sum { |r| r["cost"].to_f } }.each do |step, rows|
          cost = rows.sum { |r| r["cost"].to_f }
          pct = grand_total > 0 ? (cost / grand_total * 100).round : 0
          $stdout.puts "  %-30s $%8.2f  (%d%%)" % [step, cost, pct]
        end
        $stdout.puts ""

        $stdout.puts "By #{period.capitalize}:"
        total_rows.first(14).each do |row|
          date_str = row["period_start"].to_s[0, period == "hour" ? 16 : 10]
          $stdout.puts "  %-16s $%8.2f" % [date_str, row["cost"].to_f]
        end
        $stdout.puts "  ..." if total_rows.size > 14
        $stdout.puts ""

        $stdout.puts "Total:  $#{"%.2f" % grand_total}"
      end
      private_class_method :print_results
    end
  end
end
