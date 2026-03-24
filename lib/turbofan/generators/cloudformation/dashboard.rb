require "json"

module Turbofan
  module Generators
    class CloudFormation
      module Dashboard
        DISPLAY_MAP = {
          line: "timeSeries",
          bar: "bar",
          number: "singleValue",
          stacked: "timeSeries"
        }.freeze

        STAT_MAP = {
          sum: "Sum",
          average: "Average",
          p90: "p90",
          min: "Minimum",
          max: "Maximum"
        }.freeze

        REGION_PLACEHOLDER = "${AWS::Region}"

        def self.generate(prefix:, pipeline:, steps:, stage:, **)
          pipeline_name = pipeline.turbofan_name
          namespace = "Turbofan/#{pipeline_name}"

          widgets = []
          y_offset = 0

          # Section 1: Execution Overview
          widgets.concat(execution_overview_widgets(namespace, pipeline_name, stage, steps, y_offset))
          y_offset = 7

          # Section 2: Resource Efficiency (per step)
          widgets.concat(resource_efficiency_widgets(namespace, pipeline_name, stage, steps, y_offset))
          y_offset = 14

          # Section 3: Pipeline Metrics
          widgets.concat(pipeline_metrics_widgets(namespace, pipeline_name, stage, steps, pipeline, y_offset))

          body = {"widgets" => widgets}

          {
            "Dashboard" => {
              "Type" => "AWS::CloudWatch::Dashboard",
              "Properties" => {
                "DashboardName" => "#{prefix}-dashboard",
                "DashboardBody" => {"Fn::Sub" => JSON.generate(body)}
              }
            }
          }
        end

        def self.execution_overview_widgets(namespace, pipeline_name, stage, steps, y_offset)
          step_names = steps.keys
          metrics_defs = [
            {title: "Job Success", metric: "JobSuccess", stat: "Sum"},
            {title: "Job Failure", metric: "JobFailure", stat: "Sum"},
            {title: "Job Retry", metric: "JobRetry", stat: "Sum"},
            {title: "Job Duration (p90)", metric: "JobDuration", stat: "p90"}
          ]

          metrics_defs.each_with_index.map do |defn, i|
            metric_entries = step_names.map do |sname|
              [namespace, defn[:metric], "Pipeline", pipeline_name, "Stage", stage, "Step", sname.to_s]
            end

            build_widget(
              title: defn[:title],
              metrics: metric_entries,
              stat: defn[:stat],
              view: "timeSeries",
              x: i * 6,
              y: y_offset
            )
          end
        end

        def self.resource_efficiency_widgets(namespace, pipeline_name, stage, steps, y_offset)
          widgets = []
          flat_idx = 0
          steps.each do |sname, _sclass|
            [
              {title: "CPU Utilization - #{sname}", metric: "CpuUtilization", stat: "Average"},
              {title: "Memory Utilization - #{sname}", metric: "MemoryUtilization", stat: "Average"}
            ].each do |defn|
              col = flat_idx % 4
              row = flat_idx / 4
              widgets << build_widget(
                title: defn[:title],
                metrics: [[namespace, defn[:metric], "Pipeline", pipeline_name, "Stage", stage, "Step", sname.to_s]],
                stat: defn[:stat],
                view: "timeSeries",
                x: col * 6,
                y: y_offset + row * 7
              )
              flat_idx += 1
            end
          end
          widgets
        end

        def self.pipeline_metrics_widgets(namespace, pipeline_name, stage, steps, pipeline, y_offset)
          pipeline.turbofan_metrics.each_with_index.map do |m, i|
            stat = STAT_MAP.fetch(m[:stat], m[:stat].to_s)
            view = DISPLAY_MAP[m[:display]]
            stacked = m[:display] == :stacked

            step_names = m[:step] ? [m[:step]] : steps.keys
            metric_entries = step_names.map do |sname|
              [namespace, m[:name], "Pipeline", pipeline_name, "Stage", stage, "Step", sname.to_s]
            end

            props = {
              "title" => m[:name],
              "region" => REGION_PLACEHOLDER,
              "metrics" => metric_entries,
              "stat" => stat,
              "period" => 300
            }
            props["view"] = view if view
            props["stacked"] = true if stacked

            {
              "type" => "metric",
              "x" => (i % 4) * 6,
              "y" => y_offset + (i / 4) * 7,
              "width" => 6,
              "height" => 6,
              "properties" => props
            }
          end
        end

        def self.build_widget(title:, metrics:, stat:, view:, x:, y:)
          {
            "type" => "metric",
            "x" => x,
            "y" => y,
            "width" => 6,
            "height" => 6,
            "properties" => {
              "title" => title,
              "region" => REGION_PLACEHOLDER,
              "view" => view,
              "metrics" => metrics,
              "stat" => stat,
              "period" => 300
            }
          }
        end

        private_class_method :execution_overview_widgets, :resource_efficiency_widgets,
          :pipeline_metrics_widgets, :build_widget
      end
    end
  end
end
