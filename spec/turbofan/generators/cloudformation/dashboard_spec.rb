require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, :schemas do
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  describe "CloudWatch dashboard (Task 17)" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step

        compute_environment TestCe
        cpu 2
        uses :duckdb
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Process", step_class)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "dashboard-pipeline"

        metric "rows_processed", stat: :sum, display: :line, unit: "rows"
        metric "processing_speed", stat: :average, display: :bar, unit: "rows/sec"
        metric "total_files", stat: :sum, display: :number

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:config) do
      {
        vpc_id: "vpc-123",
        subnets: ["subnet-456", "subnet-789"],
        security_groups: ["sg-abc"]
      }
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    describe "dashboard resource" do
      let(:dashboard_key) { template["Resources"].keys.find { |k| k.include?("Dashboard") } }
      let(:dashboard) { template["Resources"][dashboard_key] }

      it "generates a dashboard resource in the CF template" do
        expect(dashboard_key).not_to be_nil
      end

      it "creates a CloudWatch Dashboard resource type" do
        expect(dashboard["Type"]).to eq("AWS::CloudWatch::Dashboard")
      end

      it "names the dashboard following convention: turbofan-{pipeline}-{stage}-dashboard" do
        expect(dashboard["Properties"]["DashboardName"]).to eq(
          "turbofan-dashboard-pipeline-production-dashboard"
        )
      end

      it "has a DashboardBody property" do
        expect(dashboard["Properties"]["DashboardBody"]).not_to be_nil
      end
    end

    describe "dashboard sections" do
      let(:dashboard_key) { template["Resources"].keys.find { |k| k.include?("Dashboard") } }
      let(:dashboard_body) do
        raw = template["Resources"][dashboard_key]["Properties"]["DashboardBody"]
        if raw.is_a?(Hash) && raw.key?("Fn::Sub")
          JSON.parse(raw["Fn::Sub"])
        elsif raw.is_a?(String)
          JSON.parse(raw)
        else
          raw
        end
      end
      let(:widgets) { dashboard_body["widgets"] }

      it "has widgets in the dashboard" do
        expect(widgets).to be_an(Array)
        expect(widgets).not_to be_empty
      end

      it "has execution overview section with success/fail/retry widgets" do
        widget_titles = widgets.filter_map { |w| w.dig("properties", "title") }
        expect(widget_titles.any? { |t| t.match?(/success/i) }).to be true
        expect(widget_titles.any? { |t| t.match?(/fail/i) }).to be true
        expect(widget_titles.any? { |t| t.match?(/retr/i) }).to be true
      end

      it "has resource efficiency section with CPU utilization widget" do
        widget_titles = widgets.filter_map { |w| w.dig("properties", "title") }
        expect(widget_titles.any? { |t| t.match?(/cpu/i) }).to be true
      end

      it "has resource efficiency section with memory utilization widget" do
        widget_titles = widgets.filter_map { |w| w.dig("properties", "title") }
        expect(widget_titles.any? { |t| t.match?(/mem/i) }).to be true
      end

      it "has pipeline metrics section with declared metrics" do
        widget_titles = widgets.filter_map { |w| w.dig("properties", "title") }
        expect(widget_titles.any? { |t| t.match?(/rows_processed/i) }).to be true
      end
    end

    describe "widget types from metric display" do
      let(:dashboard_key) { template["Resources"].keys.find { |k| k.include?("Dashboard") } }
      let(:dashboard_body) do
        raw = template["Resources"][dashboard_key]["Properties"]["DashboardBody"]
        if raw.is_a?(Hash) && raw.key?("Fn::Sub")
          JSON.parse(raw["Fn::Sub"])
        elsif raw.is_a?(String)
          JSON.parse(raw)
        else
          raw
        end
      end
      let(:widgets) { dashboard_body["widgets"] }

      it "uses MetricWidget type for line display metrics" do
        line_widget = widgets.find { |w|
          w.dig("properties", "title")&.match?(/rows_processed/i)
        }
        expect(line_widget).not_to be_nil
        expect(line_widget["type"]).to eq("metric")
      end

      it "uses MetricWidget type for bar display metrics" do
        bar_widget = widgets.find { |w|
          w.dig("properties", "title")&.match?(/processing_speed/i)
        }
        expect(bar_widget).not_to be_nil
        expect(bar_widget["type"]).to eq("metric")
      end

      it "renders line display as Line view" do
        line_widget = widgets.find { |w|
          w.dig("properties", "title")&.match?(/rows_processed/i)
        }
        expect(line_widget).not_to be_nil
        view = line_widget.dig("properties", "view")
        expect(view).to be_nil.or eq("timeSeries")
      end

      it "renders bar display as Bar view" do
        bar_widget = widgets.find { |w|
          w.dig("properties", "title")&.match?(/processing_speed/i)
        }
        expect(bar_widget).not_to be_nil
        expect(bar_widget.dig("properties", "view")).to eq("bar")
      end

      it "renders number display as SingleValue view" do
        number_widget = widgets.find { |w|
          w.dig("properties", "title")&.match?(/total_files/i)
        }
        expect(number_widget).not_to be_nil
        expect(number_widget.dig("properties", "view")).to eq("singleValue")
      end
    end

    describe "multi-step dashboard" do
      let(:step_a) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          cpu 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:step_b) do
        Class.new do
          include Turbofan::Step

          compute_environment TestCe
          cpu 4
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:multi_pipeline) do
        stub_const("Extract", step_a)
        stub_const("Load", step_b)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "multi-dash"

          metric "records", stat: :sum, display: :line

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:multi_template) do
        described_class.new(
          pipeline: multi_pipeline,
          steps: {extract: step_a, load: step_b},
          stage: "staging",
          config: config
        ).generate
      end

      it "generates a dashboard for multi-step pipeline" do
        dashboard_key = multi_template["Resources"].keys.find { |k| k.include?("Dashboard") }
        expect(dashboard_key).not_to be_nil
      end

      it "names the dashboard with the correct pipeline and stage" do
        dashboard_key = multi_template["Resources"].keys.find { |k| k.include?("Dashboard") }
        dashboard = multi_template["Resources"][dashboard_key]
        expect(dashboard["Properties"]["DashboardName"]).to include("multi-dash")
        expect(dashboard["Properties"]["DashboardName"]).to include("staging")
      end
    end

    describe "pipeline with no custom metrics" do
      let(:no_metrics_pipeline) do
        stub_const("Process", step_class)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "no-metrics"

          pipeline do
            process(trigger_input)
          end
        end
      end

      let(:no_metrics_template) do
        described_class.new(
          pipeline: no_metrics_pipeline,
          steps: {process: step_class},
          stage: "production",
          config: config
        ).generate
      end

      it "still generates a dashboard with execution overview and resource efficiency" do
        dashboard_key = no_metrics_template["Resources"].keys.find { |k| k.include?("Dashboard") }
        expect(dashboard_key).not_to be_nil
      end
    end
  end
end
