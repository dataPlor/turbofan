require "spec_helper"

RSpec.describe Turbofan::Check::ResourceCheck, :schemas do
  describe ".run" do
    context "when resource resolution succeeds" do
      let(:pipeline_class) do
        stub_const("Resources::PlacesRead", Class.new {
          include Turbofan::Resource

          key :places_read
        })
        stub_const("Fetch", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :places_read
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "resource-ok"
          pipeline do
            fetch(trigger_input)
          end
        end
      end

      let(:steps) do
        {fetch: Fetch}
      end

      let(:resources) do
        {places_read: Resources::PlacesRead}
      end

      it "passes when uses resolves to a discovered Resource" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors).to be_empty
      end

      it "has no warnings" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.warnings).to be_empty
      end
    end

    context "when resource resolution fails" do
      let(:pipeline_class) do
        stub_const("Fetch", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :nonexistent
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "resource-missing"
          pipeline do
            fetch(trigger_input)
          end
        end
      end

      let(:steps) do
        {fetch: Fetch}
      end

      let(:resources) { {} }

      it "fails when uses references a nonexistent resource" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be false
      end

      it "reports an error mentioning the missing resource key" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors.any? { |e| e.include?(":nonexistent") }).to be true
      end

      it "reports an error mentioning the step that uses it" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors.any? { |e| e.include?(":fetch") }).to be true
      end
    end

    context "when duckdb is excluded from resource resolution" do
      let(:pipeline_class) do
        stub_const("Analyze", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :duckdb
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "duckdb-only"
          pipeline do
            analyze(trigger_input)
          end
        end
      end

      let(:steps) do
        {analyze: Analyze}
      end

      let(:resources) { {} }

      it "passes when a step only uses :duckdb" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors).to be_empty
      end

      it "has no warnings" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.warnings).to be_empty
      end
    end

    context "when step uses both a real resource and duckdb" do
      let(:pipeline_class) do
        stub_const("Resources::PlacesRead", Class.new {
          include Turbofan::Resource

          key :places_read
        })
        stub_const("Transform", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :places_read
          uses :duckdb
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "mixed-resources"
          pipeline do
            transform(trigger_input)
          end
        end
      end

      let(:steps) do
        {transform: Transform}
      end

      let(:resources) do
        {places_read: Resources::PlacesRead}
      end

      it "passes when the real resource resolves and duckdb is skipped" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors).to be_empty
      end
    end

    context "when fan-out step uses postgres resource" do
      let(:pipeline_class) do
        stub_const("Resources::PlacesRead", Class.new {
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
        })
        stub_const("Discover", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :places_read
        })
        stub_const("Aggregate", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-out-pg"
          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files), batch_size: 100)
            aggregate(results)
          end
        end
      end

      let(:steps) do
        {discover: Discover, process: Process, aggregate: Aggregate}
      end

      let(:resources) do
        {places_read: Resources::PlacesRead}
      end

      it "still passes (warnings do not block)" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors).to be_empty
      end

      it "warns about potential connection storm" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.warnings.any? { |w| w.match?(/connection storm|database overload/i) }).to be true
      end

      it "mentions the fan-out step in the warning" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.warnings.any? { |w| w.include?(":process") }).to be true
      end

      it "mentions the postgres resource in the warning" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.warnings.any? { |w| w.include?(":places_read") }).to be true
      end
    end

    context "when fan-out step uses non-postgres resource" do
      let(:pipeline_class) do
        stub_const("Resources::S3Bucket", Class.new {
          include Turbofan::Resource

          key :s3_bucket
        })
        stub_const("Discover", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :s3_bucket
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-out-s3"
          pipeline do
            files = discover(trigger_input)
            fan_out(process(files), batch_size: 50)
          end
        end
      end

      let(:steps) do
        {discover: Discover, process: Process}
      end

      let(:resources) do
        {s3_bucket: Resources::S3Bucket}
      end

      it "passes without warnings" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors).to be_empty
      end

      it "has no warnings" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.warnings).to be_empty
      end
    end

    context "without a uses declaration" do
      let(:pipeline_class) do
        stub_const("Simple", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "no-resources"
          pipeline do
            simple(trigger_input)
          end
        end
      end

      let(:steps) do
        {simple: Simple}
      end

      let(:resources) { {} }

      it "passes with no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors).to be_empty
      end

      it "has no warnings" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.warnings).to be_empty
      end
    end

    context "when one step resolves and one does not" do
      let(:pipeline_class) do
        stub_const("Resources::PlacesRead", Class.new {
          include Turbofan::Resource

          key :places_read
        })
        stub_const("GoodStep", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :places_read
        })
        stub_const("BadStep", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          uses :nonexistent
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "mixed-resolution"
          pipeline do
            a = good_step(trigger_input)
            bad_step(a)
          end
        end
      end

      let(:steps) do
        {good_step: GoodStep, bad_step: BadStep}
      end

      let(:resources) do
        {places_read: Resources::PlacesRead}
      end

      it "fails because one step has an unresolvable resource" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.passed?).to be false
      end

      it "reports an error for the failing step only" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors.any? { |e| e.include?(":bad_step") && e.include?(":nonexistent") }).to be true
      end

      it "does not report an error for the passing step" do
        result = described_class.run(pipeline: pipeline_class, steps: steps, resources: resources)
        expect(result.errors.none? { |e| e.include?(":good_step") }).to be true
      end
    end
  end
end
