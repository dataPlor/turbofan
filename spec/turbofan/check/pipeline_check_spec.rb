require "spec_helper"

RSpec.describe Turbofan::Check::PipelineCheck, :schemas do
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::CheckCe", klass)
    klass
  end

  describe ".run" do
    context "with a valid pipeline" do
      let(:pipeline_class) do
        ce = ce_class
        stub_const("Extract", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 2
          ram 4

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Load", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:extract_step) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 2
          ram 4
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:load_step) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:steps) { {extract: extract_step, load: load_step} }

      it "passes for a valid pipeline" do
        result = described_class.run(pipeline: pipeline_class, steps: steps)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class, steps: steps)
        expect(result.errors).to be_empty
      end
    end

    context "when pipeline name is missing" do
      let(:pipeline_class) do
        ce = ce_class
        stub_const("Extract", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 2
          ram 4

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline do
            extract(trigger_input)
          end
        end
      end

      let(:extract_step) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 2
          ram 4
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "fails when pipeline name is not set" do
        result = described_class.run(pipeline: pipeline_class, steps: {extract: extract_step})
        expect(result.passed?).to be false
      end

      it "reports an error about missing pipeline name" do
        result = described_class.run(pipeline: pipeline_class, steps: {extract: extract_step})
        expect(result.errors.any? { |e| e.match?(/name.*blank|name.*not set/i) }).to be true
      end
    end

    context "when step is missing sizes and default cpu/ram" do
      let(:pipeline_class) do
        ce = ce_class
        stub_const("BrokenStep", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"

          pipeline do
            broken_step(trigger_input)
          end
        end
      end

      let(:broken_step) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "fails when a step has neither sizes nor default cpu/ram" do
        result = described_class.run(pipeline: pipeline_class, steps: {broken_step: broken_step})
        expect(result.passed?).to be false
      end

      it "reports which step is misconfigured" do
        result = described_class.run(pipeline: pipeline_class, steps: {broken_step: broken_step})
        expect(result.errors.any? { |e| e.include?(":broken_step") }).to be true
      end
    end

    context "when step is missing compute_environment" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "no-ce-pipeline"
        end
      end

      let(:step_class) do
        Class.new do
          include Turbofan::Step

          execution :batch
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "reports an error (not just a warning) when step has no compute_environment" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        ce_errors = result.errors.select { |e| e.match?(/compute_environment/i) }
        expect(ce_errors).not_to be_empty
      end

      it "does not pass when compute_environment is missing" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
      end

      it "requires compute_environment on each step even if pipeline has one" do
        pipeline_with_ce = Class.new do
          include Turbofan::Pipeline

          pipeline_name "pipeline-with-ce"
        end
        # Even if pipeline had a CE, the step must declare its own
        result = described_class.run(pipeline: pipeline_with_ce, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?("compute_environment") }).to be true
      end
    end

    context "when step has sizes defined" do
      let(:pipeline_class) do
        ce = ce_class
        stub_const("SizedStep", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce

          input_schema "passthrough.json"
          output_schema "passthrough.json"
          size :s, cpu: 1, ram: 2
          size :l, cpu: 4, ram: 8
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"

          pipeline do
            sized_step(trigger_input)
          end
        end
      end

      let(:sized_step) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          input_schema "passthrough.json"
          output_schema "passthrough.json"
          size :s, cpu: 1, ram: 2
          size :l, cpu: 4, ram: 8
        end
      end

      it "passes when a step has sizes defined" do
        result = described_class.run(pipeline: pipeline_class, steps: {sized_step: sized_step})
        expect(result.passed?).to be true
      end
    end

    context "when step has only cpu and no ram" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "fails when step has cpu but no ram" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
      end

      it "reports that ram is missing" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?("missing ram") }).to be true
      end
    end

    context "when step has only ram and no cpu" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          ram 4
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "fails when step has ram but no cpu" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
      end

      it "reports that cpu is missing" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?("missing cpu") }).to be true
      end
    end

    context "when size is missing ram" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          input_schema "passthrough.json"
          output_schema "passthrough.json"
          size :s, cpu: 1
          size :l, cpu: 4
        end
      end

      it "fails when a size is missing ram" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
      end

      it "reports which size is missing ram" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?(":s") && e.include?("ram") }).to be true
      end
    end

    context "when size is missing cpu" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          input_schema "passthrough.json"
          output_schema "passthrough.json"
          size :m, ram: 4
        end
      end

      it "fails when a size is missing cpu" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
      end

      it "reports which size is missing cpu" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?(":m") && e.include?("cpu") }).to be true
      end
    end

    context "when DAG step names mismatch with loaded steps" do
      let(:pipeline_class) do
        ce = ce_class
        stub_const("Extract", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 2
          ram 4

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Load", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:extract_step) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 2
          ram 4
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:transform_step) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "fails when a DAG step has no corresponding Step class" do
        result = described_class.run(pipeline: pipeline_class, steps: {extract: extract_step})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":load") && e.match?(/no Step class/i) }).to be true
      end

      it "fails when a Step class has no corresponding DAG step" do
        result = described_class.run(pipeline: pipeline_class, steps: {extract: extract_step, load: extract_step, transform: transform_step})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":transform") && e.match?(/not referenced/i) }).to be true
      end
    end

    context "without a pipeline block" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "no-dag-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "does not crash when no pipeline block is defined" do
        result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
        expect(result.passed?).to be true
      end
    end

    context "when step is missing input_schema declaration" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          output_schema "passthrough.json"
        end
      end

      it "reports an error about missing input_schema" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?("input_schema") }).to be true
      end
    end

    context "when step is missing output_schema declaration" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
        end
      end

      it "reports an error about missing output_schema" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?("output_schema") }).to be true
      end
    end

    context "when schema file does not exist" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "nonexistent.json"
          output_schema "passthrough.json"
        end
      end

      it "reports an error about missing schema file" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?("not found") }).to be true
      end
    end

    context "with DAG edge schema incompatibility" do
      let(:schemas_dir) { Dir.mktmpdir("turbofan-schema-test") }
      let(:pipeline_class) do
        ce = ce_class
        stub_const("StepA", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2

          input_schema "passthrough.json"
          output_schema "latlng_output.json"
        })
        stub_const("StepB", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2

          input_schema "address_input.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
          pipeline do
            result = step_a(trigger_input)
            step_b(result)
          end
        end
      end
      let(:step_a_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "latlng_output.json"
        end
      end
      let(:step_b_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "address_input.json"
          output_schema "passthrough.json"
        end
      end

      before do
        Turbofan.schemas_path = schemas_dir
        File.write(File.join(schemas_dir, "latlng_output.json"), JSON.generate({
          "type" => "object",
          "properties" => {"lat" => {"type" => "number"}, "lng" => {"type" => "number"}}
        }))
        File.write(File.join(schemas_dir, "address_input.json"), JSON.generate({
          "type" => "object",
          "properties" => {"address" => {"type" => "string"}},
          "required" => ["address"]
        }))
        File.write(File.join(schemas_dir, "passthrough.json"), '{"type": "object"}')
      end

      after { FileUtils.rm_rf(schemas_dir) }

      it "reports an error about incompatible schemas" do
        result = described_class.run(pipeline: pipeline_class, steps: {step_a: step_a_class, step_b: step_b_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?("address") }).to be true
      end
    end

    context "when compute_environment symbol does not resolve" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :nonexistent_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "reports an error about unresolvable compute_environment" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":my_step") && e.match?(/could not resolve/i) }).to be true
      end
    end

    context "when schema file contains invalid JSON" do
      let(:schemas_dir) { Dir.mktmpdir("turbofan-bad-schema-test") }
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "broken.json"
          output_schema "passthrough.json"
        end
      end

      before do
        Turbofan.schemas_path = schemas_dir
        File.write(File.join(schemas_dir, "broken.json"), "{not valid json")
        File.write(File.join(schemas_dir, "passthrough.json"), '{"type": "object"}')
      end

      after { FileUtils.rm_rf(schemas_dir) }

      it "reports an error about invalid JSON" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?(":my_step") && e.include?("not valid JSON") }).to be true
      end
    end

    context "with a valid 6-field cron schedule" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
          schedule "0 12 * * ? *"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "passes validation" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.errors.select { |e| e.include?("cron") || e.include?("EventBridge") }).to be_empty
      end
    end

    context "when pipeline name is whitespace-only" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "   "
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "fails with a blank name error" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.match?(/name.*blank/i) }).to be true
      end
    end

    context "with an invalid cron field count" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "my-pipeline"
          schedule "0 12 * * *"
        end
      end

      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :check_ce
          cpu 1
          ram 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      it "produces an error (not a warning) for wrong cron field count" do
        result = described_class.run(pipeline: pipeline_class, steps: {my_step: step_class})
        expect(result.passed?).to be false
        expect(result.errors.any? { |e| e.include?("5 fields") && e.include?("EventBridge requires exactly 6") }).to be true
        expect(result.warnings.none? { |w| w.include?("EventBridge") }).to be true
      end
    end

    context "batch_size validation" do
      context "when fan-out step uses default batch_size" do
        let(:pipeline_class) do
          ce = ce_class
          stub_const("Process", Class.new {
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            cpu 1
            ram 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          })
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "default-batch-size"
            pipeline do
              fan_out(process(trigger_input))
            end
          end
        end

        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            cpu 1
            ram 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        it "passes with default batch_size of 1" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be true
        end
      end

      context "when fan-out step has explicit batch_size" do
        let(:pipeline_class) do
          ce = ce_class
          stub_const("Process", Class.new {
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            cpu 1
            ram 2
            batch_size 100
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          })
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "has-batch-size"
            pipeline do
              fan_out(process(trigger_input))
            end
          end
        end

        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            cpu 1
            ram 2
            batch_size 100
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        it "passes when fan-out step has explicit batch_size" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be true
        end
      end

      context "when routed fan-out sizes use default batch_size" do
        let(:pipeline_class) do
          ce = ce_class
          stub_const("Process", Class.new {
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            size :s, cpu: 1, ram: 2, batch_size: 100
            size :l, cpu: 4, ram: 8
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          })
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "routed-default-batch-size"
            pipeline do
              fan_out(process(trigger_input))
            end
          end
        end

        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            size :s, cpu: 1, ram: 2, batch_size: 100
            size :l, cpu: 4, ram: 8
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        it "passes when size :l falls back to default batch_size of 1" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be true
        end
      end

      context "when routed fan-out size falls back to explicit step default" do
        let(:pipeline_class) do
          ce = ce_class
          stub_const("Process", Class.new {
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            batch_size 10
            size :s, cpu: 1, ram: 2, batch_size: 100
            size :l, cpu: 4, ram: 8
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          })
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "routed-with-default"
            pipeline do
              fan_out(process(trigger_input))
            end
          end
        end

        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            batch_size 10
            size :s, cpu: 1, ram: 2, batch_size: 100
            size :l, cpu: 4, ram: 8
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        it "passes when size falls back to explicit step default batch_size" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be true
        end
      end
    end

    context "router validation" do
      context "when routed fan-out step has no router file" do
        let(:pipeline_class) do
          ce = ce_class
          stub_const("Process", Class.new {
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            size :s, cpu: 1, ram: 2
            size :l, cpu: 4, ram: 8
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          })
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "routed-no-router"
            pipeline do
              fan_out(process(trigger_input))
            end
          end
        end

        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            size :s, cpu: 1, ram: 2
            size :l, cpu: 4, ram: 8
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        it "warns when routed step has no router file" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be true # warning, not error
          expect(result.warnings.any? { |w| w.include?(":process") && w.include?("no router") }).to be true
        end
      end

      context "when non-routed fan-out step has no router" do
        let(:pipeline_class) do
          ce = ce_class
          stub_const("Process", Class.new {
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            cpu 1
            ram 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          })
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "non-routed"
            pipeline do
              fan_out(process(trigger_input))
            end
          end
        end

        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :batch
            compute_environment :check_ce
            cpu 1
            ram 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        it "does not warn for non-routed fan-out" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.warnings.none? { |w| w.include?("router") }).to be true
        end
      end
    end

    context "execution model validation" do
      context "when step has no execution declared" do
        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            compute_environment :check_ce
            cpu 1
            ram 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        let(:pipeline_class) do
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "no-execution"
          end
        end

        it "errors when execution is not declared" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be false
          expect(result.errors.any? { |e| e.include?(":process") && e.include?("no execution model") }).to be true
        end
      end

      context "when fan-out step uses execution :lambda" do
        let(:pipeline_class) do
          ce = ce_class
          stub_const("Process", Class.new {
            include Turbofan::Step
            execution :lambda
            compute_environment :check_ce
            ram 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          })
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "lambda-fan-out"
            pipeline do
              fan_out(process(trigger_input))
            end
          end
        end

        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :lambda
            compute_environment :check_ce
            ram 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        it "errors when fan-out step is not execution :batch" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be false
          expect(result.errors.any? { |e| e.include?(":process") && e.include?("fan-out") && e.include?(":lambda") }).to be true
        end
      end

      context "when :lambda step has no ram" do
        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :lambda
            compute_environment :check_ce
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        let(:pipeline_class) do
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "lambda-no-ram"
          end
        end

        it "errors when :lambda step has no ram" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be false
          expect(result.errors.any? { |e| e.include?(":process") && e.include?("requires `ram`") }).to be true
        end
      end

      context "when :lambda step exceeds ram limit" do
        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :lambda
            compute_environment :check_ce
            ram 12
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        let(:pipeline_class) do
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "lambda-too-much-ram"
          end
        end

        it "errors when :lambda step has ram > 10 GB" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be false
          expect(result.errors.any? { |e| e.include?(":process") && e.include?("exceeds Lambda maximum") }).to be true
        end
      end

      context "when :lambda step declares cpu" do
        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :lambda
            compute_environment :check_ce
            cpu 2
            ram 4
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        let(:pipeline_class) do
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "lambda-with-cpu"
          end
        end

        it "warns that Lambda ignores cpu" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be true
          expect(result.warnings.any? { |w| w.include?(":process") && w.include?("cpu") && w.include?("ignores") }).to be true
        end
      end

      context "when :fargate step has no cpu" do
        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :fargate
            compute_environment :check_ce
            ram 4
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        let(:pipeline_class) do
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "fargate-no-cpu"
          end
        end

        it "errors when :fargate step has no cpu" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be false
          expect(result.errors.any? { |e| e.include?(":process") && e.include?("requires `cpu`") }).to be true
        end
      end

      context "when :fargate step has no ram" do
        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :fargate
            compute_environment :check_ce
            cpu 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        let(:pipeline_class) do
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "fargate-no-ram"
          end
        end

        it "errors when :fargate step has no ram" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.passed?).to be false
          expect(result.errors.any? { |e| e.include?(":process") && e.include?("requires `ram`") }).to be true
        end
      end

      context "when :lambda step has sizes" do
        let(:step_class) do
          ce = ce_class
          Class.new do
            include Turbofan::Step
            execution :lambda
            compute_environment :check_ce
            ram 4
            size :s, cpu: 1, ram: 2
            input_schema "passthrough.json"
            output_schema "passthrough.json"
          end
        end

        let(:pipeline_class) do
          Class.new do
            include Turbofan::Pipeline
            pipeline_name "lambda-with-sizes"
          end
        end

        it "warns that sizes are only for batch fan-out" do
          result = described_class.run(pipeline: pipeline_class, steps: {process: step_class})
          expect(result.warnings.any? { |w| w.include?(":process") && w.include?("sizes") }).to be true
        end
      end
    end
  end
end
