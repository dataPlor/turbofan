require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan check" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-check-test") }

  after do
    FileUtils.rm_rf(tmpdir)
    Turbofan.schemas_path = nil
  end

  # Helper to clean up dynamically defined constants
  def remove_consts(*names)
    names.each { |n| Object.send(:remove_const, n) if Object.const_defined?(n) } # rubocop:disable RSpec/RemoveConst
  end

  # Helper to scaffold a pipeline with one step (replaces old `new` behavior)
  def scaffold_pipeline_with_step(name)
    # Ensure the compute environment constant exists for the scaffolded worker
    ComputeEnvironments.const_set(:Compute, TestCe) unless ComputeEnvironments.const_defined?(:Compute)

    Turbofan::CLI::New.call(name)
    step_name = "#{name}_step1"
    Turbofan::CLI::Add.call(step_name, duckdb: true, compute_environment: :compute, cpu: 1)
    # Wire the step into the pipeline
    class_name = name.split("_").map(&:capitalize).join
    step_name.split("_").map(&:capitalize).join
    File.write(File.join("turbofans", "pipelines", "#{name}.rb"), <<~RUBY)
      require_relative "../steps/#{step_name}/worker"

      class #{class_name}
        include Turbofan::Pipeline

        pipeline_name "#{name.tr("_", "-")}"

        pipeline do
          #{step_name}(trigger_input)
        end
      end
    RUBY
  end

  context "with a valid pipeline" do
    before { remove_consts(:CheckValidPipeline, :CheckValidPipelineStep1) }
    after { remove_consts(:CheckValidPipeline, :CheckValidPipelineStep1) }

    it "passes all checks for a valid pipeline" do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        scaffold_pipeline_with_step("check_valid_pipeline")

        output = capture_stdout do
          Turbofan::CLI.start(["check", "check_valid_pipeline", "production"])
        end
        expect(output).to match(/passed/i)
      end
    end
  end

  context "when pipeline has a missing name" do
    before { remove_consts(:CheckNonamePipeline, :CheckNonamePipelineStep1) }
    after { remove_consts(:CheckNonamePipeline, :CheckNonamePipelineStep1) }

    it "exits with code 1 when pipeline name is missing" do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        scaffold_pipeline_with_step("check_noname_pipeline")
        remove_consts(:CheckNonamePipeline)

        File.write(File.join("turbofans", "pipelines", "check_noname_pipeline.rb"), <<~RUBY)
          require_relative "../steps/check_noname_pipeline_step1/worker"

          class CheckNonamePipeline
            include Turbofan::Pipeline

            pipeline do
              check_noname_pipeline_step1(trigger_input)
            end
          end
        RUBY

        expect {
          Turbofan::CLI.start(["check", "check_noname_pipeline", "production"])
        }.to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
      end
    end

    it "reports an error about the missing pipeline name" do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        scaffold_pipeline_with_step("check_noname_pipeline")
        remove_consts(:CheckNonamePipeline)

        File.write(File.join("turbofans", "pipelines", "check_noname_pipeline.rb"), <<~RUBY)
          require_relative "../steps/check_noname_pipeline_step1/worker"

          class CheckNonamePipeline
            include Turbofan::Pipeline

            pipeline do
              check_noname_pipeline_step1(trigger_input)
            end
          end
        RUBY

        stderr_output = capture_stderr do
          Turbofan::CLI.start(["check", "check_noname_pipeline", "production"])
        rescue SystemExit
          # expected
        end
        expect(stderr_output).to match(/name/i)
      end
    end
  end

  context "when pipeline has a step missing cpu/ram" do
    before { remove_consts(:CheckNocpuPipeline, :CheckNocpuPipelineStep1) }
    after { remove_consts(:CheckNocpuPipeline, :CheckNocpuPipelineStep1) }

    it "exits with code 1 when a step has no sizes or default cpu/ram" do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        scaffold_pipeline_with_step("check_nocpu_pipeline")
        remove_consts(:CheckNocpuPipeline, :CheckNocpuPipelineStep1)

        File.write(File.join("turbofans", "steps", "check_nocpu_pipeline_step1", "worker.rb"), <<~RUBY)
          class CheckNocpuPipelineStep1
            include Turbofan::Step

            compute_environment TestCe
            input_schema "check_nocpu_pipeline_step1_input.json"
            output_schema "check_nocpu_pipeline_step1_output.json"

            def call(inputs, context)
            end
          end
        RUBY

        File.write(File.join("turbofans", "pipelines", "check_nocpu_pipeline.rb"), <<~RUBY)
          require_relative "../steps/check_nocpu_pipeline_step1/worker"

          class CheckNocpuPipeline
            include Turbofan::Pipeline

            pipeline_name "check-nocpu-pipeline"

            pipeline do
              check_nocpu_pipeline_step1(trigger_input)
            end
          end
        RUBY

        expect {
          Turbofan::CLI.start(["check", "check_nocpu_pipeline", "production"])
        }.to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
      end
    end

    it "reports an error about the misconfigured step" do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        scaffold_pipeline_with_step("check_nocpu_pipeline")
        remove_consts(:CheckNocpuPipeline, :CheckNocpuPipelineStep1)

        File.write(File.join("turbofans", "steps", "check_nocpu_pipeline_step1", "worker.rb"), <<~RUBY)
          class CheckNocpuPipelineStep1
            include Turbofan::Step

            compute_environment TestCe
            input_schema "check_nocpu_pipeline_step1_input.json"
            output_schema "check_nocpu_pipeline_step1_output.json"

            def call(inputs, context)
            end
          end
        RUBY

        File.write(File.join("turbofans", "pipelines", "check_nocpu_pipeline.rb"), <<~RUBY)
          require_relative "../steps/check_nocpu_pipeline_step1/worker"

          class CheckNocpuPipeline
            include Turbofan::Pipeline

            pipeline_name "check-nocpu-pipeline"

            pipeline do
              check_nocpu_pipeline_step1(trigger_input)
            end
          end
        RUBY

        stderr_output = capture_stderr do
          Turbofan::CLI.start(["check", "check_nocpu_pipeline", "production"])
        rescue SystemExit
          # expected
        end
        expect(stderr_output).to match(/check_nocpu_pipeline_step1/i)
      end
    end
  end

  context "without PIPELINE and STAGE positional args" do
    it "errors when no args provided" do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        Turbofan::CLI::New.call("check_flag_pipeline")
        expect {
          Turbofan::CLI.start(["check"])
        }.to output(/PIPELINE|STAGE|wrong number of arguments/i).to_stderr.or raise_error(SystemExit)
      end
    end
  end

  context "when step has invalid compute_environment" do
    before { remove_consts(:CheckBadcePipeline, :CheckBadcePipelineStep1) }
    after { remove_consts(:CheckBadcePipeline, :CheckBadcePipelineStep1) }

    it "reports error when step compute_environment does not include ComputeEnvironment" do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        scaffold_pipeline_with_step("check_badce_pipeline")
        remove_consts(:CheckBadcePipeline, :CheckBadcePipelineStep1)

        File.write(File.join("turbofans", "steps", "check_badce_pipeline_step1", "worker.rb"), <<~RUBY)
          class NotAComputeEnvironment; end

          class CheckBadcePipelineStep1
            include Turbofan::Step

            compute_environment TestCe
            cpu 1
            input_schema "check_badce_pipeline_step1_input.json"
            output_schema "check_badce_pipeline_step1_output.json"
            compute_environment NotAComputeEnvironment

            def call(inputs, context)
            end
          end
        RUBY

        File.write(File.join("turbofans", "pipelines", "check_badce_pipeline.rb"), <<~RUBY)
          require_relative "../steps/check_badce_pipeline_step1/worker"

          class CheckBadcePipeline
            include Turbofan::Pipeline

            pipeline_name "check-badce-pipeline"

            pipeline do
              check_badce_pipeline_step1(trigger_input)
            end
          end
        RUBY

        # Step's compute_environment DSL raises ArgumentError when class doesn't include
        # Turbofan::ComputeEnvironment. The error surfaces during file load.
        expect {
          Turbofan::CLI.start(["check", "check_badce_pipeline", "production"])
        }.to raise_error(ArgumentError, /must include Turbofan::ComputeEnvironment/)
      end
    end
  end

  private

  def capture_stderr
    original = $stderr
    $stderr = StringIO.new
    yield
    $stderr.string
  ensure
    $stderr = original
  end
end
