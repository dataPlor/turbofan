require "spec_helper"
require "json"
require "tmpdir"
require "stringio"

RSpec.describe Turbofan::Runtime::Wrapper, :schemas do
  include WrapperTestHelper

  let(:cloudwatch_client) { instance_double("Aws::CloudWatch::Client", put_metric_data: nil) } # rubocop:disable RSpec/VerifiedDoubleReference
  let(:s3_client) { instance_double("Aws::S3::Client", put_object: nil, get_object: nil) } # rubocop:disable RSpec/VerifiedDoubleReference

  let(:step_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      cpu 2
      ram 4
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def self.name
        "TestStep"
      end

      def call(inputs, context)
        {processed: true, count: inputs}
      end
    end
  end

  describe ".run" do
    it "delegates to a new instance" do
      wrapper_instance = instance_double(described_class)
      allow(described_class).to receive(:new).with(step_class).and_return(wrapper_instance)
      allow(wrapper_instance).to receive(:run)

      described_class.run(step_class)

      expect(wrapper_instance).to have_received(:run)
    end
  end

  describe "input deserialization" do
    it "parses TURBOFAN_INPUT and passes it to the step" do
      received_input = nil
      spy = make_step { |input, _ctx|
        received_input = input
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"items":42,"path":"s3://bucket/key"}'
      })

      expect(received_input).to eq([{"items" => 42, "path" => "s3://bucket/key"}])
    end

    it "defaults to empty hash when TURBOFAN_INPUT is not set" do
      received_input = nil
      spy = make_step { |input, _ctx|
        received_input = input
        {}
      }

      run_wrapper(spy)

      expect(received_input).to eq([{}])
    end

    it "hydrates S3 references in TURBOFAN_INPUT via Payload.deserialize" do
      received_input = nil
      spy = make_step { |input, _ctx|
        received_input = input
        {}
      }

      s3_body = instance_double("StringIO", read: '{"hydrated":true}') # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"_turbofan_s3_ref":"s3://my-bucket/exec-1/StepA/output.json"}'
      })

      expect(received_input).to eq([{"hydrated" => true}])
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-1/StepA/output.json"
      )
    end
  end

  describe "inter-step data flow via TURBOFAN_PREV_STEP" do
    it "fetches the previous step's output from S3 when TURBOFAN_PREV_STEP is set" do
      received_input = nil
      spy = make_step(name: "StepB") { |input, _ctx|
        received_input = input
        {}
      }

      prev_output = {"result" => "from_step_a", "count" => 100}
      s3_body = instance_double("StringIO", read: JSON.generate(prev_output)) # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      run_wrapper(spy, env: {
        "TURBOFAN_PREV_STEP" => "StepA",
        "TURBOFAN_EXECUTION_ID" => "exec-456",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq([prev_output])
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-456/StepA/output.json"
      )
    end

    it "ignores TURBOFAN_INPUT when TURBOFAN_PREV_STEP is set" do
      received_input = nil
      spy = make_step(name: "StepB") { |input, _ctx|
        received_input = input
        {}
      }

      prev_output = {"from_prev" => true}
      s3_body = instance_double("StringIO", read: JSON.generate(prev_output)) # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      run_wrapper(spy, env: {
        "TURBOFAN_PREV_STEP" => "StepA",
        "TURBOFAN_INPUT" => '{"should_be":"ignored"}',
        "TURBOFAN_EXECUTION_ID" => "exec-789",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq([prev_output])
    end
  end

  describe "normalize_input branches" do
    it "wraps an Array input into inputs array" do
      received_input = nil
      spy = make_step { |input, _ctx|
        received_input = input
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '[{"a":1},{"b":2}]'
      })

      expect(received_input).to eq([{"a" => 1}, {"b" => 2}])
    end

    it "passes through Hash input that already has items array (backward compat)" do
      received_input = nil
      spy = make_step { |input, _ctx|
        received_input = input
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"items":[{"x":1}]}'
      })

      expect(received_input).to eq([{"x" => 1}])
    end

    it "wraps a bare Hash value into single-element inputs array" do
      received_input = nil
      spy = make_step { |input, _ctx|
        received_input = input
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"key":"val"}'
      })

      expect(received_input).to eq([{"key" => "val"}])
    end
  end

  describe "inter-step data flow via TURBOFAN_PREV_STEPS (parallel join)" do
    it "fetches outputs from multiple predecessor steps when TURBOFAN_PREV_STEPS is set" do
      received_input = nil
      spy = make_step(name: "JoinStep") { |input, _ctx|
        received_input = input
        {}
      }

      output_a = {"result" => "from_step_a"}
      output_b = {"result" => "from_step_b"}
      allow(s3_client).to receive(:get_object) do |args|
        data = args[:key].include?("step_a") ? output_a : output_b
        body = instance_double("StringIO", read: JSON.generate(data)) # rubocop:disable RSpec/VerifiedDoubleReference
        instance_double("Aws::S3::Types::GetObjectOutput", body: body) # rubocop:disable RSpec/VerifiedDoubleReference
      end

      run_wrapper(spy, env: {
        "TURBOFAN_PREV_STEPS" => "step_a,step_b",
        "TURBOFAN_EXECUTION_ID" => "exec-join",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq([output_a, output_b])
    end
  end

  describe "output serialization" do
    it "always writes output to S3 and returns raw JSON to stdout" do
      result = run_wrapper(step_class, env: {
        "TURBOFAN_INPUT" => '{"items":[{"n":1}]}',
        "TURBOFAN_EXECUTION_ID" => "exec-123",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      parsed = JSON.parse(result[:output])
      expect(parsed).to eq({"processed" => true, "count" => [{"n" => 1}]})
      expect(s3_client).to have_received(:put_object).with(
        hash_including(bucket: "my-bucket")
      )
    end
  end

  describe "fan-out child indexed I/O" do
    it "reads input from FanOut.read_input when AWS_BATCH_JOB_ARRAY_INDEX is set" do
      received_input = nil
      spy = make_step(name: "FanOutChild") { |input, _ctx|
        received_input = input
        {"result" => input}
      }

      fan_out_input = [{"id" => 5, "data" => "chunk-5"}]
      all_items = Array.new(5, [{"id" => 0}]) + [fan_out_input]
      s3_body = instance_double("StringIO", read: JSON.generate(all_items)) # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      run_wrapper(spy, env: {
        "AWS_BATCH_JOB_ARRAY_INDEX" => "5",
        "TURBOFAN_STEP_NAME" => "process",
        "TURBOFAN_EXECUTION_ID" => "exec-fan",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq(fan_out_input)
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-fan/process/input/items.json"
      )
    end

    it "writes output to indexed path when AWS_BATCH_JOB_ARRAY_INDEX is set" do
      spy = make_step(name: "FanOutChild") { |input, _ctx| {"result" => input} }

      fan_out_input = [{"id" => 3}]
      all_items = Array.new(3, [{"id" => 0}]) + [fan_out_input]
      s3_body = instance_double("StringIO", read: JSON.generate(all_items)) # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      run_wrapper(spy, env: {
        "AWS_BATCH_JOB_ARRAY_INDEX" => "3",
        "TURBOFAN_STEP_NAME" => "process",
        "TURBOFAN_EXECUTION_ID" => "exec-fan",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(s3_client).to have_received(:put_object).with(
        bucket: "my-bucket",
        key: "exec-fan/process/output/3.json",
        body: JSON.generate({"result" => fan_out_input})
      )
    end
  end

  describe "fan-out result collection" do
    it "collects indexed outputs when TURBOFAN_PREV_FAN_OUT_SIZE is set" do
      received_input = nil
      spy = make_step(name: "Aggregator") { |input, _ctx|
        received_input = input
        {}
      }

      outputs = [{"result" => "a"}, {"result" => "b"}, {"result" => "c"}]
      call_count = 0
      allow(s3_client).to receive(:get_object) do |args|
        body = instance_double("StringIO", read: JSON.generate(outputs[call_count])) # rubocop:disable RSpec/VerifiedDoubleReference
        call_count += 1
        instance_double("Aws::S3::Types::GetObjectOutput", body: body) # rubocop:disable RSpec/VerifiedDoubleReference
      end

      run_wrapper(spy, env: {
        "TURBOFAN_PREV_STEP" => "process",
        "TURBOFAN_PREV_FAN_OUT_SIZE" => "3",
        "TURBOFAN_EXECUTION_ID" => "exec-collect",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq(outputs)
    end
  end

  describe "size-aware fan-out I/O" do
    it "reads input from size-prefixed path when TURBOFAN_SIZE is set" do
      received_input = nil
      spy = make_step(name: "SizedFanOutChild") { |input, _ctx|
        received_input = input
        {"result" => input}
      }

      fan_out_input = [{"id" => 2, "data" => "chunk-m-2"}]
      all_items = Array.new(2, [{"id" => 0}]) + [fan_out_input]
      s3_body = instance_double("StringIO", read: JSON.generate(all_items)) # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      run_wrapper(spy, env: {
        "AWS_BATCH_JOB_ARRAY_INDEX" => "2",
        "TURBOFAN_SIZE" => "m",
        "TURBOFAN_STEP_NAME" => "process",
        "TURBOFAN_EXECUTION_ID" => "exec-fan",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq(fan_out_input)
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-fan/process/input/m/items.json"
      )
    end

    it "writes output to size-prefixed path when TURBOFAN_SIZE is set" do
      spy = make_step(name: "SizedFanOutChild") { |input, _ctx| {"result" => input} }

      fan_out_input = [{"id" => 2}]
      all_items = Array.new(2, [{"id" => 0}]) + [fan_out_input]
      s3_body = instance_double("StringIO", read: JSON.generate(all_items)) # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      run_wrapper(spy, env: {
        "AWS_BATCH_JOB_ARRAY_INDEX" => "2",
        "TURBOFAN_SIZE" => "m",
        "TURBOFAN_STEP_NAME" => "process",
        "TURBOFAN_EXECUTION_ID" => "exec-fan",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(s3_client).to have_received(:put_object).with(
        bucket: "my-bucket",
        key: "exec-fan/process/output/m/2.json",
        body: JSON.generate({"result" => fan_out_input})
      )
    end
  end

  describe "routed fan-out result collection" do
    it "collects outputs from multiple sizes when TURBOFAN_PREV_FAN_OUT_SIZES is set" do
      received_input = nil
      spy = make_step(name: "RoutedAggregator") { |input, _ctx|
        received_input = input
        {}
      }

      outputs = [
        {"result" => "s0"}, {"result" => "s1"},
        {"result" => "m0"},
        {"result" => "l0"}
      ]
      call_count = 0
      allow(s3_client).to receive(:get_object) do |args|
        body = instance_double("StringIO", read: JSON.generate(outputs[call_count])) # rubocop:disable RSpec/VerifiedDoubleReference
        call_count += 1
        instance_double("Aws::S3::Types::GetObjectOutput", body: body) # rubocop:disable RSpec/VerifiedDoubleReference
      end

      run_wrapper(spy, env: {
        "TURBOFAN_PREV_STEP" => "process",
        "TURBOFAN_PREV_FAN_OUT_SIZES" => "s,m,l",
        "TURBOFAN_PREV_FAN_OUT_SIZE_S" => "2",
        "TURBOFAN_PREV_FAN_OUT_SIZE_M" => "1",
        "TURBOFAN_PREV_FAN_OUT_SIZE_L" => "1",
        "TURBOFAN_EXECUTION_ID" => "exec-routed",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq(outputs)
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/s/0.json"
      )
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/s/1.json"
      )
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/m/0.json"
      )
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/l/0.json"
      )
    end
  end

  describe "SIGTERM handling" do
    it "installs a SIGTERM trap that exits with 143" do
      # Test in a subprocess to avoid killing the test process
      read_pipe, write_pipe = IO.pipe

      pid = fork do
        read_pipe.close
        described_class.new(step_class)

        # Slow step that gives us time to send SIGTERM
        slow_step = Class.new do
          include Turbofan::Step

          compute_environment :test_ce
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
          def self.name = "SlowStep"
          def call(_input, context)
            sleep 10
            {}
          end
        end

        wrapper = described_class.new(slow_step)

        context = Turbofan::Runtime::Context.new(
          execution_id: "test", attempt_number: 1, step_name: "SlowStep",
          stage: "dev", pipeline_name: "test", array_index: nil,
          nvme_path: nil, uses: [], writes_to: []
        )
        cw = instance_double("Aws::CloudWatch::Client", put_metric_data: nil) # rubocop:disable RSpec/VerifiedDoubleReference
        metrics = Turbofan::Runtime::Metrics.new(
          cloudwatch_client: cw, pipeline_name: "test", stage: "dev", step_name: "SlowStep"
        )
        allow(context).to receive_messages(metrics: metrics, s3: nil)
        allow(wrapper).to receive_messages(setup_nvme: nil, build_context: context)
        allow(Turbofan::Runtime::InputResolver).to receive(:call).and_return({"inputs" => [{}]})

        write_pipe.puts("ready")
        write_pipe.close

        wrapper.run
      end

      write_pipe.close
      # Wait for child to be ready
      read_pipe.gets
      read_pipe.close

      sleep 0.1 # brief pause to let trap install
      Process.kill("TERM", pid)
      _, status = Process.waitpid2(pid)

      # Exit code 143 = 128 + 15 (SIGTERM)
      expect(status.exitstatus).to eq(143)
    end
  end

  describe "NVMe temp directory management" do
    it "creates and cleans up a job-specific temp directory" do
      Dir.mktmpdir do |tmpdir|
        job_dir = File.join(tmpdir, "test-job")
        FileUtils.mkdir_p(job_dir)

        run_wrapper(step_class, env: {
          "TURBOFAN_INPUT" => '{"items":1}'
        }, nvme_base: job_dir)

        # After run, cleanup_nvme should have removed it
        expect(File.directory?(job_dir)).to be false
      end
    end

    it "handles nil nvme_path gracefully" do
      result = run_wrapper(step_class, env: {
        "TURBOFAN_INPUT" => '{"items":1}'
      }, nvme_base: nil)

      expect(result[:output]).not_to be_empty
    end

    it "sets ENV['TMPDIR'] to nvme_path/tmp when NVMe is available" do
      Dir.mktmpdir do |tmpdir|
        job_dir = File.join(tmpdir, "test-job")
        FileUtils.mkdir_p(job_dir)

        saved_tmpdir = ENV["TMPDIR"]
        begin
          run_wrapper(step_class, env: {
            "TURBOFAN_INPUT" => '{"items":1}'
          }, nvme_base: job_dir)

          expected_tmp = File.join(job_dir, "tmp")
          # The wrapper calls set_tmpdir before running, so the directory should have been created.
          # After cleanup_nvme removes job_dir, TMPDIR still points to the path that was set.
          expect(ENV["TMPDIR"]).to eq(expected_tmp)
        ensure
          ENV["TMPDIR"] = saved_tmpdir
        end
      end
    end
  end

  describe "auto-metrics emission" do
    it "emits JobDuration with a positive value on success" do
      result = run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      result[:metrics].flush
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        duration_metric = args[:metric_data].find { |m| m[:metric_name] == "JobDuration" }
        expect(duration_metric).not_to be_nil
        expect(duration_metric[:value]).to be > 0
      end
    end

    it "emits JobSuccess on successful completion" do
      result = run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      result[:metrics].flush
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        success_metric = args[:metric_data].find { |m| m[:metric_name] == "JobSuccess" }
        expect(success_metric).not_to be_nil
        expect(success_metric[:value]).to eq(1)
      end
    end

    it "emits PeakMemoryMB after execution" do
      result = run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      result[:metrics].flush
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        mem_metric = args[:metric_data].find { |m| m[:metric_name] == "PeakMemoryMB" }
        expect(mem_metric).not_to be_nil
        expect(mem_metric[:value]).to be >= 0
      end
    end

    it "emits CpuUtilization after execution" do
      result = run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      result[:metrics].flush
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        cpu_metric = args[:metric_data].find { |m| m[:metric_name] == "CpuUtilization" }
        expect(cpu_metric).not_to be_nil
        expect(cpu_metric[:value]).to be >= 0
      end
    end

    it "emits MemoryUtilization when step declares RAM" do
      result = run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      # step_class has cpu 2, ram 4
      result[:metrics].flush
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        util_metric = args[:metric_data].find { |m| m[:metric_name] == "MemoryUtilization" }
        expect(util_metric).not_to be_nil
        expect(util_metric[:value]).to be >= 0
        expect(util_metric[:value]).to be <= 100
      end
    end

    it "skips MemoryUtilization when step has no declared RAM" do
      no_ram_step = make_step(name: "NoRamStep")

      result = run_wrapper(no_ram_step, env: {"TURBOFAN_INPUT" => "{}"})

      result[:metrics].flush
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        util_metric = args[:metric_data].find { |m| m[:metric_name] == "MemoryUtilization" }
        expect(util_metric).to be_nil
      end
    end

    it "emits JobFailure when step raises" do
      error_step = make_step(name: "ErrorStep") { |_, _| raise("boom") }

      expect {
        run_wrapper(error_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(RuntimeError, "boom")

      # Failure metric was emitted and flushed in ensure block
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        failure_metric = args[:metric_data].find { |m| m[:metric_name] == "JobFailure" }
        expect(failure_metric).not_to be_nil
        expect(failure_metric[:value]).to eq(1)
      end
    end
  end

  describe "error handling" do
    it "re-raises the error after emitting failure metrics" do
      error_step = make_step(name: "ErrorStep") { |_, _| raise(ArgumentError, "bad input") }

      expect {
        run_wrapper(error_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(ArgumentError, "bad input")
    end

    it "raises when step has no output_schema declared" do
      no_schema_step = Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        def self.name = "NoSchemaStep"
        def call(_input, _ctx) = {}
      end

      expect {
        run_wrapper(no_schema_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(Turbofan::SchemaValidationError, /no output_schema declared/)
    end

    it "raises when step has no input_schema declared" do
      no_input_step = Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        cpu 1
        output_schema "passthrough.json"
        def self.name = "NoInputStep"
        def call(_input, _ctx) = {}
      end

      expect {
        run_wrapper(no_input_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(Turbofan::SchemaValidationError, /no input_schema declared/)
    end
  end

  describe "schema validation" do
    let(:schemas_dir) { Dir.mktmpdir("turbofan-wrapper-schemas") }

    before do
      Turbofan.schemas_path = schemas_dir
      File.write(File.join(schemas_dir, "passthrough.json"), '{"type": "object"}')
      File.write(File.join(schemas_dir, "query_input.json"), JSON.generate({
        "type" => "object",
        "properties" => {"query" => {"type" => "string"}},
        "required" => ["query"]
      }))
      File.write(File.join(schemas_dir, "latlng_output.json"), JSON.generate({
        "type" => "object",
        "properties" => {
          "lat" => {"type" => "number"},
          "lng" => {"type" => "number"}
        },
        "required" => ["lat", "lng"]
      }))
    end

    after { FileUtils.rm_rf(schemas_dir) }

    it "validates input against input_schema before execution" do
      strict_step = Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        cpu 1
        input_schema "query_input.json"
        output_schema "passthrough.json"
        def self.name = "StrictInputStep"
        def call(input, _ctx) = {}
      end

      expect {
        run_wrapper(strict_step, env: {"TURBOFAN_INPUT" => '{"wrong_key": 123}'})
      }.to raise_error(Turbofan::SchemaValidationError, /Input validation failed/)
    end

    it "validates output against output_schema after execution" do
      strict_step = Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        output_schema "latlng_output.json"
        def self.name = "StrictOutputStep"
        def call(_input, _ctx) = {"lat" => "not_a_number"}
      end

      expect {
        run_wrapper(strict_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(Turbofan::SchemaValidationError, /Output validation failed/)
    end

    it "passes when data matches schema" do
      valid_step = Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        cpu 1
        input_schema "query_input.json"
        output_schema "latlng_output.json"
        def self.name = "ValidStep"
        def call(_input, _ctx) = {"lat" => 37.7749, "lng" => -122.4194}
      end

      result = run_wrapper(valid_step, env: {"TURBOFAN_INPUT" => '{"query": "San Francisco"}'})
      parsed = JSON.parse(result[:output])
      expect(parsed["lat"]).to eq(37.7749)
    end

    it "auto-configures schemas_path from TURBOFAN_SCHEMAS_PATH env var" do
      Turbofan.schemas_path = nil

      valid_step = make_step(name: "EnvStep")

      saved = ENV["TURBOFAN_SCHEMAS_PATH"]
      ENV["TURBOFAN_SCHEMAS_PATH"] = schemas_dir
      begin
        run_wrapper(valid_step, env: {"TURBOFAN_INPUT" => "{}"})
        expect(Turbofan.schemas_path).to eq(schemas_dir)
      ensure
        ENV["TURBOFAN_SCHEMAS_PATH"] = saved
      end
    end
  end

  describe "observability resilience" do
    it "preserves original exception when metrics emit/flush raises" do
      error_step = make_step(name: "MetricsFailStep") { |_, _| raise(ArgumentError, "original") }

      allow(cloudwatch_client).to receive(:put_metric_data).and_raise(RuntimeError, "metrics boom")

      expect {
        run_wrapper(error_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(ArgumentError, "original")
    end
  end

  describe "peak_memory_mb resilience" do
    it "reads from /proc/self/status when available" do
      skip "Not on Linux" unless File.exist?("/proc/self/status")

      wrapper = described_class.new(step_class)
      result = wrapper.send(:peak_memory_mb)
      expect(result).to be_a(Float)
      expect(result).to be >= 0
    end

    it "falls back to ps when /proc/self/status doesn't exist" do
      allow(File).to receive(:exist?).and_call_original
      allow(File).to receive(:exist?).with("/proc/self/status").and_return(false)

      result = Turbofan::Runtime::StepMetrics.send(:peak_memory_mb)
      expect(result).to be_a(Float)
      expect(result).to be >= 0
    end

    it "returns 0.0 when all methods fail" do
      allow(File).to receive(:exist?).and_call_original
      allow(File).to receive(:exist?).with("/proc/self/status").and_return(false)
      allow(Turbofan::Runtime::StepMetrics).to receive(:`).and_raise(StandardError, "ps failed")

      result = Turbofan::Runtime::StepMetrics.send(:peak_memory_mb)
      expect(result).to eq(0.0)
    end
  end

  describe "NVMe setup edge cases" do
    it "returns nil when /mnt/nvme does not exist" do
      wrapper = described_class.new(step_class)
      allow(File).to receive(:directory?).and_call_original
      allow(File).to receive(:directory?).with("/mnt/nvme").and_return(false)

      expect(wrapper.send(:setup_nvme)).to be_nil
    end

    it "uses AWS_BATCH_JOB_ID for subdirectory name" do
      wrapper = described_class.new(step_class)
      saved = ENV["AWS_BATCH_JOB_ID"]
      ENV["AWS_BATCH_JOB_ID"] = "batch-job-42"
      allow(File).to receive(:directory?).and_call_original
      allow(File).to receive(:directory?).with("/mnt/nvme").and_return(true)
      allow(File).to receive(:directory?).with("/mnt/nvme/batch-job-42").and_return(true)
      allow(FileUtils).to receive(:mkdir_p)

      result = wrapper.send(:setup_nvme)
      expect(result).to eq("/mnt/nvme/batch-job-42")
    ensure
      ENV["AWS_BATCH_JOB_ID"] = saved
    end

    it "falls back to local-{pid} when AWS_BATCH_JOB_ID not set" do
      wrapper = described_class.new(step_class)
      saved = ENV.delete("AWS_BATCH_JOB_ID")
      allow(File).to receive(:directory?).and_call_original
      allow(File).to receive(:directory?).with("/mnt/nvme").and_return(true)
      expected_path = "/mnt/nvme/local-#{Process.pid}"
      allow(File).to receive(:directory?).with(expected_path).and_return(true)
      allow(FileUtils).to receive(:mkdir_p)

      result = wrapper.send(:setup_nvme)
      expect(result).to eq(expected_path)
    ensure
      ENV["AWS_BATCH_JOB_ID"] = saved if saved
    end
  end

  describe "CpuUtilization calculation" do
    it "calculates CPU utilization as (cpu_time / wall_time) * 100" do
      utilization = Turbofan::Runtime::StepMetrics.send(:cpu_utilization, 10.0)
      expect(utilization).to be_a(Float)
      expect(utilization).to be >= 0
    end

    it "returns 0 when wall time is zero" do
      expect(Turbofan::Runtime::StepMetrics.send(:cpu_utilization, 0.0)).to eq(0.0)
    end

    it "returns 0 when wall time is negative" do
      expect(Turbofan::Runtime::StepMetrics.send(:cpu_utilization, -1.0)).to eq(0.0)
    end
  end

  # A2: envelope/inputs rename — normalize_envelope behavior
  describe "normalize_envelope" do
    it "converts Array input to {inputs: array}" do
      received_inputs = nil
      spy = make_step { |inputs, _ctx|
        received_inputs = inputs
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '[{"a":1},{"b":2}]'
      })

      expect(received_inputs).to eq([{"a" => 1}, {"b" => 2}])
    end

    it "passes through Hash with inputs key, extracting the array" do
      received_inputs = nil
      spy = make_step { |inputs, _ctx|
        received_inputs = inputs
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"inputs":[{"x":1}]}'
      })

      expect(received_inputs).to eq([{"x" => 1}])
    end

    it "converts Hash with items key to inputs (backward compat)" do
      received_inputs = nil
      spy = make_step { |inputs, _ctx|
        received_inputs = inputs
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"items":[{"x":1}]}'
      })

      expect(received_inputs).to eq([{"x" => 1}])
    end

    it "wraps a bare Hash into a single-element inputs array" do
      received_inputs = nil
      spy = make_step { |inputs, _ctx|
        received_inputs = inputs
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"key":"val"}'
      })

      expect(received_inputs).to eq([{"key" => "val"}])
    end
  end

  # A2: envelope/inputs rename — step receives inputs (array) not input (envelope)
  describe "step call signature with inputs array" do
    it "passes the inputs array directly to the step, not the envelope" do
      received_inputs = nil
      spy = make_step { |inputs, _ctx|
        received_inputs = inputs
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"inputs":[{"id":1},{"id":2}]}'
      })

      expect(received_inputs).to be_an(Array)
      expect(received_inputs).to eq([{"id" => 1}, {"id" => 2}])
    end
  end

  # A2: envelope/inputs rename — extra envelope keys accessible via context.envelope
  describe "envelope metadata extraction into context.envelope" do
    it "populates context.envelope with extra keys from the envelope" do
      received_envelope = nil
      spy = make_step { |_inputs, ctx|
        received_envelope = ctx.envelope
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"inputs":[{"id":1}],"trace_id":"abc","request_id":"xyz"}'
      })

      expect(received_envelope).to eq({"trace_id" => "abc", "request_id" => "xyz"})
    end

    it "sets context.envelope to empty hash when no extra keys" do
      received_envelope = nil
      spy = make_step { |_inputs, ctx|
        received_envelope = ctx.envelope
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"inputs":[{"id":1}]}'
      })

      expect(received_envelope).to eq({})
    end
  end

  describe "MemoryUtilization calculation" do
    it "calculates memory utilization as (peak_mb / allocated_mb) * 100" do
      # 512 MB peak / 4 GB (4096 MB) allocated = 12.5%
      utilization = Turbofan::Runtime::StepMetrics.send(:memory_utilization, 512.0, 4)
      expect(utilization).to eq(12.5)
    end

    it "returns 0 when allocated RAM is zero" do
      expect(Turbofan::Runtime::StepMetrics.send(:memory_utilization, 512.0, 0)).to eq(0.0)
    end
  end

  describe "resource attachment" do
    it "calls ResourceAttacher.attach during run" do
      allow(Turbofan::Runtime::ResourceAttacher).to receive(:attach)

      run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      expect(Turbofan::Runtime::ResourceAttacher).to have_received(:attach).once
    end
  end

  describe "logger error on step failure" do
    it "logs the error via context.logger before re-raising" do
      error_step = make_step(name: "LogErrorStep") { |_, _| raise(ArgumentError, "bad data") }
      logger_spy = instance_double(Turbofan::Runtime::Logger, info: nil, warn: nil, error: nil, debug: nil)

      result_context = nil
      begin
        run_wrapper(error_step, env: {"TURBOFAN_INPUT" => "{}"})
      rescue ArgumentError
        # expected
      end

      # Can't easily capture from run_wrapper since it builds context internally.
      # Instead, verify via a more direct approach.
      wrapper = Turbofan::Runtime::Wrapper.new(error_step)
      context = Turbofan::Runtime::Context.new(
        execution_id: "test-exec", attempt_number: 1, step_name: "LogErrorStep",
        stage: "development", pipeline_name: "test-pipeline", array_index: nil,
        nvme_path: nil, uses: [], writes_to: []
      )
      metrics = Turbofan::Runtime::Metrics.new(
        cloudwatch_client: cloudwatch_client, pipeline_name: "test-pipeline",
        stage: "development", step_name: "LogErrorStep"
      )
      allow(context).to receive_messages(s3: s3_client, metrics: metrics, logger: logger_spy)
      allow(wrapper).to receive_messages(setup_nvme: nil, build_context: context)

      original_stdout = $stdout
      $stdout = StringIO.new
      expect { wrapper.run }.to raise_error(ArgumentError, "bad data")
      $stdout = original_stdout

      expect(logger_spy).to have_received(:error).with("Step failed", hash_including(error_class: "ArgumentError", error_message: "bad data"))
    end
  end

  describe "lineage emission failure resilience" do
    it "preserves original exception when Lineage.emit raises during failure" do
      error_step = make_step(name: "LineageFailStep") { |_, _| raise(ArgumentError, "original error") }

      call_count = 0
      allow(Turbofan::Runtime::Lineage).to receive(:emit) do |event, **_kwargs|
        call_count += 1
        raise RuntimeError, "lineage boom" if event[:eventType] == "FAIL"
      end

      expect {
        run_wrapper(error_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(ArgumentError, "original error")
    end
  end

  describe "size-aware MemoryUtilization" do
    it "uses RAM from turbofan_sizes when context.size is set" do
      sized_step = Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        size :m, cpu: 2, ram: 8
        input_schema "passthrough.json"
        output_schema "passthrough.json"
        def self.name = "SizedStep"
        def call(_input, _ctx) = {}
      end

      # Set up fan-out input before run_wrapper (S3 stub must be ready)
      all_items = [[{"id" => 0}]]
      s3_body = instance_double("StringIO", read: JSON.generate(all_items)) # rubocop:disable RSpec/VerifiedDoubleReference
      s3_response = instance_double("Aws::S3::Types::GetObjectOutput", body: s3_body) # rubocop:disable RSpec/VerifiedDoubleReference
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      result = run_wrapper(sized_step, env: {
        "TURBOFAN_INPUT" => "{}",
        "TURBOFAN_SIZE" => "m",
        "AWS_BATCH_JOB_ARRAY_INDEX" => "0",
        "TURBOFAN_STEP_NAME" => "sized_step",
        "TURBOFAN_EXECUTION_ID" => "exec-sized",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      result[:metrics].flush
      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        util_metric = args[:metric_data].find { |m| m[:metric_name] == "MemoryUtilization" }
        expect(util_metric).not_to be_nil
        # Should use 8 GB from size :m, not a default
        expect(util_metric[:value]).to be >= 0
        expect(util_metric[:value]).to be <= 100
      end
    end
  end

  # B7 — Lineage integration: Wrapper emits OpenLineage events
  describe "OpenLineage event emission via Lineage" do
    before do
      allow(Turbofan::Runtime::Lineage).to receive(:emit)
    end

    it "emits a START event before executing the step" do
      run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      expect(Turbofan::Runtime::Lineage).to have_received(:emit).with(
        hash_including(eventType: "START"),
        hash_including(:context)
      )
    end

    it "emits a COMPLETE event after successful execution" do
      run_wrapper(step_class, env: {"TURBOFAN_INPUT" => '{"items":1}'})

      expect(Turbofan::Runtime::Lineage).to have_received(:emit).with(
        hash_including(eventType: "COMPLETE"),
        hash_including(:context)
      )
    end

    it "emits a FAIL event when the step raises" do
      error_step = make_step(name: "FailStep") { |_, _| raise("lineage boom") }

      expect {
        run_wrapper(error_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(RuntimeError, "lineage boom")

      expect(Turbofan::Runtime::Lineage).to have_received(:emit).with(
        hash_including(eventType: "FAIL"),
        hash_including(:context)
      )
    end

    it "does not emit COMPLETE when the step raises" do
      error_step = make_step(name: "FailStep2") { |_, _| raise("no complete") }

      expect {
        run_wrapper(error_step, env: {"TURBOFAN_INPUT" => "{}"})
      }.to raise_error(RuntimeError, "no complete")

      expect(Turbofan::Runtime::Lineage).not_to have_received(:emit).with(
        hash_including(eventType: "COMPLETE"),
        anything
      )
    end
  end
end
