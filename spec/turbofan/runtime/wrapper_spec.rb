# frozen_string_literal: true

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
      runs_on :batch
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
        "TURBOFAN_INPUT" => '{"__turbofan_s3_ref":"s3://my-bucket/exec-1/StepA/output.json"}'
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
        "TURBOFAN_PREV_FAN_OUT_SIZE_S" => JSON.generate([{"index" => 0, "size" => 2, "real_size" => 2}]),
        "TURBOFAN_PREV_FAN_OUT_SIZE_M" => JSON.generate([{"index" => 0, "size" => 2, "real_size" => 1}]),
        "TURBOFAN_PREV_FAN_OUT_SIZE_L" => JSON.generate([{"index" => 0, "size" => 2, "real_size" => 1}]),
        "TURBOFAN_EXECUTION_ID" => "exec-routed",
        "TURBOFAN_BUCKET" => "my-bucket"
      })

      expect(received_input).to eq(outputs)
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/s/parent0/0.json"
      )
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/s/parent0/1.json"
      )
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/m/parent0/0.json"
      )
      expect(s3_client).to have_received(:get_object).with(
        bucket: "my-bucket",
        key: "exec-routed/process/output/l/parent0/0.json"
      )
    end
  end

  describe "SIGTERM handling" do
    # Use the spec_helper's sandbox-writable tmp root rather than
    # Dir.mktmpdir (which hits /var/folders EPERM in restricted envs).
    let(:run_id)       { "#{Process.pid}-#{rand(100000)}" }
    let(:storage_path) { File.join(SPEC_TMP_ROOT, "sigterm-storage-#{run_id}") }

    before { FileUtils.mkdir_p(storage_path) }

    after { FileUtils.rm_rf(storage_path) }

    # Characterization spec for the SIGTERM lifecycle. Asserts observable
    # behaviors the retry strategy + operability depend on:
    #   (1) exit code 143 (Batch retry rule: on exit 143 → RETRY w/o counter)
    #   (2) storage_path cleaned (no leftover data on NVMe between retries)
    # Cross-process flag/metrics observation is covered by unit specs on
    # Context and Metrics separately — fork boundary makes mock-based
    # verification unreliable.
    it "cleans storage and exits 143 on SIGTERM" do
      child_storage_path = storage_path
      read_pipe, write_pipe = IO.pipe

      pid = fork do
        read_pipe.close

        slow_step = Class.new do
          include Turbofan::Step

          compute_environment :test_ce
          runs_on :batch
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
          def self.name = "SlowStep"
          def call(_input, _context)
            sleep 10
            {}
          end
        end

        wrapper = described_class.new(slow_step)

        context = Turbofan::Runtime::Context.new(
          execution_id: "test", attempt_number: 1, step_name: "SlowStep",
          stage: "dev", pipeline_name: "test", array_index: nil,
          storage_path: child_storage_path, uses: [], writes_to: []
        )
        cw = instance_double("Aws::CloudWatch::Client", put_metric_data: nil) # rubocop:disable RSpec/VerifiedDoubleReference
        metrics = Turbofan::Runtime::Metrics.new(
          cloudwatch_client: cw, pipeline_name: "test", stage: "dev", step_name: "SlowStep"
        )
        allow(context).to receive_messages(metrics: metrics, s3: nil)
        allow(wrapper).to receive_messages(setup_storage: child_storage_path, build_context: context)
        allow(Turbofan::Runtime::InputResolver).to receive(:call).and_return({"inputs" => [{}]})

        write_pipe.puts("ready")
        write_pipe.close

        wrapper.run
      end

      write_pipe.close
      read_pipe.gets
      read_pipe.close

      sleep 0.1 # let trap install before killing
      Process.kill("TERM", pid)
      _, status = Process.waitpid2(pid)

      # (1) Exit code 143 — Batch retry contract
      expect(status.exitstatus).to eq(143)
      # (2) Storage cleaned (was created in before block, should be gone)
      expect(File.directory?(storage_path)).to be(false),
        "expected storage_path to have been cleaned"
    end

    # Unit-level tests for the rescue branch that handles Turbofan::Interrupted.
    # These bypass the signal trap (tested via fork above) and exercise the
    # lifecycle by raising Interrupted directly from step code — cheaper + more
    # observable than round-tripping through SIGTERM.
    describe "when step raises Turbofan::Interrupted" do
      let(:interrupting_step) do
        make_step(name: "InterruptingStep") do |_inputs, _ctx|
          raise Turbofan::Interrupted.new("test interrupt")
        end
      end

      it "re-raises the Interrupted exception with exit status 143" do
        expect {
          run_wrapper(interrupting_step, env: {"TURBOFAN_INPUT" => '{"inputs":[{}]}'})
        }.to raise_error(Turbofan::Interrupted) { |e| expect(e.status).to eq(143) }
      end

      it "does not emit failure metrics for graceful shutdowns" do
        expect(Turbofan::Runtime::StepMetrics).not_to receive(:emit_failure)
        begin
          run_wrapper(interrupting_step, env: {"TURBOFAN_INPUT" => '{"inputs":[{}]}'})
        rescue Turbofan::Interrupted
          # expected
        end
      end

      it "does not emit a Lineage fail_event for graceful shutdowns" do
        allow(Turbofan::Runtime::Lineage).to receive(:fail_event).and_call_original
        begin
          run_wrapper(interrupting_step, env: {"TURBOFAN_INPUT" => '{"inputs":[{}]}'})
        rescue Turbofan::Interrupted
          # expected — Interrupted propagates through ensure
        end
        expect(Turbofan::Runtime::Lineage).not_to have_received(:fail_event)
      end
    end

    describe "#install_sigterm_handler API" do
      # Regression guard: the storage_path: kwarg was removed after cleanup
      # moved to the ensure block exclusively.
      it "accepts only a context argument" do
        wrapper = described_class.new(step_class)
        context = instance_double(Turbofan::Runtime::Context, interrupt!: nil)

        # Restore whatever trap existed so we don't leak state across tests.
        original = Signal.trap("TERM", "DEFAULT")
        begin
          expect { wrapper.send(:install_sigterm_handler, context) }.not_to raise_error
          expect { wrapper.send(:install_sigterm_handler, context, storage_path: "/tmp/x") }
            .to raise_error(ArgumentError)
        ensure
          Signal.trap("TERM", original)
        end
      end
    end
  end

  describe "storage path management" do
    it "creates and cleans up a job-specific temp directory" do
      Dir.mktmpdir(nil, SPEC_TMP_ROOT) do |tmpdir|
        job_dir = File.join(tmpdir, "test-job")
        FileUtils.mkdir_p(job_dir)

        run_wrapper(step_class, env: {
          "TURBOFAN_INPUT" => '{"items":1}'
        }, storage_base: job_dir)

        # After run, cleanup_storage should have removed it
        expect(File.directory?(job_dir)).to be false
      end
    end

    it "handles nil storage path gracefully" do
      result = run_wrapper(step_class, env: {
        "TURBOFAN_INPUT" => '{"items":1}'
      }, storage_base: nil)

      expect(result[:output]).not_to be_empty
    end

    it "sets ENV['TMPDIR'] to storage_path/tmp when local storage is available" do
      Dir.mktmpdir(nil, SPEC_TMP_ROOT) do |tmpdir|
        job_dir = File.join(tmpdir, "test-job")
        FileUtils.mkdir_p(job_dir)

        saved_tmpdir = ENV["TMPDIR"]
        begin
          run_wrapper(step_class, env: {
            "TURBOFAN_INPUT" => '{"items":1}'
          }, storage_base: job_dir)

          expected_tmp = File.join(job_dir, "tmp")
          # The wrapper calls set_tmpdir before running, so the directory should have been created.
          # After cleanup_storage removes job_dir, TMPDIR still points to the path that was set.
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
        runs_on :batch
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
        runs_on :batch
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
    let(:schemas_dir) { Dir.mktmpdir("turbofan-wrapper-schemas", SPEC_TMP_ROOT) }

    before do
      Turbofan.config.schemas_path = schemas_dir
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
        runs_on :batch
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
        runs_on :batch
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
        runs_on :batch
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
      Turbofan.config.schemas_path = nil

      valid_step = make_step(name: "EnvStep")

      saved = ENV["TURBOFAN_SCHEMAS_PATH"]
      ENV["TURBOFAN_SCHEMAS_PATH"] = schemas_dir
      begin
        run_wrapper(valid_step, env: {"TURBOFAN_INPUT" => "{}"})
        expect(Turbofan.config.schemas_path).to eq(schemas_dir)
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

  describe "storage setup edge cases" do
    it "returns nil when /mnt/nvme does not exist and not on Fargate" do
      wrapper = described_class.new(step_class)
      allow(File).to receive(:directory?).and_call_original
      allow(File).to receive(:directory?).with("/mnt/nvme").and_return(false)
      saved_ecs = ENV.delete("ECS_CONTAINER_METADATA_URI_V4")

      expect(wrapper.send(:setup_storage)).to be_nil
    ensure
      ENV["ECS_CONTAINER_METADATA_URI_V4"] = saved_ecs if saved_ecs
    end

    it "uses AWS_BATCH_JOB_ID and attempt number for subdirectory name" do
      wrapper = described_class.new(step_class)
      saved_id = ENV["AWS_BATCH_JOB_ID"]
      saved_attempt = ENV["AWS_BATCH_JOB_ATTEMPT"]
      ENV["AWS_BATCH_JOB_ID"] = "batch-job-42"
      ENV["AWS_BATCH_JOB_ATTEMPT"] = "2"
      allow(File).to receive(:directory?).and_call_original
      allow(File).to receive(:directory?).with("/mnt/nvme").and_return(true)
      allow(FileUtils).to receive(:mkdir_p)

      result = wrapper.send(:setup_storage)
      expect(result).to eq("/mnt/nvme/batch-job-42-attempt2")
    ensure
      ENV["AWS_BATCH_JOB_ID"] = saved_id
      ENV["AWS_BATCH_JOB_ATTEMPT"] = saved_attempt
    end

    it "falls back to local-{pid} when AWS_BATCH_JOB_ID not set" do
      wrapper = described_class.new(step_class)
      saved = ENV.delete("AWS_BATCH_JOB_ID")
      allow(File).to receive(:directory?).and_call_original
      allow(File).to receive(:directory?).with("/mnt/nvme").and_return(true)
      expected_path = "/mnt/nvme/local-#{Process.pid}-attempt1"
      allow(File).to receive(:directory?).with(expected_path).and_return(true)
      allow(FileUtils).to receive(:mkdir_p)

      result = wrapper.send(:setup_storage)
      expect(result).to eq(expected_path)
    ensure
      ENV["AWS_BATCH_JOB_ID"] = saved if saved
    end
  end

  describe "framework field handling" do
    it "passes __turbofan_* fields through to step (not stripped)" do
      received_inputs = nil
      spy = make_step { |inputs, _ctx|
        received_inputs = inputs
        {}
      }

      run_wrapper(spy, env: {
        "TURBOFAN_INPUT" => '{"inputs":[{"gkey":"9q5ct","__turbofan_size":"l"}]}'
      })

      expect(received_inputs).to eq([{"gkey" => "9q5ct", "__turbofan_size" => "l"}])
    end

    it "does not fail schema validation on __ fields even with additionalProperties: false" do
      schemas_dir = Dir.mktmpdir("turbofan-strict-schema", SPEC_TMP_ROOT)
      Turbofan.config.schemas_path = schemas_dir
      File.write(File.join(schemas_dir, "strict_input.json"), JSON.generate({
        "type" => "object",
        "properties" => {"gkey" => {"type" => "string"}},
        "additionalProperties" => false
      }))
      File.write(File.join(schemas_dir, "passthrough.json"), '{"type": "object"}')

      strict_step = Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        runs_on :batch
        cpu 1
        input_schema "strict_input.json"
        output_schema "passthrough.json"
        def self.name = "StrictStep"
        def call(_inputs, _ctx) = {}
      end

      expect {
        run_wrapper(strict_step, env: {
          "TURBOFAN_INPUT" => '{"inputs":[{"gkey":"9q5ct","__turbofan_size":"l"}]}'
        })
      }.not_to raise_error

      FileUtils.rm_rf(schemas_dir)
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
        storage_path: nil, uses: [], writes_to: []
      )
      metrics = Turbofan::Runtime::Metrics.new(
        cloudwatch_client: cloudwatch_client, pipeline_name: "test-pipeline",
        stage: "development", step_name: "LogErrorStep"
      )
      allow(context).to receive_messages(s3: s3_client, metrics: metrics, logger: logger_spy)
      allow(wrapper).to receive_messages(setup_storage: nil, build_context: context)

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
        runs_on :batch
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
