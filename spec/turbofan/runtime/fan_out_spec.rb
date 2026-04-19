# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Turbofan::Runtime::FanOut do
  let(:s3_client) { instance_double(Aws::S3::Client) }
  let(:bucket) { "turbofan-test-pipeline-production-bucket" }
  let(:execution_id) { "exec-abc123" }
  let(:step_name) { "process" }

  # Shared kwargs for all FanOut methods
  let(:s3_args) { {s3_client: s3_client, bucket: bucket, execution_id: execution_id, step_name: step_name} }

  def stub_s3_read(data)
    s3_body = instance_double(StringIO, read: JSON.generate(data))
    s3_response = instance_double(Aws::S3::Types::GetObjectOutput, body: s3_body)
    allow(s3_client).to receive(:get_object).and_return(s3_response)
    s3_response
  end

  def stub_s3_read_for_key(key, data)
    s3_body = instance_double(StringIO, read: JSON.generate(data))
    s3_response = instance_double(Aws::S3::Types::GetObjectOutput, body: s3_body)
    allow(s3_client).to receive(:get_object).with(
      bucket: bucket,
      key: key
    ).and_return(s3_response)
  end

  describe ".write_inputs" do
    before do
      allow(s3_client).to receive(:put_object)
    end

    it "writes all items to a single S3 file at input/items.json" do
      items = [{"file" => "a.csv"}, {"file" => "b.csv"}, {"file" => "c.csv"}]

      described_class.write_inputs(items, **s3_args)

      expect(s3_client).to have_received(:put_object).once.with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/input/items.json",
        body: JSON.generate(items)
      )
    end

    it "makes exactly one S3 put regardless of item count" do
      items = Array.new(100) { |i| {"id" => i} }

      described_class.write_inputs(items, **s3_args)

      expect(s3_client).to have_received(:put_object).once
    end

    it "handles an empty input list without writing" do
      described_class.write_inputs([], **s3_args)

      expect(s3_client).not_to have_received(:put_object)
    end

    it "handles a single item" do
      items = [{"file" => "only.csv"}]

      described_class.write_inputs(items, **s3_args)

      expect(s3_client).to have_received(:put_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/input/items.json",
        body: JSON.generate([{"file" => "only.csv"}])
      )
    end

    it "handles large item counts in a single file" do
      items = Array.new(10_001) { |i| {"id" => i} }

      described_class.write_inputs(items, **s3_args)

      expect(s3_client).to have_received(:put_object).once
    end
  end

  describe ".read_input" do
    it "reads items.json and returns the item at array_index" do
      items = [{"file" => "a.csv"}, {"file" => "target.csv"}, {"file" => "c.csv"}]
      stub_s3_read(items)

      result = described_class.read_input(array_index: 1, **s3_args)

      expect(result).to eq({"file" => "target.csv"})
      expect(s3_client).to have_received(:get_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/input/items.json"
      )
    end

    it "returns the first item for array_index 0" do
      items = [{"id" => 0}, {"id" => 1}]
      stub_s3_read(items)

      result = described_class.read_input(array_index: 0, **s3_args)

      expect(result).to eq({"id" => 0})
    end

    it "returns the last item for array_index == size - 1" do
      items = [{"id" => 0}, {"id" => 1}, {"id" => 2}]
      stub_s3_read(items)

      result = described_class.read_input(array_index: 2, **s3_args)

      expect(result).to eq({"id" => 2})
    end

    it "returns nil for out-of-bounds array_index" do
      items = [{"id" => 0}]
      stub_s3_read(items)

      result = described_class.read_input(array_index: 5, **s3_args)

      expect(result).to be_nil
    end

    it "reads from routed path when chunk is specified" do
      items = [{"file" => "routed.csv"}]
      stub_s3_read(items)

      result = described_class.read_input(array_index: 0, chunk: "large", **s3_args)

      expect(result).to eq({"file" => "routed.csv"})
      expect(s3_client).to have_received(:get_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/input/large/items.json"
      )
    end
  end

  describe "input format contract" do
    it "write_inputs produces a file that read_input can consume for each index" do
      items = [{"id" => 0, "name" => "first"}, {"id" => 1, "name" => "second"}, {"id" => 2, "name" => "third"}]

      written_body = nil
      allow(s3_client).to receive(:put_object) { |args| written_body = args[:body] }

      described_class.write_inputs(items, **s3_args)

      items.each_with_index do |expected_item, index|
        s3_body = instance_double(StringIO, read: written_body)
        s3_response = instance_double(Aws::S3::Types::GetObjectOutput, body: s3_body)
        allow(s3_client).to receive(:get_object).and_return(s3_response)

        result = described_class.read_input(array_index: index, **s3_args)
        expect(result).to eq(expected_item)
      end
    end

    it "items.json is a flat JSON array of items" do
      items = [{"a" => 1}, {"b" => 2}]
      written_body = nil
      allow(s3_client).to receive(:put_object) { |args| written_body = args[:body] }

      described_class.write_inputs(items, **s3_args)

      parsed = JSON.parse(written_body)
      expect(parsed).to be_an(Array)
      expect(parsed.size).to eq(2)
      expect(parsed[0]).to eq({"a" => 1})
      expect(parsed[1]).to eq({"b" => 2})
    end

    it "preserves complex nested item structures through write/read roundtrip" do
      items = [{"config" => {"nested" => true, "list" => [1, 2, 3]}, "tags" => ["a", "b"]}]
      written_body = nil
      allow(s3_client).to receive(:put_object) { |args| written_body = args[:body] }

      described_class.write_inputs(items, **s3_args)

      s3_body = instance_double(StringIO, read: written_body)
      s3_response = instance_double(Aws::S3::Types::GetObjectOutput, body: s3_body)
      allow(s3_client).to receive(:get_object).and_return(s3_response)

      result = described_class.read_input(array_index: 0, **s3_args)
      expect(result).to eq(items[0])
    end
  end

  describe ".collect_outputs" do
    it "reads all outputs and returns an ordered list" do
      outputs = [{"result" => "a"}, {"result" => "b"}, {"result" => "c"}]

      outputs.each_with_index do |output, index|
        stub_s3_read_for_key("#{execution_id}/#{step_name}/output/#{index}.json", output)
      end

      result = described_class.collect_outputs(count: 3, **s3_args)

      expect(result).to eq(outputs)
      expect(result.size).to eq(3)
    end

    it "returns outputs in the correct order regardless of S3 read order" do
      outputs = Array.new(5) { |i| {"index" => i, "value" => "result_#{i}"} }

      outputs.each_with_index do |output, index|
        stub_s3_read_for_key("#{execution_id}/#{step_name}/output/#{index}.json", output)
      end

      result = described_class.collect_outputs(count: 5, **s3_args)

      result.each_with_index do |output, i|
        expect(output["index"]).to eq(i)
      end
    end

    it "handles chunked outputs" do
      # 2 chunks: chunk 0 has 3 items, chunk 1 has 2 items
      chunks = {0 => 3, 1 => 2}

      chunks.each do |chunk, count|
        count.times do |index|
          stub_s3_read_for_key(
            "#{execution_id}/#{step_name}/output/#{chunk}/#{index}.json",
            {"chunk" => chunk, "index" => index}
          )
        end
      end

      result = described_class.collect_outputs(chunks: chunks, **s3_args)

      expect(result.size).to eq(5)
    end

    it "raises ArgumentError when both count and chunks are nil" do
      expect {
        described_class.collect_outputs(**s3_args)
      }.to raise_error(ArgumentError, /count.*chunks/)
    end

    it "propagates errors from write_inputs" do
      allow(s3_client).to receive(:put_object).and_raise(RuntimeError, "S3 write failed")

      expect {
        described_class.write_inputs([{"id" => 1}], **s3_args)
      }.to raise_error(RuntimeError, "S3 write failed")
    end

    it "returns empty list for zero count" do
      result = described_class.collect_outputs(count: 0, **s3_args)

      expect(result).to eq([])
    end
  end

  # ---------------------------------------------------------------------------
  # B3 — Streaming each_output Enumerator
  # ---------------------------------------------------------------------------
  describe ".each_output (B3)" do
    it "yields outputs one at a time in order when given a block" do
      outputs = [{"result" => "a"}, {"result" => "b"}, {"result" => "c"}]

      outputs.each_with_index do |output, index|
        stub_s3_read_for_key("#{execution_id}/#{step_name}/output/#{index}.json", output)
      end

      yielded = []
      described_class.each_output(count: 3, **s3_args) { |output| yielded << output }

      expect(yielded).to eq(outputs)
    end

    it "returns an Enumerator when no block is given" do
      outputs = [{"result" => "a"}, {"result" => "b"}]

      outputs.each_with_index do |output, index|
        stub_s3_read_for_key("#{execution_id}/#{step_name}/output/#{index}.json", output)
      end

      enum = described_class.each_output(count: 2, **s3_args)

      expect(enum).to be_a(Enumerator)
    end

    it "yields across all chunks in order" do
      chunks = {0 => 2, 1 => 2}

      chunks.each do |chunk, count|
        count.times do |index|
          stub_s3_read_for_key(
            "#{execution_id}/#{step_name}/output/#{chunk}/#{index}.json",
            {"chunk" => chunk, "index" => index}
          )
        end
      end

      yielded = []
      described_class.each_output(chunks: chunks, **s3_args) { |output| yielded << output }

      expect(yielded.size).to eq(4)
      expect(yielded.first["chunk"]).to eq(0)
      expect(yielded.last["chunk"]).to eq(1)
    end

    it "raises ArgumentError when neither count nor chunks provided" do
      expect {
        described_class.each_output(**s3_args) { |_| }
      }.to raise_error(ArgumentError, /count.*chunks/)
    end

    it "yields nothing for count: 0" do
      yielded = []
      described_class.each_output(count: 0, **s3_args) { |output| yielded << output }

      expect(yielded).to be_empty
    end

    it "returns an empty Enumerator for count: 0 without block" do
      enum = described_class.each_output(count: 0, **s3_args)

      expect(enum).to be_a(Enumerator)
      expect(enum.to_a).to be_empty
    end
  end

  describe ".collect_outputs delegates to each_output internally (B3)" do
    it "collect_outputs still works as before" do
      outputs = [{"result" => "x"}, {"result" => "y"}]

      outputs.each_with_index do |output, index|
        stub_s3_read_for_key("#{execution_id}/#{step_name}/output/#{index}.json", output)
      end

      result = described_class.collect_outputs(count: 2, **s3_args)

      expect(result).to eq(outputs)
    end
  end

  describe "threaded_work error handling" do
    it "raises WorkerError wrapping the failing work item + original cause" do
      work = [[0], [1], [2]]
      expect {
        described_class.send(:threaded_work, work) do |i|
          raise "boom-#{i}" if i == 1
        end
      }.to raise_error(Turbofan::Runtime::FanOut::WorkerError) do |e|
        expect(e.work_item).to eq([1])
        expect(e.cause).to be_a(RuntimeError)
        expect(e.cause.message).to eq("boom-1")
        expect(e.message).to include("[1]")
        expect(e.message).to include("boom-1")
      end
    end

    it "raises WorkerErrors aggregate when multiple workers fail" do
      work = [[0], [1], [2], [3]]
      expect {
        described_class.send(:threaded_work, work) do |i|
          raise "boom-#{i}" if i.even?
        end
      }.to raise_error(Turbofan::Runtime::FanOut::WorkerErrors) do |e|
        expect(e.errors.size).to eq(2)
        expect(e.errors.map(&:work_item)).to match_array([[0], [2]])
        expect(e.errors.map { |err| err.cause.class }).to all(eq(RuntimeError))
      end
    end

    it "WorkerErrors message summarizes up to 3 failures + notes beyond" do
      errors = (0..4).map do |i|
        Turbofan::Runtime::FanOut::WorkerError.new([i], RuntimeError.new("e#{i}"))
      end
      agg = Turbofan::Runtime::FanOut::WorkerErrors.new(errors)
      expect(agg.message).to include("5 worker(s) failed")
      expect(agg.message).to include("and 2 more")
    end

    it "preserves original backtrace through WorkerError" do
      caught = nil
      begin
        described_class.send(:threaded_work, [[:a]]) { |_| raise "x" }
      rescue Turbofan::Runtime::FanOut::WorkerError => e
        caught = e
      end
      expect(caught.backtrace).not_to be_nil
      expect(caught.backtrace.first).to match(/fan_out_spec/)
    end

    it "chunked work items (3-tuple) are preserved in WorkerError" do
      work = [["chunk_a", 0, 0], ["chunk_b", 1, 1]]
      expect {
        described_class.send(:threaded_work, work) do |_chunk, index, _ri|
          raise "boom" if index == 1
        end
      }.to raise_error(Turbofan::Runtime::FanOut::WorkerError) do |e|
        expect(e.work_item).to eq(["chunk_b", 1, 1])
      end
    end
  end

  describe "threaded_work early-exit (Turbofan.config.fan_out_early_exit_threshold)" do
    after { Turbofan.config.fan_out_early_exit_threshold = nil }

    it "preserves all-workers-complete when threshold is nil (default, backward-compat)" do
      invocations = Queue.new
      work = 20.times.map { |i| [i] }
      begin
        described_class.send(:threaded_work, work) do |i|
          invocations << i
          raise "non-transient boom" if i < 3
        end
      rescue Turbofan::Runtime::FanOut::WorkerErrors
        # expected — 3 failures aggregated
      end
      drained = []
      drained << invocations.pop until invocations.empty?
      expect(drained.size).to eq(20) # all items processed despite failures
    end

    it "stops dequeueing remaining items after N non-transient failures" do
      Turbofan.config.fan_out_early_exit_threshold = 2
      invocations = Queue.new
      work = 200.times.map { |i| [i] }
      begin
        described_class.send(:threaded_work, work) do |i|
          invocations << i
          raise "non-transient boom"
        end
      rescue Turbofan::Runtime::FanOut::WorkerErrors
        # expected
      end
      drained = []
      drained << invocations.pop until invocations.empty?
      # Workers already in flight when the threshold was hit may complete;
      # the key property is that MOST items are skipped, not all.
      expect(drained.size).to be < work.size,
        "expected early-exit to skip some items; saw #{drained.size}/#{work.size} processed"
    end

    it "does NOT trigger early-exit on transient errors (throttle-storm safety)" do
      Turbofan.config.fan_out_early_exit_threshold = 1
      invocations = Queue.new
      work = 10.times.map { |i| [i] }
      begin
        described_class.send(:threaded_work, work) do |i|
          invocations << i
          # Simulate an AWS throttle — Retryable.transient? returns true
          # for SlowDown, so this should NOT count toward the threshold.
          err = Aws::S3::Errors::SlowDown.allocate
          err.define_singleton_method(:code) { "SlowDown" }
          err.define_singleton_method(:context) { nil }
          raise err
        end
      rescue Turbofan::Runtime::FanOut::WorkerErrors
        # expected
      end
      drained = []
      drained << invocations.pop until invocations.empty?
      expect(drained.size).to eq(10),
        "all items should process despite 'transient' errors; saw #{drained.size}/10"
    end
  end

  describe "threaded_work stall detection (Turbofan.config.worker_stall_seconds)" do
    after { Turbofan.config.worker_stall_seconds = nil }

    it "emits a WorkerStall warn when a worker holds an item past the threshold" do
      Turbofan.config.worker_stall_seconds = 0.2
      captured = StringIO.new
      orig = $stderr
      $stderr = captured
      begin
        described_class.send(:threaded_work, [[:slow_item]]) do |_|
          sleep 0.5
        end
      ensure
        $stderr = orig
      end
      expect(captured.string).to include("WorkerStall")
      expect(captured.string).to include(":slow_item")
    end

    it "does not warn when workers finish before the threshold" do
      Turbofan.config.worker_stall_seconds = 0.5
      captured = StringIO.new
      orig = $stderr
      $stderr = captured
      begin
        described_class.send(:threaded_work, [[:fast_item]]) do |_|
          sleep 0.05
        end
      ensure
        $stderr = orig
      end
      expect(captured.string).not_to include("WorkerStall")
    end

    it "has zero overhead when config is nil (no coordinator thread)" do
      # Baseline: threads + 1 main thread. Coordinator thread would add 1.
      before_count = Thread.list.size
      described_class.send(:threaded_work, [[:a], [:b], [:c]]) { |_| }
      after_count = Thread.list.size
      # All worker threads joined. No coordinator should linger.
      expect(after_count).to eq(before_count)
    end
  end
end
