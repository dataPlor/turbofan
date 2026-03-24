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

    it "writes each item to S3 at {execution_id}/{step_name}/input/{index}.json" do
      items = [{"file" => "a.csv"}, {"file" => "b.csv"}, {"file" => "c.csv"}]

      described_class.write_inputs(items, **s3_args)

      items.each_with_index do |item, index|
        expect(s3_client).to have_received(:put_object).with(
          bucket: bucket,
          key: "#{execution_id}/#{step_name}/input/#{index}.json",
          body: JSON.generate(item)
        )
      end
    end

    it "writes the correct number of items" do
      items = Array.new(5) { |i| {"id" => i} }

      described_class.write_inputs(items, **s3_args)

      expect(s3_client).to have_received(:put_object).exactly(5).times
    end

    it "handles an empty input list" do
      described_class.write_inputs([], **s3_args)

      expect(s3_client).not_to have_received(:put_object)
    end

    it "handles a single item" do
      items = [{"file" => "only.csv"}]

      described_class.write_inputs(items, **s3_args)

      expect(s3_client).to have_received(:put_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/input/0.json",
        body: JSON.generate({"file" => "only.csv"})
      )
    end
  end

  describe ".write_inputs with chunking (>10,000 items)" do
    before do
      allow(s3_client).to receive(:put_object)
    end

    it "writes chunked paths: {execution_id}/{step_name}/input/{chunk}/{index}.json" do
      # Use a smaller number but simulate chunked writes
      items = Array.new(10_001) { |i| {"id" => i} }

      described_class.write_inputs(items, **s3_args)

      # First chunk: items 0-9999 at chunk/0/
      expect(s3_client).to have_received(:put_object).with(
        hash_including(key: "#{execution_id}/#{step_name}/input/0/0.json")
      )

      # Second chunk: items 10000+ at chunk/1/
      expect(s3_client).to have_received(:put_object).with(
        hash_including(key: "#{execution_id}/#{step_name}/input/1/0.json")
      )
    end
  end

  describe ".read_input" do
    it "reads the correct item by AWS_BATCH_JOB_ARRAY_INDEX" do
      item = {"file" => "target.csv", "size_mb" => 42}
      stub_s3_read(item)

      result = described_class.read_input(array_index: 7, **s3_args)

      expect(result).to eq(item)
      expect(s3_client).to have_received(:get_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/input/7.json"
      )
    end

    it "reads from chunked path when chunk is specified" do
      item = {"file" => "chunked.csv"}
      stub_s3_read(item)

      result = described_class.read_input(array_index: 5, chunk: 2, **s3_args)

      expect(result).to eq(item)
      expect(s3_client).to have_received(:get_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/input/2/5.json"
      )
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

    it "propagates errors from threaded work" do
      allow(s3_client).to receive(:put_object).and_raise(RuntimeError, "S3 write failed")

      expect {
        described_class.write_inputs([{"id" => 1}], **s3_args)
      }.to raise_error(RuntimeError, "S3 write failed")
    end

    it "includes count of other errors when multiple threads fail" do
      mu = Mutex.new
      call_count = 0
      allow(s3_client).to receive(:put_object) do
        n = mu.synchronize { call_count += 1 }
        raise "S3 write failed" if n <= 3
      end

      items = Array.new(10) { |i| {"id" => i} }

      expect {
        described_class.write_inputs(items, **s3_args)
      }.to raise_error(RuntimeError, /and 2 other error\(s\) in parallel work/)
    end

    it "raises the first error unchanged when only one thread fails" do
      allow(s3_client).to receive(:put_object) # default: success
      allow(s3_client).to receive(:put_object).with(
        hash_including(key: "#{execution_id}/#{step_name}/input/0.json")
      ).and_raise(RuntimeError, "single failure")

      items = Array.new(5) { |i| {"id" => i} }

      expect {
        described_class.write_inputs(items, **s3_args)
      }.to raise_error(RuntimeError, "single failure")
    end

    it "joins all threads even when errors occur" do
      allow(s3_client).to receive(:put_object).and_raise(RuntimeError, "fail")

      expect {
        described_class.write_inputs(Array.new(5) { |i| {"id" => i} }, **s3_args)
      }.to raise_error(RuntimeError)

      live = Thread.list.select(&:alive?) - [Thread.current]
      expect(live).to be_empty
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
end
