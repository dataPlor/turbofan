# frozen_string_literal: true

require "spec_helper"

# Phase 3 / Task #48 — concurrency audit pass.
#
# The Context carries six lazily-memoized attributes (logger, metrics, s3,
# secrets_client, uses_resources, writes_to_resources). Before this work
# they used `@foo ||= ...`, which is vulnerable to a race when multiple
# fan_out workers hit the same Context simultaneously: two threads can
# both see @foo as nil, both construct fresh instances, and one assigns
# last — losing any state accumulated on the other instance.
#
# For Metrics specifically this was a silent-data-loss bug: two racing
# Metrics instances each accept emit() calls on their own @pending array,
# and only one gets flushed. That's the bug Mike Perham flagged.
#
# These specs exercise the double-checked-locking fix by hammering each
# memoized attribute from many threads at once and asserting every
# observer sees the SAME object.
RSpec.describe Turbofan::Runtime::Context do
  let(:context) do
    described_class.new(
      execution_id: "test-exec",
      attempt_number: 1,
      step_name: "test_step",
      stage: "dev",
      pipeline_name: "test_pipe",
      array_index: nil,
      storage_path: nil,
      uses: [{type: :resource, key: :postgres}],
      writes_to: [{type: :s3, uri: "s3://bucket/key"}]
    )
  end

  # Stub AWS client construction so the concurrency tests don't need real
  # region config. We specifically care about the memoization path here,
  # not the client's RPC behavior.
  before do
    stub_const("FakeS3Client", Class.new)
    stub_const("FakeSecretsClient", Class.new)
    allow(Aws::S3::Client).to receive(:new).and_return(FakeS3Client.new)
    allow(Aws::SecretsManager::Client).to receive(:new).and_return(FakeSecretsClient.new)
  end

  THREAD_COUNT = 50

  # Assert that N threads accessing a memoized attribute all receive the
  # same object identity — i.e., only one instance was constructed.
  def assert_single_instance(attr)
    gate = Queue.new
    results = Queue.new
    threads = Array.new(THREAD_COUNT) do
      Thread.new do
        gate.pop
        results << context.public_send(attr)
      end
    end
    THREAD_COUNT.times { gate << :go }
    threads.each(&:join)
    instances = []
    instances << results.pop until results.empty?
    expect(instances.size).to eq(THREAD_COUNT)
    expect(instances.uniq(&:object_id).size).to eq(1),
      "expected exactly one #{attr} instance across #{THREAD_COUNT} threads, got #{instances.uniq(&:object_id).size}"
  end

  it "returns a single logger instance under concurrent access" do
    assert_single_instance(:logger)
  end

  it "returns a single metrics instance under concurrent access (prevents silent emit-loss)" do
    assert_single_instance(:metrics)
  end

  it "returns a single s3 client instance under concurrent access" do
    assert_single_instance(:s3)
  end

  it "returns a single secrets_client instance under concurrent access" do
    assert_single_instance(:secrets_client)
  end

  it "returns a single uses_resources array under concurrent access" do
    assert_single_instance(:uses_resources)
  end

  it "returns a single writes_to_resources array under concurrent access" do
    assert_single_instance(:writes_to_resources)
  end

  it "concurrent emit() calls from many threads land on the same Metrics @pending" do
    # Warm the metrics instance on the main thread so the race we test
    # below is specifically about emit()'s @pending mutation, not lazy
    # Metrics construction (which is already covered above).
    context.metrics
    threads = Array.new(THREAD_COUNT) do |i|
      Thread.new { context.metrics.emit("TestMetric", i) }
    end
    threads.each(&:join)
    pending = context.metrics.instance_variable_get(:@pending)
    expect(pending.size).to eq(THREAD_COUNT),
      "expected #{THREAD_COUNT} queued entries, got #{pending.size} — " \
      "suggests Metrics#emit append on @pending is still racy"
  end
end
