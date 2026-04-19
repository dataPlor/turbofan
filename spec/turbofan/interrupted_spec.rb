# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Interrupted do
  it "has EXIT_CODE 143 (Batch retry contract)" do
    expect(described_class::EXIT_CODE).to eq(143)
  end

  it "is a SystemExit subclass so ensure blocks still run" do
    expect(described_class.ancestors).to include(SystemExit)
  end

  it "is NOT a StandardError — `rescue => e` must not swallow it" do
    expect(described_class.ancestors).not_to include(StandardError)

    caught = nil
    begin
      raise described_class
    rescue => e
      caught = e
    rescue SystemExit => e
      caught = e
    end

    expect(caught).to be_a(described_class)
  end

  it "sets SystemExit status to EXIT_CODE when raised" do
    raised = nil
    begin
      raise described_class
    rescue SystemExit => e
      raised = e
    end
    expect(raised.status).to eq(described_class::EXIT_CODE)
  end

  it "carries a default message 'SIGTERM received'" do
    expect(described_class.new.message).to eq("SIGTERM received")
  end

  it "accepts a custom message" do
    expect(described_class.new("spot reclaim").message).to eq("spot reclaim")
  end

  it "triggers ensure blocks on raise" do
    ensure_ran = false
    begin
      begin
        raise described_class
      ensure
        ensure_ran = true
      end
    rescue SystemExit
      # swallow for test
    end
    expect(ensure_ran).to be(true)
  end

  it "preserves exit status through ensure-and-rescue chain" do
    final_status = nil
    begin
      begin
        raise described_class
      ensure
        # ensure runs, exception propagates with preserved status
      end
    rescue SystemExit => e
      final_status = e.status
    end
    expect(final_status).to eq(143)
  end
end
