# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe Turbofan::CLI::Prompt do
  let(:input) { StringIO.new }
  let(:output) { StringIO.new }

  before do
    described_class.input = input
    described_class.output = output
  end

  def make_tty(io)
    allow(io).to receive(:tty?).and_return(true)
  end

  describe ".ask" do
    context "without TTY" do
      it "returns default without prompting" do
        expect(described_class.ask("Name?", default: "foo")).to eq("foo")
        expect(output.string).to be_empty
      end

      it "returns nil when no default" do
        expect(described_class.ask("Name?")).to be_nil
      end
    end

    context "with TTY" do
      before { make_tty(input) }

      it "prompts and returns user input" do
        input.string = "bar\n"
        expect(described_class.ask("Name?")).to eq("bar")
        expect(output.string).to eq("Name?: ")
      end

      it "shows default in prompt" do
        input.string = "\n"
        expect(described_class.ask("Name?", default: "foo")).to eq("foo")
        expect(output.string).to eq("Name? [foo]: ")
      end

      it "returns user input over default" do
        input.string = "baz\n"
        expect(described_class.ask("Name?", default: "foo")).to eq("baz")
      end

      it "returns default on empty input" do
        input.string = "\n"
        expect(described_class.ask("Name?", default: "foo")).to eq("foo")
      end
    end
  end

  describe ".yes?" do
    context "without TTY" do
      it "returns default true" do
        expect(described_class.yes?("Continue?")).to be true
      end

      it "returns default false when specified" do
        expect(described_class.yes?("Continue?", default: false)).to be false
      end
    end

    context "with TTY" do
      before { make_tty(input) }

      it "shows [Y/n] when default is true" do
        input.string = "\n"
        described_class.yes?("Continue?")
        expect(output.string).to eq("Continue? [Y/n] ")
      end

      it "shows [y/N] when default is false" do
        input.string = "\n"
        described_class.yes?("Continue?", default: false)
        expect(output.string).to eq("Continue? [y/N] ")
      end

      it "returns true for 'y'" do
        input.string = "y\n"
        expect(described_class.yes?("Continue?")).to be true
      end

      it "returns true for 'yes'" do
        input.string = "yes\n"
        expect(described_class.yes?("Continue?")).to be true
      end

      it "returns true for 'Y'" do
        input.string = "Y\n"
        expect(described_class.yes?("Continue?")).to be true
      end

      it "returns false for 'n'" do
        input.string = "n\n"
        expect(described_class.yes?("Continue?")).to be false
      end

      it "returns default on empty input" do
        input.string = "\n"
        expect(described_class.yes?("Continue?", default: false)).to be false
      end
    end
  end

  describe ".select" do
    let(:choices) { %w[alpha beta gamma] }

    context "without TTY" do
      it "returns first choice as default" do
        expect(described_class.select("Pick:", choices)).to eq("alpha")
      end

      it "returns specified default" do
        expect(described_class.select("Pick:", choices, default: "beta")).to eq("beta")
      end
    end

    context "with TTY" do
      before { make_tty(input) }

      it "displays numbered choices" do
        input.string = "\n"
        described_class.select("Pick:", choices)
        expect(output.string).to include("1) alpha")
        expect(output.string).to include("2) beta")
        expect(output.string).to include("3) gamma")
      end

      it "returns selected choice by number" do
        input.string = "2\n"
        expect(described_class.select("Pick:", choices)).to eq("beta")
      end

      it "returns default on empty input" do
        input.string = "\n"
        expect(described_class.select("Pick:", choices)).to eq("alpha")
      end

      it "returns default for out-of-range input" do
        input.string = "99\n"
        expect(described_class.select("Pick:", choices)).to eq("alpha")
      end

      it "returns default for zero input" do
        input.string = "0\n"
        expect(described_class.select("Pick:", choices)).to eq("alpha")
      end

      it "shows correct default number in prompt" do
        input.string = "\n"
        described_class.select("Pick:", choices, default: "beta")
        expect(output.string).to include("Choice [2]:")
      end
    end
  end

  describe ".confirm_destructive" do
    context "without TTY" do
      it "returns false without prompting" do
        expect(
          described_class.confirm_destructive("WARNING!", expected_input: "delete")
        ).to be false
        expect(output.string).to be_empty
      end
    end

    context "with TTY" do
      before { make_tty(input) }

      it "returns true when input matches expected" do
        input.string = "delete\n"
        expect(
          described_class.confirm_destructive("WARNING!", expected_input: "delete")
        ).to be true
      end

      it "returns false when input does not match" do
        input.string = "nope\n"
        expect(
          described_class.confirm_destructive("WARNING!", expected_input: "delete")
        ).to be false
      end

      it "displays the message and prompt" do
        input.string = "delete\n"
        described_class.confirm_destructive("WARNING!", expected_input: "delete")
        expect(output.string).to include("WARNING!")
        expect(output.string).to include("Type 'delete' to confirm:")
      end
    end
  end

  describe ".reset!" do
    it "clears input and output" do
      described_class.input = StringIO.new
      described_class.output = StringIO.new
      described_class.reset!
      expect(described_class.input).to eq($stdin)
      expect(described_class.output).to eq($stdout)
    end
  end
end
