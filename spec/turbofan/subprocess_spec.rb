require "spec_helper"

RSpec.describe Turbofan::Subprocess do
  describe ".capture" do
    it "returns [stdout, stderr, status] on success" do
      stdout, stderr, status = described_class.capture("printf", "hello")
      expect(stdout).to eq("hello")
      expect(stderr).to eq("")
      expect(status.success?).to be(true)
    end

    it "raises Turbofan::Subprocess::Error on non-zero exit with command + stderr + stdout" do
      err = nil
      begin
        described_class.capture("sh", "-c", "echo out; echo err 1>&2; exit 3")
      rescue Turbofan::Subprocess::Error => e
        err = e
      end
      expect(err).not_to be_nil
      expect(err.exit_code).to eq(3)
      expect(err.stderr).to include("err")
      expect(err.stdout).to include("out")
      expect(err.command).to eq(["sh", "-c", "echo out; echo err 1>&2; exit 3"])
    end

    it "includes stderr excerpt in the Error message" do
      err = nil
      begin
        described_class.capture("sh", "-c", "echo 'oh no' 1>&2; exit 1")
      rescue Turbofan::Subprocess::Error => e
        err = e
      end
      expect(err.message).to include("exit 1")
      expect(err.message).to include("oh no")
    end

    it "does not raise on failure when allow_failure: true" do
      stdout, stderr, status = described_class.capture("sh", "-c", "exit 7", allow_failure: true)
      expect(status.exitstatus).to eq(7)
      expect(stdout).to eq("")
      expect(stderr).to eq("")
    end

    it "lets Errno::ENOENT bubble up for missing commands (not wrapped)" do
      expect { described_class.capture("definitely-not-a-real-cmd-xyz123") }
        .to raise_error(Errno::ENOENT)
    end

    it "accepts stdin_data for commands that read stdin" do
      stdout, _, _ = described_class.capture("cat", stdin_data: "piped-input")
      expect(stdout).to eq("piped-input")
    end

    it "accepts env: hash to pass environment variables" do
      stdout, _, _ = described_class.capture("sh", "-c", 'printf "%s" "$MY_VAR"', env: {"MY_VAR" => "hello-env"})
      expect(stdout).to eq("hello-env")
    end
  end

  describe Turbofan::Subprocess::Error do
    it "exposes command, exit_code, stdout, stderr" do
      err = described_class.new(command: ["ls"], exit_code: 2, stdout: "o", stderr: "e")
      expect(err.command).to eq(["ls"])
      expect(err.exit_code).to eq(2)
      expect(err.stdout).to eq("o")
      expect(err.stderr).to eq("e")
    end

    it "formats a readable message" do
      err = described_class.new(command: ["git", "status"], exit_code: 1, stdout: "", stderr: "not a repo")
      expect(err.message).to include("Command failed (exit 1)")
      expect(err.message).to include("git status")
      expect(err.message).to include("not a repo")
    end

    it "truncates long commands in the default message to avoid leaking sensitive args" do
      sensitive_cmd = ["docker", "build", "--build-arg", "HTTP_PROXY=http://user:secret@proxy.example", "-t", "image", "."]
      err = described_class.new(command: sensitive_cmd, exit_code: 1, stdout: "", stderr: "")
      expect(err.message).not_to include("secret")
      expect(err.message).not_to include("proxy.example")
      expect(err.message).to include("...")
      # Full command is still on the attribute for debugging
      expect(err.command).to eq(sensitive_cmd)
    end
  end
end
