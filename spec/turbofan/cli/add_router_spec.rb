require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan step router" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-test", SPEC_TMP_ROOT) }

  after { FileUtils.rm_rf(tmpdir) }

  context "when adding a router to an existing step" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        Turbofan::CLI.start(["new", "my_pipeline"])
        Turbofan::CLI.start(["step", "new", "process_data"])
        Turbofan::CLI.start(["step", "router", "process_data"])
      end
    end

    let(:router_dir) { File.join(tmpdir, "turbofans", "steps", "process_data", "router") }

    it "creates the router directory inside the step" do
      expect(Dir.exist?(router_dir)).to be true
    end

    it "creates router.rb" do
      expect(File.exist?(File.join(router_dir, "router.rb"))).to be true
    end

    it "creates Gemfile" do
      expect(File.exist?(File.join(router_dir, "Gemfile"))).to be true
    end

    it "includes Turbofan::Router in router.rb" do
      content = File.read(File.join(router_dir, "router.rb"))
      expect(content).to include("Turbofan::Router")
    end

    it "uses the step name in the router class name" do
      content = File.read(File.join(router_dir, "router.rb"))
      expect(content).to include("ProcessDataRouter")
    end
  end

  context "with a step name containing underscores" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        Turbofan::CLI.start(["new", "my_pipeline"])
        Turbofan::CLI.start(["step", "new", "my_complex_step"])
        Turbofan::CLI.start(["step", "router", "my_complex_step"])
      end
    end

    let(:router_dir) { File.join(tmpdir, "turbofans", "steps", "my_complex_step", "router") }

    it "creates the router directory" do
      expect(Dir.exist?(router_dir)).to be true
    end

    it "converts underscored name to PascalCase for class name" do
      content = File.read(File.join(router_dir, "router.rb"))
      expect(content).to include("MyComplexStepRouter")
    end
  end
end
