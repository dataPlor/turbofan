require "spec_helper"
require "turbofan/deploy/dependency_resolver"

RSpec.describe Turbofan::Deploy::DependencyResolver do
  let(:fixtures_root) { File.join(__dir__, "..", "..", "fixtures", "dependency_resolver") }
  let(:project_root) { File.join(fixtures_root, "project_root") }
  let(:steps_root) { File.join(project_root, "turbofans", "steps") }

  describe ".resolve" do
    it "returns external deps for steps that have them" do
      step_dirs = {
        my_step: File.join(steps_root, "my_step"),
        no_deps_step: File.join(steps_root, "no_deps_step")
      }

      result = described_class.resolve(step_dirs, project_root: project_root)

      expect(result[:my_step]).not_to be_empty
      expect(result[:no_deps_step]).to eq([])
    end

    it "skips steps without a worker.rb" do
      step_dirs = {missing: File.join(steps_root, "nonexistent")}
      result = described_class.resolve(step_dirs, project_root: project_root)
      expect(result).to be_empty
    end

    it "detects the direct external dep" do
      step_dirs = {my_step: File.join(steps_root, "my_step")}
      result = described_class.resolve(step_dirs, project_root: project_root)

      shared_service = File.expand_path("services/shared_service.rb", project_root)
      expect(result[:my_step]).to include(shared_service)
    end

    it "detects transitive deps" do
      step_dirs = {my_step: File.join(steps_root, "my_step")}
      result = described_class.resolve(step_dirs, project_root: project_root)

      helper = File.expand_path("services/nested/helper.rb", project_root)
      expect(result[:my_step]).to include(helper)
    end

    it "excludes gems" do
      step_dirs = {my_step: File.join(steps_root, "my_step")}
      result = described_class.resolve(step_dirs, project_root: project_root)

      gem_files = result[:my_step].select { |f| Gem.path.any? { |gp| f.start_with?(File.join(gp, "gems")) } }
      expect(gem_files).to be_empty
    end

    it "excludes stdlib" do
      step_dirs = {my_step: File.join(steps_root, "my_step")}
      result = described_class.resolve(step_dirs, project_root: project_root)

      stdlib_prefixes = Turbofan::Deploy::DependencyResolver::STDLIB_PREFIXES
      stdlib_files = result[:my_step].select { |f| stdlib_prefixes.any? { |sp| f.start_with?(sp) } }
      expect(stdlib_files).to be_empty
    end

    it "returns empty array for a worker that fails to load" do
      step_dirs = {broken_step: File.join(steps_root, "broken_step")}
      result = described_class.resolve(step_dirs, project_root: project_root)
      expect(result[:broken_step]).to eq([])
    end

    it "detects deps even when parent already loaded them" do
      # Simulate PipelineLoader having already loaded the worker (and its deps)
      shared_service = File.expand_path("services/shared_service.rb", project_root)
      helper = File.expand_path("services/nested/helper.rb", project_root)
      $LOADED_FEATURES.push(shared_service, helper)

      step_dirs = {my_step: File.join(steps_root, "my_step")}
      result = described_class.resolve(step_dirs, project_root: project_root)

      expect(result[:my_step]).to include(shared_service)
      expect(result[:my_step]).to include(helper)
    ensure
      $LOADED_FEATURES.delete(shared_service)
      $LOADED_FEATURES.delete(helper)
    end

    it "does not pollute parent $LOADED_FEATURES with step deps" do
      step_dirs = {my_step: File.join(steps_root, "my_step")}
      described_class.resolve(step_dirs, project_root: project_root)

      shared_service = File.expand_path("services/shared_service.rb", project_root)
      helper = File.expand_path("services/nested/helper.rb", project_root)
      expect($LOADED_FEATURES).not_to include(shared_service)
      expect($LOADED_FEATURES).not_to include(helper)
    end
  end

  describe ".prepare_build_context" do
    it "creates a tmpdir with project-relative layout" do
      deps = [
        File.expand_path("services/shared_service.rb", project_root),
        File.expand_path("services/nested/helper.rb", project_root)
      ]

      tmpdir = described_class.prepare_build_context(deps, project_root)

      expect(File.exist?(File.join(tmpdir, "services", "shared_service.rb"))).to be true
      expect(File.exist?(File.join(tmpdir, "services", "nested", "helper.rb"))).to be true
    ensure
      described_class.cleanup_build_context(tmpdir)
    end

    it "returns a tmpdir even when deps are empty" do
      tmpdir = described_class.prepare_build_context([], project_root)

      expect(tmpdir).to be_a(String)
      expect(Dir.exist?(tmpdir)).to be true
      expect(Dir.children(tmpdir)).to be_empty
    ensure
      described_class.cleanup_build_context(tmpdir)
    end
  end

  describe ".cleanup_build_context" do
    it "removes the tmpdir completely" do
      tmpdir = Dir.mktmpdir("turbofan-deps-test-")
      FileUtils.touch(File.join(tmpdir, "test.rb"))

      described_class.cleanup_build_context(tmpdir)

      expect(Dir.exist?(tmpdir)).to be false
    end

    it "is safe with nil" do
      expect { described_class.cleanup_build_context(nil) }.not_to raise_error
    end
  end
end
