require "spec_helper"
require "tmpdir"
require "fileutils"
require "digest"

RSpec.describe Turbofan::Deploy::ImageBuilder do
  let(:tmpdir) { Dir.mktmpdir("turbofan-image-builder", SPEC_TMP_ROOT) }

  after { FileUtils.rm_rf(tmpdir) }

  describe ".content_tag" do
    let(:step_dir) { File.join(tmpdir, "step") }
    let(:schemas_dir) { File.join(tmpdir, "schemas") }

    before do
      FileUtils.mkdir_p(step_dir)
      FileUtils.mkdir_p(schemas_dir)
      File.write(File.join(step_dir, "worker.rb"), "class MyStep; end")
      File.write(File.join(step_dir, "Dockerfile"), "FROM amazonlinux:2023")
      File.write(File.join(schemas_dir, "input.json"), '{"type": "object"}')
    end

    it "returns a tag starting with 'sha-'" do
      tag = described_class.content_tag(step_dir, schemas_dir)
      expect(tag).to start_with("sha-")
    end

    it "returns a tag with 12 hex characters after 'sha-'" do
      tag = described_class.content_tag(step_dir, schemas_dir)
      expect(tag).to match(/\Asha-[a-f0-9]{12}\z/)
    end

    it "is deterministic: same inputs produce same tag" do
      tag1 = described_class.content_tag(step_dir, schemas_dir)
      tag2 = described_class.content_tag(step_dir, schemas_dir)
      expect(tag1).to eq(tag2)
    end

    it "different content produces different tags" do
      tag1 = described_class.content_tag(step_dir, schemas_dir)
      File.write(File.join(step_dir, "worker.rb"), "class MyStep; def call; end; end")
      tag2 = described_class.content_tag(step_dir, schemas_dir)
      expect(tag1).not_to eq(tag2)
    end

    it "changing schema files changes the tag" do
      tag1 = described_class.content_tag(step_dir, schemas_dir)
      File.write(File.join(schemas_dir, "input.json"), '{"type": "object", "required": ["id"]}')
      tag2 = described_class.content_tag(step_dir, schemas_dir)
      expect(tag1).not_to eq(tag2)
    end

    it "uses relative paths so tags are stable across machines" do
      tag1 = described_class.content_tag(step_dir, schemas_dir)

      other_tmpdir = Dir.mktmpdir("turbofan-image-builder-other", SPEC_TMP_ROOT)
      other_step_dir = File.join(other_tmpdir, "step")
      other_schemas_dir = File.join(other_tmpdir, "schemas")
      FileUtils.mkdir_p(other_step_dir)
      FileUtils.mkdir_p(other_schemas_dir)
      File.write(File.join(other_step_dir, "worker.rb"), "class MyStep; end")
      File.write(File.join(other_step_dir, "Dockerfile"), "FROM amazonlinux:2023")
      File.write(File.join(other_schemas_dir, "input.json"), '{"type": "object"}')

      tag2 = described_class.content_tag(other_step_dir, other_schemas_dir)
      FileUtils.rm_rf(other_tmpdir)

      expect(tag1).to eq(tag2)
    end
  end

  describe ".image_exists?" do
    let(:ecr_client) { instance_double(Aws::ECR::Client) }

    it "returns true when image is found" do
      allow(ecr_client).to receive(:describe_images).and_return(
        double(image_details: [double])
      )
      expect(described_class.image_exists?(ecr_client, "my-repo", "sha-abc123")).to be true
    end

    it "returns false when ImageNotFoundException is raised" do
      allow(ecr_client).to receive(:describe_images).and_raise(
        Aws::ECR::Errors::ImageNotFoundException.new(nil, "Image not found")
      )
      expect(described_class.image_exists?(ecr_client, "my-repo", "sha-abc123")).to be false
    end

    it "creates the repository and returns false when RepositoryNotFoundException is raised" do
      allow(ecr_client).to receive(:describe_images).and_raise(
        Aws::ECR::Errors::RepositoryNotFoundException.new(nil, "Repo not found")
      )
      allow(ecr_client).to receive(:create_repository)
      allow(ecr_client).to receive(:put_lifecycle_policy)

      expect(described_class.image_exists?(ecr_client, "my-repo", "sha-abc123")).to be false
      expect(ecr_client).to have_received(:create_repository).with(
        repository_name: "my-repo",
        image_scanning_configuration: {scan_on_push: true}
      )
      expect(ecr_client).to have_received(:put_lifecycle_policy)
    end
  end

  describe ".build" do
    let(:step_dir) { File.join(tmpdir, "step") }
    let(:schemas_dir) { File.join(tmpdir, "schemas") }
    let(:repository_uri) { "123456789.dkr.ecr.us-east-1.amazonaws.com/my-repo" }
    let(:success_status) { instance_double(Process::Status, success?: true) }

    before do
      FileUtils.mkdir_p(step_dir)
      FileUtils.mkdir_p(schemas_dir)
    end

    it "calls Subprocess.capture with splatted docker build command" do
      allow(Turbofan::Subprocess).to receive(:capture).and_return(["", "", success_status])

      described_class.build(step_dir, schemas_dir, tag: "sha-abc123", repository_uri: repository_uri)

      expect(Turbofan::Subprocess).to have_received(:capture) do |*args|
        expect(args.first).to eq("docker")
        expect(args[1]).to eq("build")
        expect(args).to include("--build-context", "schemas=#{schemas_dir}")
        expect(args).to include("-t", "#{repository_uri}:sha-abc123")
        expect(args.last).to eq(step_dir)
      end
    end

    it "raises when docker build fails" do
      allow(Turbofan::Subprocess).to receive(:capture).and_raise(
        Turbofan::Subprocess::Error.new(command: ["docker", "build"], exit_code: 1, stdout: "", stderr: "boom")
      )

      expect {
        described_class.build(step_dir, schemas_dir, tag: "sha-abc123", repository_uri: repository_uri)
      }.to raise_error(/Command failed/)
    end

    it "raises a descriptive error when docker binary is missing (ENOENT)" do
      allow(Turbofan::Subprocess).to receive(:capture).and_raise(Errno::ENOENT)

      expect {
        described_class.build(step_dir, schemas_dir, tag: "sha-abc123", repository_uri: repository_uri)
      }.to raise_error(/Command failed.*command not found/m)
    end
  end

  describe ".push" do
    let(:repository_uri) { "123456789.dkr.ecr.us-east-1.amazonaws.com/my-repo" }
    let(:success_status) { instance_double(Process::Status, success?: true) }

    it "calls Subprocess.capture with splatted docker push command" do
      allow(Turbofan::Subprocess).to receive(:capture).and_return(["", "", success_status])

      described_class.push(tag: "sha-abc123", repository_uri: repository_uri)

      expect(Turbofan::Subprocess).to have_received(:capture).with(
        "docker", "push", "#{repository_uri}:sha-abc123"
      )
    end

    it "raises when docker push fails" do
      allow(Turbofan::Subprocess).to receive(:capture).and_raise(
        Turbofan::Subprocess::Error.new(command: ["docker", "push"], exit_code: 1, stdout: "", stderr: "boom")
      )

      expect {
        described_class.push(tag: "sha-abc123", repository_uri: repository_uri)
      }.to raise_error(/Command failed/)
    end
  end

  describe ".authenticate_ecr" do
    let(:ecr_client) { instance_double(Aws::ECR::Client) }
    let(:success_status) { instance_double(Process::Status, success?: true) }

    before do
      auth_data = double(
        authorization_token: Base64.encode64("AWS:my-password"),
        proxy_endpoint: "https://123456789.dkr.ecr.us-east-1.amazonaws.com"
      )
      allow(ecr_client).to receive(:get_authorization_token).and_return(
        double(authorization_data: [auth_data])
      )
      allow(Turbofan::Subprocess).to receive(:capture).and_return(["", "", success_status])
    end

    it "calls get_authorization_token on ECR client" do
      described_class.authenticate_ecr(ecr_client)
      expect(ecr_client).to have_received(:get_authorization_token)
    end

    it "uses --password-stdin for docker login" do
      described_class.authenticate_ecr(ecr_client)

      expect(Turbofan::Subprocess).to have_received(:capture).with(
        "docker", "login", "--username", "AWS", "--password-stdin",
        "https://123456789.dkr.ecr.us-east-1.amazonaws.com",
        stdin_data: "my-password"
      )
    end

    it "returns the registry host without protocol" do
      result = described_class.authenticate_ecr(ecr_client)
      expect(result).to eq("123456789.dkr.ecr.us-east-1.amazonaws.com")
    end

    it "raises when docker login fails" do
      allow(Turbofan::Subprocess).to receive(:capture).and_raise(
        Turbofan::Subprocess::Error.new(command: ["docker", "login"], exit_code: 1, stdout: "", stderr: "auth boom")
      )

      expect {
        described_class.authenticate_ecr(ecr_client)
      }.to raise_error(Turbofan::Subprocess::Error)
    end
  end

  describe ".git_sha" do
    let(:success_status) { instance_double(Process::Status, success?: true) }
    let(:failure_status) { instance_double(Process::Status, success?: false) }

    it "returns a tag prefixed with 'git-'" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("git", "rev-parse", "--short", "HEAD", allow_failure: true)
        .and_return(["abc1234\n", "", success_status])
      expect(described_class.git_sha).to eq("git-abc1234")
    end

    it "returns nil outside a git repo" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("git", "rev-parse", "--short", "HEAD", allow_failure: true)
        .and_return(["", "fatal: not a git repository\n", failure_status])
      expect(described_class.git_sha).to be_nil
    end
  end

  describe ".build_and_push" do
    let(:ecr_client) { instance_double(Aws::ECR::Client) }
    let(:step_dir) { File.join(tmpdir, "step") }
    let(:schemas_dir) { File.join(tmpdir, "schemas") }
    let(:repository_uri) { "123456789.dkr.ecr.us-east-1.amazonaws.com/my-repo" }

    let(:success_status) { instance_double(Process::Status, success?: true) }

    before do
      FileUtils.mkdir_p(step_dir)
      FileUtils.mkdir_p(schemas_dir)
      File.write(File.join(step_dir, "worker.rb"), "class MyStep; end")
      File.write(File.join(schemas_dir, "input.json"), '{"type": "object"}')
      allow(Turbofan::Subprocess).to receive(:capture).and_return(["", "", success_status])
    end

    context "when image already exists in ECR" do
      before do
        allow(described_class).to receive(:image_exists?).and_return(true)
      end

      it "skips docker build" do
        described_class.build_and_push(
          step_dir: step_dir,
          schemas_dir: schemas_dir,
          ecr_client: ecr_client,
          repository_name: "my-repo",
          repository_uri: repository_uri
        )

        expect(Turbofan::Subprocess).not_to have_received(:capture)
      end
    end

    context "when image does not exist in ECR" do
      before do
        allow(described_class).to receive(:image_exists?).and_return(false)
      end

      it "builds and pushes the image" do
        described_class.build_and_push(
          step_dir: step_dir,
          schemas_dir: schemas_dir,
          ecr_client: ecr_client,
          repository_name: "my-repo",
          repository_uri: repository_uri
        )

        # Should have called Subprocess.capture at least twice (build + push)
        expect(Turbofan::Subprocess).to have_received(:capture).at_least(:twice)
      end

      it "also tags and pushes with git commit SHA" do
        allow(described_class).to receive(:git_sha).and_return("git-abc1234")

        described_class.build_and_push(
          step_dir: step_dir,
          schemas_dir: schemas_dir,
          ecr_client: ecr_client,
          repository_name: "my-repo",
          repository_uri: repository_uri
        )

        expect(Turbofan::Subprocess).to have_received(:capture).with(
          "docker", "tag", /#{Regexp.escape(repository_uri)}:sha-/, "#{repository_uri}:git-abc1234"
        )
        expect(Turbofan::Subprocess).to have_received(:capture).with(
          "docker", "push", "#{repository_uri}:git-abc1234"
        )
      end
    end
  end

  describe ".build_and_push_all" do
    let(:ecr_client) { instance_double(Aws::ECR::Client) }
    let(:step_configs) do
      [
        {step_dir: dir1, schemas_dir: schemas_dir, ecr_client: ecr_client, repository_name: "repo1", repository_uri: repository_uri1},
        {step_dir: dir2, schemas_dir: schemas_dir, ecr_client: ecr_client, repository_name: "repo2", repository_uri: repository_uri2}
      ]
    end
    let(:dir1) { File.join(tmpdir, "step1") } # rubocop:disable RSpec/IndexedLet
    let(:dir2) { File.join(tmpdir, "step2") } # rubocop:disable RSpec/IndexedLet
    let(:schemas_dir) { File.join(tmpdir, "schemas") }
    let(:repository_uri1) { "123456789.dkr.ecr.us-east-1.amazonaws.com/repo1" } # rubocop:disable RSpec/IndexedLet
    let(:repository_uri2) { "123456789.dkr.ecr.us-east-1.amazonaws.com/repo2" } # rubocop:disable RSpec/IndexedLet

    before do
      FileUtils.mkdir_p(dir1)
      FileUtils.mkdir_p(dir2)
      FileUtils.mkdir_p(schemas_dir)
      File.write(File.join(dir1, "worker.rb"), "class Step1; end")
      File.write(File.join(dir2, "worker.rb"), "class Step2; end")
      File.write(File.join(schemas_dir, "input.json"), '{"type": "object"}')
    end

    it "builds all steps and returns a hash of results" do
      allow(described_class).to receive(:build_and_push).and_return("sha-aaa111bbb222")

      result = described_class.build_and_push_all(step_configs: step_configs)

      expect(result).to be_a(Hash)
      expect(result.size).to eq(2)
      expect(result[dir1]).to eq("sha-aaa111bbb222")
      expect(result[dir2]).to eq("sha-aaa111bbb222")
    end

    it "delegates to build_and_push for each config" do
      allow(described_class).to receive(:build_and_push).and_return("sha-existing123")

      result = described_class.build_and_push_all(step_configs: step_configs)

      expect(described_class).to have_received(:build_and_push).exactly(2).times
      expect(result.values).to all(eq("sha-existing123"))
    end

    it "propagates build errors" do
      call_count = 0
      allow(described_class).to receive(:build_and_push) do
        call_count += 1
        raise "Command failed: docker build" if call_count == 1
        "sha-aaa111bbb222"
      end

      expect {
        described_class.build_and_push_all(step_configs: step_configs)
      }.to raise_error(/Command failed/)
    end

    it "returns an empty hash when given no configs" do
      result = described_class.build_and_push_all(step_configs: [])

      expect(result).to eq({})
    end
  end

  describe ".empty_repository" do
    let(:ecr_client) { instance_double(Aws::ECR::Client) }
    let(:repository_name) { "my-repo" }

    it "deletes all images in batches of 100" do
      batch1 = Array.new(100) { |i| Aws::ECR::Types::ImageIdentifier.new(image_digest: "sha256:#{i}") }
      batch2 = Array.new(20) { |i| Aws::ECR::Types::ImageIdentifier.new(image_digest: "sha256:#{100 + i}") }

      call_count = 0
      allow(ecr_client).to receive(:list_images) do
        call_count += 1
        case call_count
        when 1 then Aws::ECR::Types::ListImagesResponse.new(image_ids: batch1)
        when 2 then Aws::ECR::Types::ListImagesResponse.new(image_ids: batch2)
        else Aws::ECR::Types::ListImagesResponse.new(image_ids: [])
        end
      end
      allow(ecr_client).to receive(:batch_delete_image)

      described_class.empty_repository(ecr_client, repository_name)

      expect(ecr_client).to have_received(:batch_delete_image).twice
    end

    it "handles already-deleted repositories" do
      allow(ecr_client).to receive(:list_images)
        .and_raise(Aws::ECR::Errors::RepositoryNotFoundException.new(nil, "not found"))

      expect { described_class.empty_repository(ecr_client, repository_name) }.not_to raise_error
    end

    it "does nothing for empty repositories" do
      allow(ecr_client).to receive(:list_images)
        .and_return(Aws::ECR::Types::ListImagesResponse.new(image_ids: []))

      described_class.empty_repository(ecr_client, repository_name)

      expect(ecr_client).not_to have_received(:batch_delete_image) if ecr_client.respond_to?(:batch_delete_image)
    end
  end

  describe ".garbage_collect" do
    let(:ecr_client) { instance_double(Aws::ECR::Client) }
    let(:repository_name) { "my-repo" }

    def make_image(tag, pushed_at)
      double(image_tags: [tag], image_pushed_at: pushed_at, image_digest: "sha256:#{tag}")
    end

    context "when there are more images than the keep count" do
      let(:images) do
        (1..5).map { |i| make_image("sha-#{format("%012d", i)}", Time.now - (5 - i) * 86400) }
      end

      before do
        allow(ecr_client).to receive(:describe_images).and_return(
          double(image_details: images, next_token: nil)
        )
        allow(ecr_client).to receive(:batch_delete_image)
      end

      it "keeps the N most recent and deletes the rest" do
        described_class.garbage_collect(ecr_client, repository_name, keep: 3)

        expect(ecr_client).to have_received(:batch_delete_image).with(
          repository_name: repository_name,
          image_ids: [
            {image_digest: "sha256:sha-000000000001"},
            {image_digest: "sha256:sha-000000000002"}
          ]
        )
      end
    end

    context "when there are fewer images than the keep count" do
      let(:images) do
        (1..2).map { |i| make_image("sha-#{format("%012d", i)}", Time.now - i * 86400) }
      end

      before do
        allow(ecr_client).to receive(:describe_images).and_return(
          double(image_details: images, next_token: nil)
        )
        allow(ecr_client).to receive(:batch_delete_image)
      end

      it "does not delete any images" do
        described_class.garbage_collect(ecr_client, repository_name, keep: 5)

        expect(ecr_client).not_to have_received(:batch_delete_image)
      end
    end

    context "when images have non-sha- prefixed tags" do
      let(:images) do
        [
          make_image("sha-aaa111bbb222", Time.now - 86400),
          make_image("latest", Time.now),
          make_image("v1.0.0", Time.now - 2 * 86400),
          make_image("sha-bbb222ccc333", Time.now - 3 * 86400)
        ]
      end

      before do
        allow(ecr_client).to receive(:describe_images).and_return(
          double(image_details: images, next_token: nil)
        )
        allow(ecr_client).to receive(:batch_delete_image)
      end

      it "only targets sha- prefixed tags" do
        described_class.garbage_collect(ecr_client, repository_name, keep: 1)

        expect(ecr_client).to have_received(:batch_delete_image).with(
          repository_name: repository_name,
          image_ids: [{image_digest: "sha256:sha-bbb222ccc333"}]
        )
      end
    end

    context "when results are paginated across multiple pages" do
      let(:page1_images) do
        (1..3).map { |i| make_image("sha-#{format("%012d", i)}", Time.now - (10 - i) * 86400) }
      end
      let(:page2_images) do
        (4..6).map { |i| make_image("sha-#{format("%012d", i)}", Time.now - (10 - i) * 86400) }
      end

      before do
        allow(ecr_client).to receive(:describe_images)
          .and_return(
            double(image_details: page1_images, next_token: "token1"),
            double(image_details: page2_images, next_token: nil)
          )
        allow(ecr_client).to receive(:batch_delete_image)
      end

      it "collects images across all pages before deciding what to delete" do
        described_class.garbage_collect(ecr_client, repository_name, keep: 4)

        expect(ecr_client).to have_received(:describe_images).twice
        expect(ecr_client).to have_received(:batch_delete_image).with(
          repository_name: repository_name,
          image_ids: [
            {image_digest: "sha256:sha-000000000001"},
            {image_digest: "sha256:sha-000000000002"}
          ]
        )
      end
    end

    # Bug 4: garbage_collect calls img.image_tags.any? but image_tags can be nil
    # for untagged images in ECR, causing a NoMethodError on NilClass.
    context "when some images have nil tags (untagged manifests)" do
      let(:images) do
        [
          make_image("sha-aaa111bbb222", Time.now - 86400),
          double(image_tags: nil, image_pushed_at: Time.now - 2 * 86400, image_digest: "sha256:untagged"),
          make_image("sha-bbb222ccc333", Time.now - 3 * 86400)
        ]
      end

      before do
        allow(ecr_client).to receive(:describe_images).and_return(
          double(image_details: images, next_token: nil)
        )
        allow(ecr_client).to receive(:batch_delete_image)
      end

      it "does not crash on images with nil tags" do
        expect {
          described_class.garbage_collect(ecr_client, repository_name, keep: 5)
        }.not_to raise_error
      end
    end

    context "when the repository is empty" do
      before do
        allow(ecr_client).to receive(:describe_images).and_return(
          double(image_details: [], next_token: nil)
        )
      end

      it "handles gracefully" do
        expect {
          described_class.garbage_collect(ecr_client, repository_name, keep: 30)
        }.not_to raise_error
      end
    end
  end

  describe ".build_and_push_all error messages" do
    let(:ecr_client) { instance_double(Aws::ECR::Client) }
    let(:step_dir) { File.join(tmpdir, "my_step") }
    let(:schemas_dir) { File.join(tmpdir, "schemas") }

    before do
      FileUtils.mkdir_p(step_dir)
      FileUtils.mkdir_p(schemas_dir)
      File.write(File.join(step_dir, "worker.rb"), "class MyStep; end")
      File.write(File.join(schemas_dir, "input.json"), '{"type": "object"}')
    end

    it "includes step name in error message on failure" do
      allow(described_class).to receive(:build_and_push).and_raise("Command failed: docker build")

      expect {
        described_class.build_and_push_all(step_configs: [
          {step_dir: step_dir, schemas_dir: schemas_dir, ecr_client: ecr_client,
           repository_name: "repo", repository_uri: "123.dkr.ecr.us-east-1.amazonaws.com/repo"}
        ])
      }.to raise_error(/Build failed for step 'my_step'/)
    end

    it "includes original error message" do
      allow(described_class).to receive(:build_and_push).and_raise("Command failed: docker build")

      expect {
        described_class.build_and_push_all(step_configs: [
          {step_dir: step_dir, schemas_dir: schemas_dir, ecr_client: ecr_client,
           repository_name: "repo", repository_uri: "123.dkr.ecr.us-east-1.amazonaws.com/repo"}
        ])
      }.to raise_error(/Command failed: docker build/)
    end
  end
end
