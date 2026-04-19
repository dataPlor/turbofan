# frozen_string_literal: true

require "spec_helper"

RSpec.describe "turbofan resources" do # rubocop:disable RSpec/DescribeClass
  describe "resources list" do
    context "when resources are defined" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          consumable 100
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_class)
      end

      it "lists discovered resources with key, consumable, and type" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to include("places_read")
        expect(output).to include("100")
        expect(output).to include("postgres")
      end
    end

    context "when no resources are defined" do
      before do
        allow(Turbofan::Resource).to receive(:discover).and_return([])
      end

      it "outputs graceful empty message" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to match(/no resources/i)
      end
    end

    context "when resource has no Postgres mixin" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource

          key :rate_limiter
          consumable 50
        end
      end

      before do
        stub_const("Turbofan::Resources::RateLimiter", resource_class)
      end

      it "shows the resource without a type-specific label" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to include("rate_limiter")
        expect(output).to include("50")
      end

      it "does not include a type: field in output" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).not_to include("type:")
      end
    end

    context "with mixed resource types" do
      let(:postgres_resource) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          consumable 100
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      let(:plain_resource) do
        Class.new do
          include Turbofan::Resource

          key :api_limiter
          consumable 50
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", postgres_resource)
        stub_const("Turbofan::Resources::ApiLimiter", plain_resource)
      end

      it "lists both resources" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to include("places_read")
        expect(output).to include("api_limiter")
      end

      it "shows type: postgres only for the Postgres resource" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        lines = output.strip.split("\n")
        postgres_line = lines.find { |l| l.include?("places_read") }
        plain_line = lines.find { |l| l.include?("api_limiter") }

        expect(postgres_line).to include("type: postgres")
        expect(plain_line).not_to include("type:")
      end
    end

    context "with multiple resources" do
      let(:resource_a) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          consumable 100
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      let(:resource_b) do
        Class.new do
          include Turbofan::Resource

          key :api_limiter
          consumable 50
        end
      end

      let(:resource_c) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :brands_write
          consumable 200
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:brands-write"
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_a)
        stub_const("Turbofan::Resources::ApiLimiter", resource_b)
        stub_const("Turbofan::Resources::Databases::BrandsWrite", resource_c)
      end

      it "lists all resources with one line per resource" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        lines = output.strip.split("\n")
        keys = lines.map { |l| l.split(/\s+/).first }

        expect(keys).to include("places_read", "api_limiter", "brands_write")
      end

      it "shows consumable values for each resource" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to include("100")
        expect(output).to include("50")
        expect(output).to include("200")
      end
    end

    context "when resource has no consumable" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_class)
      end

      it "includes resources with nil consumable in the list" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to include("places_read")
      end

      it "shows consumable as nil for resources without consumable" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to include("consumable:")
      end
    end

    context "when formatting list with many resources" do
      let(:resource_a) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          consumable 100
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      let(:resource_b) do
        Class.new do
          include Turbofan::Resource

          key :rate_limiter
          consumable 50
        end
      end

      let(:resource_c) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :brands_write
          consumable 200
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:brands-write"
        end
      end

      let(:resource_d) do
        Class.new do
          include Turbofan::Resource

          key :cache_pool
          consumable 75
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_a)
        stub_const("Turbofan::Resources::RateLimiter", resource_b)
        stub_const("Turbofan::Resources::Databases::BrandsWrite", resource_c)
        stub_const("Turbofan::Resources::CachePool", resource_d)
      end

      it "lists all four resources" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        expect(output).to include("places_read")
        expect(output).to include("rate_limiter")
        expect(output).to include("brands_write")
        expect(output).to include("cache_pool")
      end

      it "shows type: postgres only for Postgres resources among many" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        lines = output.strip.split("\n")
        postgres_lines = lines.select { |l| l.include?("type: postgres") }
        non_postgres_lines = lines.reject { |l| l.include?("type:") }

        expect(postgres_lines.length).to eq(2)
        expect(non_postgres_lines.length).to eq(2)
      end

      it "outputs one line per resource" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "list"])
        end

        lines = output.strip.split("\n")
        expect(lines.length).to eq(4)
      end
    end
  end

  describe "resources deploy" do
    let(:cf_client) { instance_double(Aws::CloudFormation::Client) }

    before do
      allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
    end

    context "with consumable resources" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          consumable 100
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_class)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "calls StackManager.deploy with consumable resource template" do
        Turbofan::CLI.start(["resources", "deploy", "production"])

        expect(Turbofan::Deploy::StackManager).to have_received(:deploy).with(
          cf_client,
          hash_including(
            stack_name: a_string_matching(/turbofan.*resource.*production/i)
          )
        )
      end

      it "generates a CloudFormation template containing ConsumableResource" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        expect(template_body).to include("ConsumableResource")
        expect(template_body).to include("REPLENISHABLE")
        expect(template_body).to include("100")
      end

      it "includes the resource key in the stack tags" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        expect(template_body).to include("places_read")
      end
    end

    context "with multiple resources" do
      let(:resource_a) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          consumable 100
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      let(:resource_b) do
        Class.new do
          include Turbofan::Resource

          key :api_limiter
          consumable 50
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_a)
        stub_const("Turbofan::Resources::ApiLimiter", resource_b)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "deploys a template including all consumable resources" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        expect(template_body).to include("places_read")
        expect(template_body).to include("api_limiter")
      end
    end

    context "with no resources defined" do
      before do
        allow(Turbofan::Resource).to receive(:discover).and_return([])
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "does not call StackManager.deploy" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "deploy", "production"])
        end

        expect(Turbofan::Deploy::StackManager).not_to have_received(:deploy)
        expect(output).to match(/no resources/i)
      end
    end

    it "accepts a STAGE positional argument" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :test_res
        consumable 10
      end
      stub_const("Turbofan::Resources::TestRes", resource_class)
      allow(Turbofan::Deploy::StackManager).to receive(:deploy)

      Turbofan::CLI.start(["resources", "deploy", "staging"])

      expect(Turbofan::Deploy::StackManager).to have_received(:deploy).with(
        cf_client,
        hash_including(stack_name: a_string_including("staging"))
      )
    end

    context "with 3+ resources" do
      let(:resource_a) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          consumable 100
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      let(:resource_b) do
        Class.new do
          include Turbofan::Resource

          key :api_limiter
          consumable 50
        end
      end

      let(:resource_c) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :brands_write
          consumable 200
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:brands-write"
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_a)
        stub_const("Turbofan::Resources::ApiLimiter", resource_b)
        stub_const("Turbofan::Resources::Databases::BrandsWrite", resource_c)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "generates a template with all three resources" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        resource_names = parsed["Resources"].keys

        expect(resource_names.length).to eq(3)
        expect(template_body).to include("places_read")
        expect(template_body).to include("api_limiter")
        expect(template_body).to include("brands_write")
      end

      it "generates unique logical IDs using pascal case for each resource" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        resource_names = parsed["Resources"].keys

        expect(resource_names).to include("ConsumableResourcePlacesRead")
        expect(resource_names).to include("ConsumableResourceApiLimiter")
        expect(resource_names).to include("ConsumableResourceBrandsWrite")
      end
    end

    context "when resource has no consumable" do
      let(:resource_without_consumable) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      let(:resource_with_consumable) do
        Class.new do
          include Turbofan::Resource

          key :api_limiter
          consumable 50
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_without_consumable)
        stub_const("Turbofan::Resources::ApiLimiter", resource_with_consumable)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "excludes non-consumable resources from the deploy template" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        expect(template_body).to include("api_limiter")
        expect(template_body).not_to include("places_read")
      end
    end

    context "with stack name format" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource

          key :test_res
          consumable 10
        end
      end

      before do
        stub_const("Turbofan::Resources::TestRes", resource_class)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "passes stack name in turbofan-resources-STAGE format" do
        Turbofan::CLI.start(["resources", "deploy", "production"])

        expect(Turbofan::Deploy::StackManager).to have_received(:deploy).with(
          cf_client,
          hash_including(stack_name: "turbofan-resources-production")
        )
      end

      it "varies stack name by stage" do
        Turbofan::CLI.start(["resources", "deploy", "staging"])

        expect(Turbofan::Deploy::StackManager).to have_received(:deploy).with(
          cf_client,
          hash_including(stack_name: "turbofan-resources-staging")
        )
      end
    end

    context "with deploy template structure" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource

          key :test_res
          consumable 25
        end
      end

      before do
        stub_const("Turbofan::Resources::TestRes", resource_class)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "generates valid JSON with AWSTemplateFormatVersion" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        expect(parsed["AWSTemplateFormatVersion"]).to eq("2010-09-09")
      end

      it "includes stage in the template description" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        expect(parsed["Description"]).to include("production")
      end

      it "tags resources with turbofan:managed, turbofan:resource, and turbofan:stage" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        tags = parsed["Resources"]["ConsumableResourceTestRes"]["Properties"]["Tags"]

        expect(tags["turbofan:managed"]).to eq("true")
        expect(tags["turbofan:resource"]).to eq("test_res")
        expect(tags["turbofan:stage"]).to eq("production")
      end
    end

    context "without a stage argument" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource

          key :test_res
          consumable 10
        end
      end

      before do
        stub_const("Turbofan::Resources::TestRes", resource_class)
      end

      it "Thor reports an error when STAGE is omitted" do
        original_stderr = $stderr
        $stderr = StringIO.new # rubocop:disable RSpec/ExpectOutput
        capture_stdout do
          Turbofan::CLI.start(["resources", "deploy"])
        end
        stderr_output = $stderr.string
        $stderr = original_stderr # rubocop:disable RSpec/ExpectOutput

        expect(stderr_output).to match(/ERROR|wrong number of arguments|no value provided/i)
      end
    end

    context "when all resources lack consumable" do
      let(:resource_without_consumable) do
        Class.new do
          include Turbofan::Resource
          include Turbofan::Postgres

          key :places_read
          secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-read"
        end
      end

      before do
        stub_const("Turbofan::Resources::Databases::PlacesRead", resource_without_consumable)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "does not call StackManager.deploy when no consumable resources exist" do
        output = capture_stdout do
          Turbofan::CLI.start(["resources", "deploy", "production"])
        end

        expect(Turbofan::Deploy::StackManager).not_to have_received(:deploy)
        expect(output).to match(/no resources/i)
      end
    end

    context "when ConsumableResourceName includes stage" do
      let(:resource_class) do
        Class.new do
          include Turbofan::Resource

          key :test_res
          consumable 25
        end
      end

      before do
        stub_const("Turbofan::Resources::TestRes", resource_class)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "sets ConsumableResourceName to turbofan-KEY-STAGE" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "staging"])

        parsed = JSON.parse(template_body)
        props = parsed["Resources"]["ConsumableResourceTestRes"]["Properties"]
        expect(props["ConsumableResourceName"]).to eq("turbofan-test_res-staging")
      end
    end

    context "when TotalQuantity matches each resource's consumable" do
      let(:resource_a) do
        Class.new do
          include Turbofan::Resource

          key :small_pool
          consumable 10
        end
      end

      let(:resource_b) do
        Class.new do
          include Turbofan::Resource

          key :large_pool
          consumable 500
        end
      end

      before do
        stub_const("Turbofan::Resources::SmallPool", resource_a)
        stub_const("Turbofan::Resources::LargePool", resource_b)
        allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      end

      it "sets TotalQuantity per resource from consumable value" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        small = parsed["Resources"]["ConsumableResourceSmallPool"]["Properties"]
        large = parsed["Resources"]["ConsumableResourceLargePool"]["Properties"]

        expect(small["TotalQuantity"]).to eq(10)
        expect(large["TotalQuantity"]).to eq(500)
      end

      it "includes Outputs with Export names matching export_name pattern" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        expect(parsed).to have_key("Outputs")
        outputs = parsed["Outputs"]

        expect(outputs["ConsumableResourceSmallPoolArn"]).not_to be_nil
        expect(outputs["ConsumableResourceSmallPoolArn"]["Export"]["Name"]).to eq("turbofan-resources-production-small-pool")
        expect(outputs["ConsumableResourceLargePoolArn"]).not_to be_nil
        expect(outputs["ConsumableResourceLargePoolArn"]["Export"]["Name"]).to eq("turbofan-resources-production-large-pool")
      end

      it "sets ResourceType to REPLENISHABLE for all resources" do
        template_body = nil
        allow(Turbofan::Deploy::StackManager).to receive(:deploy) do |_client, **opts|
          template_body = opts[:template_body]
        end

        Turbofan::CLI.start(["resources", "deploy", "production"])

        parsed = JSON.parse(template_body)
        parsed["Resources"].each_value do |resource|
          expect(resource["Properties"]["ResourceType"]).to eq("REPLENISHABLE")
        end
      end
    end
  end
end
