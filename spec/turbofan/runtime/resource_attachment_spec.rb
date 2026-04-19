# frozen_string_literal: true

require "spec_helper"
require "json"
require "tmpdir"
require "stringio"

RSpec.describe "Runtime resource attachment", :schemas do # rubocop:disable RSpec/DescribeClass
  include WrapperTestHelper

  let(:cloudwatch_client) { instance_double("Aws::CloudWatch::Client", put_metric_data: nil) } # rubocop:disable RSpec/VerifiedDoubleReference
  let(:s3_client) { instance_double("Aws::S3::Client", put_object: nil, get_object: nil) } # rubocop:disable RSpec/VerifiedDoubleReference
  let(:secrets_client) { instance_double("Aws::SecretsManager::Client") } # rubocop:disable RSpec/VerifiedDoubleReference

  let(:duckdb_conn) { nil }
  let(:connection_string) { "postgresql://user:pass@host:5432/places" }

  # Shared resource definitions
  let(:places_read_secret) { "arn:aws:secretsmanager:us-east-1:123:secret:places-db" }

  # Common helper: run wrapper with default empty input
  def run_with_input(step_class)
    run_wrapper(step_class, env: {"TURBOFAN_INPUT" => "{}"})
  end

  describe "when step declares uses with a matching Postgres resource" do
    let(:resource_class) { make_resource(key: :places_read, secret_arn: places_read_secret) }
    let(:step_class) { make_step(name: "ResourceStep", uses: %i[places_read duckdb]) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      stub_const("Resources::PlacesRead", resource_class)
      allow(duckdb_conn).to receive(:execute)
      stub_secret(places_read_secret, connection_string)
    end

    it "fetches the secret from SecretsManager before calling step.call" do
      run_with_input(step_class)

      expect(secrets_client).to have_received(:get_secret_value).with(
        secret_id: places_read_secret
      )
    end

    it "loads postgres extension then uses ATTACH on DuckDB" do
      run_with_input(step_class)

      expect(duckdb_conn).to have_received(:execute).with("LOAD postgres")
      expect(duckdb_conn).to have_received(:execute).with(
        "ATTACH '#{connection_string}' AS \"places_read\" (TYPE POSTGRES, READ_ONLY)"
      )
    end

    it "attaches with READ_ONLY for uses (read-only) dependencies" do
      run_with_input(step_class)

      expect(duckdb_conn).to have_received(:execute).with(
        include("READ_ONLY")
      )
    end
  end

  describe "when step declares writes_to with a matching Postgres resource" do
    let(:resource_class) { make_resource(key: :places_write, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:places-db") }
    let(:step_class) { make_step(name: "WriteStep", writes_to: :places_write) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      stub_const("Resources::PlacesWrite", resource_class)
      allow(duckdb_conn).to receive(:execute)
      stub_secret("arn:aws:secretsmanager:us-east-1:123:secret:places-db", connection_string)
    end

    it "attaches without READ_ONLY for writes_to dependencies" do
      run_with_input(step_class)

      expect(duckdb_conn).to have_received(:execute).with(
        "ATTACH '#{connection_string}' AS \"places_write\" (TYPE POSTGRES)"
      )
    end
  end

  describe "when same key appears in both uses and writes_to" do
    let(:resource_class) { make_resource(key: :shared_db, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:shared-db") }
    let(:step_class) { make_step(name: "MergeStep", uses: :shared_db, writes_to: :shared_db) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      stub_const("Resources::SharedDb", resource_class)
      allow(duckdb_conn).to receive(:execute)
      stub_secret("arn:aws:secretsmanager:us-east-1:123:secret:shared-db", connection_string)
    end

    it "merges to read-write (writes_to wins)" do
      run_with_input(step_class)

      expect(duckdb_conn).to have_received(:execute).with(
        "ATTACH '#{connection_string}' AS \"shared_db\" (TYPE POSTGRES)"
      )
    end

    it "only attaches once (not twice)" do
      run_with_input(step_class)

      expect(duckdb_conn).to have_received(:execute).with(/\AATTACH/).once
    end
  end

  describe "when step has no resource uses" do
    let(:step_class) { make_step(name: "NoResourceStep") }

    it "does not attempt any resource attachment" do
      result = run_with_input(step_class)
      parsed = JSON.parse(result[:output])
      expect(parsed).to eq({})
    end
  end

  describe "when DuckDB is nil (step does not use duckdb)" do
    let(:resource_class) { make_resource(key: :places_read, secret_arn: places_read_secret) }
    let(:step_class) { make_step(name: "NoDuckStep", uses: :places_read) }

    before do
      stub_const("Resources::PlacesRead", resource_class)
    end

    it "raises ResourceUnavailableError when resources are requested but duckdb is unavailable" do
      expect {
        run_with_input(step_class)
      }.to raise_error(Turbofan::ResourceUnavailableError, /DuckDB is not available/)
    end
  end

  describe "when resource is not Postgres type" do
    let(:non_pg_resource) { make_resource(key: :api_token, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:api-token", postgres: false) }
    let(:step_class) { make_step(name: "NonPgStep", uses: %i[api_token duckdb]) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      stub_const("Resources::ApiToken", non_pg_resource)
      allow(duckdb_conn).to receive(:execute)
      stub_secret("arn:aws:secretsmanager:us-east-1:123:secret:api-token", "super-secret-token")
    end

    it "raises an error for the unknown resource type" do
      expect {
        run_with_input(step_class)
      }.to raise_error(Turbofan::ResourceUnavailableError, /Unknown resource type/)
    end

    it "includes the resource key in the error message" do
      expect {
        run_with_input(step_class)
      }.to raise_error(Turbofan::ResourceUnavailableError, /api_token/)
    end
  end

  describe "resource with no secret defined" do
    let(:no_secret_resource) { make_resource(key: :local_pg) }
    let(:step_class) { make_step(name: "NoSecretStep", uses: %i[local_pg duckdb]) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      stub_const("Resources::LocalPg", no_secret_resource)
      allow(duckdb_conn).to receive(:execute)
    end

    it "skips secret fetch and does not call ATTACH" do
      run_with_input(step_class)

      # secrets_client should never be called since turbofan_secret is nil
      expect(duckdb_conn).not_to have_received(:execute)
    end
  end

  describe "multiple resources used by one step" do
    let(:pg_resource_a) { make_resource(key: :places_read, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:places-db") }
    let(:pg_resource_b) { make_resource(key: :analytics_db, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:analytics-db") }
    let(:step_class) { make_step(name: "MultiResourceStep", uses: %i[places_read analytics_db duckdb]) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      stub_const("Resources::PlacesRead", pg_resource_a)
      stub_const("Resources::AnalyticsDb", pg_resource_b)
      allow(duckdb_conn).to receive(:execute)
      stub_secret("arn:aws:secretsmanager:us-east-1:123:secret:places-db", "postgresql://user:pass@host1:5432/places")
      stub_secret("arn:aws:secretsmanager:us-east-1:123:secret:analytics-db", "postgresql://user:pass@host2:5432/analytics")
    end

    it "fetches secrets for both resources" do
      run_with_input(step_class)

      expect(secrets_client).to have_received(:get_secret_value).with(
        secret_id: "arn:aws:secretsmanager:us-east-1:123:secret:places-db"
      )
      expect(secrets_client).to have_received(:get_secret_value).with(
        secret_id: "arn:aws:secretsmanager:us-east-1:123:secret:analytics-db"
      )
    end

    it "uses ATTACH for each resource with its own catalog name" do
      run_with_input(step_class)

      expect(duckdb_conn).to have_received(:execute).with(
        "ATTACH 'postgresql://user:pass@host1:5432/places' AS \"places_read\" (TYPE POSTGRES, READ_ONLY)"
      )
      expect(duckdb_conn).to have_received(:execute).with(
        "ATTACH 'postgresql://user:pass@host2:5432/analytics' AS \"analytics_db\" (TYPE POSTGRES, READ_ONLY)"
      )
    end

    it "loads postgres extension only once" do
      run_with_input(step_class)

      expect(duckdb_conn).to have_received(:execute).with("LOAD postgres").once
    end
  end

  describe "resource key that does not match any discovered resource" do
    let(:step_class) { make_step(name: "MissingResourceStep", uses: %i[nonexistent_resource duckdb]) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      allow(duckdb_conn).to receive(:execute)
    end

    it "raises an error for the unrecognized resource key" do
      expect {
        run_with_input(step_class)
      }.to raise_error(Turbofan::ResourceUnavailableError, /nonexistent_resource/)
    end
  end

  describe "resource attachment fail-fast" do
    let(:step_class) { make_step(name: "FailFastStep", uses: %i[places_read duckdb]) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      allow(duckdb_conn).to receive(:execute)
      # discover_components returns no resources at all
      allow(Turbofan).to receive(:discover_components).and_return(
        steps: {}, pipelines: {}, resources: {}
      )
    end

    it "raises an error when a declared resource key is not found" do
      expect {
        run_with_input(step_class)
      }.to raise_error(Turbofan::ResourceUnavailableError, /places_read/)
    end

    it "includes discovered resource keys in the error message" do
      other_resource = make_resource(key: :other_db, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:other")
      allow(Turbofan).to receive(:discover_components).and_return(
        steps: {}, pipelines: {}, resources: {other_db: other_resource}
      )

      expect {
        run_with_input(step_class)
      }.to raise_error(Turbofan::ResourceUnavailableError, /other_db/)
    end
  end

  describe "empty uses array behavior" do
    let(:step_class) { make_step(name: "EmptyUsesStep") }

    it "returns early without querying discover_components" do
      # Spy on discover_components
      allow(Turbofan).to receive(:discover_components).and_call_original

      run_with_input(step_class)

      expect(Turbofan).not_to have_received(:discover_components)
    end
  end

  describe "step uses only :duckdb (no other resource keys)" do
    let(:step_class) { make_step(name: "DuckdbOnlyStep", uses: :duckdb) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      allow(duckdb_conn).to receive(:execute)
    end

    it "filters out :duckdb from resource keys and returns early" do
      allow(Turbofan).to receive(:discover_components).and_call_original

      run_with_input(step_class)

      # :duckdb is rejected from resource_keys, leaving empty list -> early return
      expect(Turbofan).not_to have_received(:discover_components)
      expect(duckdb_conn).not_to have_received(:execute)
    end
  end

  describe "mixed postgres and non-postgres resources in one step" do
    let(:pg_resource) { make_resource(key: :main_db, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:main-db") }
    let(:api_resource) { make_resource(key: :api_creds, secret_arn: "arn:aws:secretsmanager:us-east-1:123:secret:api-creds", postgres: false) }
    let(:step_class) { make_step(name: "MixedResourceStep", uses: %i[main_db api_creds duckdb]) }
    let(:duckdb_conn) { instance_double("DuckDB::Connection") } # rubocop:disable RSpec/VerifiedDoubleReference

    before do
      stub_const("Resources::MainDb", pg_resource)
      stub_const("Resources::ApiCreds", api_resource)
      allow(duckdb_conn).to receive(:execute)
      stub_secret("arn:aws:secretsmanager:us-east-1:123:secret:main-db", "postgresql://user:pass@host:5432/main")
      stub_secret("arn:aws:secretsmanager:us-east-1:123:secret:api-creds", "sk-abc123")
    end

    it "raises an error when it encounters the non-postgres resource" do
      expect {
        run_with_input(step_class)
      }.to raise_error(Turbofan::ResourceUnavailableError, /Unknown resource type.*api_creds/)
    end
  end
end
