require "spec_helper"

RSpec.describe "Turbofan::Postgres" do
  describe "resource type" do
    it "sets turbofan_resource_type to :postgres" do
      pg_class = Class.new do
        include Turbofan::Postgres
      end
      stub_const("Resources::PlacesDb", pg_class)

      expect(pg_class.turbofan_resource_type).to eq(:postgres)
    end
  end

  describe "coexistence with Resource" do
    it "can include both Postgres and Resource" do
      pg_resource = Class.new do
        include Turbofan::Resource
        include Turbofan::Postgres

        key :places_read
        secret "arn:aws:secretsmanager:us-east-1:123:secret:places"
        consumable 50
      end
      stub_const("Resources::PlacesPostgres", pg_resource)

      expect(pg_resource.turbofan_key).to eq(:places_read)
      expect(pg_resource.turbofan_resource_type).to eq(:postgres)
      expect(pg_resource.turbofan_secret).to eq("arn:aws:secretsmanager:us-east-1:123:secret:places")
      expect(pg_resource.turbofan_consumable).to eq(50)
    end

    it "is discoverable as a Resource" do
      pg_resource = Class.new do
        include Turbofan::Resource
        include Turbofan::Postgres

        key :pg_discoverable
      end
      stub_const("Resources::PgDiscoverable", pg_resource)

      discovered = Turbofan::Resource.discover
      expect(discovered).to include(pg_resource)
    end
  end

  describe "without Resource" do
    it "still provides turbofan_resource_type when only Postgres is included" do
      pg_only = Class.new do
        include Turbofan::Postgres
      end
      stub_const("Resources::PgOnly", pg_only)

      expect(pg_only.turbofan_resource_type).to eq(:postgres)
    end
  end

  describe "inclusion order edge cases" do
    it "works when Postgres is included before Resource" do
      pg_first = Class.new do
        include Turbofan::Postgres
        include Turbofan::Resource

        key :pg_first_res
        secret "arn:aws:secretsmanager:us-east-1:123:secret:pg-first"
      end
      stub_const("Resources::PgFirst", pg_first)

      expect(pg_first.turbofan_resource_type).to eq(:postgres)
      expect(pg_first.turbofan_key).to eq(:pg_first_res)
      expect(pg_first.turbofan_secret).to eq("arn:aws:secretsmanager:us-east-1:123:secret:pg-first")
    end

    it "auto-includes Resource when only Postgres is included" do
      pg_only = Class.new do
        include Turbofan::Postgres
      end
      stub_const("Resources::PgAutoResource", pg_only)

      expect(pg_only).to respond_to(:turbofan_resource_type)
      expect(pg_only).to respond_to(:turbofan_key)
      expect(pg_only).to respond_to(:turbofan_secret)
      expect(pg_only).to respond_to(:turbofan_consumable)
    end

    it "makes Postgres-only class discoverable via Resource.discover" do
      pg_only = Class.new do
        include Turbofan::Postgres
      end
      stub_const("Resources::PgAutoDiscoverable", pg_only)

      discovered = Turbofan::Resource.discover
      expect(discovered).to include(pg_only)
    end
  end
end
