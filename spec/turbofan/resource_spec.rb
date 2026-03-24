require "spec_helper"

RSpec.describe "Turbofan::Resource" do
  describe "DSL class methods" do
    it "stores key as a symbol" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :places_read
      end
      stub_const("PlacesReadResource", resource_class)

      expect(resource_class.turbofan_key).to eq(:places_read)
    end

    it "stores consumable quantity as an integer" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :places_read
        consumable 100
      end
      stub_const("PlacesConsumable", resource_class)

      expect(resource_class.turbofan_consumable).to eq(100)
    end

    it "stores secret ARN as a string" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :places_read
        secret "arn:aws:secretsmanager:us-east-1:123456789:secret:places-db"
      end
      stub_const("PlacesSecret", resource_class)

      expect(resource_class.turbofan_secret).to eq("arn:aws:secretsmanager:us-east-1:123456789:secret:places-db")
    end

    it "defaults consumable to nil when not set" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :minimal
      end
      stub_const("MinimalResource", resource_class)

      expect(resource_class.turbofan_consumable).to be_nil
    end

    it "defaults secret to nil when not set" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :minimal
      end
      stub_const("MinimalResource", resource_class)

      expect(resource_class.turbofan_secret).to be_nil
    end
  end

  describe "DSL edge cases" do
    it "coerces a string key to a symbol" do
      resource_class = Class.new do
        include Turbofan::Resource

        key "string_key"
      end
      stub_const("StringKeyResource", resource_class)

      expect(resource_class.turbofan_key).to eq(:string_key)
      expect(resource_class.turbofan_key).to be_a(Symbol)
    end

    it "allows overwriting key with a second call" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :first_key
        key :second_key
      end
      stub_const("OverwriteKeyResource", resource_class)

      expect(resource_class.turbofan_key).to eq(:second_key)
    end

    it "stores consumable as whatever value is passed (non-integer)" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :flexible_consumable
        consumable "unlimited"
      end
      stub_const("FlexConsumable", resource_class)

      expect(resource_class.turbofan_consumable).to eq("unlimited")
    end

    it "returns export_name with stage and key" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :places_read
        consumable 100
      end
      stub_const("PlacesExportName", resource_class)

      expect(resource_class.export_name("production")).to eq("turbofan-resources-production-places-read")
    end

    it "allows all three DSL attributes to be set independently" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :full_resource
        consumable 200
        secret "arn:aws:secretsmanager:us-east-1:123:secret:full"
      end
      stub_const("FullResource", resource_class)

      expect(resource_class.turbofan_key).to eq(:full_resource)
      expect(resource_class.turbofan_consumable).to eq(200)
      expect(resource_class.turbofan_secret).to eq("arn:aws:secretsmanager:us-east-1:123:secret:full")
    end

    it "defaults key to nil when not declared" do
      resource_class = Class.new do
        include Turbofan::Resource
      end
      stub_const("NoKeyResource", resource_class)

      expect(resource_class.turbofan_key).to be_nil
    end
  end

  describe ".discover" do
    it "finds all classes including Resource via ObjectSpace" do
      res_a = Class.new do
        include Turbofan::Resource

        key :alpha_db
      end
      stub_const("Resources::Alpha", res_a)

      res_b = Class.new do
        include Turbofan::Resource

        key :beta_db
      end
      stub_const("Resources::Beta", res_b)

      discovered = Turbofan::Resource.discover
      expect(discovered).to include(res_a)
      expect(discovered).to include(res_b)
    end

    it "excludes stale constants (liveness guard)" do
      res = Class.new do
        include Turbofan::Resource

        key :stale_db
      end
      stub_const("Resources::Stale", res)

      Resources.send(:remove_const, :Stale) # rubocop:disable RSpec/RemoveConst

      discovered = Turbofan::Resource.discover
      expect(discovered).not_to include(res)

      # Re-define so stub_const teardown succeeds
      Resources.const_set(:Stale, res)
    end

    it "excludes anonymous classes (no name)" do
      Class.new { include Turbofan::Resource }

      discovered = Turbofan::Resource.discover
      discovered.each do |klass|
        expect(Turbofan::GET_CLASS_NAME.bind_call(klass)).not_to be_nil
      end
    end

    it "includes resource classes that have no key set" do
      res_no_key = Class.new do
        include Turbofan::Resource
        # no key declared
      end
      stub_const("Resources::NoKey", res_no_key)

      discovered = Turbofan::Resource.discover
      expect(discovered).to include(res_no_key)
    end

    it "includes resource classes regardless of whether they have Postgres mixin" do
      plain = Class.new do
        include Turbofan::Resource

        key :plain_res
      end
      stub_const("Resources::Plain", plain)

      pg = Class.new do
        include Turbofan::Resource
        include Turbofan::Postgres

        key :pg_res
      end
      stub_const("Resources::Pg", pg)

      discovered = Turbofan::Resource.discover
      expect(discovered).to include(plain)
      expect(discovered).to include(pg)
    end
  end

  describe "discover_components integration" do
    it "includes resources keyed by turbofan_key" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :places_read
      end
      stub_const("PlacesReadResource", resource_class)

      components = Turbofan.discover_components
      expect(components[:resources]).to be_a(Hash)
      expect(components[:resources][:places_read]).to eq(resource_class)
    end

    it "returns resources alongside steps and pipelines" do
      components = Turbofan.discover_components
      expect(components).to have_key(:steps)
      expect(components).to have_key(:pipelines)
      expect(components).to have_key(:resources)
    end

    it "excludes resources with no key from the resources hash" do
      resource_class = Class.new do
        include Turbofan::Resource
        # no key declared - turbofan_key is nil
      end
      stub_const("NoKeyDiscoverResource", resource_class)

      components = Turbofan.discover_components
      expect(components[:resources].values).not_to include(resource_class)
    end

    it "excludes stale resource constants from resources hash" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :stale_discover
      end
      stub_const("Resources::StaleDiscover", resource_class)

      Resources.send(:remove_const, :StaleDiscover) # rubocop:disable RSpec/RemoveConst

      components = Turbofan.discover_components
      expect(components[:resources]).not_to have_key(:stale_discover)

      # Re-define so stub_const teardown succeeds
      Resources.const_set(:StaleDiscover, resource_class)
    end

    it "discovers multiple resources with distinct keys" do
      res_a = Class.new do
        include Turbofan::Resource

        key :alpha
      end
      stub_const("Resources::AlphaMulti", res_a)

      res_b = Class.new do
        include Turbofan::Resource

        key :beta
      end
      stub_const("Resources::BetaMulti", res_b)

      components = Turbofan.discover_components
      expect(components[:resources][:alpha]).to eq(res_a)
      expect(components[:resources][:beta]).to eq(res_b)
    end

    it "last-wins when two resources share the same key" do
      res_a = Class.new do
        include Turbofan::Resource

        key :shared_key
      end
      stub_const("Resources::SharedA", res_a)

      res_b = Class.new do
        include Turbofan::Resource

        key :shared_key
      end
      stub_const("Resources::SharedB", res_b)

      components = Turbofan.discover_components
      # ObjectSpace iteration order is non-deterministic, but one of them should win
      expect(components[:resources][:shared_key]).to satisfy("be one of the two resources") { |v|
        v == res_a || v == res_b
      }
    end
  end
end
