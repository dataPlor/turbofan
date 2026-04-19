# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Router do
  describe "DSL" do
    let(:router_class) do
      Class.new do
        include Turbofan::Router

        sizes :s, :m, :l

        def route(input)
          case input[:file_size_mb]
          when 0..100 then :s
          when 101..500 then :m
          else :l
          end
        end
      end
    end

    it "stores declared sizes" do
      expect(router_class.turbofan_sizes).to eq(%i[s m l])
    end

    it "stores sizes as an array of symbols" do
      expect(router_class.turbofan_sizes).to all(be_a(Symbol))
    end
  end

  describe "sizes with two entries" do
    let(:router_class) do
      Class.new do
        include Turbofan::Router

        sizes :small, :large

        def route(input)
          input[:big] ? :large : :small
        end
      end
    end

    it "stores the two declared sizes" do
      expect(router_class.turbofan_sizes).to eq(%i[small large])
    end
  end

  describe "#route" do
    it "is callable on an instance" do
      router_class = Class.new do
        include Turbofan::Router

        sizes :s, :l

        def route(input)
          :s
        end
      end

      router = router_class.new
      expect(router.route({file_size_mb: 10})).to eq(:s)
    end

    it "raises NotImplementedError on base when not overridden" do
      base_router = Class.new do
        include Turbofan::Router

        sizes :s, :l
      end

      router = base_router.new
      expect { router.route({}) }.to raise_error(NotImplementedError)
    end
  end

  describe "#group_inputs" do
    let(:router_class) do
      Class.new do
        include Turbofan::Router

        sizes :s, :m, :l

        def route(input)
          case input["file_size_mb"]
          when 0..100 then :s
          when 101..500 then :m
          else :l
          end
        end
      end
    end

    it "groups inputs by route result" do
      inputs = [
        {"file" => "a.csv", "file_size_mb" => 10},
        {"file" => "b.csv", "file_size_mb" => 200},
        {"file" => "c.csv", "file_size_mb" => 50},
        {"file" => "d.csv", "file_size_mb" => 1000},
        {"file" => "e.csv", "file_size_mb" => 300}
      ]

      router = router_class.new
      grouped = router.group_inputs(inputs)

      expect(grouped[:s]).to contain_exactly(
        {"file" => "a.csv", "file_size_mb" => 10},
        {"file" => "c.csv", "file_size_mb" => 50}
      )
      expect(grouped[:m]).to contain_exactly(
        {"file" => "b.csv", "file_size_mb" => 200},
        {"file" => "e.csv", "file_size_mb" => 300}
      )
      expect(grouped[:l]).to contain_exactly(
        {"file" => "d.csv", "file_size_mb" => 1000}
      )
    end

    it "returns empty arrays for sizes with no matching inputs" do
      inputs = [{"file" => "small.csv", "file_size_mb" => 10}]

      router = router_class.new
      grouped = router.group_inputs(inputs)

      expect(grouped[:s]).to eq([{"file" => "small.csv", "file_size_mb" => 10}])
      expect(grouped[:m]).to eq([])
      expect(grouped[:l]).to eq([])
    end

    it "handles an empty input list" do
      router = router_class.new
      grouped = router.group_inputs([])

      expect(grouped[:s]).to eq([])
      expect(grouped[:m]).to eq([])
      expect(grouped[:l]).to eq([])
    end

    it "preserves all inputs in the grouped output" do
      inputs = Array.new(10) { |i| {"file" => "#{i}.csv", "file_size_mb" => i * 100} }

      router = router_class.new
      grouped = router.group_inputs(inputs)

      total = grouped.values.flatten
      expect(total.size).to eq(inputs.size)
    end
  end

  describe "validation" do
    it "validates returned size symbols match declared sizes" do
      router_class = Class.new do
        include Turbofan::Router

        sizes :s, :l

        def route(_input)
          :xl # not a declared size
        end
      end

      router = router_class.new

      expect {
        router.group_inputs([{"file" => "test.csv"}])
      }.to raise_error(Turbofan::Router::InvalidSizeError, /xl/)
    end

    it "accepts valid size symbols without error" do
      router_class = Class.new do
        include Turbofan::Router

        sizes :s, :l

        def route(_input)
          :s
        end
      end

      router = router_class.new

      expect {
        router.group_inputs([{"file" => "test.csv"}])
      }.not_to raise_error
    end

    it "rejects nil as a route result" do
      router_class = Class.new do
        include Turbofan::Router

        sizes :s, :l

        def route(_input)
          nil
        end
      end

      router = router_class.new

      expect {
        router.group_inputs([{"file" => "test.csv"}])
      }.to raise_error(Turbofan::Router::InvalidSizeError)
    end
  end

  describe "class isolation" do
    it "does not leak sizes between router classes" do
      router_a = Class.new do
        include Turbofan::Router

        sizes :s, :m, :l
      end

      router_b = Class.new do
        include Turbofan::Router

        sizes :small, :large
      end

      expect(router_a.turbofan_sizes).to eq(%i[s m l])
      expect(router_b.turbofan_sizes).to eq(%i[small large])
    end
  end
end
