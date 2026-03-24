module Turbofan
  module InstanceCatalog
    Entry = Struct.new(:type, :family, :vcpus, :ram_gb, :nvme, keyword_init: true) do
      def nvme?
        nvme
      end
    end

    # [type, family, vcpus, ram_gb, has_nvme]
    # Graviton ARM instances: c (compute), m (general), r (memory)
    INSTANCES = [
      # c6g — compute-optimized, no NVMe
      Entry.new(type: "c6g.medium", family: :c, vcpus: 1, ram_gb: 2, nvme: false),
      Entry.new(type: "c6g.large", family: :c, vcpus: 2, ram_gb: 4, nvme: false),
      Entry.new(type: "c6g.xlarge", family: :c, vcpus: 4, ram_gb: 8, nvme: false),
      Entry.new(type: "c6g.2xlarge", family: :c, vcpus: 8, ram_gb: 16, nvme: false),
      Entry.new(type: "c6g.4xlarge", family: :c, vcpus: 16, ram_gb: 32, nvme: false),
      Entry.new(type: "c6g.8xlarge", family: :c, vcpus: 32, ram_gb: 64, nvme: false),
      Entry.new(type: "c6g.12xlarge", family: :c, vcpus: 48, ram_gb: 96, nvme: false),
      Entry.new(type: "c6g.16xlarge", family: :c, vcpus: 64, ram_gb: 128, nvme: false),
      # c6gd — compute-optimized, NVMe
      Entry.new(type: "c6gd.medium", family: :c, vcpus: 1, ram_gb: 2, nvme: true),
      Entry.new(type: "c6gd.large", family: :c, vcpus: 2, ram_gb: 4, nvme: true),
      Entry.new(type: "c6gd.xlarge", family: :c, vcpus: 4, ram_gb: 8, nvme: true),
      Entry.new(type: "c6gd.2xlarge", family: :c, vcpus: 8, ram_gb: 16, nvme: true),
      Entry.new(type: "c6gd.4xlarge", family: :c, vcpus: 16, ram_gb: 32, nvme: true),
      Entry.new(type: "c6gd.8xlarge", family: :c, vcpus: 32, ram_gb: 64, nvme: true),
      Entry.new(type: "c6gd.12xlarge", family: :c, vcpus: 48, ram_gb: 96, nvme: true),
      Entry.new(type: "c6gd.16xlarge", family: :c, vcpus: 64, ram_gb: 128, nvme: true),
      # c7g — compute-optimized, no NVMe
      Entry.new(type: "c7g.medium", family: :c, vcpus: 1, ram_gb: 2, nvme: false),
      Entry.new(type: "c7g.large", family: :c, vcpus: 2, ram_gb: 4, nvme: false),
      Entry.new(type: "c7g.xlarge", family: :c, vcpus: 4, ram_gb: 8, nvme: false),
      Entry.new(type: "c7g.2xlarge", family: :c, vcpus: 8, ram_gb: 16, nvme: false),
      Entry.new(type: "c7g.4xlarge", family: :c, vcpus: 16, ram_gb: 32, nvme: false),
      Entry.new(type: "c7g.8xlarge", family: :c, vcpus: 32, ram_gb: 64, nvme: false),
      Entry.new(type: "c7g.12xlarge", family: :c, vcpus: 48, ram_gb: 96, nvme: false),
      Entry.new(type: "c7g.16xlarge", family: :c, vcpus: 64, ram_gb: 128, nvme: false),
      # c7gd — compute-optimized, NVMe
      Entry.new(type: "c7gd.medium", family: :c, vcpus: 1, ram_gb: 2, nvme: true),
      Entry.new(type: "c7gd.large", family: :c, vcpus: 2, ram_gb: 4, nvme: true),
      Entry.new(type: "c7gd.xlarge", family: :c, vcpus: 4, ram_gb: 8, nvme: true),
      Entry.new(type: "c7gd.2xlarge", family: :c, vcpus: 8, ram_gb: 16, nvme: true),
      Entry.new(type: "c7gd.4xlarge", family: :c, vcpus: 16, ram_gb: 32, nvme: true),
      Entry.new(type: "c7gd.8xlarge", family: :c, vcpus: 32, ram_gb: 64, nvme: true),
      Entry.new(type: "c7gd.12xlarge", family: :c, vcpus: 48, ram_gb: 96, nvme: true),
      Entry.new(type: "c7gd.16xlarge", family: :c, vcpus: 64, ram_gb: 128, nvme: true),
      # c8g — compute-optimized, no NVMe
      Entry.new(type: "c8g.medium", family: :c, vcpus: 1, ram_gb: 2, nvme: false),
      Entry.new(type: "c8g.large", family: :c, vcpus: 2, ram_gb: 4, nvme: false),
      Entry.new(type: "c8g.xlarge", family: :c, vcpus: 4, ram_gb: 8, nvme: false),
      Entry.new(type: "c8g.2xlarge", family: :c, vcpus: 8, ram_gb: 16, nvme: false),
      Entry.new(type: "c8g.4xlarge", family: :c, vcpus: 16, ram_gb: 32, nvme: false),
      Entry.new(type: "c8g.8xlarge", family: :c, vcpus: 32, ram_gb: 64, nvme: false),
      Entry.new(type: "c8g.12xlarge", family: :c, vcpus: 48, ram_gb: 96, nvme: false),
      Entry.new(type: "c8g.16xlarge", family: :c, vcpus: 64, ram_gb: 128, nvme: false),
      # c8gd — compute-optimized, NVMe
      Entry.new(type: "c8gd.medium", family: :c, vcpus: 1, ram_gb: 2, nvme: true),
      Entry.new(type: "c8gd.large", family: :c, vcpus: 2, ram_gb: 4, nvme: true),
      Entry.new(type: "c8gd.xlarge", family: :c, vcpus: 4, ram_gb: 8, nvme: true),
      Entry.new(type: "c8gd.2xlarge", family: :c, vcpus: 8, ram_gb: 16, nvme: true),
      Entry.new(type: "c8gd.4xlarge", family: :c, vcpus: 16, ram_gb: 32, nvme: true),
      Entry.new(type: "c8gd.8xlarge", family: :c, vcpus: 32, ram_gb: 64, nvme: true),
      Entry.new(type: "c8gd.12xlarge", family: :c, vcpus: 48, ram_gb: 96, nvme: true),
      Entry.new(type: "c8gd.16xlarge", family: :c, vcpus: 64, ram_gb: 128, nvme: true),
      # m7g — general-purpose, no NVMe
      Entry.new(type: "m7g.medium", family: :m, vcpus: 1, ram_gb: 4, nvme: false),
      Entry.new(type: "m7g.large", family: :m, vcpus: 2, ram_gb: 8, nvme: false),
      Entry.new(type: "m7g.xlarge", family: :m, vcpus: 4, ram_gb: 16, nvme: false),
      Entry.new(type: "m7g.2xlarge", family: :m, vcpus: 8, ram_gb: 32, nvme: false),
      Entry.new(type: "m7g.4xlarge", family: :m, vcpus: 16, ram_gb: 64, nvme: false),
      Entry.new(type: "m7g.8xlarge", family: :m, vcpus: 32, ram_gb: 128, nvme: false),
      Entry.new(type: "m7g.12xlarge", family: :m, vcpus: 48, ram_gb: 192, nvme: false),
      Entry.new(type: "m7g.16xlarge", family: :m, vcpus: 64, ram_gb: 256, nvme: false),
      # m7gd — general-purpose, NVMe
      Entry.new(type: "m7gd.medium", family: :m, vcpus: 1, ram_gb: 4, nvme: true),
      Entry.new(type: "m7gd.large", family: :m, vcpus: 2, ram_gb: 8, nvme: true),
      Entry.new(type: "m7gd.xlarge", family: :m, vcpus: 4, ram_gb: 16, nvme: true),
      Entry.new(type: "m7gd.2xlarge", family: :m, vcpus: 8, ram_gb: 32, nvme: true),
      Entry.new(type: "m7gd.4xlarge", family: :m, vcpus: 16, ram_gb: 64, nvme: true),
      Entry.new(type: "m7gd.8xlarge", family: :m, vcpus: 32, ram_gb: 128, nvme: true),
      Entry.new(type: "m7gd.12xlarge", family: :m, vcpus: 48, ram_gb: 192, nvme: true),
      Entry.new(type: "m7gd.16xlarge", family: :m, vcpus: 64, ram_gb: 256, nvme: true),
      # r7g — memory-optimized, no NVMe
      Entry.new(type: "r7g.medium", family: :r, vcpus: 1, ram_gb: 8, nvme: false),
      Entry.new(type: "r7g.large", family: :r, vcpus: 2, ram_gb: 16, nvme: false),
      Entry.new(type: "r7g.xlarge", family: :r, vcpus: 4, ram_gb: 32, nvme: false),
      Entry.new(type: "r7g.2xlarge", family: :r, vcpus: 8, ram_gb: 64, nvme: false),
      Entry.new(type: "r7g.4xlarge", family: :r, vcpus: 16, ram_gb: 128, nvme: false),
      Entry.new(type: "r7g.8xlarge", family: :r, vcpus: 32, ram_gb: 256, nvme: false),
      Entry.new(type: "r7g.12xlarge", family: :r, vcpus: 48, ram_gb: 384, nvme: false),
      Entry.new(type: "r7g.16xlarge", family: :r, vcpus: 64, ram_gb: 512, nvme: false),
      # r7gd — memory-optimized, NVMe
      Entry.new(type: "r7gd.medium", family: :r, vcpus: 1, ram_gb: 8, nvme: true),
      Entry.new(type: "r7gd.large", family: :r, vcpus: 2, ram_gb: 16, nvme: true),
      Entry.new(type: "r7gd.xlarge", family: :r, vcpus: 4, ram_gb: 32, nvme: true),
      Entry.new(type: "r7gd.2xlarge", family: :r, vcpus: 8, ram_gb: 64, nvme: true),
      Entry.new(type: "r7gd.4xlarge", family: :r, vcpus: 16, ram_gb: 128, nvme: true),
      Entry.new(type: "r7gd.8xlarge", family: :r, vcpus: 32, ram_gb: 256, nvme: true),
      Entry.new(type: "r7gd.12xlarge", family: :r, vcpus: 48, ram_gb: 384, nvme: true),
      Entry.new(type: "r7gd.16xlarge", family: :r, vcpus: 64, ram_gb: 512, nvme: true)
    ].freeze

    def self.for_family(family, nvme:)
      INSTANCES.select { |e| e.family == family && e.nvme? == nvme }
    end
  end
end
