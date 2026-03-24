module Turbofan
  module InstanceSelector
    WASTE_THRESHOLD = 0.10

    Result = Struct.new(:instance_types, :details, :spot_availability, keyword_init: true)

    def self.select(cpu:, ram:, duckdb:)
      family = derive_family(cpu, ram)
      candidates = InstanceCatalog.for_family(family, nvme: duckdb)

      details = candidates.filter_map { |entry|
        next if entry.vcpus < cpu || entry.ram_gb < ram

        waste = compute_waste(family, entry, cpu, ram)
        next if waste >= WASTE_THRESHOLD

        jobs = [entry.vcpus / cpu, entry.ram_gb / ram].min

        {
          type: entry.type,
          vcpus: entry.vcpus,
          ram_gb: entry.ram_gb,
          waste: waste,
          jobs_per_instance: jobs
        }
      }

      Result.new(
        instance_types: details.map { |d| d[:type] },
        details: details,
        spot_availability: assess_spot(details.size)
      )
    end

    def self.derive_family(cpu, ram)
      ratio = ram.to_f / cpu
      if ratio >= 8
        :r
      elsif ratio >= 4
        :m
      else
        :c
      end
    end
    private_class_method :derive_family

    def self.compute_waste(family, entry, job_cpu, job_ram)
      case family
      when :c, :m
        (entry.vcpus % job_cpu).to_f / entry.vcpus
      when :r
        (entry.ram_gb % job_ram).to_f / entry.ram_gb
      end
    end
    private_class_method :compute_waste

    def self.assess_spot(pool_size)
      if pool_size >= 9
        :good
      elsif pool_size >= 4
        :moderate
      else
        :risky
      end
    end
    private_class_method :assess_spot
  end
end
