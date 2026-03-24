module Turbofan
  module Postgres
    def self.included(base)
      base.include(Turbofan::Resource) unless base.include?(Turbofan::Resource)
      base.instance_variable_set(:@turbofan_resource_type, :postgres)
      base.extend(ClassMethods)
    end

    module ClassMethods
      attr_reader :turbofan_resource_type, :turbofan_database

      def database(value)
        @turbofan_database = value
      end
    end
  end
end
