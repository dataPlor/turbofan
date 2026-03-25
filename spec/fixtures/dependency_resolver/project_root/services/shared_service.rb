require_relative "nested/helper"

module DepResolverFixtures
  module SharedService
    def self.call
      NestedHelper.greet
    end
  end
end
