# frozen_string_literal: true

require_relative "nested/helper"

module DepResolverFixtures
  module SharedService
    def self.call
      NestedHelper.greet
    end
  end
end
