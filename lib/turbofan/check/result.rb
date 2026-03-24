module Turbofan
  module Check
    Result = Struct.new(:passed, :errors, :warnings, :report, keyword_init: true) do
      alias_method :passed?, :passed
    end
  end
end
