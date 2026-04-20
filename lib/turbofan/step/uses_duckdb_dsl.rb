# frozen_string_literal: true

module Turbofan
  module Step
    # Block-form receiver for `uses :duckdb do ... end`. Delegates the
    # single meaningful verb (extensions) back to the owning step class
    # so the public side of the DSL doesn't leak a private receiver type.
    class UsesDuckdbDSL
      def initialize(step_class)
        @step_class = step_class
      end

      def extensions(*names)
        @step_class.send(:add_duckdb_extensions, names)
      end
    end
  end
end
