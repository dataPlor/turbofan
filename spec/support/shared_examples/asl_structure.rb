# Shared examples that validate ASL structural self-consistency.
# Any spec that generates ASL can include these to verify the output
# is well-formed regardless of the specific pipeline shape.
#
# Usage:
#   let(:asl) { generator.generate }
#   include_examples "valid ASL structure"

RSpec.shared_examples "valid ASL structure" do
  it "StartAt references a state that exists in States" do
    expect(asl["States"]).to have_key(asl["StartAt"]),
      "StartAt '#{asl["StartAt"]}' does not exist in States: #{asl["States"].keys}"
  end

  it "every state has either End:true, a Next field, or is a Fail state" do
    asl["States"].each do |state_name, state|
      next if state["Type"] == "Fail" # Fail states are terminal by definition
      has_end = state["End"] == true
      has_next = state.key?("Next")
      expect(has_end || has_next).to be(true),
        "state '#{state_name}' has neither End:true nor a Next field"
    end
  end

  it "every state's Next references a state that exists in States" do
    asl["States"].each do |state_name, state|
      next unless state.key?("Next")

      expect(asl["States"]).to have_key(state["Next"]),
        "state '#{state_name}' has Next='#{state["Next"]}' which does not exist in States"
    end
  end

  it "every Catch target references a state that exists in States" do
    asl["States"].each do |state_name, state|
      next unless state.key?("Catch")

      state["Catch"].each do |catch_clause|
        expect(asl["States"]).to have_key(catch_clause["Next"]),
          "state '#{state_name}' has Catch target '#{catch_clause["Next"]}' which does not exist in States"
      end
    end
  end

  it "Parallel branch states are internally consistent" do
    asl["States"].each do |_state_name, state|
      next unless state["Type"] == "Parallel"

      state["Branches"].each_with_index do |branch, idx|
        branch_states = branch["States"]
        start_at = branch["StartAt"]
        expect(branch_states).to have_key(start_at),
          "Parallel branch #{idx} StartAt '#{start_at}' not in branch States"

        branch_states.each do |bs_name, bs|
          has_end = bs["End"] == true
          has_next = bs.key?("Next")
          expect(has_end || has_next).to be(true),
            "branch state '#{bs_name}' has neither End:true nor Next"

          if bs.key?("Next")
            expect(branch_states).to have_key(bs["Next"]),
              "branch state '#{bs_name}' Next='#{bs["Next"]}' not in branch States"
          end
        end
      end
    end
  end
end
