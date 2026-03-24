# Helper methods for building common DAG shapes in specs.
#
# Usage:
#   include DagBuilders
#   let(:dag) { build_diamond_dag }
module DagBuilders
  # A -> {B, C} -> D
  def build_diamond_dag
    dag = Turbofan::Dag.new
    dag.add_step(:step_a)
    dag.add_step(:step_b)
    dag.add_step(:step_c)
    dag.add_step(:step_d)
    dag.add_edge(from: :trigger, to: :step_a)
    dag.add_edge(from: :step_a, to: :step_b)
    dag.add_edge(from: :step_a, to: :step_c)
    dag.add_edge(from: :step_b, to: :step_d)
    dag.add_edge(from: :step_c, to: :step_d)
    dag.freeze!
    dag
  end

  # A -> {B, C} -> D -> {E, F} -> G
  def build_sequential_forks_dag
    dag = Turbofan::Dag.new
    dag.add_step(:step_a)
    dag.add_step(:step_b)
    dag.add_step(:step_c)
    dag.add_step(:step_d)
    dag.add_step(:step_e)
    dag.add_step(:step_f)
    dag.add_step(:step_g)
    dag.add_edge(from: :trigger, to: :step_a)
    dag.add_edge(from: :step_a, to: :step_b)
    dag.add_edge(from: :step_a, to: :step_c)
    dag.add_edge(from: :step_b, to: :step_d)
    dag.add_edge(from: :step_c, to: :step_d)
    dag.add_edge(from: :step_d, to: :step_e)
    dag.add_edge(from: :step_d, to: :step_f)
    dag.add_edge(from: :step_e, to: :step_g)
    dag.add_edge(from: :step_f, to: :step_g)
    dag.freeze!
    dag
  end

  # A -> {B, C, D} -> E
  def build_three_way_fork_dag
    dag = Turbofan::Dag.new
    dag.add_step(:step_a)
    dag.add_step(:step_b)
    dag.add_step(:step_c)
    dag.add_step(:step_d)
    dag.add_step(:step_e)
    dag.add_edge(from: :trigger, to: :step_a)
    dag.add_edge(from: :step_a, to: :step_b)
    dag.add_edge(from: :step_a, to: :step_c)
    dag.add_edge(from: :step_a, to: :step_d)
    dag.add_edge(from: :step_b, to: :step_e)
    dag.add_edge(from: :step_c, to: :step_e)
    dag.add_edge(from: :step_d, to: :step_e)
    dag.freeze!
    dag
  end

  # A -> {B->B2, C->C2} -> D
  def build_multi_step_branch_dag
    dag = Turbofan::Dag.new
    dag.add_step(:step_a)
    dag.add_step(:step_b)
    dag.add_step(:step_b2)
    dag.add_step(:step_c)
    dag.add_step(:step_c2)
    dag.add_step(:step_d)
    dag.add_edge(from: :trigger, to: :step_a)
    dag.add_edge(from: :step_a, to: :step_b)
    dag.add_edge(from: :step_a, to: :step_c)
    dag.add_edge(from: :step_b, to: :step_b2)
    dag.add_edge(from: :step_c, to: :step_c2)
    dag.add_edge(from: :step_b2, to: :step_d)
    dag.add_edge(from: :step_c2, to: :step_d)
    dag.freeze!
    dag
  end

  # Builds a pipeline class that uses a pre-built DAG (via stub).
  # The pipeline_name is used for the turbofan name.
  def build_pipeline_for_dag(dag, pipeline_name:)
    pname = pipeline_name
    klass = Class.new do
      include Turbofan::Pipeline
      pipeline_name pname
      pipeline { step_a(trigger_input) }
    end
    allow(klass).to receive(:turbofan_dag).and_return(dag)
    klass
  end
end

RSpec.configure do |config|
  config.include DagBuilders, type: :asl_generator
  config.include DagBuilders, :schemas
end
