require "spec_helper"

RSpec.describe "docker_image DSL" do # rubocop:disable RSpec/DescribeClass
  describe "Step with docker_image" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest"
      end
    end

    it "stores the docker image URI" do
      expect(step_class.turbofan_docker_image).to eq("123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest")
    end

    it "reports as external" do
      expect(step_class.turbofan_external?).to be true
    end
  end

  describe "Step without docker_image" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
    end

    it "defaults docker_image to nil" do
      expect(step_class.turbofan_docker_image).to be_nil
    end

    it "reports as not external" do
      expect(step_class.turbofan_external?).to be false
    end
  end

  describe "docker_image with empty string" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image ""
      end
    end

    it "stores the empty string" do
      expect(step_class.turbofan_docker_image).to eq("")
    end

    it "reports as not external (empty string is not a valid image)" do
      expect(step_class.turbofan_external?).to be false
    end
  end

  describe "docker_image overwrite on second call" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image "first-image:v1"
        docker_image "second-image:v2"
      end
    end

    it "last docker_image call wins" do
      expect(step_class.turbofan_docker_image).to eq("second-image:v2")
    end
  end

  describe "docker_image combined with tags" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest"
        tags stack: "ml", model: "sentiment"
      end
    end

    it "stores both docker_image and tags independently" do
      expect(step_class.turbofan_docker_image).to eq("123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest")
      expect(step_class.turbofan_tags).to eq("stack" => "ml", "model" => "sentiment")
      expect(step_class.turbofan_external?).to be true
    end
  end

  describe "external step with docker_image and family/cpu" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest"
        compute_environment :test_ce
        cpu 4
      end
    end

    it "allows docker_image alongside family and cpu" do
      expect(step_class.turbofan_docker_image).to eq("123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest")
      expect(step_class.turbofan_external?).to be true
      expect(step_class.turbofan_default_cpu).to eq(4)
    end
  end

  describe "external step still requires schemas in check", :schemas do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "ext-pipeline"
      end
    end

    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/foo:latest"
        # No input_schema or output_schema
      end
    end

    it "reports missing schema errors even for external steps" do
      result = Turbofan::Check::PipelineCheck.run(
        pipeline: pipeline_class,
        steps: {ext_step: step_class}
      )
      expect(result.errors).to include(
        a_string_matching(/input_schema/),
        a_string_matching(/output_schema/)
      )
    end
  end

  describe "class isolation" do
    let(:step_a) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest"
      end
    end

    let(:step_b) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
    end

    it "does not leak docker_image between step classes" do
      step_a
      step_b

      expect(step_a.turbofan_docker_image).to eq("123456789.dkr.ecr.us-east-1.amazonaws.com/sentiment:latest")
      expect(step_a.turbofan_external?).to be true
      expect(step_b.turbofan_docker_image).to be_nil
      expect(step_b.turbofan_external?).to be false
    end
  end
end
