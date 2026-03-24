# frozen_string_literal: true

module Turbofan
  module ComputeEnvironment
    NVME_MOUNT_PATH = "/mnt/nvme"

    NVME_USERDATA = <<~'BASH'
      Content-Type: multipart/mixed; boundary="MIMEBOUNDARY"
      MIME-Version: 1.0

      --MIMEBOUNDARY
      Content-Type: text/x-shellscript; charset="us-ascii"

      #!/bin/bash
      exec > >(tee /var/log/nvme-setup.log) 2>&1
      echo "=== NVMe Setup Script Starting ==="
      echo "Date: $(date)"
      echo "Instance type: $(curl -s http://169.254.169.254/latest/meta-data/instance-type)"

      set -x

      # Install required tools
      yum install -y nvme-cli mdadm xfsprogs 2>/dev/null || dnf install -y nvme-cli mdadm xfsprogs 2>/dev/null || true

      echo "=== All NVMe devices ==="
      nvme list || echo "nvme list failed"
      lsblk || echo "lsblk failed"

      # Identify instance-store NVMe devices (exclude EBS)
      mapfile -t DEVICES < <(nvme list 2>/dev/null | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')

      echo "=== Found ${#DEVICES[@]} instance-store NVMe devices ==="

      if [ "${#DEVICES[@]}" -eq 0 ]; then
        echo "WARNING: No instance-store NVMe devices found"
        echo "Creating /mnt/nvme as regular directory on root filesystem"
        mkdir -p /mnt/nvme
        chmod 1777 /mnt/nvme
        echo "=== NVMe Setup Completed (fallback mode) ==="
        exit 0
      fi

      # Wipe existing signatures
      for d in "${DEVICES[@]}"; do
        echo "Wiping $d"
        wipefs -fa "$d" || true
      done

      # Create RAID0 if multiple devices, otherwise use single device
      if [ "${#DEVICES[@]}" -gt 1 ]; then
        echo "Creating RAID0 with ${#DEVICES[@]} devices"
        mdadm --create /dev/md0 --level=0 --raid-devices="${#DEVICES[@]}" "${DEVICES[@]}"
        TARGET=/dev/md0
      else
        TARGET="${DEVICES[0]}"
      fi

      echo "Formatting $TARGET with XFS"
      mkfs.xfs -f "$TARGET"

      echo "Mounting to /mnt/nvme"
      mkdir -p /mnt/nvme
      echo "$TARGET /mnt/nvme xfs defaults,noatime,nofail 0 2" >> /etc/fstab
      mount -a
      chmod 1777 /mnt/nvme

      echo "=== NVMe Setup Completed Successfully ==="
      df -h /mnt/nvme

      --MIMEBOUNDARY--
    BASH

    def self.included(base)
      base.extend(ClassMethods)
      base.instance_variable_set(:@turbofan_instance_types, ["optimal"])
      base.instance_variable_set(:@turbofan_max_vcpus, 256)
      base.instance_variable_set(:@turbofan_min_vcpus, 0)
      base.instance_variable_set(:@turbofan_allocation_strategy, "SPOT_PRICE_CAPACITY_OPTIMIZED")
      base.instance_variable_set(:@turbofan_subnets, nil)
      base.instance_variable_set(:@turbofan_security_groups, nil)
    end

    module ClassMethods
      attr_reader :turbofan_instance_types, :turbofan_max_vcpus, :turbofan_min_vcpus,
        :turbofan_allocation_strategy, :turbofan_subnets, :turbofan_security_groups

      def instance_types(types)
        @turbofan_instance_types = Array(types)
      end

      def max_vcpus(value)
        @turbofan_max_vcpus = value
      end

      def min_vcpus(value)
        @turbofan_min_vcpus = value
      end

      def allocation_strategy(value)
        @turbofan_allocation_strategy = value
      end

      def subnets(value)
        @turbofan_subnets = Array(value)
      end

      def security_groups(value)
        @turbofan_security_groups = Array(value)
      end

      def resolved_subnets
        @turbofan_subnets || Turbofan.config.subnets
      end

      def resolved_security_groups
        @turbofan_security_groups || Turbofan.config.security_groups
      end

      def stack_name(stage)
        slug = name.split("::").last
          .gsub(/([a-z])([A-Z])/, '\1_\2').downcase.tr("_", "-")
        "turbofan-ce-#{slug}-#{stage}"
      end

      def export_name(stage)
        "#{stack_name(stage)}-arn"
      end

      def generate_template(stage:)
        account_id = Turbofan.config.aws_account_id
        raise "Turbofan.config.aws_account_id is required for CE template generation" unless account_id

        slug = name.split("::").last
          .gsub(/([a-z])([A-Z])/, '\1_\2').downcase.tr("_", "-")

        subnet_list = resolved_subnets
        sg_list = resolved_security_groups
        raise "No subnets configured. Set subnets on the CE or in Turbofan.config.subnets" if subnet_list.empty?
        raise "No security_groups configured. Set security_groups on the CE or in Turbofan.config.security_groups" if sg_list.empty?

        instance_types_yaml = @turbofan_instance_types.map { |t| "            - #{t}" }.join("\n")
        subnets_yaml = subnet_list.map { |s| "            - #{s}" }.join("\n")
        sgs_yaml = sg_list.map { |s| "            - #{s}" }.join("\n")

        # Indent UserData for YAML embedding (10 spaces for Fn::Base64 value position)
        userdata_yaml = NVME_USERDATA.lines.map { |l| "              #{l}" }.join

        <<~YAML
          AWSTemplateFormatVersion: '2010-09-09'
          Description: 'Turbofan Compute Environment: #{name.split("::").last}'

          Resources:
            LaunchTemplate:
              Type: AWS::EC2::LaunchTemplate
              Properties:
                LaunchTemplateName: turbofan-ce-#{slug}-launchtemplate
                LaunchTemplateData:
                  UserData:
                    Fn::Base64: |
          #{userdata_yaml}
            ComputeEnvironment:
              Type: AWS::Batch::ComputeEnvironment
              Properties:
                Type: MANAGED
                State: ENABLED
                ComputeResources:
                  Type: SPOT
                  AllocationStrategy: #{@turbofan_allocation_strategy}
                  MinvCpus: #{@turbofan_min_vcpus}
                  MaxvCpus: #{@turbofan_max_vcpus}
                  InstanceRole: arn:aws:iam::#{account_id}:instance-profile/ecsInstanceRole
                  SpotIamFleetRole: arn:aws:iam::#{account_id}:role/AmazonEC2SpotFleetTaggingRole
                  InstanceTypes:
          #{instance_types_yaml}
                  Subnets:
          #{subnets_yaml}
                  SecurityGroupIds:
          #{sgs_yaml}
                  Ec2Configuration:
                    - ImageType: ECS_AL2023
                  LaunchTemplate:
                    LaunchTemplateId:
                      Ref: LaunchTemplate
                    Version: "$Latest"
                Tags:
                  turbofan:managed: 'true'
                  turbofan:compute-environment: #{slug}

          Outputs:
            ComputeEnvironmentArn:
              Value:
                Ref: ComputeEnvironment
              Export:
                Name: turbofan-ce-#{slug}-#{stage}-arn
        YAML
      end
    end

    def self.discover
      ObjectSpace.each_object(Class).select { |c|
        next false unless c < self
        class_name = Turbofan::GET_CLASS_NAME.bind_call(c)
        next false unless class_name
        live = begin
          Object.const_get(class_name)
        rescue NameError
          nil
        end
        live == c
      }
    end

    def self.resolve(sym)
      class_name = "ComputeEnvironments::#{Turbofan::Naming.pascal_case(sym)}"
      klass = Object.const_get(class_name)
      unless klass.include?(Turbofan::ComputeEnvironment)
        raise ArgumentError, "#{class_name} does not include Turbofan::ComputeEnvironment"
      end
      klass
    rescue NameError
      raise ArgumentError, "Could not resolve compute_environment :#{sym} (expected #{class_name})"
    end
  end
end
