# Sends objects to the DPS for a given workflow state

module Hyrax
  module Workflow
    class SendToDps
      def self.call(target:, user: nil, **)
        dps_config = Rails.application.config_for(:dps).symbolize_keys
        if dps_config[:enabled].present?
          DPSWorker.perform_async(target.id, false)
        else
          Rails.logger.error "DPS is not enabled: #{target.id} not preserved"
        end
        true
      end
    end
  end
end
