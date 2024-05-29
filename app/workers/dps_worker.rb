require 'active_support/core_ext/hash/indifferent_access'
require 'ora/dps'

class DPSWorker
  include Sidekiq::Worker
  sidekiq_options queue: :dps_save, retry: 3, backtrace: true

  DPS_LOGGER = Logger.new('/data/log/ORA4/sidekiq/dps.log')
  DPS_LOGGER.level = Logger::DEBUG

  sidekiq_retries_exhausted do |msg, exception|
    pid = msg['args'].first
    title = "DPS failure: #{pid} - save to DPS failed after three retries"
    GitlabHelper.create_alert_ticket(title)
    DPS_LOGGER.fatal(title)
  end

  def perform(pid, update_files=false)
    ocfl = ORA::DPS.new
    success = ocfl.save(pid, update_files: update_files)
    raise ORA::DPS::DPSException.new("#{pid} not saved to DPS - will retry") unless success
  end
end
