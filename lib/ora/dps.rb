require 'fileutils'
require "net/http"
require "uri"
require "#{Rails.root}/app/helpers/export_helper"

module ORA
  class DPS
    #TIMESTAMP=$(date --date='2022-11-23T15:48:49.174176Z' +"%Y%m%d%H%M%S")
    include ::ExportHelper
    attr_accessor :dps_config

    DPS_LOGGER = Logger.new('/data/log/ORA4/sidekiq/dps.log')
    DPS_LOGGER.level = Logger::DEBUG

    class DPSException < StandardError
      def initialize(msg="DPS Exception")
        super(msg)
      end
    end

    def initialize
      # Do not allow a connection to initialize unless the DPS
      # is enabled for this server
      @dps_config = Rails.application.config_for(:dps).symbolize_keys
      @dps_config[:base] = "https://#{@dps_config[:server]}:#{@dps_config[:port]}/#{@dps_config[:root_path]}"
      raise ::ORA::DPS::DPSException.new("DPS is not enabled") unless @dps_config[:enabled].present?
    end

    def save(uuid, update_files: true)
      success = false
      ora_object = OraBase.find(uuid)
      files = parse_binary_files(ora_object)
      tx = Fedora6::Client::Transaction.new(@dps_config)
      tx_uri = tx.uri
      ocfl_object = Fedora6::Client::Container.new(@dps_config, uuid)
      DPS_LOGGER.info "UUID: #{uuid}, Transaction URI: #{tx.uri} - Save to DPS start."
      begin
        unless ocfl_object.exists?
          ocfl_object.save(archival_group: true, transaction_uri: tx.uri)
          DPS_LOGGER.debug "UUID: #{uuid}, Transaction URI: #{tx.uri} - OCFL object created."
        end

        ocfl_files = get_ocfl_binary_files(ocfl_object)

        save_object_metadata(ora_object, ocfl_object, tx: tx)
        DPS_LOGGER.debug "UUID: #{uuid}, Transaction URI: #{tx.uri} - Object metadata saved."

        if ora_object.admin_information.first.has_public_url.present?
          save_public_metadata(ora_object, ocfl_object, tx: tx)
          DPS_LOGGER.debug "UUID: #{uuid}, Transaction URI: #{tx.uri} - public metadata saved."
        end

        files.each do |file|
          # Check if we want only new files saved
          next if ocfl_files.include?(file[:id]) and not update_files
          binary = Fedora6::Client::Binary.new(@dps_config, ocfl_object.uri, file[:id])
          if File.exists?(file[:local_path])
            binary.save_by_stream(file[:local_path], transaction_uri: tx.uri, mime_type: file[:mime_type])
            DPS_LOGGER.debug "UUID: #{uuid}, Transaction URI: #{tx.uri} - File #{file[:id]} saved by streaming."
          else
            fileset = FileSet.find(file[:id])
            if fileset.file_by_reference?
              # file_format is the field used to store file-by-reference information
              #    format: "message/external-body;access-type=URL;url=\"file:///home/bodl-ora-svc/testfile.pdf\""
              file_by_reference_information = fileset.file_format
              _message, _access_type, url = file_by_reference_information.split(';')
              full_file_path = url.split('"').last
              local_file_path = full_file_path.split('file://').last
              binary.save_by_stream(local_file_path, transaction_uri: tx_uri, mime_type: 'application/octet-stream')
              DPS_LOGGER.debug "UUID: #{uuid}, Transaction URI: #{tx.uri} - File by reference #{file[:id]} saved by streaming."
            else
              binary_data = fileset.original_file.content
              binary.save(binary_data, file[:id], transaction_uri: tx.uri)
              DPS_LOGGER.debug "UUID: #{uuid}, Transaction URI: #{tx.uri} - File #{file[:id]} saved by content."
            end
          end
        end
        deleted_files = remove_deleted_files(files, ocfl_files, ocfl_object, tx: tx)
        deleted_files.each do |deleted_file|
          DPS_LOGGER.debug "UUID: #{uuid}, Transaction URI: #{tx.uri} - File removed - #{deleted_file}."
        end
      rescue Exception => ex
        DPS_LOGGER.warn "UUID: #{uuid}, Transaction URI: #{tx.uri} - Save to DPS Error - message: #{ex.message}"
        DPS_LOGGER.warn "UUID: #{uuid}, Transaction URI: #{tx.uri} - Save to DPS Error - backtrace: #{ex.backtrace.join("\n")}"
        tx.rollback
        DPS_LOGGER.warn "UUID: #{uuid}, Transaction URI: #{tx.uri} - Save to DPS Error - Transaction rolled back."
      else
        commit_transaction(tx, uuid)
        DPS_LOGGER.info "UUID: #{uuid}, Transaction URI: #{tx.uri} - Save to DPS successful."
        success = true
      ensure
        DPS_LOGGER.info "UUID: #{uuid}, Transaction URI: #{tx.uri} - Save to DPS end."
      end
      return success
    end

    def object_metadata_json_file(uuid)
      "#{uuid}.metadata.ora.v2.json"
    end

    def public_metadata_datacite_file(uuid)
      "#{uuid}.public_metadata.datacite.v4.xml"
    end

    def commit_transaction(tx, uuid, current_retries=0)
      # Try for 30 minutes to commit the transaction
      max_retries = 5
      begin
        tx.commit
      ensure
        status = Fedora6::Client::Transaction.get_transaction(@dps_config, tx.uri)
      end
      if current_retries > max_retries
        DPS_LOGGER.error "UUID: #{uuid}, Transaction URI: #{tx.uri} - Transaction timed out"
        DPS_LOGGER.error "UUID: #{uuid}, Transaction URI: #{tx.uri} - Transaction rolled back"
        tx.rollback
        raise DPSException("Transaction timed out")
      elsif status.code == "410"
        DPS_LOGGER.info "UUID: #{uuid}, Transaction URI: #{tx.uri} - Transaction committed"
      elsif status.code == "204"
        DPS_LOGGER.warn "UUID: #{uuid}, Transaction URI: #{tx.uri} - Transaction not yet committed"
        sleep 300
        transaction_commit(tx, uuid, current_retries + 1)
      else
        DPS_LOGGER.error "UUID: #{uuid}, Transaction URI: #{tx.uri} - Transaction commit failed, transaction has code #{status.code}"
        DPS_LOGGER.error "UUID: #{uuid}, Transaction URI: #{tx.uri} - Transaction rolled back"
        tx.rollback
        raise DPSException("Transaction failed to commit. Transaction status code #{status.code}")
      end
    end

    def save_object_metadata(ora_object, ocfl_object, tx: nil)
      json_file = object_metadata_json_file(ora_object.id)
      object_json = ora_object.export('json')
      object_metadata = Fedora6::Client::Binary.new(@dps_config, ocfl_object.uri, json_file)
      object_metadata.save(object_json, json_file, transaction_uri: tx.uri)
    end

    def save_public_metadata(ora_object, ocfl_object, tx: nil)
      datacite_file = public_metadata_datacite_file(ora_object.id)
      datacite_payload = export_datacite_metadata(ora_object.doi_data)
      public_metadata = Fedora6::Client::Binary.new(@dps_config, ocfl_object.uri, datacite_file)
      public_metadata.save(datacite_payload, datacite_file, transaction_uri: tx.uri)
    end

    def get_ocfl_binary_files(ocfl_object)
      return [] unless ocfl_object.exists?
      files = ocfl_object.children
      file_ids = files.map{|f| f.split("/").last}
      metadata_files = [object_metadata_json_file(ocfl_object.identifier), public_metadata_datacite_file(ocfl_object.identifier)]
      return file_ids - metadata_files
    end

    def remove_deleted_files(files, ocfl_files, ocfl_object, tx: nil)
      ora_file_ids = files.map{|fs| fs[:id]}
      files_to_delete = ocfl_files - ora_file_ids
      files_to_delete.each do |file|
        ocfl_file = Fedora6::Client::Binary.new(@dps_config, ocfl_object.uri, file)
        ocfl_file.delete(tx.uri)
      end
    end

    def parse_binary_files(ora_object)
      files = []
      # Output binary files
      ora_object.file_sets.each do |fs|
        file_digest = fs.original_file.digest.first.object[:path].to_s
        sha1 = file_digest.split(':')[1]
        pair_tree_path = File.join(sha1[0,2], sha1[2,2], sha1[4,2])
        file_location = File.join(@dps_config[:remote_file_root], pair_tree_path, sha1)
        local_file_path = File.join(@dps_config[:local_root], pair_tree_path, sha1)
        files.append({
                       id: fs.id,
                       remote_path: file_location,
                       local_path: local_file_path,
                       mime_type: fs.mime_type
                     })
      end
      return files
    end
  end
end

# TODO: remove once in use
def call_code(uuid)
  uuid = 'uuid_55d2db31-0423-4fd0-970c-a593d85b3ace'
  require 'ora/dps'
  ocfl = ORA::DPS.new
  ocfl.save(uuid)

  # ora_object = OraBase.find(uuid)
  # ocfl_object = Fedora6::Client::Container.new(nil, uuid)
  # ocfl_files = get_ocfl_binary_files(ocfl_object)
  # files = ocfl.parse_binary_files(ora_object)
  # ocfl.remove_deleted_files(files, ocfl_files, ocfl_object)
  # ocfl.save(uuid)
end
