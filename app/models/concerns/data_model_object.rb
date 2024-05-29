##
# DataModelObject
#
# a set of tools for exporting and importing datamodel version 2 conforant objects
# including configuration for minor version compatibility > v2.3
# 
# objects can be exported as Ruby objects, JSON, or yaml

require 'yaml'

class FileSetAttacher
  # import is called as a module's class method, so doesn't import
  # behaviours which expect a real object or class cleanly
  include Ora::IngestBehaviour
  include Ora::TransferBehaviour
  include Hyrax::IngestHelper
  def attach(new_work, file_metadata, new_work_id, ora_admin_user)
    create_and_attach_file_set(new_work, file_metadata, new_work_id, ora_admin_user)
  end
end
 
module DataModelObject
  include Concerns::ContributorHelper

  extend self

  def generate_uuid
    # A duplicate of OraMinterBehaviour, required to protect
    # the method from overwrite. See
    uuid = SecureRandom.uuid
    return "uuid_#{uuid}"
  end

  def export(method='json')
    datamodel_object = export_hyrax_object_as_datamodel2_object
    if method == 'json'
      JSON.pretty_generate datamodel_object
    elsif method == 'ruby'
      datamodel_object
    elsif method == 'yaml'
      datamodel_object.to_yaml
    end
  end

  def import(json_string, source=nil)
    ora_admin_user = User.find_by_user_key('ora.system@bodleian.ox.ac.uk')

    datamodel_hash = JSON.parse(json_string)
    datamodel_hash = datamodel_hash.deep_symbolize_keys
    new_work = object_datamodel2_metadata_to_hyrax(datamodel_hash)
    datamodel_hash[:binary_files].each do |file_hash|
      file_metadata = binary_file_datamodel2_metadata_to_hyrax(file_hash, source, new_work.id)
      attacher = FileSetAttacher.new
      attacher.attach(new_work, file_metadata, new_work.id, ora_admin_user)
    end
    admin_set_permission_template = Hyrax::PermissionTemplate.find_by(source_id: "admin_set/default")
    new_work = apply_admin_set(new_work, admin_set_permission_template)
    # We have to create the work before adding a sipity entity to it
    new_work.save
    new_work = set_sipity_entity_and_tagging(new_work, datamodel_hash, ora_admin_user, admin_set_permission_template)
    new_work.save
    new_work
  end
  
  ### 
  # Workflow functions
  ###

  def apply_admin_set(new_work, admin_set_permission_template)
    new_work.admin_set_id = "admin_set/default"
    # set work visibility
    new_work.visibility = admin_set_permission_template.visibility
    # apply permission template
    Hyrax::PermissionTemplateApplicator.apply(admin_set_permission_template).to(model: new_work)
    new_work
  end

  def set_sipity_entity_and_tagging(new_work, datamodel_hash, ora_admin_user, admin_set_permission_template)
    active_workflow = admin_set_permission_template.active_workflow

    last_reviewer = datamodel_hash[:history].select { |h| h[:action_comment] == "Last updated by" }.first
    review_histories = datamodel_hash[:history].select { |h| h[:action_category] == "review_workflow" && h[:action_date].present? }

    if last_reviewer.present?
      new_work.date_modified = last_reviewer[:action_date]&.to_datetime
    else
      first_reviewer = datamodel_hash[:history].select { |h| h[:action_comment] == "First reviewed by" }.first
      
      if first_reviewer.present?
        new_work.date_modified = first_reviewer[:action_date]&.to_datetime
      else
        if review_histories.present?
          last_history = review_histories.sort_by { |h| h[:action_date].to_s }.last
          new_work.date_modified = last_history[:action_date]&.to_datetime
        end
      end
    end

    first_state = Sipity::WorkflowState.where(workflow_id: active_workflow, name: "draft").first

    entity = Sipity::Entity.find_by_proxy_for_global_id(new_work.to_global_id.to_s)
    unless entity.present?
      entity = Sipity::Entity.create(
        proxy_for_global_id: new_work.to_global_id.to_s,
        workflow: active_workflow,
        workflow_state: first_state
      )
    end

    tagging_histories = datamodel_hash[:history].select { |h| h[:action_category] == "tagging" }

    if tagging_histories.present?
      last_priority_history = tagging_histories.last
      tag_context, tag_action, tag_value = last_priority_history[:action_description].split(" ")
      entity.set_tag_list_on(tag_context, tag_value)
    end

    # last_reviewer is established above
    if last_reviewer.present?
      entity.set_tag_list_on("reviewers", last_reviewer[:action_responsibility])
    elsif datamodel_hash[:record_first_reviewed_by].present?
      entity.set_tag_list_on("reviewers", datamodel_hash[:record_first_reviewed_by])
    end

    # Ensure priority is set explitly and can override default behaviours
    if datamodel_hash[:record_review_priority].present?
      entity.priority_list = datamodel_hash[:record_review_priority]
      entity.log_to_proxy_history(
        category: "tagging",
        description: "priorities set #{entity.priority_list.first}"
        )
    end

    entity.save

    # review_histories is established earlier
    if review_histories.present?
      last_history = review_histories.sort_by { |h| h[:action_date].to_s }.last
      last_action = last_history[:action_description]
      sipity_action = Sipity::WorkflowAction.where(workflow: active_workflow, name: last_action).first
    else
      sipity_action = Sipity::WorkflowAction.where(workflow: active_workflow, name: 'submit').first
    end

    # This must be the last action called before completion.
    # Hyrax::Workflow::WorkflowActionSerice calls an update to indexing at the end of workflow action commitment
    # This being called last will ensure that the complete object including tags and priorities will be re-indexed into Solr
    subject = Hyrax::WorkflowActionInfo.new(new_work, ora_admin_user)
    Hyrax::Workflow::WorkflowActionService.run(subject: subject, action: sipity_action)
      
    new_work
  end

  
  #####
  # Config functions
  #####

  def unmapped_metadata_fields
    [
      :access_control_id, 
      :admin_set_id, 
      :arkivo_checksum, 
      :based_near, 
      :bibliographic_citation, 
      :contributor, 
      :creator, 
      :date_created, 
      :date_modified, 
      :date_uploaded, 
      :depositor, 
      :description, 
      :embargo_id, 
      :has_funding,
      :head, 
      :identifier, 
      :import_url, 
      :label, 
      :lease_id, 
      :license, 
      :on_behalf_of,
      :owner, 
      :proxy_depositor, 
      :publisher,
      :related_url, 
      :relative_path, 
      :relative_url, 
      :rendering_ids, 
      :representative_id, 
      :resource_type, 
      :rights_statement, 
      :source,
      :state, 
      :tail, 
      :thesis_access_condition,
      :thumbnail_id, 
      :workflow_state_name, 
    ]
  end

  def unmapped_identifier_fields
    [
      :id, :ora_base_id, :admin_info_id, :bibliographic_info_id, 
      :contributor_info_id, :funder_info_id, :item_desc_and_embargo_info_id
    ]
  end

  def unmapped_binary_file_metadata_fields
    unmapped_metadata_fields + [:license, :title, :subject, :keyword, :language]
  end

  def renamed_fields
    {
      data_digital_data_total_file_size: :data_digital_total_file_size,
      identifier_isbn_10: :identifier_isbn10,
      identifier_isbn_13: :identifier_isbn13
    }
  end

  def unmapped_admin_fields
    [
      :deposit_in_progress, :symplectic_review_status, :record_first_reviewed_time,
      :record_subsequent_review_time

    ]
  end

  def data_model_to_hyrax_class_mappings
    {
      "Book" => "Book",
      "Book section" => "BookSection",
      "Composition" => "Composition",
      "Conference item" => "ConferenceItem",
      "Dataset" => "Dataset",
      "Ephemera" => "Ephemera",
      "Internet publication" => "InternetPublication",
      "Journal article" => "JournalArticle",
      "Patent" => "Patent",
      "Physical object" => "PhysicalObject",
      # Record is not yet a Hyrax Work Type. This will be fixed by
      # https://trello.com/c/vl94Tpjj/
      "Record" => "UniversalTestObject",
      "Report" => "Report",
      "Thesis" => "Thesis",
      "Working paper" => "WorkingPaper"
    }
  end

  def hyrax_to_object_root_mappings
    # All of these Hyrax object types map directly to the datamodel root
    # e.g. self.bibliographic_information.first.identifier_doi maps to datamodel_object[:identifier_doi]
    {
      archiving_costs_information: {parent: nil, class: 'ArchivingCostsInfo'},
      bibliographic_information: {parent: nil, class: 'BibliographicInfo'},
      publishers: {parent: :bibliographic_information, class: 'PublisherInfo'},
      events: {parent: :bibliographic_information, class: 'EventInfo'},
      licence_and_rights_information: {parent: nil, class: 'LicenceAndRightsInfo'},
      item_description_and_embargo_information: {parent: nil, class: 'ItemDescAndEmbargoInfo'},
      admin_information: {parent: nil, class: 'AdminInfo'}
    } 
  end

  def interpret_binary_file_path_from_source(binary_file_metadata, source, parent_uuid)
    binary_file_metadata = binary_file_metadata.deep_symbolize_keys
    binary_file_path = ''
    dps_config = Rails.application.config_for(:dps).symbolize_keys
    metadata_file_path = binary_file_metadata[:file_path]
    if metadata_file_path.present? and File.exists?(metadata_file_path)
      binary_file_path = metadata_file_path
    elsif metadata_file_path.present? and file_url_exists?(metadata_file_path)
      binary_file_path = metadata_file_path
    elsif source == 'dps' and dps_config[:enabled].present?
      auth = "#{dps_config[:user]}:#{dps_config[:password]}"
      base = "https://#{auth}@#{dps_config[:server]}/#{dps_config[:root_path]}"
      binary_file_path = "#{base}/#{parent_uuid}/#{binary_file_metadata[:file_admin_hyrax_fileset_identifier]}"
    elsif binary_file_metadata[:file_sha1].present?
      # Try to load the binary file from Fedora4's binary directory
      # The SHA1 can sometimes have a prefix, eg. urn:sha1:0000000000000
      sha1 = binary_file_metadata[:file_sha1].split(':').last
      binary_path = "/#{sha1[0,2]}/#{sha1[2,2]}/#{sha1[4,2]}/#{sha1}"
      fedora4_binary_path = dps_config[:local_root] + binary_path
      if File.exists?(fedora4_binary_path)
        binary_file_path = fedora4_binary_path
      end
    end
    # TODO: add guesswork for file by reference
    return binary_file_path
  end

  #####
  # Utility functions
  #####

  def file_url_exists?(file_path)
    # Taken from https://stackoverflow.com/questions/5908017/check-if-url-exists-in-ruby
    url = URI.parse(file_path)
    req = Net::HTTP.new(url.host, url.port)
    req.use_ssl = (url.scheme == 'https')
    path = url.path if url.path.present?
    res = req.request_head(path)
    res.code != "404" # false if returns 404 - not found
  rescue
    false # false if can't find the server
  end
  
  def map_hyrax_to_hash(target, datamodel_object)
    # Map hyrax child objects to a hash that has a 1:1 releationship with the datamodel, e.g.
    # bibliographic information 
    hyrax_to_object_root_mappings.keys.map do |key|
      datamodel_object = datamodel_object.merge(convert_hyrax_object_to_hash(target, key.to_s))
    end
    datamodel_object
  end

  def convert_hyrax_object_to_hash(target, hyrax_object_name)
    # Ensure we have a clean hyrax object, or a blank value
    begin
      mapping = hyrax_to_object_root_mappings[hyrax_object_name.to_sym]
      if mapping[:parent].present?
        hyrax_object = target.send(mapping[:parent]).first.send(hyrax_object_name).first
      else
        hyrax_object = target.send(hyrax_object_name).first
      end
      if hyrax_object.nil?
        hyrax_object = hyrax_to_object_root_mappings[hyrax_object_name.to_sym][:class].constantize.new
      end
    rescue NoMethodError
      hyrax_object = hyrax_to_object_root_mappings[hyrax_object_name.to_sym][:class].constantize.new
    end
    convert_to_hash(hyrax_object, unmapped_identifier_fields)
  end

  def remove_blank_values(object_hash)
    object_hash.keys.map do |k|
      if object_hash[k].is_a? Array
        object_hash[k].reject!{|a| a.blank?}
      end
      object_hash.delete(k) unless object_hash[k].present?
    end
    object_hash
  end

  def convert_to_hash(hyrax_object, excluded_keys)
    # Convert a hyrax object, e.g. an AdminInfo object
    # into a hash with symbolized keys, removing keys we don't
    # want, like ora_base_id.
    #
    # Doing this via JSON because Hyrax objects lack a to_h method
    json_string = hyrax_object.to_json(except: excluded_keys)
    object_hash = JSON.parse(json_string).deep_symbolize_keys
    remove_blank_values(object_hash)
  end

  def sort_hash(hash)
    # Sort the keys of a hash alphabetically
    hash = hash.deep_stringify_keys
    sorted_hash = {}.tap do |h2|
      hash.sort.each do |k,v|
        h2[k] = v.is_a?(Hash) ? sort_hash(v) : v
      end
    end
    sorted_hash.deep_symbolize_keys
  end

  def clean_object_metadata(datamodel_object, minor_version='3')
    # Cleaning function
    datamodel_object[:title] = datamodel_object[:title].first
    renamed_fields.each do |k,v|
      if datamodel_object[k].present?
        datamodel_object[v] = datamodel_object[k]
        datamodel_object.delete(k)
      end
    end
    unmapped_admin_fields.each do |field|
      datamodel_object.delete(field) if datamodel_object[field].present?
    end
    if datamodel_object[:type_of_work] == 'UniversalTestObject'
      datamodel_object[:type_of_work] = 'Record'
    end
    datamodel_object
  end

  ##### 
  # Export Methods
  #####

  def export_hyrax_object_as_datamodel2_object(minor_version='3')
    datamodel_object = hyrax_metadata_to_datamodel2
    datamodel_object = clean_object_metadata(datamodel_object, minor_version)
    datamodel_object[:binary_files] = self.file_sets.map{|fs| binary_file_metadata_to_datamodel2(fs, minor_version)}
    datamodel_object
  end

  def hyrax_metadata_to_datamodel2(minor_version='3')
      ### Create base object
    datamodel_object = convert_to_hash(self, unmapped_metadata_fields.append(:id))
    datamodel_object[:ora_data_model_version] = "2.#{minor_version}"
      
      # Add flat mapping fields, e.g. bibliographic info, publishers
    datamodel_object = map_hyrax_to_hash(self, datamodel_object)

      # Map nested objects, eg. contributors, funders

      # Contributors
    datamodel_object[:contributors] = []
    self.contributors.each do |c|
      c_hash = convert_to_hash(c, [:id, :ora_base_id])
      c_hash[:contributor_record_identifier] = c.id

      c_hash[:roles] = []
      c.roles.each do |r|
        role_hash = convert_to_hash(r, [:id, :contributor_info_id])
        role_hash = remove_blank_values(role_hash)
        if role_hash.present?
          role_hash = sort_hash(role_hash)
          c_hash[:roles] << role_hash
        end
      end
      c[:roles] = c[:roles].sort_by{|r| r[:role_title].to_s}

      c_hash[:contributor_identifiers] = []
      c.schemes.each do |s|
        ci_hash = {}
        ci_hash[:contributor_identifier] = s.contributor_identifier
        ci_hash[:contributor_identifier_scheme] = s.contributor_identifier_scheme
        ci_hash = remove_blank_values(ci_hash)
        if ci_hash.present?
          ci_hash = sort_hash(ci_hash)
          c_hash[:contributor_identifiers] << ci_hash
        end
      end
      c_hash[:contributor_identifiers] = c_hash[:contributor_identifiers].sort_by{|ci| ci[:contributor_identifier_scheme].to_s}

      c_hash = remove_blank_values(c_hash)
      if c_hash.present?
        c_hash = sort_hash(c_hash)
        datamodel_object[:contributors] << c_hash
      end            
    end
    datamodel_object[:contributors] = datamodel_object[:contributors].sort_by{|c| c[:display_name].to_s}

      # Funders
    datamodel_object[:funding] = []
    self.funders.each do |f|
      funder_hash = convert_to_hash(f, [:id, :ora_base_id])
      funder_hash[:funder_grant] = []
      f.grant_information.each do |g|
        grant_hash = convert_to_hash(g, [:id, :funder_info_id])
        grant_hash = remove_blank_values(grant_hash)
        if grant_hash.present?
          grant_hash = sort_hash(grant_hash)
          funder_hash[:funder_grant] << (grant_hash) 
        end
      end
      funder_hash[:funder_grant] = funder_hash[:funder_grant].sort_by{|g| g[:grant_identifier].to_s}
      funder_hash = remove_blank_values(funder_hash)
      if funder_hash.present?
        funder_hash = sort_hash(funder_hash)
        datamodel_object[:funding] << funder_hash
      end
    end
    datamodel_object[:funding] = datamodel_object[:funding].sort_by{|f| f[:funder_name].to_s}

      # Related items
    datamodel_object[:related_items] = []
    self.related_items.each do |ri|
      ri_hash = convert_to_hash(ri, [:id, :ora_base_id])
      ri_hash.delete(:related_item_type_of_relationship)
      ri_hash = remove_blank_values(ri_hash)
      if ri_hash.present?
        ri_hash = sort_hash(ri_hash)
        datamodel_object[:related_items] << ri_hash
      end
    end
    datamodel_object[:related_items] = datamodel_object[:related_items].sort_by{
      |ri| [ri[:related_item_title].to_s, ri[:related_item_url].to_s, ri[:related_item_citation_text]].to_s}

      # Record identifiers (child of item description and embargo info)
    datamodel_object[:record_identifiers] = []
    self&.item_description_and_embargo_information&.first&.record_identifiers.each do |rid|
      rid_hash = convert_to_hash(rid, [:id, :item_desc_and_embargo_info_id])
      rid_hash = remove_blank_values(rid_hash)
      if rid_hash.present?
        rid_hash = sort_hash(rid_hash)
        datamodel_object[:record_identifiers] << rid_hash
      end
    end
    datamodel_object[:record_identifiers] = datamodel_object[:record_identifiers].sort_by{|rid| rid[:record_identifier_scheme].to_s}

      # History information (child of admin info)
    datamodel_object[:history] = []
    if self.admin_information.present?
      self&.admin_information&.first&.history_information.each do |h|
        h_hash = convert_to_hash(h, [:id, :admin_info_id])
        h_hash = remove_blank_values(h_hash)
        if h_hash.present?
          h_hash = sort_hash(h_hash)
          datamodel_object[:history] << h_hash
        end
      end
    end
    datamodel_object[:history] = datamodel_object[:history].sort_by{|h| h[:action_date].to_s}

      # Versions

    datamodel_object[:versions] = []
    self.ora_versions.each do |v|
      v_hash = convert_to_hash(v, [:id, :ora_base_id])
      v_hash = remove_blank_values(v_hash)
      if v_hash.present?
        v_hash = sort_hash(v_hash)
        datamodel_object[:versions] << v_hash
      end
    end
    datamodel_object[:versions] = datamodel_object[:versions].sort_by{
      |v| [v[:version_created_date].to_s, v[:version_identifier_doi].to_s]}


      # Ensure record review priority is extracted from Sipity
    if self.record_review_priority.present?
      datamodel_object[:record_review_priority] = self.record_review_priority
    end

    datamodel_object = remove_blank_values(datamodel_object)
      # ensure datamodel object has an identifier, which can be missing on
      # some hyrax objects
    [:pid, :identifier_uuid].each do | id_field |
      unless datamodel_object[id_field].present?
        datamodel_object[id_field] = self.id
      end
    end

    sort_hash(datamodel_object)        
  end

  def binary_file_metadata_to_datamodel2(fileset, minor_version='3')
    binary_file = convert_to_hash(fileset, unmapped_binary_file_metadata_fields)
    binary_file[:file_admin_hyrax_fileset_identifier] = binary_file[:id]
    binary_file[:file_admin_access_condition_at_deposit] = binary_file[:access_condition_at_deposit]
    binary_file[:file_size] = binary_file[:file_size].to_a.first
    # Strip 'urn:sha1:' prefix from sha1 hash
    binary_file[:file_sha1] = binary_file[:file_sha1].gsub('urn:sha1:', '') if binary_file[:file_sha1].present?
    binary_file.delete(:access_condition_at_deposit)
    # 'id' is not a property on datamodel 2.3 and below
    unless minor_version.to_i > 3
      binary_file.delete(:id)
    end
    binary_file = sort_hash(binary_file)
    # Ensure null values are set to ''
    remove_blank_values(binary_file)
  end

  def binary_file_datamodel2_metadata_to_hyrax(binary_file_hash, source, parent_uuid)
      # Takes a binary file JSON object and returns a metadata set ready for
      # use in the create_and_attach_file_set() method
    binary_file = binary_file_hash.deep_symbolize_keys
      # Allow for non-datamodel structures
    binary_file[:file_size] = [binary_file[:file_size]]
    binary_file[:access_condition_at_deposit] = binary_file[:file_admin_access_condition_at_deposit]
    binary_file[:id] = binary_file[:id].present? ? binary_file[:id] : binary_file[:file_admin_hyrax_fileset_identifier]
    binary_file.delete(:file_admin_access_condition_at_deposit)
    binary_file.delete(:file_admin_hyrax_fileset_identifier)
    binary_file[:file_path] = interpret_binary_file_path_from_source(binary_file_hash, source, parent_uuid)
      # Ensure null values are set to ''
    binary_file.keys.map{|v| binary_file[v] = binary_file[v].present? ? binary_file[v] : ''}
    binary_file.deep_stringify_keys
  end

  def object_datamodel2_metadata_to_hyrax(datamodel_hash, minor_version='3')
      # Originally derived from /app/workers/object_migration_worker
      # Returns an ORA object with metadata mapped, ready to be saved
    datamodel_hash = datamodel_hash.deep_symbolize_keys
    pid = datamodel_hash[:pid] || datamodel_hash[:identifier_uuid] || generate_uuid

    new_work_class_name = data_model_to_hyrax_class_mappings[datamodel_hash[:type_of_work]]

    new_work = new_work_class_name.constantize.new(id: pid, title: [datamodel_hash[:title]])

      #----------------------------------------------------------------------------------
      #                               Details
      #----------------------------------------------------------------------------------
      
    depositor = datamodel_hash[:contributors].select { |c| c[:roles].map {|r| r[:role_title]}.include?('Depositor') }.try(:first)
    depositor_email = get_best_contributor_email(depositor) || 'ora.system@bodleian.ox.ac.uk'

    new_work.alternative_title = datamodel_hash[:alternative_title]
    new_work.abstract = datamodel_hash[:abstract]
    new_work.host_peer_review_status = datamodel_hash[:host_peer_review_status]
    new_work.host_publication_status = datamodel_hash[:host_publication_status]
    new_work.additional_information = datamodel_hash[:additional_information]
    new_work.subject = datamodel_hash[:subject]
    new_work.keyword = datamodel_hash[:keyword]
    new_work.depositor = depositor_email
    new_work.date_uploaded = datamodel_hash[:record_created_date]
    new_work.language = datamodel_hash[:language]

      #----------------------------------------------------------------------------------
      #                             Archiving costs
      #----------------------------------------------------------------------------------
    archiving_costs = new_work.archiving_costs_information.build
    archiving_costs.data_financier = datamodel_hash[:data_financier]
    archiving_costs.data_archiving_fee = datamodel_hash[:data_archiving_fee]
    archiving_costs.data_archiving_fee_details = datamodel_hash[:data_archiving_fee_details]

      #----------------------------------------------------------------------------------
      #                               Contributors
      #----------------------------------------------------------------------------------
    contributors = datamodel_hash[:contributors] || []
    contributors.each do |contributor_info|
      contributor = new_work.contributors.build(id: contributor_info[:contributor_record_identifier])

      roles_json = contributor_info[:roles] || []

      roles_json.each do |every_role|
        role = contributor.roles.build
        role.role_title = every_role[:role_title]
        role.role_order = every_role[:role_order]
        if every_role[:et_al] == "Yes"
          role.et_al = "yes"
        end
      end

      schemes_json = contributor_info[:contributor_identifiers] || []

      schemes_json.each do |each_scheme|
        scheme = contributor.schemes.build
        scheme.contributor_identifier = each_scheme[:contributor_identifier]
        scheme.contributor_identifier_scheme = each_scheme[:contributor_identifier_scheme]
      end

      contributor.contributor_type = contributor_info[:contributor_type]
      contributor.family_name = contributor_info[:family_name]
      contributor.given_names = contributor_info[:given_names]
      contributor.initials = contributor_info[:initials]
      contributor.display_name = contributor_info[:display_name]
      contributor.preferred_family_name = contributor_info[:preferred_family_name]
      contributor.preferred_given_names = contributor_info[:preferred_given_names]
      contributor.preferred_contributor_email = contributor_info[:preferred_contributor_email]
      contributor.contributor_email = contributor_info[:contributor_email]
      contributor.contributor_website_url = contributor_info[:contributor_website_url]
      contributor.orcid_identifier = contributor_info[:orcid_identifier]
      contributor.contributor_record_identifier = contributor_info[:contributor_record_identifier]
      contributor.institutional_identifier = contributor_info[:institutional_identifier]
      contributor.institution = contributor_info[:institution]
      contributor.institution_identifier = contributor_info[:institution_identifier]
      contributor.division = contributor_info[:division]
      contributor.department = contributor_info[:department]
      contributor.sub_department = contributor_info[:sub_department]
      contributor.sub_unit = contributor_info[:sub_unit]
      contributor.research_group = contributor_info[:research_group]
      contributor.oxford_college = contributor_info[:oxford_college]
      contributor.ora3_affiliation = contributor_info[:ora3_affiliation]
    end
        
      # puts "contributors: #{new_work.contributors.size} contributors mapped: #{new_work.contributors.to_json}"
      # Sidekiq.logger.info("#{datamodel_hash[:pid]} contributors: #{new_work.contributors.size} contributors mapped: #{new_work.contributors.to_json}")

      #----------------------------------------------------------------------------------
      #                               bibliographic_information
      #----------------------------------------------------------------------------------
    bibliographic_section = new_work.bibliographic_information.build

    bibliographic_section.paper_number = datamodel_hash[:paper_number]
    bibliographic_section.thesis_degree_institution = datamodel_hash[:thesis_degree_institution]
    bibliographic_section.thesis_degree_name = datamodel_hash[:thesis_degree_name]
    bibliographic_section.thesis_degree_level = datamodel_hash[:thesis_degree_level]
    bibliographic_section.thesis_leave_to_supplicate_date = datamodel_hash[:thesis_leave_to_supplicate_date]
    bibliographic_section.summary_documentation = datamodel_hash[:summary_documentation]
    bibliographic_section.data_coverage_temporal_start_date = datamodel_hash[:data_coverage_temporal_start_date]
    bibliographic_section.data_coverage_temporal_end_date = datamodel_hash[:data_coverage_temporal_end_date]
    bibliographic_section.data_coverage_spatial = datamodel_hash[:data_coverage_spatial]
    bibliographic_section.data_collection_start_date = datamodel_hash[:data_collection_start_date]
    bibliographic_section.data_collection_end_date = datamodel_hash[:data_collection_end_date]
    bibliographic_section.data_format = datamodel_hash[:data_format]
    bibliographic_section.data_digital_storage_location = datamodel_hash[:data_digital_storage_location]
    bibliographic_section.data_digital_data_total_file_size = datamodel_hash[:data_digital_data_total_file_size]
    bibliographic_section.data_digital_data_format = datamodel_hash[:data_digital_data_format]
    bibliographic_section.data_digital_data_version = datamodel_hash[:data_digital_data_version]
    bibliographic_section.data_physical_storage_location = datamodel_hash[:data_physical_storage_location]
    bibliographic_section.data_management_plan_url = datamodel_hash[:data_management_plan_url]
    bibliographic_section.patent_number = datamodel_hash[:patent_number]
    bibliographic_section.patent_publication_number = datamodel_hash[:patent_publication_number]
    bibliographic_section.patent_application_number = datamodel_hash[:patent_application_number]
    bibliographic_section.patent_territory = datamodel_hash[:patent_territory]
    bibliographic_section.patent_filed_date = datamodel_hash[:patent_filed_date]
    bibliographic_section.patent_awarded_date = datamodel_hash[:patent_awarded_date]
    bibliographic_section.patent_status = datamodel_hash[:patent_status]
    bibliographic_section.patent_international_classification = datamodel_hash[:patent_international_classification]
    bibliographic_section.patent_cooperative_classification = datamodel_hash[:patent_cooperative_classification]
    bibliographic_section.patent_european_classification = datamodel_hash[:patent_european_classification]
    bibliographic_section.confidential_report = datamodel_hash[:confidential_report]
    bibliographic_section.commissioning_body = datamodel_hash[:commissioning_body]
    bibliographic_section.physical_form = datamodel_hash[:physical_form]
    bibliographic_section.physical_dimensions = datamodel_hash[:physical_dimensions]
    bibliographic_section.collection_name = datamodel_hash[:collection_name]
    bibliographic_section.manufacturer = datamodel_hash[:manufacturer]
    bibliographic_section.manufacturer_website_url = datamodel_hash[:manufacturer_website_url]
    bibliographic_section.production_date = datamodel_hash[:production_date]
    bibliographic_section.physical_location = datamodel_hash[:physical_location]

    publisher = bibliographic_section.publishers.build

    publisher.publisher_name = datamodel_hash[:publisher_name]
    publisher.publisher_website_url = datamodel_hash[:publisher_website_url]
    publisher.journal_title = datamodel_hash[:journal_title]
    publisher.journal_website_url = datamodel_hash[:journal_website_url]
    publisher.series_title = datamodel_hash[:series_title]
    publisher.series_number = datamodel_hash[:series_number]
    publisher.volume = datamodel_hash[:volume]
    publisher.issue_number = datamodel_hash[:issue_number]
    publisher.article_number = datamodel_hash[:article_number]
    publisher.pagination = datamodel_hash[:pagination]
    publisher.citation_publication_date = datamodel_hash[:citation_publication_date]
    publisher.citation_place_of_publication = datamodel_hash[:citation_place_of_publication]
    publisher.identifier_issn = datamodel_hash[:identifier_issn]
    publisher.identifier_eissn = datamodel_hash[:identifier_eissn]
    publisher.doi_requested = datamodel_hash[:doi_requested]
    # identifier_doi is multivalued in hyrax, so set to an array to be sure
    publisher.identifier_doi = Array(datamodel_hash[:identifier_doi])
    publisher.identifier_pii = datamodel_hash[:identifier_pii]
    publisher.publication_website_url = datamodel_hash[:publication_website_url]
    publisher.acceptance_date = datamodel_hash[:acceptance_date]
    publisher.host_title = datamodel_hash[:host_title]
    publisher.chapter_number = datamodel_hash[:chapter_number]
    publisher.edition = datamodel_hash[:edition]
    publisher.identifier_isbn_10 = datamodel_hash[:identifier_isbn10]
    publisher.identifier_isbn_13 = datamodel_hash[:identifier_isbn13]
    publisher.identifier_eisbn = datamodel_hash[:identifier_eisbn]

    event = bibliographic_section.events.build

    event.event_title = datamodel_hash[:event_title]
    event.event_series_title = datamodel_hash[:event_series_title]
    event.event_location = datamodel_hash[:event_location]
    event.event_website_url = datamodel_hash[:event_website_url]
    event.event_start_date = datamodel_hash[:event_start_date]
    event.event_end_date = datamodel_hash[:event_end_date]

      #----------------------------------------------------------------------------------
      #                                     funders
      #----------------------------------------------------------------------------------

    funders_json = datamodel_hash[:funding] || []
      # funders_array = Array.new

    funders_json.each do |funder_info|
      funder = new_work.funders.build

      funder.funder_name = funder_info[:funder_name]
      funder.funder_identifier = funder_info[:funder_identifier]
      funder.funder_funding_programme = datamodel_hash[:funder_funding_programme]
      funder.funder_compliance_met = funder_info[:funder_compliance_met]

      grants_json = funder_info[:funder_grant] || []

      grants_json.each do |grant_info|
        grant = funder.grant_information.build

        grant.grant_identifier = grant_info[:grant_identifier]
        grant.is_funding_for = grant_info[:is_funding_for]
      end
    end
      #----------------------------------------------------------------------------------
      #                              licence_and_rights_information
      #----------------------------------------------------------------------------------
    licence = new_work.licence_and_rights_information.build

    licence.rights_holders = datamodel_hash[:rights_holders]
    licence.rights_statement = datamodel_hash[:rights_statement]
    licence.rights_copyright_date = datamodel_hash[:rights_copyright_date]
    licence.licence = datamodel_hash[:licence]
    licence.licence_url = datamodel_hash[:licence_url]
    licence.licence_statement = datamodel_hash[:licence_statement]
    licence.licence_start_date = datamodel_hash[:licence_start_date]
    licence.rights_third_party_copyright_material = datamodel_hash[:rights_third_party_copyright_material]
    licence.rights_third_party_copyright_permission_received = datamodel_hash[:rights_third_party_copyright_permission_received]
    licence.deposit_note = datamodel_hash[:deposit_note]

      #----------------------------------------------------------------------------------
      #                              related_items
      #----------------------------------------------------------------------------------
    related_items_json = datamodel_hash[:related_items] || []

    related_items_json.each do |related_item_info|
      related_item = new_work.related_items.build
      related_item.related_item_title = related_item_info[:related_item_title]
      related_item.related_item_url = related_item_info[:related_item_url]
      related_item.related_item_citation_text = related_item_info[:related_item_citation_text]
      related_item.related_data_location = related_item_info[:related_data_location]
      related_item.related_item_type_of_relationship = "Is related to"

    end
        
      #----------------------------------------------------------------------------------
      #                              item_description_and_embargo_information
      #----------------------------------------------------------------------------------

    description = new_work.item_description_and_embargo_information.build

    description.type_of_work = datamodel_hash[:type_of_work]
    description.sub_type_of_work = datamodel_hash[:sub_type_of_work]
    description.identifier_uuid = datamodel_hash[:identifier_uuid]
    description.pid = datamodel_hash[:pid]
    description.tinypid = datamodel_hash[:tinypid]
    description.identifier_pmid = datamodel_hash[:identifier_pmid]
    description.identifier_pubs_identifier = datamodel_hash[:identifier_pubs_identifier]

    description.record_embargo_status = datamodel_hash[:record_embargo_status]
    description.record_embargo_end_date = datamodel_hash[:record_embargo_end_date]
    description.record_embargo_release_method = datamodel_hash[:record_embargo_release_method]
    description.record_embargo_reason = datamodel_hash[:record_embargo_reason]

    record_identifiers_json = datamodel_hash[:record_identifiers] || []

    record_identifiers_json.each do |record_identifier|
      identifier = description.record_identifiers.build
      identifier.record_identifier = record_identifier[:record_identifier]
      identifier.record_identifier_scheme = record_identifier[:record_identifier_scheme]
    end

      #----------------------------------------------------------------------------------
      #                              admin_information
      #----------------------------------------------------------------------------------
    admin_info = new_work.admin_information.build

    admin_info.record_ora_deposit_licence = datamodel_hash[:record_ora_deposit_licence]
    admin_info.symplectic_review_status = datamodel_hash[:symplectic_review_status]
    admin_info.has_public_url = datamodel_hash[:has_public_url]
    admin_info.deposit_in_progress = datamodel_hash[:deposit_in_progress]
    admin_info.record_content_source = datamodel_hash[:record_content_source]
    admin_info.identifier_source_identifier = datamodel_hash[:identifier_source_identifier]
    admin_info.thesis_voluntary_deposit = datamodel_hash[:thesis_voluntary_deposit]
    admin_info.thesis_archive_version_complete = datamodel_hash[:thesis_archive_version_complete]
    admin_info.thesis_student_system_updated = datamodel_hash[:thesis_student_system_updated]
    admin_info.thesis_dispensation_from_consultation_granted = datamodel_hash[:thesis_dispensation_from_consultation_granted]
    admin_info.ref_exception_required = datamodel_hash[:ref_exception_required]
    admin_info.ref_other_exception_note = datamodel_hash[:ref_other_exception_note]
    admin_info.ref_compliant_at_deposit = datamodel_hash[:ref_compliant_at_deposit]
    admin_info.ref_compliant_availability = datamodel_hash[:ref_compliant_availability]
    admin_info.rights_retention_statement_included = datamodel_hash[:rights_retention_statement_included]
    admin_info.record_created_date = datamodel_hash[:record_created_date]
    admin_info.record_deposit_date = datamodel_hash[:record_deposit_date]
    admin_info.record_first_reviewed_by = datamodel_hash[:record_first_reviewed_by]
    admin_info.record_version = datamodel_hash[:record_version]
    admin_info.record_publication_date = datamodel_hash[:record_publication_date]
    admin_info.record_check_back_date = datamodel_hash[:record_check_back_date]
    admin_info.record_review_status = datamodel_hash[:record_review_status]
    admin_info.record_review_status_other = datamodel_hash[:record_review_status_other]
    admin_info.record_requires_review = datamodel_hash[:record_requires_review]
    admin_info.identifier_tombstone_record_identifier = datamodel_hash[:identifier_tombstone_record_identifier]
    admin_info.record_first_reviewed_time = datamodel_hash[:record_first_reviewed_time]
    admin_info.record_subsequent_review_time = datamodel_hash[:record_subsequent_review_time]
    admin_info.admin_incorrect_version_deposited = datamodel_hash[:admin_incorrect_version_deposited]
    admin_info.depositor_contacted = datamodel_hash[:depositor_contacted]
    admin_info.depositor_contact_email_template = datamodel_hash[:depositor_contact_email_template]
    admin_info.rt_ticket_number = datamodel_hash[:rt_ticket_number]
    admin_info.record_accept_updates = datamodel_hash[:record_accept_updates]
    admin_info.ora_data_model_version = datamodel_hash[:ora_data_model_version]
    admin_info.apc_admin_apc_requested = datamodel_hash[:apc_admin_apc_requested]
    admin_info.apc_admin_apc_review_status = datamodel_hash[:apc_admin_apc_review_status]
    admin_info.apc_admin_apc_spreadsheet_identifier = datamodel_hash[:apc_admin_apc_spreadsheet_identifier]
    admin_info.apc_admin_apc_number = datamodel_hash[:apc_admin_apc_number]
    admin_info.pre_counter_downloads = datamodel_hash[:pre_counter_downloads]
    admin_info.pre_counter_views = datamodel_hash[:pre_counter_views]
    admin_info.admin_notes = datamodel_hash[:admin_notes ]
      # value is probably not being sent, so set a default value
    admin_info.ora_collection = datamodel_hash.fetch(:ora_collection, '')

    history_information_json = datamodel_hash[:history] || []

    history_information_json.each do |history_info|
      history_information = admin_info.history_information.build
      history_information.action_category = history_info[:action_category]
      history_information.action_date = history_info[:action_date]
      history_information.action_description = history_info[:action_description]
      history_information.action_responsibility = history_info[:action_responsibility]
      history_information.action_duration = history_info[:action_duration]
      history_information.action_comment = history_info[:action_comment]
      history_information.automatically_updated_fields = history_info[:automatically_updated_fields]
    end

      #----------------------------------------------------------------------------------
      #                              versions
      #----------------------------------------------------------------------------------

    versions_json = datamodel_hash[:versions] || []

    versions_json.each do |version_info|
      version = new_work.ora_versions.build
      version.version_title = version_info[:version_title]
      version.version_created_date = version_info[:version_created_date]
      version.version_identifier_doi = version_info[:version_identifier_doi]
      version.version_public_note = version_info[:version_public_note]
    end



      # Set default last modified date
      #
      # note: using to_date.to_datetime sets the time back to midnight
    last_modified = DateTime.now.to_date.to_datetime
    if datamodel_hash[:history].present?
      last_modified_action = datamodel_hash[:history].sort_by { |h| h[:action_date].to_s }.last
      if last_modified_action.present? && last_modified_action[:action_date].present?
        last_modified = last_modified_action[:action_date].to_datetime
      end
    end

    new_work.date_modified = last_modified

      # puts 'completed mapping fields'
    Sidekiq.logger.info("#{datamodel_hash[:pid]} mapped JSON to ORA Object")
    new_work
  end
end
