# Tom Wrobel, Open Repositories 2024 Code

Code released as part of Tom Wrobel's presentation to Open Repositories 2024, discussing the use of [Fedora6](https://fedora.lyrasis.org/) as a lightweight, turnkey, open source API layer over an OCFL Data Preservation Service.

The files in this repository were taken from ORA's (Oxford Research Archive, https://ora.ox.ac.uk) repository application based on Samvera Hyrax. The repository code is known internally as ora4.reviewinterface, and is a Ruby on Rails application built upon the [Samvera Hyrax](https://hyrax.samvera.org/) platform (v2.5.1). It cannot be released as open source in time for Open Repositories 2024 due to potential hard set configuration values in the code. The attached code shows how ORA content is sent to and from the ORA DPS (Data Preservation Service), and how it integrates with existing workflows. ora4.reviewinterface is available on request under MIT licence to trusted partners, but the code here - alongside ORA's [Fedora6.Client Ruby on Rails gem](https://github.com/bodleian/fedora6_client) - should provide enough information for Samvera implementers looking to adopt a similar solution.

For guidance on ORA's data model, see https://github.com/bodleian/ora_data_model and http://dx.doi.org/10.5287/bodleian:pr22x1bjE​

To understand how ORA stores its content in OCFL / the ORA DPS: https://github.com/bodleian/ora_ocfl ​

ORA's Fedora6 load testing code: https://github.com/bodleian/fedora6-load-testing  

## Files

### app/models/concerns/data_model_object.rb

Model code that converts an ORA Hyrax object to and from a JSON serialisation. It will also import an object from a JSON representation - so long as that JSON representation has links to binary file storage locations.

### app/services/hyrax/workflow/save_to_dps.rb

The action called by the Hyrax Workflow called in the ORA workflow configuration file. This pushes the object to the DPS worker for saving in the DPS.

### app/workers/dps_worker.rb

A sidekiq worker to send ORA objects to the DPS

### config/workflows/ora_workflow.json

This is ORA's workflow configuration file, used by Hyrax to perform automated actions when moving an object between workflow states. The majority of DPS saves take place because of a workflow action, e.g. on ingest, publication, unpublication, rather than human interaction.

### lib/ora/dps.rb

This is the code that acts as an interface between the ORA Hyrax system and Fedora6.Client, and is the code envoked by the DPS Worker. It is responsible for loading ORA object from Hyrax, deciding what parts of those objects need to be preserved, and preserving them in the ORA DPS. To save an ORA object in the DPS in the console, the code is as simple as:

```ruby
dps = ORA::DPS.new
dps.save(object_identifier)
```
