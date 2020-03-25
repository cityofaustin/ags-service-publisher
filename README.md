# ArcGIS Server Service Publisher

**Note:** See the [`ags-service-publisher-gui`](https://github.com/cityofaustin/ags-service-publisher-gui) repository if you are looking for a simple, user-friendly GUI application for working with AGS services! This repository contains the library code that the GUI application depends on.

## Overview

The primary purpose of this tool is to automate the publishing of MXD files to Map Services on ArcGIS Server, using
[YAML][1] configuration files to define the service folders, environments, services, 
[service properties](#service-properties), [data source mappings](#data-source-mappings) and more. Publishing geocoding services is also
supported, with some limitations (e.g. no data source mapping).

Additional features include [cleaning up](#clean-up-services) outdated services and
[generating reports](#generate-reports) about existing services and the datasets they reference on ArcGIS Server.

By default, configuration files are looked for in the `./configs` subdirectory, reports are written to `./reports`, and logs are written to `./logs`. See the [Tips](#tips) section for details on overriding these default locations.

You create one configuration file per service folder -- each service folder can contain many services.

You must also create a [`userconfig.yml`](#userconfigyml) file specifying your environments and the properties for each
of your ArcGIS Server instances.

## Requirements

- Windows 7+
- ArcGIS Desktop 10.3+
- Python 2.7+
- [pip][2]
- Various Python libraries (will be installed by pip as described in the [Installation](#installation) section):
    - [PyYAML][3] 3.12
    - [requests][4] 2.13

## Installation

1. Clone this repository to a local directory
2. Add Python scripts directory to your PATH environment variable.  This is typically located at C:\Python(version)\ArcGIS(version)\Scripts 
3. Open a Windows command prompt in the local directory
4. Type `pip install -e .`

## Configuration

1. Create a folder named `configs` in the local directory, or, alternatively, set the `AGS_SERVICE_PUBLISHER_CONFIG_DIR`
    environment variable to a directory of your choosing, as described in the [Tips](#tips) section.
2. Create a file named [`userconfig.yml`](#userconfigyml) in the aforementioned configuration folder, and populate it
    with a top-level `environments` key containing one key for each of your environments, e.g. `dev`, `test`, and
    `prod`.

    Within each environment, specify the following keys:
    - `ags_instances`: contains a mapping of ArcGIS Server instance names, each having the following properties:
        - `url`: Base URL (scheme and hostname) of your ArcGIS Server instance
        - `ags_connection`: Path to an `.ags` connection file for each instance.
        - `token` (optional): [ArcGIS Admin REST API token][5] (see the ["Generate tokens"](#generate-tokens) section  below for more details)
        - `site_mode` (optional): If specified, determines what [site mode][11] to set the site to after publishing. If not specified, the site mode is not checked or changed. May be one of the following values:
          - `editable`: Sets the site mode to editable before and after publishing.
          - `read_only`: Sets the site mode to editable before publishing and read-only after publishing.
          - `initial`: Sets the site mode to editable before publishing and restores it to the initial site mode after publishing.
        
          **Note:** Specifying a `site_mode` requires a valid `token` to be set for a user with Administrator privileges on the site.
        - `proxies` (optional): If specified, uses a proxy to connect to the ArcGIS Server instance. See the [Python Requests][12] documentation for details. Overrides any values set by the top-level `proxies` key.
    - `sde_connnections_dir` (optional): path to a directory containing any SDE connection files you want to
        [import](#import-sde-connection-files) to each of the instances in that environment

    You may also optionally create a top-level `proxies` key to specify any proxy servers you need to use to connect to ArcGIS Server. See the [Python Requests][12] documentation for details. May be overriden by the `proxies` key of individual ArcGIS Server instances`.
3. Create additional configuration files for each service folder you want to publish. Configuration files must have a
    `.yml` extension.
    1. Create a top-level `service_folder` key with the name of the service folder as its value.
    2. Create a top-level `services` key with a list of service names to publish, with each service name preceded by a
        hyphen (`-`) and a space.
    3. Create a top-level `environments` key containing one key for each of your environments, e.g. `dev`, `test`, and
        `prod`.
    4. Within each environment, specify the following keys:
        - `ags_instances`: List of ArcGIS Server instances (as defined in [`userconfig.yml`](#userconfigyml)) to publish
            to.
        - `data_source_mappings` (optional): See [data source mappings](#data-source-mappings)
        - `source_dir`: Directory containing the source files (MXDs, locator files, etc.) to publish.
        - `staging_dir` (optional): Directory containing staging files to copy into `source_dir` prior to mapping data
            sources and publishing.
            - Can also be a list of multiple staging directories. Each service may only have one corresponding staging
                file among all of the staging directories. Duplicates will result in a validation error.
    5. (Optional) Set [service properties](#service-properties).

### Example configuration files

#### `CouncilDistrictMap.yml`:

``` yml
service_folder: CouncilDistrictMap
services:
  - CouncilDistrictMap
  - CouncilDistrictsFill:
      instances_per_container: 4 # example of specifying a service-level property; note the level of indentation
default_service_properties:
  isolation: low
  instances_per_container: 8
  cache_dir: D:\arcgisserver\directories\arcgiscache
environments:
  dev:
    ags_instances:
      - coagisd1
      - coagisd2
    source_dir: \\coacd.org\gis\AGS\Config\AgsEntDev\mxd-source\CouncilDistrictMap
    service_properties: # example of specifying environment-level properties
      isolation: high
      instances_per_container: 1
    data_source_mappings:
      # Example of mapping by source workspace path, using a simple mapping; note that when using a wildcard in the key it must be wrapped in quotes due to the special character
      '*\gisDmDev (LPUGH).sde': \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections\gisDmDev (COUNCILDISTRICTMAP_SERVICE).sde
      '*\gisDmDev (LPUGH) external.sde': \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections\gisDmDev (COUNCILDISTRICTMAP_SERVICE) external.sde
  test:
    ags_instances:
      - coagist1
      - coagist2
    data_source_mappings:
      # Example of mapping by specific connection properties, using a list of mappings and source/target keys
      # Note that order is significant; we list our more specific mapping first so that it short-circuits the evalution of subsequent mappings
      - source:
          database: gisdmdev
          version: sde_external
        target: \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE) external.sde
      # Example of mapping by source database name, using a list of mappings and a simple mapping
      - gisdmdev: \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE).sde
    source_dir: \\coacd.org\gis\AGS\Config\AgsEntTest\mxd-source\CouncilDistrictMap
    staging_dir: \\coacd.org\gis\AGS\Config\AgsEntDev\mxd-source\CouncilDistrictMap
  prod:
    ags_instances:
      - coagisp1
      - coagisp2
    data_source_mappings:
      - source:
          database: gisdm* # Example of using a wildcard in the value (no quotes necessary)
          version: sde_external
        target: \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections\gisDm (COUNCILDISTRICTMAP_SERVICE) external.sde
      # Example of using a wildcard in the key (must be wrapped in quotes due to the special character)
      - 'gisdm*': \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections\gisDm (COUNCILDISTRICTMAP_SERVICE).sde
    source_dir: \\coacd.org\gis\AGS\Config\AgsEntProd\mxd-source\CouncilDistrictMap
    staging_dir: \\coacd.org\gis\AGS\Config\AgsEntTest\mxd-source\CouncilDistrictMap
```

#### `userconfig.yml`:

``` yml
environments:
  dev:
    ags_instances:
      coagisd1:
        url: http://coagisd1.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd1-pughl (admin).ags
        site_mode: initial # Enable support for publishing to read-only mode site, restores site mode to its initial value after publishing
      coagisd2:
        url: http://coagisd2.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd2-pughl (admin).ags
        proxies: # Instance-specific proxy settings
          http: proxy-example.com:4567
    sde_connections_dir: \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections
  test:
    ags_instances:
      coagist1:
        url: http://coagist1.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagist1-pughl (admin).ags
        site_mode: editable # Enable support for publishing to read-only mode site, sets site mode to editable after publishing
      coagist2:
        url: http://coagist2.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagist2-pughl (admin).ags
    sde_connections_dir: \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections
  prod:
    ags_instances:
      coagisp1:
        url: http://coagisp1.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisp1-pughl (admin).ags
        site_mode: read_only # Enable support for publishing to read-only mode site, sets site mode to read-only after publishing
      coagisp2:
        url: http://coagisp2.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisp2-pughl (admin).ags
    sde_connections_dir: \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections
proxies: # Top-level proxy settings
  http: proxy-example.com:1234
```

## Example usage

### Publish services

- Publish the `dev` environment in the [`CouncilDistrictMap.yml`](#councildistrictmapyml) configuration file:
    
    ```
    python -c "from ags_service_publisher import Runner; Runner().run_batch_publishing_job(['CouncilDistrictMap'], included_envs=['dev'])"
    ```

- Same as above, but publish all **except** for the `dev` environment (e.g. `test` and `prod`) using `excluded_envs`:
    
    ```
    python -c "from ags_service_publisher import Runner; Runner().run_batch_publishing_job(['CouncilDistrictMap'], excluded_envs=['dev'])"
    ```

- Publish all of the environments in the [`CouncilDistrictMap.yml`](#councildistrictmapyml) configuration file, but
    **only** publish the `CouncilDistrictsFill` service:
    
    ```
    python -c "from ags_service_publisher import Runner; Runner().run_batch_publishing_job(['CouncilDistrictMap'], included_services=['CouncilDistrictsFill'])"
    ```

- Publish the `dev` environment in the [`CouncilDistrictMap.yml`](#councildistrictmapyml) configuration file, adding a
    "`_temp`" suffix to the published service names:
    
    ```
    python -c "from ags_service_publisher import Runner; Runner().run_batch_publishing_job(['CouncilDistrictMap'], included_envs=['dev'], service_suffix='_temp')"
    ```
    
    - **Note:** Similarly, a prefix can also be specified using `service_prefix`.

#### Additional arguments

- `create_backups`: By default, backups are created when publishing MapServer and GeocodeServer services.

    A `Backups` subdirectory is created in the same directory as the source file(s), and a copy of the services to be published are placed there with a timestamp appended.
    
    To disable creating backups, pass the `create_backups=False` argument.
- `update_timestamps`: By default, when a service is successfully published, the `summary` field of the service is updated with a message including the publisher's username and the date and time of publishing.

    This requires a valid [ArcGIS Admin REST API token][5] be set for each ArcGIS Server instance being published to (see the ["Generate tokens"](#generate-tokens) section  below for more details).
    
    To disable updating timestamps, pass the `update_timestamps=False` argument.

### Clean up services

- Clean up (remove) any existing services in the `CouncilDistrictMap` service folder that have not been defined in the
    `CouncilDistrictMap.yml` configuration file:
    
    ```
    python -c "from ags_service_publisher import Runner; Runner().run_batch_cleanup_job(['CouncilDistrictMap'])"
    ```

**Note:** To clean up services, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens) for each ArcGIS
    Server instance defined in [`userconfig.yml`](#userconfigyml).

### Generate reports

#### MXD Data Sources report

This report type inspects map document (MXD) files corresponding to services defined in YAML configuration files and
reports which layers are present in each MXD as well as information about each layer's data source (workspace path,
database, user, version, SQL where clause, etc.).

Useful for determining what data sources are present in an MXD prior to publishing it, so that you
can specify [data source mappings](#data-source-mappings), register data sources with ArcGIS Server, or look for potential problems with SQL
where clauses.

##### Examples:

- Generate a report in CSV format of all the layers and data sources in each staging and source MXD corresponding to
    each service defined in the [`CouncilDistrictMap.yml`](#councildistrictmapyml) configuration file:
    
    ```
    python -c "from ags_service_publisher import Runner; Runner().run_mxd_data_sources_report(included_configs=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap-MXD-Report.csv')"
    ```

- Same as above, but exclude staging MXDs (MXDs located within the `staging_dir`) from the report:
    
    ```
    python -c "from ags_service_publisher import Runner; Runner().run_mxd_data_sources_report(included_configs=['CouncilDistrictMap'], include_staging_mxds=False, output_filename='../ags-service-reports/CouncilDistrictMap-MXD-Report-no-staging.csv')"
    ```

#### Dataset Usages report

This report type inspects services on ArcGIS Server and reports which datasets (feature classes, tables,
etc.) are referenced by each service.

Useful for determining which services would be impacted by a change to one or more
particular datasets.

##### Examples:

- Generate a report in CSV format of all the datasets referenced by all services within the `CouncilDistrictMap`
    service folder on on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_dataset_usages_report(included_service_folders=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap-Dataset-Usages-Report.csv')"
    ```

- Generate a report in CSV format of all the usages of a dataset named `BOUNDARIES.single_member_districts` within all
    services on the `coagisd1` ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_dataset_usages_report(included_datasets=['BOUNDARIES.single_member_districts'], included_instances=['coagisd1'], output_filename='../ags_service_reports/single_member_districts-Dataset-Usages-Report.csv')"
    ```

**Note:** To generate Dataset Usage reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens)
    for each ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml).


#### Data Stores Report

This report type lists all of the data stores registered on ArcGIS Server.

Useful for determining which data stores and database connections are available for services to be published to ArcGIS Server.

##### Examples:

- Generate a report in CSV format of the data stores registered on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_data_stores_report(output_filename='../ags-service-reports/Data-Stores-Report.csv')"
    ```

- Generate a report in CSV format of the data stores registered on the `coagisd1` ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml)

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_data_stores_report(included_instances=['coagisd1'], output_filename='../ags-service-reports/Data-Stores-Report_coagisd1.csv')"
    ```

**Note:** To generate Data Stores reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens)
    for each ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml).


#### Service Health Report

This report type checks the health of services on ArcGIS Server and reports whether each service is started or stopped.

Additionally, for MapServer and GeocodeServer services, a query is run against each service and information about the
results, including response time and any error messages, are added to the report.

Useful for determining which services are stopped, running slowly, or returning errors.

**Note:**  The `warn_on_errors` argument can be set to `True` (i.e. `warn_on_errors=True`) when running this and many other functions of AGS Service Publisher.  It is particularly helpful to set warn_on_errors to true when running the Service Health Report, as the script will halt if it encounters an error when processing any one service. 

##### Examples:

- Generate a report in CSV format of the health status of all the services within the `CouncilDistrictMap` service
    folder on on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_health_report(included_service_folders=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap-Service-Health-Report.csv')"
    ```

- Generate a report in CSV format of the health status of all services on the `coagisd1` ArcGIS Server instance defined
    in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_health_report(included_instances=['coagisd1'], output_filename='../ags_service_reports/coagisd1-Service_Health-Report.csv')"
    ```

**Note:** To generate Service Health reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens)
    for each ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml).

#### Service Analysis Report

This report type queries ArcGIS Server for a list of MapServer or GeocodeServer services, finds the source file (MXD or locator) used to publish that service, and then runs the [Analyze][9] step of service publishing against that file, reporting any issues it finds. You can look up the [error codes][10] in the ArcGIS Server Help for more information about them.

Useful for determining possible performance or other issues with published services.

**Note:**  The `warn_on_errors` argument can be set to `True` (i.e. `warn_on_errors=True`) when running this and many other functions of AGS Service Publisher.  It is particularly helpful to set warn_on_errors to true when running the Service Analysis Report, as the script will halt if it encounters an error when processing any one service. 

##### Examples:

- Generate a report in CSV format of the analysis results of all the services within the `CouncilDistrictMap` service
    folder on on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_analysis_report(included_service_folders=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap-Service-Analysis-Report.csv')"
    ```

- Generate a report in CSV format of the analysis of all services on the `coagisd1` ArcGIS Server instance defined
    in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_analysis_report(included_instances=['coagisd1'], output_filename='../ags_service_reports/coagisd1-Service_Analysis-Report.csv')"
    ```

**Note:** To generate Service Analysis reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens)
    for each ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml).

#### Service Comparison Report

This report type compares a list of services across two AGS instances and reports which services are present on one but not the other.

A warning will be given if more or less than two AGS instances are matched by the input filters.

Services are grouped by environment and AGS instance names, then matched by service folder, service name and service type.

Matches are made case-sensitively by default; set the optional keyword argument `case_insensitive` to `True` to override this

##### Examples:

- Generate a report in CSV format of the missing services between the `coagisd1` and `coagisd2` AGS instances specified within the `dev` environment defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_comparison_report(included_envs=['dev'], included_instances=['coagisd1', 'coagisd2'], output_filename='../ags-service-reports/Service_Comparison_Report_coagisd1_coagisd2.csv')"
    ```

- Same as above but match case-insensitively:

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_comparison_report(included_envs=['dev'], included_instances=['coagisd1', 'coagisd2'], case_insensitive=True, output_filename='../ags-service-reports/Service_Comparison_Report_coagisd1_coagisd2.csv')"
    ```

**Note:** To generate Service Comparison reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens)
    for each ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml).

#### Service Layer Fields Report

This report type queries ArcGIS Server for a list of MapServer services, finds the source MXD used to publish each service, and for each layer in the MXD, reports information about its data source, labels, symbology, fields and indexes.

Useful for determining which fields are being used by a service and whether they are indexed or should be indexed.

The report will output one record for each field in a given service layer, showing whether it has labeling enabled, its symbology type, the field name, field type, whether the field is indexed or should be indexed based on various criteria such as being referenced in the definition query, label classes, or symbology. Shape fields without a spatial index will be indicated as needing an index.

**Note:**  The `warn_on_errors` argument can be set to `True` (i.e. `warn_on_errors=True`) when running this and many other functions of AGS Service Publisher. An error can occur while running this report if the MXD referenced by a service is not accessible. If this occurs, setting `warn_on_errors` to `True` will cause the script to report the error and continue with the rest of the services.

##### Examples:

- Generate a report in CSV format of the layers and fields of all the services within the `CouncilDistrictMap` service
    folder on on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_layer_fields_report(included_service_folders=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap-Service-Layer-Fields-Report.csv')"
    ```

- Generate a report in CSV format of the layers and fields of all services on the `coagisd1` ArcGIS Server instance defined
    in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_service_layer_fields_report(included_instances=['coagisd1'], output_filename='../ags_service_reports/coagisd1-Service-Layer-Fields-Report.csv')"
    ```

**Note:** To generate Service Layer Field reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens)
    for each ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml).

#### Dataset Geometry Statistics Report

This report type queries ArcGIS Server for the datasets used by each service, and for each dataset, reports information about its data source, shape type, feature count, average part count, and average vertex count.

Useful for determining the size and geometric complexity of the datasets being used by services.

##### Examples:

- Generate a report in CSV format of the dataset geometry statistics of all the services within the `CouncilDistrictMap` service folder on on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_dataset_geometry_statistics_report(included_service_folders=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap-Service-Layer-Fields-Report.csv')"
    ```

- Generate a report in CSV format of the dataset geometry statistics of all services on the `coagisd1` ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().run_dataset_geometry_statistics_report(included_instances=['coagisd1'], output_filename='../ags_service_reports/coagisd1-Service-Layer-Fields-Report.csv')"
    ```

**Note:** To generate Dataset Geometry Statistics reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens) for each ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml).

### Generate tokens

- Generate an [ArcGIS Admin REST API token][5] for each ArcGIS Server instance defined in
   [`userconfig.yml`](#userconfigyml) that expires in 30 days:
   
   ```
   python -c "from ags_service_publisher import Runner; Runner().generate_tokens(reuse_credentials=True, expiration=43200)"
   ```
   
   **Notes:**
    - This will prompt you for your credentials (ArcGIS Server username and password) unless the `username` and
        `password` arguments are specified, in which case the same credentials are used for each instance.
    - The `reuse_credentials` argument, if set to `True`, **and** if the `username` and `password` arguments are not
        specified, will only prompt you once and use the same credentials for each instance. Otherwise you will be
        prompted for each instance. Defaults to `False`.
    - The `expiration` argument is the duration in minutes for which the token is valid. Defaults to `15`.
    - You can limit which ArcGIS Server instances are used with the `included_instances` and `excluded_instances`
        arguments.
    - This will automatically update [`userconfig.yml`](#userconfigyml) with the generated tokens.

### Import SDE connection files

- Import all SDE connection files whose name contains `COUNCILDISTRICTMAP_SERVICE` to each of the ArcGIS Server
    instances in the `dev` environment specified within [`userconfig.yml`](#userconfigyml):

    ```
    python -c "from ags_service_publisher import Runner; Runner().batch_import_connection_files(['*COUNCILDISTRICTMAP_SERVICE*'], included_envs=['dev'])"
    ```
    
    **Note:** This looks for `.sde` files located within the directory specified by `sde_connections_dir` for each
    environment specified within [`userconfig.yml`](#userconfigyml).

## Service properties

Service properties are settings that change how a service is defined in the [Service Draft (`.sddraft`)][6] file
prior to being published to ArcGIS Server. Examples of service properties include isolation level, number of
instances per container, cache directory, etc.

To specify service properties, create key/value pairs for the properties to set and the values to set them to.

**Tip:** Keys are matched to service property names case-insensitively, and any underscores are stripped so that
you can use `snake_case` to specify them; for example `instances_per_container` will match the
`InstancesPerContainer` property.

Additionally, the following "special" service properties are recognized:

- `service_type`: The type of service to publish. Defaults to `MapServer`. Currently the only supported
    types are below:
    - `MapServer`
    - `GeocodeServer`
- `copy_data_to_server`: Whether to copy data used by services to the server
- `replace_service`: If set to `True`, specifies that any existing service is to be replaced. This can be
    useful to enable if you find duplicate services with a timestamp suffix are being created on the server.
- `rebuild_locators`: Whether to rebuild locators before publishing them (only applies to `GeocodeServer`
    services).
- `calling_context`: Sets the `CallingContext` property under the `StagingSettings` property set.
    Setting this to `0` may resolve some errors with the `StageService` tool.
- `tile_scheme_file`: Path to a tile scheme file in XML format as created by the
    [Generate Map Server Cache Tiling Scheme][7] geoprocessing tool. Used for specifying the tile scheme of
    cached map services.
- `cache_tile_format`: Format for cached tile images, may be one of the following: `PNG`, `PNG8`, `PNG24`,
    `PNG32`, `JPEG`, `MIXED`, `LERC`
- `compression_quality`: Compression quality for cached tile images, may be a number from `0` to `100`
- `keep_existing_cache`: Specifies that any existing cache is to be preserved, rather than overwritten.
- `feature_access`: A set of key/value pairs specifying the following feature service-related properties:
    - `enabled`: Whether to enable feature access
    - `capabilities`: A list of capabilities to enable on the feature service. Can be one or more of the
    following:
        - `query`
        - `create`
        - `update`
        - `delete`
        - `uploads`
        - `editing`

Service properties may be set at multiple different "levels", allowing you to define properties applicable to all services, specific environments, or specific services.

1. Service folder level:
    - Create a top-level `default_service_properties` key and then specify the service properties as above.
2. Environment level:
    - Within the top-level `environments` key, for each environment you want to set properties for, create a
    `service_properties` key and then specify the service properties as above.
3. Service level:
    - Within the top-level `services` key, for each service you want to set properties for, end the service
        name with a colon (`:`) to denote that it is a mapping object, and then specify the service
        properties as above.
                            
        Ensure you indent the service properties by exactly 4 spaces relative to the hyphen (`-`) before the
        service name.

**Note:** Service properties are applied in the order given above, e.g. service folder, then environment, then
service-level. So when the same property is specified at a subsequent level, it overrides the value of the
previous level.

See the [example configuration files](#example-configuration-files) section for more details.

## Data source mappings

When publishing services it is often necessary to change which data sources are referenced by the layers in the map being published.

This is supported by the `data_source_mappings` key, specified within each environment of a service folder configuration file.

**Note:** Data source mappings are supported by `MapServer` services, but not `GeocodeServer` services.

  - Can be specified as either a mapping (simpler) or a list of mappings (more flexible)
  - If a mapping is provided, the keys are the source database names or workspace paths to match on, and the values are paths to the target workspace paths to map to
  - If a list of mappings is provided, you can use either of the following forms for each mapping:
    - A mapping, as above, whose keys are the source database names or workspace paths to match on, and the values are paths to the target workspace paths to map to
    - A mapping with `source` and `target` keys:
        - `source` is a mapping of connection property names and values to match on. All of the specified criteria must match for the mapping to be applied.
            - Supported connection properties:
                - `layer_name`
                - `dataset_name`
                - `user`
                - `database`
                - `version`
                - `workspace_path`
        - `target` is the target workspace path (e.g. SDE connection file or file geodatabase) to map to
  - [`fnmatch`][8]-style wildcards are supported, but note that in YAML you must enclose keys in single quotes if they contain a non-alphanumeric character.

See the [example configuration files](#example-configuration-files) section for more details.

## Tips

- You can use [`fnmatch`][8]-style wildcards in any of the strings in the list arguments to the runner functions, so,
    for example, you could put `included_services=['CouncilDistrict*']` and both the `CouncilDistrictMap` and
    `CouncilDistrictsFill` services would be published.
- The `Runner` constructor accepts several optional keyword arguments:
    - `verbose`: if set to `True`, will output more granular information to the console to help troubleshoot issues.
      Defaults to `False`.
    - `quiet`: if set to `True`, will suppress all output except for critical errors. Defaults to `False`.
    - `config_dir`: allows you to override which directory is used for your configuration files. Defaults to the
      `./configs` directory beneath the script's root directory. Alternatively, you can set the
      `AGS_SERVICE_PUBLISHER_CONFIG_DIR` environment variable to your desired directory.
    - `log_dir`: allows you to override which directory is used for storing log files. Defaults to the `./logs`
        directory beneath the script's root directory. Alternatively, you can set the `AGS_SERVICE_PUBLISHER_LOG_DIR`
        environment variable to your desired directory.
    - `report_dir`: allows you to override which directory is used for writing reports. Default to the `./reports` directory beneath the script's root directory. Alternatively, you can set the `AGS_SERVICE_PUBLISHER_REPORT_DIR` environment variable to your desired directory.
      - Note that if the `output_filename` parameter is specified to the reporter function, it will take precedence over the `report_dir` value, unless the `output_filename` value does not include a path component, in which case the report will be placed in the `report_dir` directory and be given the `output_filename`. If no `output_filename` value is provided, one will be automatically generated based on the report type and the current date.

## TODO

- Create a nicer command line interface
- Support other types of services
- Probably lots of other stuff

## License

As a work of the City of Austin, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).

[1]: https://en.wikipedia.org/wiki/YAML
[2]: https://pip.pypa.io/en/stable/installing/
[3]: https://pypi.python.org/pypi/PyYAML
[4]: http://docs.python-requests.org/en/master/
[5]: http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/API_Security/02r3000001z7000000/
[6]: http://desktop.arcgis.com/en/arcmap/latest/analyze/arcpy-mapping/createmapsddraft.htm
[7]: http://desktop.arcgis.com/en/arcmap/latest/tools/server-toolbox/generate-map-server-cache-tiling-scheme.htm
[8]: https://docs.python.org/2/library/fnmatch.html
[9]: http://server.arcgis.com/en/server/latest/publish-services/windows/analyzing-your-gis-resource.htm
[10]: http://server.arcgis.com/en/server/latest/publish-services/windows/00001-data-frame-does-not-have-layers.htm
[11]: http://enterprise.arcgis.com/en/server/latest/administer/windows/about-arcgis-server-site-mode.htm
[12]: http://docs.python-requests.org/en/master/user/advanced/#proxies
