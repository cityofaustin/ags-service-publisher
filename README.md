# ArcGIS Server Service Publisher

**Note: This is a work in progress!**

## Overview

The primary purpose of this tool is to automate the publishing of MXD files to Map Services on ArcGIS Server, using
[YAML][1] configuration files to define the service folders, environments, services, service properties, data source
mappings and more. Publishing geocoding services is also supported, with some limitations (e.g. no data source mapping).

Additional features include [cleaning up](#clean-up-services) outdated services and
[generating reports](#generate-reports) about existing services and the datasets they reference on ArcGIS Server.

By default, configuration files are looked for in the `./config` subdirectory, and logs are written to `./logs`.

You create one configuration file per service folder -- each service folder can contain many services.

You must also create a [`userconfig.yml`](#userconfigyml) file specifying your environments and the properties for each
of your ArcGIS Server instances.

## Requirements

  - Windows 7+
  - ArcGIS Desktop 10.3+
  - Python 2.7+
  - [pip][2]
  - [PyYAML][3] 3.11 (will be installed by pip as described in the Setup Instructions)

## Setup instructions

  1. Clone this repository to a local directory
  2. Open a Windows command prompt in the local directory
  3. Type `pip install -r requirements.txt`
  4. Create a folder named `config` in the local directory
  5. Create a file named [`userconfig.yml`](#userconfigyml) in the `config` folder, and populate it with a top-level
     `environments` key containing one key for each of your environments, e.g. `dev`, `test`, and `prod`.

     Within each environment, specify the following keys:
       - `ags_instances`: contains a mapping of ArcGIS Server instance names, each having the following properties:
         - `url`: Base URL (scheme and hostname) of your ArcGIS Server instance
         - `ags_connection`: Path to an `.ags` connection file for each instance.
         - `token` (optional): [ArcGIS Admin REST API token][4] (see the ["Generate tokens"](#generate-tokens) section below
            for more details)
       - `sde_connnections_dir` (optional): path to a directory containing any SDE connection files you want to
         [import](#import-sde-connection-files) to each of the instances in that environment
  6. Create additional configuration files for each service folder you want to publish. Configuration files must have a
     `.yml` extension.
     - Create a top-level `service_folder` key with the name of the service folder as its value.
     - Create a top-level `services` key with a list of service names to publish, with each service name preceded by a hyphen
       (`-`) and a space.
     - Create a top-level `environments` key containing one key for each of your environments, e.g. `dev`, `test`, and
       `prod`.

       Within each environment, specify the following keys:
       - `ags_instances`: List of ArcGIS Server instances to publish to.
       - `data_source_mappings` (optional): Mappings of source data paths to destination data paths (e.g. development
          environment SDE connection files to test environment SDE connection files)
          - Supported by `MapServer` services, but not `GeocodeServer` services.
       - `source_dir`: Directory containing the source files (MXDs, locator files, etc.) to publish.
       - `staging_dir` (optional): Directory containing staging files to copy into `source_dir` prior to mapping data
          sources and publishing.
          - Can also be a list of multiple staging directories. Each service may only have one corresponding staging
            file among all of the staging directories. Duplicates will result in a validation error.
     - You can set service properties (e.g. isolation level, number of instances per container, cache directory, etc.).
       - To specify service properties, create key/value pairs for the properties to set and the values to set them to.
         Keys are matched to service property names case-insensitively, and any underscores are stripped so that you can
         use `snake_case` to specify them; for example `instances_per_container` will match the `InstancesPerContainer`
         property.
         - Additionally, the following "special" service properties are recognized:
            - `service_type`: The type of service to publish. Defaults to `MapServer`. Currently the only supported
              types are below:
               - `MapServer`
               - `GeocodeServer`
            - `replace_service`: If set to `True`, specifies that any existing service is to be replaced. This can be
              useful to enable if you find duplicate services with a timestamp suffix are being created on the server.
            - `rebuild_locators`: Whether to rebuild locators before publishing them (only applies to `GeocodeServer`
              services).
            - `tile_scheme_file`: Path to a tile scheme file in XML format as created by the
              [Generate Map Server Cache Tiling Scheme][5] geoprocessing tool. Used for specifying the tile scheme of
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
       - Service properties may be set at either at the service folder level or at the service level:
         - Service folder level:
           - Create a top-level `default_service_properties` key and then specify the service properties as above.
         - Service level:
           - Within the top-level `services` key, for each service you want to set properties for, end the service name
             with a colon (`:`) to denote that it is a mapping object, and then specify the service properties as above.
             Ensure you indent the service properties by exactly 4 spaces relative to the hyphen (`-`) before the
             service name.

          **Note:** If both service folder level and service level properties are specified, service level properties
          override service folder level properties when there is a conflict.

     - See the [example configuration files](#example-configuration-files) section below for more details.

## Example configuration files

###`CouncilDistrictMap.yml`:

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
  test:
    ags_instances:
      - coagist1
      - coagist2
    data_source_mappings:
      \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections\gisDmDev (COUNCILDISTRICTMAP_SERVICE).sde: \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE).sde
      \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections\gisDmDev (COUNCILDISTRICTMAP_SERVICE) external.sde: \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE) external.sde
    source_dir: \\coacd.org\gis\AGS\Config\AgsEntTest\mxd-source\CouncilDistrictMap
    staging_dir: \\coacd.org\gis\AGS\Config\AgsEntDev\mxd-source\CouncilDistrictMap
  prod:
    ags_instances:
      - coagisp1
      - coagisp2
    data_source_mappings:
      \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE).sde: \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections\gisDm (COUNCILDISTRICTMAP_SERVICE).sde
      \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE) external.sde: \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections\gisDm (COUNCILDISTRICTMAP_SERVICE) external.sde
    source_dir: \\coacd.org\gis\AGS\Config\AgsEntProd\mxd-source\CouncilDistrictMap
    staging_dir: \\coacd.org\gis\AGS\Config\AgsEntTest\mxd-source\CouncilDistrictMap
```

###`userconfig.yml`:

``` yml
environments:
  dev:
    ags_instances:
      coagisd1:
        url: http://coagisd1.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd1-pughl (admin).ags
      coagisd2:
        url: http://coagisd2.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd2-pughl (admin).ags
    sde_connections_dir: \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections
  test:
    ags_instances:
      coagist1:
        url: http://coagist1.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagist1-pughl (admin).ags
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
      coagisp2:
        url: http://coagisp2.austintexas.gov
        token: <automatically set by runner.generate_tokens>
        ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisp2-pughl (admin).ags
    sde_connections_dir: \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections
```

## Example usage

### Publish services

- Publish the `dev` environment in the [`CouncilDistrictMap.yml`](#councildistrictmapyml) configuration file:

    ```
    python -c "import runner; runner.run_batch_publishing_job(['CouncilDistrictMap'], included_envs=['dev'])"
    ```

- Same as above, but publish all **except** for the `dev` environment (e.g. `test` and `prod`) using `excluded_envs`:

    ```
    python -c "import runner; runner.run_batch_publishing_job(['CouncilDistrictMap'], excluded_envs=['dev'])"
    ```

- Publish all of the environments in the [`CouncilDistrictMap.yml`](#councildistrictmapyml) configuration file, but
  **only** publish the `CouncilDistrictsFill` service:

    ```
    python -c "import runner; runner.run_batch_publishing_job(['CouncilDistrictMap'], included_services=['CouncilDistrictsFill'])"
    ```

### Clean up services

- Clean up (remove) any existing services in the `CouncilDistrictMap` service folder that have not been defined in the
   `CouncilDistrictMap.yml` configuration file:

   ```
   python -c "import runner; runner.run_batch_cleanup_job(['CouncilDistrictMap'])"
   ```

**Note:** To clean up services, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens) for each ArcGIS
Server instance defined in [`userconfig.yml`](#userconfigyml).

### Generate reports

- Generate a report in CSV format of all the datasets referenced by all services within the `CouncilDistrictMap`
   service folder on on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "import runner; runner.run_dataset_usages_report(included_service_folders=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap.csv')"
    ```

- Generate a report in CSV format of all the usages of a dataset named `BOUNDARIES.single_member_districts` within all
   services on the `coagisd1` ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml):

   ```
   python -c "import runner; runner.run_dataset_usages_report(included_datasets=['BOUNDARIES.single_member_districts'], included_instances=['coagisd1'], output_filename='../ags_service_reports/single_member_districts.csv')"
   ```

**Note:** To generate reports, you must first [generate ArcGIS Admin REST API tokens](#generate-tokens) for each ArcGIS
Server instance defined in [`userconfig.yml`](#userconfigyml).

### Generate tokens

- Generate an [ArcGIS Admin REST API token][4] for each ArcGIS Server instance defined in
   [`userconfig.yml`](#userconfigyml) that expires in 30 days:
   
   ```
   python -c "import runner; runner.generate_tokens(reuse_credentials=True, expiration=43200)"
   ```
   **Notes:**
     - This will prompt you for your credentials (ArcGIS Server username and password) unless the `username` and
       `password` arguments are specified, in which case the same credentials are used for each instance.
     - The `reuse_credentials` argument, if set to `True`, **and** if the `username` and `password` arguments are not
        specified, will only prompt you once and use the same credentials for each instance. Otherwise you will be
        prompted for each instance. Defaults to `False`.
     - The `expiration` argument is the duration in minutes for which the token is valid. Defaults to `15`.
     - You can limit which ArcGIS Server instances are used with the `included_instances` and
       `excluded_instances` arguments.
     - This will automatically update [`userconfig.yml`](#userconfigyml) with the generated tokens.

### Import SDE connection files

- Import all SDE connection files whose name contains `COUNCILDISTRICTMAP_SERVICE` to each of the ArcGIS Server instances in the
`dev` environment specified within [`userconfig.yml`](#userconfigyml):

   ```
   python -c "import runner; runner.batch_import_connection_files(['*COUNCILDISTRICTMAP_SERVICE*'], included_envs=['dev'])"
   ```
   **Note:** This looks for `.sde` files located within the directory specified by `sde_connections_dir` for each
   environment specified within [`userconfig.yml`](#userconfigyml).

## Tips

- You can use [`fnmatch`][6]-style wildcards in any of the strings in the list arguments to the runner functions, so,
  for example, you could put `included_services=['CouncilDistrict*']` and both the `CouncilDistrictMap` and
  `CouncilDistrictsFill` services would be published.
- All of the runner functions accept a `verbose` argument that, if set to `True`, will output more granular information
  to the console to help troubleshoot issues. Defaults to `False`.
- All of the runner functions accept a `quiet` argument that, if set to `True`, will suppress all output except for
  critical errors. Defaults to `False`.
- All of the runner functions accept a `config_dir` argument that allows you to override which directory is used for
  your configuration files. Defaults to the `./config` directory beneath the script's root directory. Alternatively,
  you can set the `AGS_SERVICE_PUBLISHER_CONFIG_DIR` environment variable to your desired directory.
- Some of the runner functions accept a `log_dir` argument that allows you to override which directory is used for
  storing log files. Defaults to the `./logs` directory beneath the script's root directory. Alternatively, you can set
  the `AGS_SERVICE_PUBLISHER_LOG_DIR` environment variable to your desired directory.

## TODO

- Create a nicer command line interface
- Support other types of services
- Probably lots of other stuff

[1]: https://en.wikipedia.org/wiki/YAML
[2]: https://pip.pypa.io/en/stable/installing/
[3]: https://pypi.python.org/pypi/PyYAML
[4]: http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/API_Security/02r3000001z7000000/
[5]: http://desktop.arcgis.com/en/arcmap/latest/tools/server-toolbox/generate-map-server-cache-tiling-scheme.htm
[6]: https://docs.python.org/2/library/fnmatch.html
