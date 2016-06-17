# ArcGIS Server Service Publisher

**Note: This is a work in progress!**

## Overview

The primary purpose of this tool is to automate the publishing of MXD files to Map Services on ArcGIS Server, using
[YAML](https://en.wikipedia.org/wiki/YAML) configuration files to define the service folders, environments, services,
service properties, data source mappings and more.

Additional features include [cleaning up](#clean-up-services) outdated services and [generating reports](#generate-reports) about existing services and the
datasets they reference on ArcGIS Server.

By default, configuration files are looked for in the `./config` subdirectory, and logs are written to `./logs`.

You create one configuration file per service folder -- each service folder can contain many services.

You must also create a [`userconfig.yml`](#userconfigyml) file specifying the properties for each of your ArcGIS Server
instances.

## Requirements

  - Windows 7+
  - ArcGIS Desktop 10.3+
  - Python 2.7+
  - [pip](https://pip.pypa.io/en/stable/installing/)
  - [PyYAML](https://pypi.python.org/pypi/PyYAML) 3.11 (will be installed by pip as described in the Setup Instructions)

## Setup instructions

  - Clone this repository to a local directory
  - Open a Windows command prompt in the local directory
  - Type `pip install -r requirements.txt`
  - Create a folder named `config` in the local directory
  - Create a file named [`userconfig.yml`](#userconfigyml) in the `config` folder, and populate it with a key named
  `ags_instances` containing a mapping of ArcGIS Server instance names and the following properties:
    - `url`: Base URL (scheme and hostname) of your ArcGIS Server instance
    - `token`: [ArcGIS Admin REST API token](http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#/API_Security/02r3000001z7000000/) (see the ["Generate an ArcGIS Admin REST API token"](#generate-token) example below for more details.)
    - `ags_connection`: Path to an `.ags` connection file for each instance.
  - Create additional configuration files for each service folder you want to publish.
    - MXD files are matched based on the names of the services, for example `CouncilDistrictsFill` maps to
    `CouncilDistrictsFill.mxd`.
    - Configuration files must have a `.yml` extension.
  - See the [example configuration files](#example-configuration-files) section below for more details.

## Example configuration files

###`CouncilDistrictMap.yml`:

``` yml
service_folder: CouncilDistrictMap
services:
  - CouncilDistrictMap
  - CouncilDistrictsFill
default_service_properties:
  isolation: low
  instances_per_container: 8
environments:
  dev:
    ags_instances:
      - coagisd1
      - coagisd2
    mxd_dir: \\coacd.org\gis\AGS\Config\AgsEntDev\mxd-source\CouncilDistrictMap
  test:
    ags_instances:
      - coagist1
      - coagist2
    data_source_mappings:
      \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections\gisDmDev (COUNCILDISTRICTMAP_SERVICE).sde: \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE).sde
      \\coacd.org\gis\AGS\Config\AgsEntDev\Service-Connections\gisDmDev (COUNCILDISTRICTMAP_SERVICE) external.sde: \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE) external.sde
    mxd_dir: \\coacd.org\gis\AGS\Config\AgsEntTest\mxd-source\CouncilDistrictMap
    mxd_dir_to_copy_from: \\coacd.org\gis\AGS\Config\AgsEntDev\mxd-source\CouncilDistrictMap
  prod:
    ags_instances:
      - coagisp1
      - coagisp2
    data_source_mappings:
      \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE).sde: \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections\gisDm (COUNCILDISTRICTMAP_SERVICE).sde
      \\coacd.org\gis\AGS\Config\AgsEntTest\Service-Connections\gisDmTest (COUNCILDISTRICTMAP_SERVICE) external.sde: \\coacd.org\gis\AGS\Config\AgsEntProd\Service-Connections\gisDm (COUNCILDISTRICTMAP_SERVICE) external.sde
    mxd_dir: \\coacd.org\gis\AGS\Config\AgsEntProd\mxd-source\CouncilDistrictMap
    mxd_dir_to_copy_from: \\coacd.org\gis\AGS\Config\AgsEntTest\mxd-source\CouncilDistrictMap
```

###`userconfig.yml`:

``` yml
ags_instances:
  coagisd1:
    url: http://coagisd1.austintexas.gov
    token: <token obtained from ags_utils.generate_token>
    ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd1-pughl (admin).ags
  coagisd2:
    url: http://coagisd2.austintexas.gov
    token: <token obtained from ags_utils.generate_token>
    ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd2-pughl (admin).ags
  coagist1:
    url: http://coagist1.austintexas.gov
    token: <token obtained from ags_utils.generate_token>
    ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagist1-pughl (admin).ags
  coagist2:
    url: http://coagist2.austintexas.gov
    token: <token obtained from ags_utils.generate_token>
    ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagist2-pughl (admin).ags
  coagisp1:
    url: http://coagisp1.austintexas.gov
    token: <token obtained from ags_utils.generate_token>
    ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisp1-pughl (admin).ags
  coagisp2:
    url: http://coagisp2.austintexas.gov
    token: <token obtained from ags_utils.generate_token>
    ags_connection: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisp2-pughl (admin).ags
```

## Example usage

### Publish services

1. Publish the `dev` environment in the [`CouncilDistrictMap.yml`](#councildistrictmapyml) configuration file:

    ```
    python -c "import runner; runner.run_batch_publishing_job(['CouncilDistrictMap'], included_envs=['dev'])"
    ```

2. Same as above, but publish all **except** for the `dev` environment (e.g. `test` and `prod`) using `excluded_envs`:

    ```
    python -c "import runner; runner.run_batch_publishing_job(['CouncilDistrictMap'], excluded_envs=['dev'])"
    ```

3. Same as above, but **only** publish the `CouncilDistrictsFill` service:

    ```
    python -c "import runner; runner.run_batch_publishing_job(['CouncilDistrictMap'], included_services=['CouncilDistrictsFill'])"
    ```

### Clean up services

1. Clean up (remove) any existing services in the `CouncilDistrictMap` service folder that have not been defined in the
`CouncilDistrictMap.yml` configuration file:

   ```
   python -c "import runner; runner.run_batch_cleanup_job(['CouncilDistrictMap'])
   ```

**Note:** To clean up services, you must [generate ArcGIS Admin REST API tokens](#generate-token) for each ArcGIS Server
instance defined in [`userconfig.yml`](#userconfigyml).

### Generate reports

1. Generate a report in CSV format of all the datasets referenced by all services within the `CouncilDistrictMap`
   service folder on on all ArcGIS Server instances defined in [`userconfig.yml`](#userconfigyml):

    ```
    python -c "import runner; runner.run_dataset_usages_report(included_service_folders=['CouncilDistrictMap'], output_filename='../ags-service-reports/CouncilDistrictMap.csv')"
    ```

2. Generate a report in CSV format of all the usages of a dataset named `BOUNDARIES.single_member_districts` within all
   services on the `coagisd1` ArcGIS Server instance defined in [`userconfig.yml`](#userconfigyml):

   ```
   python -c "import runner; runner.run_dataset_usages_report(included_datasets=['BOUNDARIES.single_member_districts'], included_instances=['coagisd1'], output_filename='../ags_service_reports/single_member_districts.csv')"
   ```

**Note:** To generate reports, you must [generate ArcGIS Admin REST API tokens](#generate-token) for each ArcGIS Server
instance defined in [`userconfig.yml`](#userconfigyml).

### Generate token

1. Generate an ArcGIS Admin REST API token for an ArcGIS Server instance named `coagisd1`
   that expires in 30 days:

   ```
   python -c "import ags_utils; print ags_utils.generate_token('coagisd1', expiration=43200)"
   ```

**Note:** Copy and paste the generated token into [`userconfig.yml`](#userconfigyml) as the value for the `token` key
corresponding to the ArcGIS server instance it was generated on.

## Tips

- You can use [`fnmatch`](https://docs.python.org/2/library/fnmatch.html)-style wildcards in any of the
   strings in the list arguments to the runner functions, so, for example, you could put `included_services=['CouncilDistrict*']`
   and both the `CouncilDistrictMap` and `CouncilDistrictsFill` services would be published.
- All of the runner functions accept a `verbose` argument that, if set to `True`, will output more granular information to
the console to help troubleshoot issues.
Defaults to `False`.
- All of the runner functions accept a `quiet` argument that, if set to `True`, will suppress all output except for
critical errors. Defaults to `False`.

## TODO

- Create a nicer command line interface
- Support other types of services
- Probably lots of other stuff
