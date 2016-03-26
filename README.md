# ArcGIS Server Service Publisher

**Note: This is a work in progress!**

Publishes MXD files to Map Services on ArcGIS Server using [YAML](https://en.wikipedia.org/wiki/YAML) configuration files.

MXD files are matched based on the names of the services, for example `CouncilDistrictsFill` maps to `CouncilDistrictsFill.mxd`.

By default, configuration files are looked for in the `./config` subdirectory, and logs are written to `./logs`.

Configuration files must have a `.yml` extension.

You create one configuration file per service folder -- each service folder can contain many services.

You must also create a `userconfig.yml` file specifying the paths to the ArcGIS Server connection file for each of your server instances.

See the example configuration files below for more details.

### Requirements

  - ArcGIS Desktop 10.3+
  - Python 2.7+
  - PyYAML 3.11

### Example configuration files

 - `CouncilDistrictMap.yml`

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

 - `userconfig.yml`

    ``` yml
    ags_connections:
      coagisd1: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd1-pughl (admin).ags
      coagisd2: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisd2-pughl (admin).ags
      coagist1: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagist1-pughl (admin).ags
      coagist2: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagist2-pughl (admin).ags
      coagisp1: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisp1-pughl (admin).ags
      coagisp2: C:\Users\pughl\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\coagisp2-pughl (admin).ags
    ```

### Example Usage

1. Publish the `dev` environment in the `./config/CouncilDistrictMap.yml` configuration file:

    ```
    python -c "import runner; runner.run_batch_publishing_job(['CouncilDistrictMap'], ['dev'])"
    ```

### TODO

- Create a nicer command line interface
- Support other types of services
- Probably lots of other stuff
