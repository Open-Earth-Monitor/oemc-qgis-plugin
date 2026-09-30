# oemc-qgis-plugin

## Overview

**OEMC QGIS plugin** gives easy access to OEMC STAC catalogs from inside QGIS. Browse a catalog, select a collection and its items, preview collection metadata and add layers to the map.

The plugin targets two kinds of analysis-ready data:

- Wall-to-wall raster mosaics served as Cloud Optimized GeoTIFF (COG) or VRT files with overview files (ovr)
- Analysis-ready in-situ data (ARIS Data) in vector format readable by GDAL/OGR

Tiled raster data is not supported and will not display correctly. Catalogs that publish tiled assets, for example the [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/docs/quickstarts/reading-stac/), are out of scope.

## Supported STAC catalogs

- OpenLandMap: https://stac.opengeohub.org/v1/cat/openlandmap
- LandMetric: https://stac.opengeohub.org/v1/cat/landmetric
- EcoDataCube: https://stac.opengeohub.org/v1/cat/ecodatacube
- OEMC (in-situ): https://s3.eu-central-1.wasabisys.com/stac/oemc/catalog.json

## Installation

_To use dev version_
- Download this repo as a zip file.
- In the QGIS go __Plugins > Manage and Install Plugins... > Install from ZIP__ and select the zip file you downloaded from this repo and install it.

_To use official release_
- Spin-up the QGIS instance you have on your machine
- Navigate through **Plugins** > **Manage and Install plugins..** 
- Search for **OEMC** 
- Select the plugin and hit the **Install** button


## License
© OpenGeoHub Foundation, 2023-2024. Licensed under the [MIT License](LICENSE).

## Acknowledgements & Funding
This work is supported by [OpenGeoHub Foundation](https://opengeohub.org/) and has received 
funding from the European Commission (EC) through the projects:

- [Open-Earth-Monitor Cyberinfrastructure](https://earthmonitor.org/): Environmental information 
  to support EU’s Green Deal (1 Jun. 2022 – 31 May 2026 - 
  [101059548](https://cordis.europa.eu/project/id/101059548))
