# 🚦 NYC Collision Data Geoprocessing Workflow (ArcGIS + Python)

This project automates a **complete spatial analysis pipeline** using **ArcPy**, **PostgreSQL**, and **NYC collision data**. The workflow fetches crash data from a PostGIS database, transforms it into spatial formats, analyzes it using kernel density estimation, and performs spatial joins with NYC's LION street network.

---

## 🧩 Key Features

- ✅ Creates XY Point features from PostgreSQL crash data
- ✅ Kernel Density Estimation (KDE) of crash severity (EPDO)
- ✅ Raster reclassification into severity levels (gridcode)
- ✅ Raster-to-polygon and polygon-to-line conversion
- ✅ Two-level spatial join with the NYC LION street network
- ✅ Exports results to GeoJSON and (optionally) uploads to PostgreSQL

---

## 🔧 Technologies Used

- Python 3.x
- ArcPy (ArcGIS Pro)
- PostgreSQL + PostGIS
- GeoJSON
- NYC LION street dataset
