# Spotify Analytics — Top Songs & Artists 2025
## Arquitectura Medallion en Azure Databricks

Pipeline automatizado de datos para analisis de canciones y charts de Spotify implementando la Arquitectura Medallion (Bronze-Silver-Golden) en Azure Databricks con Unity Catalog, Delta Lake y despliegue continuo via GitHub Actions.

---

## Descripcion

Pipeline ETL que transforma datos crudos de dos fuentes Spotify:
- Ranking historico de las 100 canciones mas escuchadas de todos los tiempos (`spotify_alltime_top100_songs.csv`)
- Wrapped 2025 con las 50 canciones del año (`spotify_wrapped_2025_top50_songs.csv`)

Implementa la Arquitectura Medallion en Azure Databricks con Delta Lake para garantizar consistencia ACID.

---

## Arquitectura

```
CSV Raw Data (ADLS Gen2)
    |
Bronze Layer  (Ingesta sin transformacion)
    |
Silver Layer  (Limpieza + Transformaciones + Join)
    |
Golden Layer  (Agregaciones de negocio)
    |
Dashboard Databricks (Visualizacion)
```

---

## Capas del Pipeline

### Bronze Layer
Proposito: Zona de aterrizaje

Tablas:
- `spotify_tracks` — Datos del ranking historico alltime top 100
- `spotify_daily_charts` — Datos del Wrapped 2025 top 50

Caracteristicas:
- Datos tal como vienen del origen
- Timestamp de ingesta
- Sin validaciones ni transformaciones
- Schema explicito definido con PySpark

### Silver Layer
Proposito: Datos limpios y enriquecidos

Tablas:
- `spotify_transformed` — Join de tracks con charts mas columnas derivadas

Transformaciones aplicadas:
- Limpieza de nulos
- UDF de categoria de popularidad con umbrales ajustados a billions (Baja / Media / Alta)
- Calculo de dias desde el chart (`days_since_chart`)
- Clasificacion de mercado (`market_type`: Major Market / Global Market) basado en paises reales del dataset
- Clasificacion de duracion (`track_duration_type`: Long Track / Standard Track)

### Golden Layer
Proposito: Analytics-ready

Tablas:
- `spotify_final_summary` — Agregacion por fecha con metricas de streams, pais, mercado y nivel de popularidad

Vistas para Dashboard:
- `vw_top20_canciones`
- `vw_top10_artistas`
- `vw_explicitas`
- `vw_popularidad`
- `vw_market`
- `vw_energia_dance`

---

## Dashboard

![Dashboard Spotify Analytics](./images/dashboard.jpg)

El dashboard fue construido en Databricks con las siguientes visualizaciones:

| Visualizacion | Tipo | Fuente |
|---|---|---|
| Total canciones | Counter | `spotify_transformed` |
| Cancion mas escuchada | Counter | `spotify_transformed` |
| Artista con mas streams | Counter | `spotify_transformed` |
| Promedio de streams | Counter | `spotify_transformed` |
| Top 20 Canciones | Bar chart | `spotify_transformed` |
| Top 10 Artistas | Bar chart | `spotify_transformed` |
| Energia vs Danceability | Scatter plot | `spotify_transformed` |
| Explicitas vs No Explicitas | Pie chart | `spotify_transformed` |
| Popularidad Alta / Media / Baja | Pie chart | `spotify_transformed` |
| Major vs Global Market | Bar chart | `spotify_transformed` |
| Canciones por año de lanzamiento | Bar chart | `spotify_transformed` |

---

## Estructura del Proyecto

```
spotify-medallion/
|
├── .github/
│   └── workflows/
│       └── deploy.yml                   # Pipeline CI/CD GitHub Actions
├── images/
│   └── dashboard.jpg                    # Captura del dashboard
├── Preparacion_Ambiente.ipynb           # Creacion de catalog, schemas y tablas Delta
├── Ingest_Tracks_Data.ipynb             # Bronze: spotify_alltime_top100_songs.csv
├── Ingest_Spotify_Charts.ipynb          # Bronze: spotify_wrapped_2025_top50_songs.csv
├── Transform.ipynb                      # Silver: limpieza, join y enriquecimiento
├── Load.ipynb                           # Golden: agregaciones finales
├── Dashboard_Spotify.ipynb              # Vistas y queries del dashboard
├── Grants_Spotify_Medallion.ipynb       # Control de acceso Unity Catalog
├── Drop_Spotify_Medallion.ipynb         # Eliminacion de tablas y schemas
└── README.md
```

---

## Fuentes de Datos

| Archivo | Descripcion | Columnas clave |
|---|---|---|
| `spotify_alltime_top100_songs.csv` | Ranking historico de canciones | alltime_rank, song_title, artist, total_streams_billions |
| `spotify_wrapped_2025_top50_songs.csv` | Wrapped 2025 top 50 | wrapped_2025_rank, song_title, streams_2025_billions |

Ubicacion en ADLS: `abfss://raw@adlsantodata1703.dfs.core.windows.net/`

---

## Requisitos Previos

- Cuenta de Azure con acceso a Databricks
- Workspace de Databricks con Unity Catalog habilitado
- Cluster activo (`cluster_SD`)
- Azure Data Lake Storage Gen2 configurado (`adlsantodata1703`)
- External Location configurada sobre `unit-catalog` para managed storage
- Cuenta de GitHub con permisos de administrador en el repositorio

---

## Servicios Aprovisionados en Azure

![Servicios Azure](./evidencias/azure_services.png)

| Servicio | Nombre | Tipo |
|---|---|---|
| Access Connector | ac-antodata | Access Connector for Azure Databricks |
| Azure Databricks Dev | adbantodata1403 | Azure Databricks Service |
| Azure Databricks Prod | adbantodata1503prod | Azure Databricks Service |
| Data Factory | adfantodata2603 | Data Factory V2 |
| Storage Account | adlsantodata1703 | Azure Data Lake Storage Gen2 |

---

## Configuracion de Infraestructura

El catalogo `catalog_au` usa managed storage en:
```
abfss://unit-catalog@adlsantodata1703.dfs.core.windows.net/catalog_au/
```

Schemas creados con MANAGED LOCATION explicita:
```
catalog_au.raw
catalog_au.bronze
catalog_au.silver
catalog_au.golden
catalog_au.exploratory
```

---

## Orden de Ejecucion Manual

```
1. Preparacion_Ambiente.ipynb    -- Crear schemas y tablas Delta
2. Ingest_Tracks_Data.ipynb      -- Cargar alltime top 100 a Bronze
3. Ingest_Spotify_Charts.ipynb   -- Cargar Wrapped 2025 a Bronze
4. Transform.ipynb               -- Transformar y join a Silver
5. Load.ipynb                    -- Agregar y cargar a Golden
6. Dashboard_Spotify.ipynb       -- Crear vistas para el dashboard
```

---

## CI/CD con GitHub Actions

### Como funciona

Cada vez que se hace un push a la rama `main`, el workflow de GitHub Actions ejecuta automaticamente los siguientes pasos:

```
Push a main
    |
Exportar notebooks desde workspace de desarrollo
    |
Desplegar notebooks al workspace de produccion (/proyecto/smartdata)
    |
Eliminar workflow WF_ADB anterior si existe
    |
Buscar cluster existente cluster_SD
    |
Crear workflow WF_ADB con las 5 tareas en orden
    |
Ejecutar WF_ADB automaticamente
    |
Monitorear ejecucion por hasta 10 minutos
```

### Workflow WF_ADB

```
Preparacion_Ambiente
        |
   _____|_____
   |         |
Ingest_    Ingest_
Tracks_    Spotify_
Data       Charts
   |         |
   |_________|
        |
    Transform
        |
      Load
```

### Configuracion de GitHub Secrets

| Secret | Descripcion |
|---|---|
| `DATABRICKS_ORIGIN_HOST` | URL del workspace de desarrollo |
| `DATABRICKS_ORIGIN_TOKEN` | Token del workspace de desarrollo |
| `DATABRICKS_DEST_HOST` | URL del workspace de produccion |
| `DATABRICKS_DEST_TOKEN` | Token del workspace de produccion |

### Despliegue Manual desde GitHub

1. Ir al tab `Actions` en el repositorio
2. Seleccionar `Dynamic Databricks Notebook Deploy`
3. Click en `Run workflow`
4. Seleccionar rama `main`
5. Click en `Run workflow`

---

## Monitoreo

### En Databricks

Ir a `Workflows` en el menu lateral y buscar `WF_ADB`. Desde ahi se puede ver el historial de ejecuciones, el estado de cada tarea y los logs detallados por notebook.

### En GitHub Actions

Ir al tab `Actions` del repositorio. Cada push genera una ejecucion nueva donde se puede ver el log de cada paso incluyendo el estado final del workflow en Databricks y la duracion total.

---

## Stack Tecnologico

- Azure Databricks (PySpark)
- Azure Data Lake Storage Gen2
- Unity Catalog
- Delta Lake
- Azure Data Factory (orquestacion)
- Key Vault (seguridad)
- GitHub Actions (CI/CD)
- GitHub (control de versiones)
- Databricks Dashboards (visualizacion)

