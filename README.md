# Comparativa Síncrona vs. Asíncrona con Python y MongoDB (mflix)
**Integrantes:** 
- Alexis Aidan Leon Rodelas
- Angel Damian Villalobos Ochoa 
- Dominik Eliel Armendariz Mimbela

**Materia:** Computo Paralelo y Distribuido 
**Entorno de Ejecución:** Local (`mongodb://localhost:27017/`)  
**Base de Datos:** `sample_mflix`  
**Colección:** `movies` (Dataset oficial completo: **23,539 documentos BSON**)  
**Librería Oficial:** `pymongo` 4.19.0 (Síncrono `MongoClient` vs. Asíncrono nativo `AsyncMongoClient`)  

---

## 1. Objetivo

El propósito fundamental de este proyecto es diseñar, implementar y comparar cuantitativamente dos arquitecturas de software para la interacción con bases de datos documentales en MongoDB: un modelo **síncrono bloqueante** frente a un modelo **asíncrono no bloqueante**.

Se utilizó el dataset oficial completo de MongoDB `sample_mflix` (colección `movies.json` con **23,539 registros reales**), deserializado con `bson.json_util.loads` para preservar íntegramente los tipos BSON nativos (`$oid`, `$date`, `$numberInt`). Se evaluó el rendimiento en operaciones de ingesta masiva (`bulk_write`), operaciones CRUD concurrentes (`asyncio.TaskGroup`) y una prueba de carga de **100 consultas filtradas concurrentes** (`asyncio.gather` + `asyncio.Semaphore`).

---

## 2. Estructura del Proyecto

El repositorio se compone estrictamente de los siguientes archivos modulares e independientes:

```text
mflix_sync_async/
│
├── movies.json            # Dataset oficial sample_mflix (23,539 registros JSON/BSON)
├── app_sincrona.py        # Implementación síncrona secuencial (MongoClient)
├── app_asincrona.py       # Implementación asíncrona concurrente (AsyncMongoClient + asyncio)
└── README.md              # Reporte técnico y análisis de rendimiento comparativo
```

### 2.1. Arquitectura de Clases
```mermaid
classDiagram
    class AppSincronaMflix {
        +MongoClient client
        +Database db
        +Collection collection
        +cargar_datos_masivos(peliculas: list) int
        +ejecutar_crud() void
        +ejecutar_consultas(total_consultas: int) list
        +cerrar() void
    }

    class AppAsincronaMflix {
        +AsyncMongoClient client
        +AsyncDatabase db
        +AsyncCollection collection
        +cargar_datos_masivos(peliculas: list) int
        +ejecutar_crud() void
        +ejecutar_consultas(total_consultas: int) list
        +cerrar() void
    }
```

- **[`app_sincrona.py`](app_sincrona.py)**: Ejecuta solicitudes lineales bloqueando el hilo de ejecución principal en cada respuesta del socket TCP.
- **[`app_asincrona.py`](app_asincrona.py)**: Delega la entrada y salida de red al bucle de eventos (`asyncio EventLoop`), permitiendo despachar ráfagas de consultas simultáneamente.

## 3. Comparación Experimental de Rendimiento (Dataset Completo: 23,539 Registros)

### 3.1. Metodología de Medición
- **Volumen de datos:** 23,539 documentos insertados y almacenados en `sample_mflix.movies`.
- **Volumen de lectura:** 100 consultas filtradas con límite de 20 documentos cada una (2,000 documentos recuperados en total).
- **Entorno:** MongoDB Community Server 8.0 en `localhost:27017`, Python 3.13.14 x64, Windows 11.
- **Temporizador:** `time.perf_counter()` evaluado en corridas sucesivas.

### 3.2. Tabla Comparativa de Tiempos Reales

| Etapa Evaluada | Síncrono (`MongoClient`) | Asíncrono (`AsyncMongoClient`) | Ganancia / Diferencia | Análisis Técnico |
| :--- | :---: | :---: | :---: | :--- |
| **(1) Carga Masiva (23,539 docs)** | `0.3443 s` | `0.3408 s` | **+0.0035 s más rápido en Async** | Inserción ultra-eficiente de 23k registros en ~0.34 s gracias a un único paquete binario `bulk_write`. |
| **(2) Operaciones CRUD** | `0.0443 s` | `0.0353 s` | **+20.3% más rápido en Async** | Las operaciones atómicas concurrentes en `TaskGroup` solapan la escritura y lectura simultánea. |
| **(3) 100 Consultas Variadas** | `0.0544 s` | `0.0458 s` | **+15.8% más rápido en Async** | `gather` con `Semaphore(10)` distribuye el escaneo de la colección entre múltiples hilos de CPU en MongoDB. |
| **[*] TIEMPO TOTAL BD** | **`0.4431 s`** | **`0.4220 s`** | **Async supera en todas las etapas** | Menor tiempo total de retención de CPU y sockets de red. |

---

## 4. Análisis del Impacto Computacional

### 4.1. Memoria RAM
- El dataset de 23,539 documentos ocupa $\approx 55\text{ MB}$ en memoria RAM de Python y $\approx 45\text{ MB}$ en la caché WiredTiger de MongoDB.
- Esto representa menos del **0.4% de la memoria** de una computadora personal estándar, garantizando alta eficiencia sin degradar el sistema operativo.

### 4.2. Uso de CPU Multinúcleo
- Al no existir índices secundarios precreados sobre `genres` y `year`, MongoDB debe realizar un escaneo de colección para evaluar las condiciones de filtro en 23,539 registros.
- **Enfoque Síncrono:** Las consultas llegan una por una; MongoDB utiliza un único hilo de ejecución a la vez, subutilizando la CPU del equipo.
- **Enfoque Asíncrono:** Con `Semaphore(10)`, MongoDB recibe ráfagas paralelas y distribuye las tareas entre los **múltiples núcleos del procesador**, acelerando la respuesta global.

# Ejecución de la aplicación asíncrona
python app_asincrona.py
```
