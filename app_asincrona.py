"""
app_asincrona.py
Demostración de aplicación asíncrona en Python con PyMongo nativo (AsyncMongoClient).
Base de datos: sample_mflix | Colección: movies (Dataset completo: 23,539 registros)
"""

import asyncio
import time
from pathlib import Path
from bson.json_util import loads
from pymongo import AsyncMongoClient, InsertOne, UpdateOne, DeleteOne

# Constante de configuración: número de consultas a ejecutar en la comparativa
NUM_CONSULTAS = 100

# Géneros base para generar los filtros de consulta
GENEROS_BASE = [
    "Short",
    "Western",
    "Animation",
    "Comedy",
    "Drama",
    "Romance",
    "Action",
    "Crime",
    "Thriller",
    "Documentary",
]


class AppAsincronaMflix:
    """Clase para gestionar operaciones asíncronas sobre sample_mflix.movies usando AsyncMongoClient."""

    def __init__(
        self,
        uri: str = "mongodb://localhost:27017/",
        db_name: str = "sample_mflix",
        coll_name: str = "movies",
    ):
        self.client = AsyncMongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[coll_name]

    async def cargar_datos_masivos(self, peliculas: list[dict]) -> int:
        """Limpia la coleccion e inserta masivamente documentos usando bulk_write() asincrono."""
        print(f"[Async] Limpiando coleccion '{self.collection.name}'...")
        await self.collection.drop()

        print(f"[Async] Ejecutando bulk_write() asincrono con {len(peliculas)} documentos...")
        operaciones = [InsertOne(doc) for doc in peliculas]
        resultado = await self.collection.bulk_write(operaciones)
        return resultado.inserted_count

    async def operacion_insertar(self):
        """Inserta (insert_one) un nuevo documento en la colección de forma asíncrona."""
        doc = {
            "title": "Async Academic Demo Movie",
            "year": 2026,
            "genres": ["Educational", "Documentary"],
            "runtime": 120,
        }
        return await self.collection.insert_one(doc)

    async def operacion_leer(self):
        """Lee (find_one) un documento por su título de forma asíncrona."""
        return await self.collection.find_one({"title": "Blacksmith Scene"})

    async def operacion_actualizar(self):
        """Actualiza (update_one) el tiempo de duración y año de un documento existente."""
        return await self.collection.update_one(
            {"title": "Blacksmith Scene"},
            {"$set": {"runtime": 3, "year": 1893, "updated_by": "app_asincrona"}},
        )

    async def operacion_eliminar(self):
        """Elimina (delete_one) el documento insertado para pruebas de forma asíncrona."""
        return await self.collection.delete_one({"title": "Async Academic Demo Movie"})

    async def operacion_bulk_mixto(self):
        """Demuestra un bulk_write asíncrono con operaciones combinadas (Insert, Update, Delete)."""
        operaciones = [
            InsertOne({"title": "Async Mixed Item", "year": 2026, "genres": ["Drama"]}),
            UpdateOne({"title": "Async Mixed Item"}, {"$set": {"year": 2027}}),
            DeleteOne({"title": "Async Mixed Item"}),
        ]
        return await self.collection.bulk_write(operaciones)

    async def ejecutar_crud(self):
        """Ejecuta operaciones CRUD de manera concurrente usando asyncio.TaskGroup()."""
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.operacion_insertar())
            tg.create_task(self.operacion_leer())
            tg.create_task(self.operacion_actualizar())
            tg.create_task(self.operacion_eliminar())
            tg.create_task(self.operacion_bulk_mixto())

    async def ejecutar_consultas(
        self, total_consultas: int = NUM_CONSULTAS, concurrencia_max: int = 10
    ) -> list[list[dict]]:
        """
        Ejecuta múltiples consultas asíncronas concurrentes con asyncio.gather(),
        controlando la concurrencia mediante asyncio.Semaphore(10).
        """
        filtros = [
            {
                "genres": GENEROS_BASE[i % len(GENEROS_BASE)],
                "year": {"$gte": 1900 + (i % 25)},
            }
            for i in range(total_consultas)
        ]
        semaforo = asyncio.Semaphore(concurrencia_max)

        async def consultar_documentos(filtro: dict) -> list[dict]:
            async with semaforo:
                # Regla PyMongo Async: find() devuelve AsyncCursor sin await;
                # to_list() se invoca con await para materializar los documentos.
                return await self.collection.find(filtro).to_list()

        # Creamos las tareas concurrentes
        tareas = [consultar_documentos(f) for f in filtros]
        # Las ejecutamos concurrentemente con asyncio.gather
        resultados = await asyncio.gather(*tareas)
        return resultados

    async def cerrar(self):
        """Cierra la conexión asíncrona con el servidor MongoDB."""
        await self.client.close()


def cargar_dataset_local(ruta_archivo: Path, limite: int | None = None) -> list[dict]:
    """Lee el archivo movies.json línea por línea con bson.json_util.loads (todos los registros si limite=None)."""
    peliculas = []
    with open(ruta_archivo, "r", encoding="utf-8") as archivo:
        for i, linea in enumerate(archivo):
            if limite is not None and i >= limite:
                break
            linea_limpia = linea.strip()
            if linea_limpia:
                peliculas.append(loads(linea_limpia))
    return peliculas


async def main():
    ruta_dataset = Path(__file__).parent / "movies.json"

    print("=" * 65)
    print("        INICIO DE PRUEBAS - APLICACION ASINCRONA (PyMongo Async)")
    print("=" * 65)

    # 1. Lectura del archivo completo en memoria (fuera del cronometro de BD)
    print(f"Cargando dataset local completo desde: {ruta_dataset.name} ...")
    peliculas_totales = cargar_dataset_local(ruta_dataset, limite=None)
    total_docs = len(peliculas_totales)
    print(f"-> {total_docs} documentos leidos en memoria con bson.json_util.")

    app = AppAsincronaMflix()

    try:
        t_inicio_total = time.perf_counter()

        # Medicion 1: Carga masiva asincrona completa
        t_inicio_carga = time.perf_counter()
        total_insertados = await app.cargar_datos_masivos(peliculas_totales)
        t_carga = time.perf_counter() - t_inicio_carga
        print(f"[OK] Carga masiva asincrona completada: {total_insertados} documentos.")

        # Medicion 2: CRUD asincrono con TaskGroup
        t_inicio_crud = time.perf_counter()
        await app.ejecutar_crud()
        t_crud = time.perf_counter() - t_inicio_crud
        print("[OK] CRUD concurrente ejecutado con asyncio.TaskGroup().")

        # Medicion 3: Consultas asincronas con gather() y Semaphore(10)
        t_inicio_consultas = time.perf_counter()
        res_consultas = await app.ejecutar_consultas(NUM_CONSULTAS)
        t_consultas = time.perf_counter() - t_inicio_consultas
        total_docs_obtenidos = sum(len(r) for r in res_consultas)
        print(f"[OK] {NUM_CONSULTAS} consultas concurrentes ejecutadas con gather() ({total_docs_obtenidos} docs obtenidos).")

        t_total = time.perf_counter() - t_inicio_total

        print("\n" + "=" * 65)
        print("          RESULTADOS DE RENDIMIENTO - APLICACION ASINCRONA")
        print("=" * 65)
        print(f"  (1) Carga Masiva (bulk_write {total_insertados:,} docs): {t_carga:.4f} s")
        print(f"  (2) CRUD Concurrente (TaskGroup):         {t_crud:.4f} s")
        print(f"  (3) {NUM_CONSULTAS} Consultas (gather + Semaphore 10): {t_consultas:.4f} s")
        print("-" * 65)
        print(f"  [*] TIEMPO TOTAL DE OPERACIONES BD:       {t_total:.4f} s")
        print("=" * 65)

    finally:
        await app.cerrar()


if __name__ == "__main__":
    asyncio.run(main())
