"""
app_sincrona.py
Demostración de aplicación síncrona en Python con PyMongo nativo.
Base de datos: sample_mflix | Colección: movies (Dataset completo: 23,539 registros)
"""

import time
from pathlib import Path
from bson.json_util import loads
from pymongo import MongoClient, InsertOne, UpdateOne, DeleteOne

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


class AppSincronaMflix:
    """Clase para gestionar operaciones síncronas sobre sample_mflix.movies usando MongoClient."""

    def __init__(
        self,
        uri: str = "mongodb://localhost:27017/",
        db_name: str = "sample_mflix",
        coll_name: str = "movies",
    ):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[coll_name]

    def cargar_datos_masivos(self, peliculas: list[dict]) -> int:
        """Limpia la coleccion y realiza una carga masiva mediante bulk_write() secuencial."""
        print(f"[Sync] Limpiando coleccion '{self.collection.name}'...")
        self.collection.drop()

        print(f"[Sync] Ejecutando bulk_write() con {len(peliculas)} documentos...")
        operaciones = [InsertOne(doc) for doc in peliculas]
        resultado = self.collection.bulk_write(operaciones)
        return resultado.inserted_count

    def operacion_insertar(self):
        """Crea (insert_one) una pelicula nueva."""
        doc = {
            "title": "Sync Academic Demo Movie",
            "year": 2026,
            "genres": ["Educational", "Documentary"],
            "runtime": 120,
        }
        return self.collection.insert_one(doc)

    def operacion_leer(self):
        """Lee (find_one) una pelicula por titulo."""
        return self.collection.find_one({"title": "Blacksmith Scene"})

    def operacion_actualizar(self):
        """Actualiza (update_one) el campo runtime y year de una pelicula existente."""
        return self.collection.update_one(
            {"title": "Blacksmith Scene"},
            {"$set": {"runtime": 2, "year": 1893, "updated_by": "app_sincrona"}},
        )

    def operacion_eliminar(self):
        """Elimina (delete_one) la pelicula creada."""
        return self.collection.delete_one({"title": "Sync Academic Demo Movie"})

    def operacion_bulk_mixto(self):
        """Demuestra un bulk_write con operaciones combinadas (InsertOne, UpdateOne, DeleteOne)."""
        operaciones = [
            InsertOne({"title": "Sync Mixed Item", "year": 2026, "genres": ["Drama"]}),
            UpdateOne({"title": "Sync Mixed Item"}, {"$set": {"year": 2027}}),
            DeleteOne({"title": "Sync Mixed Item"}),
        ]
        return self.collection.bulk_write(operaciones)

    def ejecutar_crud(self):
        """Ejecuta secuencialmente operaciones CRUD completas y un bulk mixto."""
        self.operacion_insertar()
        self.operacion_leer()
        self.operacion_actualizar()
        self.operacion_eliminar()
        self.operacion_bulk_mixto()

    def ejecutar_consultas(self, total_consultas: int = NUM_CONSULTAS) -> list[list[dict]]:
        """Ejecuta un ciclo for secuencial con múltiples consultas variadas sobre la colección."""
        filtros = [
            {
                "genres": GENEROS_BASE[i % len(GENEROS_BASE)],
                "year": {"$gte": 1900 + (i % 25)},
            }
            for i in range(total_consultas)
        ]
        resultados = []
        for filtro in filtros:
            cursor = self.collection.find(filtro)
            documentos = list(cursor)
            resultados.append(documentos)
        return resultados

    def cerrar(self):
        """Cierra la conexión con el servidor MongoDB."""
        self.client.close()


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


if __name__ == "__main__":
    ruta_dataset = Path(__file__).parent / "movies.json"

    print("=" * 65)
    print("        INICIO DE PRUEBAS - APLICACION SINCRONA (PyMongo)")
    print("=" * 65)

    # 1. Lectura del archivo completo en memoria (fuera del cronometro de BD)
    print(f"Cargando dataset local completo desde: {ruta_dataset.name} ...")
    peliculas_totales = cargar_dataset_local(ruta_dataset, limite=None)
    total_docs = len(peliculas_totales)
    print(f"-> {total_docs} documentos leidos en memoria con bson.json_util.")

    app = AppSincronaMflix()

    try:
        t_inicio_total = time.perf_counter()

        # Medicion 1: Carga masiva completa
        t_inicio_carga = time.perf_counter()
        total_insertados = app.cargar_datos_masivos(peliculas_totales)
        t_carga = time.perf_counter() - t_inicio_carga
        print(f"[OK] Carga masiva completada: {total_insertados} documentos.")

        # Medicion 2: CRUD
        t_inicio_crud = time.perf_counter()
        app.ejecutar_crud()
        t_crud = time.perf_counter() - t_inicio_crud
        print("[OK] Operaciones CRUD secuenciales ejecutadas exitosamente.")

        # Medicion 3: Consultas
        t_inicio_consultas = time.perf_counter()
        res_consultas = app.ejecutar_consultas(NUM_CONSULTAS)
        t_consultas = time.perf_counter() - t_inicio_consultas
        total_docs_obtenidos = sum(len(r) for r in res_consultas)
        print(f"[OK] {NUM_CONSULTAS} consultas secuenciales ejecutadas ({total_docs_obtenidos} docs obtenidos).")

        t_total = time.perf_counter() - t_inicio_total

        print("\n" + "=" * 65)
        print("          RESULTADOS DE RENDIMIENTO - APLICACION SINCRONA")
        print("=" * 65)
        print(f"  (1) Carga Masiva (bulk_write {total_insertados:,} docs): {t_carga:.4f} s")
        print(f"  (2) Operaciones CRUD Secuenciales:       {t_crud:.4f} s")
        print(f"  (3) {NUM_CONSULTAS} Consultas Filtradas (limit 20):  {t_consultas:.4f} s")
        print("-" * 65)
        print(f"  [*] TIEMPO TOTAL DE OPERACIONES BD:       {t_total:.4f} s")
        print("=" * 65)

    finally:
        app.cerrar()
