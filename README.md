# Práctica: Spark

## Datos generales

**Alumno:** Hernández Huerta Ari Leonardo

**Materia:** Manejo Masivo de Datos

**Profesor:** José Javier Moya Moya

**Grupo:** IDIA222

**Universidad:** Universidad Politécnica de Querétaro

**Fecha:** 18 / 11 / 2026

---

# Objetivo

Implementar y ejecutar análisis de datos utilizando Apache Spark en un entorno Docker con Jupyter Notebook, procesando conjuntos de datos mediante operaciones sobre RDDs.

---

# Requisitos previos

Antes de comenzar, es necesario contar con:

- Docker Desktop instalado y en ejecución.
- Conexión a Internet para descargar la imagen de Docker y clonar el repositorio.
- Git instalado en el equipo local.

---

# Despliegue del entorno

## 1. Crear carpeta de trabajo

Desde una terminal local:

```bash
mkdir spark-local
cd spark-local
```

---

## 2. Ejecutar contenedor Docker con Jupyter y PySpark

### Linux o PowerShell

```bash
docker run -it -p 8888:8888 -p 4040:4040 \
-v ${pwd}:/home/jovyan/work/projects/ \
jupyter/pyspark-notebook
```

### CMD (Símbolo del sistema de Windows)

```cmd
docker run -it -p 8888:8888 -p 4040:4040 \
-v "%cd%:/home/jovyan/work/projects/" \
jupyter/pyspark-notebook
```

### Explicación

- **8888**: Puerto utilizado por Jupyter Lab.
- **4040**: Puerto utilizado por la interfaz web de Spark.
- **-v**: Permite montar una carpeta local dentro del contenedor.

---

## 3. Acceder a Jupyter

Al finalizar la creación del contenedor aparecerá una salida similar a:

```text
http://127.0.0.1:8888/lab?token=xxxxxxxxxxxxxxxxxxxxxxxx
```

Abrir dicho enlace en el navegador para acceder directamente a Jupyter Lab.

---

# Preparación del proyecto

## 1. Abrir una terminal en Jupyter

Dentro de Jupyter:

1. Pulsar el botón **"+"** ubicado en la parte superior izquierda.
2. Desplazarse hasta la sección **Other**.
3. Seleccionar **Terminal**.

---

## 2. Clonar el repositorio

En la terminal de Jupyter ejecutar:

```bash
git clone https://github.com/AriLeo-droid/Spark-practica.git
```

Posteriormente ingresar a la carpeta descargada:

```bash
cd Spark-practica
```

---

## 3. Crear carpeta para resultados

Ejecutar:

```bash
mkdir -p RESULTADOS
```

Esta carpeta será utilizada por Spark para almacenar los archivos de salida generados durante la ejecución de la práctica.

---

# Estructura del proyecto

```text
.
├── DATOS/
├── work/
├── Ejercicios.ipynb
├── Proyecto.ipynb
├── requirements.txt
└── RESULTADOS/
```

---

# Ejecución

1. Abrir el archivo **Ejercicios.ipynb** para realizar las actividades guiadas.
2. Abrir el archivo **Proyecto.ipynb** para ejecutar el análisis completo.
3. Ejecutar las celdas secuencialmente.
4. Verificar que los resultados sean almacenados dentro de la carpeta:

```text
RESULTADOS/
```

---

# Interfaz de Spark

Durante la ejecución de transformaciones y acciones de Spark se puede consultar la interfaz web en:

```text
http://localhost:4040
```

Esta interfaz permite monitorear:

- Jobs ejecutados.
- Stages.
- Tasks.
- Uso de memoria.
- Tiempo de ejecución.

---

# Dependencias

Instalar dependencias mediante:

```bash
pip install -r requirements.txt
```

---

# Conclusiones

Spark resulta muy útil para la optimización en los procesos que tengan qué ver con operaciones con datos, sean estos provenientes de Dataframes de gran peso o simples pruebas con listas de tuplas creadas para experimentar. 

---

# Preguntas para reflexionar

## Eficiencia: ¿Por qué es importante hacer `split()` una sola vez por línea?, ¿Cuánto tiempo ahorrarías con 1 millón de líneas?
### Respuesta

Es importante porque `split()` es una operación relativamente costosa cuando se ejecuta millones de veces. En el ejercicio de optimización se observó que realizar tres llamadas a `split()` por cada línea implica dividir repetidamente la misma cadena para obtener columnas diferentes. En cambio, al realizar un único `split()` y reutilizar el resultado, se reduce el trabajo realizado por Spark sobre cada registro.
Si el conjunto de datos creciera hasta 1 millón de líneas, la diferencia sería significativa, ya que pasaríamos de realizar aproximadamente 3 millones de divisiones de cadenas a solo 1 millón. Aunque el ahorro exacto depende del hardware y del tamaño de los registros, se reduciría aproximadamente dos tercios del trabajo asociado al procesamiento de texto.


---

## Particionamiento: ¿Cuántas particiones tiene tu RDD?, ¿Cómo afecta esto al rendimiento?, ¿Qué pasaría si tuvieras solo una partición?
### Respuesta


La sesión fue ejecutada en modo local `(local[*])`, permitiendo que Spark utilice todos los núcleos disponibles del equipo. El número exacto de particiones puede consultarse mediante:
`rdd.getNumPartitions()`
El particionamiento es importante porque Spark distribuye el trabajo entre las particiones disponibles. Cuantas más particiones adecuadamente distribuidas existan, mejor puede aprovecharse el paralelismo.
Si el RDD tuviera solamente una partición, todas las transformaciones y acciones serían procesadas por una única tarea, desaprovechando el paralelismo y aumentando considerablemente el tiempo de ejecución en conjuntos de datos grandes.


---

## Lazy Evaluation: ¿En qué momento se ejecutaron realmente las transformaciones?, ¿Qué ventajas tiene este modelo?
### Respuesta

Las transformaciones (`map, filter, reduceByKey, sortBy`, etc.) no se ejecutan inmediatamente cuando son declaradas. Spark únicamente construye un plan de ejecución. La ejecución real ocurre cuando se invoca una acción como:
`collect()`
`count()`
`first()`
`take()`
`saveAsTextFile()`
las cuales aparecen repetidamente a lo largo de la práctica.
La principal ventaja es que Spark puede optimizar el flujo de procesamiento 

---

## Memoria: ¿Qué pasaría si intentaras hacer `collect()` en un RDD de 10GB?, ¿Cómo evitarías este problema?
### Respuesta

`collect()` transfiere todos los datos desde los ejecutores hacia el Driver. En un RDD de 10 GB podría provocar un consumo excesivo de memoria e incluso terminar en errores por falta de memoria. Este problema ya fue señalado en el ejercicio de depuración, donde se menciona que `collect()` puede resultar muy pesado en datasets grandes.
Para evitarlo conviene utilizar:
`take(n)`
`first()`
`count()`
o realizar agregaciones distribuidas con Spark para traer únicamente los resultados resumidos en lugar del conjunto completo de datos.

---

## Optimización: Si tuvieras que procesar 100x más datos, ¿Qué cambiarías en tu código?, ¿Usarías más particiones?, ¿Cachearías algún RDD?
### Respuesta

Si el volumen creciera cien veces, aplicaría varias optimizaciones:

- Mantener el uso de una sola operación `split()` por registro.
- Incrementar el número de particiones para aprovechar mejor el paralelismo.
- Evitar el uso innecesario de `collect()`.
- Utilizar `cache()` o `persist()` sobre RDDs reutilizados varias veces, como los RDDs ya parseados.
- Mantener agregaciones distribuidas mediante `reduceByKey()` y `aggregateByKey()`.
Estas medidas reducirían el tiempo de procesamiento y el consumo de recursos.

---

## HDFS vs Local: ¿Cuáles son las ventajas de usar HDFS en lugar de archivos locales para este análisis?
### Respuesta

En esta práctica los archivos fueron leídos desde el sistema local utilizando rutas absolutas con el prefijo `file://`.
Sin embargo, HDFS presenta varias ventajas:

- Permite almacenar datasets mucho más grandes que los que caben en una sola máquina.
- Distribuye automáticamente los datos entre varios nodos.
- Proporciona tolerancia a fallos mediante replicación.
- Reduce la necesidad de mover grandes volúmenes de información por la red.
- Permite escalar el análisis aumentando el número de nodos del clúster.
Para datasets pequeños, como los utilizados en esta práctica, el almacenamiento local resulta suficiente. Sin embargo, para escenarios reales de Big Data, HDFS ofrece mayor rendimiento, disponibilidad y escalabilidad.
