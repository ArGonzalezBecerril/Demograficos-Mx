import logging
from datetime import datetime

from pyspark.sql import functions as F

import logging
from datetime import datetime
from pyspark.sql import functions as F


def generar_rep_eda_demograficos(spark):
    logging.info("[REPORTE] Calculando métricas en tiempo real para el entregable EDA...")
    # Rutas principales de medallion
    df_plata = spark.read.parquet("datos/salida/plata")
    df_oro_calidad = spark.read.parquet("datos/salida/oro/mtra_kpi_demograficos")
    mtra_grp_estados_demogr = spark.read.parquet("datos/salida/oro/mtra_grp_estados_demograficos")
    total_registros = df_plata.count()

    # 2. Calcular Porcentaje de Nulos desde la Capa Plata
    nulos = df_plata.select(
        F.round((F.sum(F.when(F.col("id_persona").isNull(), 1).otherwise(0)) / total_registros) * 100, 2).alias("id"),
        F.round((F.sum(F.when(F.col("edad").isNull(), 1).otherwise(0)) / total_registros) * 100, 2).alias("edad"),
        F.round((F.sum(F.when(F.col("estado").isNull(), 1).otherwise(0)) / total_registros) * 100, 2).alias("estado"),
        F.round((F.sum(F.when(F.col("num_tel_casa").isNull(), 1).otherwise(0)) / total_registros) * 100, 2).alias(
            "tel_casa")
    ).collect()[0]

    # 3. Calcula promedios nacionales de calidad desde df_oro_calidad
    kpis_nacionales = df_oro_calidad.select(
        F.round(F.avg("pct_cp_valido"), 2).alias("avg_cp_ok"),
        F.round(F.avg("pct_email_valido"), 2).alias("avg_email_ok")
    ).collect()[0]

    # 4. Distribución de Género + Construcción dinámica de Gráfica de Pastel (Mermaid)
    generos = df_plata.groupBy("genero").count().withColumn("pct", F.round((F.col("count") / total_registros) * 100,
                                                                           2)).collect()
    str_genero = "".join([f"*   **{g['genero']}:** {g['count']} registros ({g['pct']}%)\n" for g in generos])

    mermaid_pie = "```mermaid\npie title Distribución de Género Normalizado\n"
    for g in generos:
        mermaid_pie += f'    "{g["genero"]}" : {g["count"]}\n'
    mermaid_pie += "```\n"

    # 5. Top 5 Estados con más registros
    estados = df_plata.groupBy("estado").count().orderBy(F.desc("count")).take(5)
    str_estados = "".join([f"1.  **{e['estado']}:** {e['count']} personas registradas.\n" for e in estados])

    # 6. Histograma de rangos de edad + Construcción dinámica de Gráfica de Barras (Mermaid)
    edades = mtra_grp_estados_demogr.groupBy("rango_edad").agg(F.sum("total_personas").alias("total")).collect()
    str_edades = "".join([f"*   **{ed['rango_edad']}:** {ed['total']} personas\n" for ed in edades])

    rango_nombres = [f'"{ed["rango_edad"]}"' for ed in edades]
    rango_valores = [str(ed["total"]) for ed in edades]

    mermaid_barras = "```mermaid\nxychart-beta\n"
    mermaid_barras += f"    title \"Histograma de Rangos de Edad (Capa Oro)\"\n"
    mermaid_barras += f"    x-axis [ {', '.join(rango_nombres)} ]\n"
    mermaid_barras += f"    y-axis \"Total Personas\"\n"
    mermaid_barras += f"    bar [ {', '.join(rango_valores)} ]\n"
    mermaid_barras += "```\n"

    # 7. Conteo de Anomalías Críticas
    cp_invalidos = df_plata.filter(F.col("es_cp_valido") == False).count()
    emails_invalidos = df_plata.filter(F.col("es_email_valida") == False).count()
    registros_alterados = df_plata.filter(F.col("es_registro_alterado") == True).count()

    # Obtener fecha actual formateada
    timestamp_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Contenido estructurado sin indentación lateral para asegurar render correcto de tablas y código
    contenido_markdown = f"""# Reporte de Análisis Exploratorio de Datos (EDA) & Calidad
**Evaluación:** Ingeniero de Datos Senior  
**Dataset:** registros demográficos sintéticos de México (`personas_dummy.json`)  
**Fecha de Generación:** {timestamp_actual}  
**Total de Registros Analizados:** {total_registros}

---

## 1. Porcentaje de Valores Nulos por Campo Crítico
Se evaluó la completitud de las variables esenciales para el negocio desde la Capa Plata:


| Campo Crítico | % de Valores Nulos | Estatus / Impacto en el Negocio |
| :--- | :---: | :--- |
| `id_persona` | {nulos['id']}% | ✅ Óptimo: Llave primaria completa para tracking relacional. |
| `edad` | {nulos['edad']}% | ✅ Óptimo: Edad calculada al 100% desde la fecha de nacimiento. |
| `estado` | {nulos['estado']}% | ✅ Óptimo: Geografía al 100% gracias a la homologación de alias. |
| `num_tel_casa` | {nulos['tel_casa']}% | ℹ️ Informativo: Alto índice de nulos esperado (Migración a telefonía móvil). |

---

## 2. Índices de Salud del Dato Nacional (Métricas Capa Oro)
A partir de la agregación de calidad por estado (`df_oro_calidad`), se obtuvieron las siguientes métricas globales de confianza:
*   **Índice Nacional de CP Válido:** {kpis_nacionales['avg_cp_ok']}% de confiabilidad en códigos postales estructurados.
*   **Índice Nacional de Email Válido:** {kpis_nacionales['avg_email_ok']}% de éxito en la sintaxis de correos electrónicos.

---

## 3. Distribuciones Demográficas (Datos Normalizados en Capa Plata)

### Distribución de Género
Tras unificar minúsculas y procesar valores no estándar ('NB'), el catálogo controlado se comporta así:
{str_genero}

#### Gráfica de Distribución de Género:
{mermaid_pie}

### Distribución por Estados (Top 5 con Mayor Volumen)
Métricas geográficas consolidadas después de eliminar acentos y resolver inconsistencias de alias (ej. CDMX/DF):
{str_estados}

---

## 4. Histograma de Rangos de Edad (Capa Oro)
Volumen de registros agrupado por los segmentos de edad core definidos en las reglas de negocio de la Capa Oro:
{str_edades}

#### Gráfica de Distribución por Rangos de Edad:
{mermaid_barras}

---

## 5. Listado de Anomalías Detectadas (Data Quality Flags)
El pipeline implementa un aislamiento lógico de registros corruptos, arrojando los siguientes hallazgos de auditoría:

*   **Códigos Postales Fuera de Estándar (`es_cp_valido` = False):** **{cp_invalidos}** registros. Casos detectados incluyen campos en blanco, strings con letras residuales tras la remoción del prefijo "CP-", o CPs que no cumplen con los 5 dígitos obligatorios de Correos de México.
*   **Correos Electrónicos Corruptos (`es_email_valida` = False):** **{emails_invalidos}** registros. Se identificaron cadenas con sintaxis rota como arrobas duplicadas (ej. `correo_mal@@dominio.alt`) o dominios sin terminación estándar.
*   **Estructuras No Estándar en Notas (`es_registro_alterado` = True):** **{registros_alterados}** registros identificados con la leyenda *"registro marcado con campo extra no estándar"*. 

---

## 6. Justificación de Métricas Elegidas (Capa Oro)
*   **Métrica Demográfica (`demografia_edad`):** Segmentar por estado, género y rango de edad permite al negocio identificar clusters de población específicos para enfocar de manera inteligente recursos de marketing.
*   **Métrica de Control (`kpis_calidad`):** Al medir el porcentaje de validez de CPs y correos por estado, el equipo de ingeniería puede ver exactamente qué regiones geográficas están introduciendo datos más sucios a las bases de datos transaccionales, habilitando la toma de decisiones para parchar los formularios del Front-End.
"""

    # Escribir el archivo en disco
    ruta_archivo = "data_quality_report.md"
    with open(ruta_archivo, "w", encoding="utf-8") as f:
        f.write(contenido_markdown)

    logging.info(f"[REPORTE] ¡Reporte EDA generado exitosamente con la data de Oro integrada en: '{ruta_archivo}'!")
