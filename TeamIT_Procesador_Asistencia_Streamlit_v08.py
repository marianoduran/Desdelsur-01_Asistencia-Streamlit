# ===============================================================================
#  Script Name:        TeamIT_Procesador_Asistencia_Streamlit_vXX.py
#  Author:             Mariano Duran
#  Created Date:       01-10-2025
#  Last Modified:      01-10-2025
# ===============================================================================
#  Description:
#   Este script procesa los registros capturados en el sistema de ingreso / egreso
#   y lo formatea al disenio definido por DesdelSur
#
#  Usage:
#   streamlit run TeamIT_Procesador_Asistencia_Streamlit_vXX.py
#
#  Parameters:
#   --input     Archivo con informacion de reloj y calendario en el mismo path
#   --output    Archivo Excel
#
#  Dependencies:
#   - platform, psutil
#   - pandas
#   - openpyxl, numpy
#   - re (Python built-in)
#
#
#  Change control history:
#   - 01-10-2025:  Mariano Duran - v0.08 
#       - Initial creation of script. Alineado con la versión stand alone
#   - 03-10-2025:  Mariano Duran - v0.08 
#       - Agregado de logo, icono, sidebar, page 02 TBD para futuras mejoras
# ===============================================================================


import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import datetime
from PIL import Image
from lib.common import LOGO_URL, header


# ===============================
# Funciones (adaptadas de v08)
# ===============================

def load_csv_to_pandas(file_like) -> pd.DataFrame:
    return pd.read_csv(file_like)

def remove_false_identifications(df: pd.DataFrame, pseconds: int) -> pd.DataFrame:
    df = df.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df = df.sort_values(["Usuarios", "Fecha"]).reset_index(drop=True)
    df["_gap_s"] = df.groupby("Usuarios")["Fecha"].diff().dt.total_seconds()
    df["_new_cluster"] = df["_gap_s"].isna() | (df["_gap_s"] >= pseconds)
    df["_cluster"] = df.groupby("Usuarios")["_new_cluster"].cumsum()
    idx_keep = df.groupby(["Usuarios", "_cluster"])["Fecha"].idxmax()
    out = (df.loc[idx_keep]
             .sort_values(["Usuarios", "Fecha"])
             .drop(columns=["_gap_s", "_new_cluster", "_cluster"])
             .reset_index(drop=True))
    return out

def filter_pandas_auth_succeeded(df: pd.DataFrame) -> pd.DataFrame:
    f = df[df["Evento"].str.contains(r"1:N authentication succeeded \(Face\)", na=False)].copy()
    f["Fecha"] = pd.to_datetime(f["Fecha"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    f["CodigoUsuario"] = f["Usuarios"].str.extract(r"^(\d+)\(")
    f["NombreUsuario"] = f["Usuarios"].str.extract(r"\((.+)\)")
    return f[["Fecha", "Usuarios", "CodigoUsuario", "NombreUsuario"]]

def elapsed_time_per_day(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    assert pd.api.types.is_datetime64_any_dtype(d["Fecha"]), "Fecha debe ser datetime"
    d["Fecha_dia"] = d["Fecha"].dt.date

    rows = []
    for (cod, nombre, dia), g in d.groupby(["CodigoUsuario","NombreUsuario","Fecha_dia"]):
        g = g.sort_values("Fecha").reset_index(drop=True)
        total = pd.Timedelta(0)
        for i in range(0, len(g)-1, 2):
            total += g.loc[i+1,"Fecha"] - g.loc[i,"Fecha"]
        hora_ing = g["Fecha"].min()
        hora_egr = g["Fecha"].max()
        rows.append({
            "CodigoUsuario": cod,
            "Usuario": nombre,
            "Fecha": pd.to_datetime(dia),
            "HoraIngreso_hms": hora_ing.strftime("%H:%M:%S"),
            "HoraIngreso": round(hora_ing.hour + hora_ing.minute/60 + hora_ing.second/3600, 2),
            "HoraEgreso_hms": hora_egr.strftime("%H:%M:%S"),
            "HoraEgreso": round(hora_egr.hour + hora_egr.minute/60 + hora_egr.second/3600, 2),
            "Total_elapsed_hours": round(total.total_seconds()/3600.0, 2),
            "Total_elapsed_hms": str(total).split(".")[0],
            "Cantidad_registros": len(g),
            "Marca": "PAR" if len(g)%2==0 else "IMPAR"
        })
    return pd.DataFrame(rows)

def add_horas_dia(detalle_df: pd.DataFrame, horas_df: pd.DataFrame) -> pd.DataFrame:
    detalle = detalle_df.copy()
    horas   = horas_df.copy()
    detalle["Fecha"] = pd.to_datetime(detalle["Fecha"], errors="coerce")
    horas["Fecha"]   = pd.to_datetime(horas["Fecha"], errors="coerce")
    detalle["_Fecha_dia"] = detalle["Fecha"].dt.date
    horas["_Fecha_dia"]   = horas["Fecha"].dt.date
    horas_merge = horas[["_Fecha_dia", "Horas", "DiaSemana"]].drop_duplicates("_Fecha_dia")
    detalle = detalle.merge(horas_merge, on="_Fecha_dia", how="left").drop(columns=["_Fecha_dia"])
    detalle = detalle.rename(columns={"Horas": "HorasDiaSemana"})
    return detalle

def enrich_with_missing_dates(vraw: pd.DataFrame, vhs: pd.DataFrame) -> pd.DataFrame:
    vraw = vraw.copy()
    vraw["Fecha"] = pd.to_datetime(vraw["Fecha"], errors="coerce", dayfirst=True)
    vhs = vhs.copy()
    vhs["Fecha"] = pd.to_datetime(vhs["Fecha"], errors="coerce", dayfirst=True)
    usuarios = vraw[["CodigoUsuario","Usuario"]].drop_duplicates()
    fechas = vhs[["Fecha"]].drop_duplicates()
    usuarios["key"] = 1; fechas["key"] = 1
    full = pd.merge(usuarios, fechas, on="key").drop("key", axis=1)
    enriched = pd.merge(full, vraw, on=["CodigoUsuario","Usuario","Fecha"], how="left", suffixes=("", "_orig"))
    enriched = pd.merge(enriched, vhs, on="Fecha", how="left")
    if "HorasDiaSemana" in enriched.columns:
        enriched = enriched.drop(columns=["HorasDiaSemana"])
    if "DiaSemana_x" in enriched.columns:
        enriched = enriched.drop(columns=["DiaSemana_x"])
    enriched = enriched.rename(columns={"DiaSemana_y": "DiaSemana", "Horas": "HorasDiaSemana"})
    enriched.loc[enriched["Total_elapsed_hours"].isna(), "Diferencia_Horas"] = (0 - enriched["HorasDiaSemana"])
    col = enriched.pop("Diferencia_Horas")
    enriched["Diferencia_Horas"] = col
    return enriched

def resumen_totales_con_total(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["Fecha"] = pd.to_datetime(d["Fecha"], errors="coerce", dayfirst=True)
    for col in ["Total_elapsed_hours", "HorasDiaSemana", "Diferencia_Horas"]:
        if col in d.columns:
            d[col] = d[col].fillna(0)
    out = (d.groupby(["CodigoUsuario","Usuario"], as_index=False)
             .agg(**{
                 "Min(Fecha)": ("Fecha","min"),
                 "Max(Fecha)": ("Fecha","max"),
                 "SUM(Total_elapsed_hours)": ("Total_elapsed_hours","sum"),
                 "SUM(HorasDiaSemana)": ("HorasDiaSemana","sum"),
                 "SUM(Diferencia_Horas)": ("Diferencia_Horas","sum")
             }).round(2).sort_values("Usuario", kind="stable"))
    out["Min(Fecha)"] = out["Min(Fecha)"].dt.date
    out["Max(Fecha)"] = out["Max(Fecha)"].dt.date
    total_row = {
        "CodigoUsuario": "TOTAL", "Usuario": "TOTAL",
        "Min(Fecha)": out["Min(Fecha)"].min(),
        "Max(Fecha)": out["Max(Fecha)"].max(),
        "SUM(Total_elapsed_hours)": out["SUM(Total_elapsed_hours)"].sum(),
        "SUM(HorasDiaSemana)": out["SUM(HorasDiaSemana)"].sum(),
        "SUM(Diferencia_Horas)": out["SUM(Diferencia_Horas)"].sum(),
    }
    return pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)

def make_pivot_like_sheet(df: pd.DataFrame, writer: pd.ExcelWriter, sheet_name: str = "Resumen x dia"):
    # Escribe una hoja “formateada” similar a build_pivot_table usando xlsxwriter
    df = df.copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
    df["Fecha_dia"] = df["Fecha"].dt.normalize()
    dias_es = {0:"lunes",1:"martes",2:"miércoles",3:"jueves",4:"viernes",5:"sábado",6:"domingo"}
    if "DiaSemana" in df.columns and df["DiaSemana"].notna().any():
        df["DiaSemana_lbl"] = df["DiaSemana"].astype(str).str.strip()
        df.loc[df["DiaSemana_lbl"].eq("") | df["DiaSemana_lbl"].isna(),"DiaSemana_lbl"] = df["Fecha"].dt.weekday.map(dias_es)
    else:
        df["DiaSemana_lbl"] = df["Fecha"].dt.weekday.map(dias_es)

    fechas = sorted(df["Fecha_dia"].dropna().unique())
    day_to_label = (df.loc[df["Fecha_dia"].isin(fechas), ["Fecha_dia","DiaSemana_lbl"]]
                      .drop_duplicates(subset=["Fecha_dia"])
                      .set_index("Fecha_dia")["DiaSemana_lbl"].to_dict())
    key_cols = ["CodigoUsuario","Usuario","Fecha_dia"]
    value_cols = ["HoraIngreso","HoraEgreso","Total_elapsed_hours"]
    lookup = {
        (row.CodigoUsuario, row.Usuario, row.Fecha_dia): {
            "HoraIngreso": row.HoraIngreso, "HoraEgreso": row.HoraEgreso,
            "Total_elapsed_hours": row.Total_elapsed_hours
        }
        for row in df[key_cols + value_cols].itertuples(index=False)
    }
    users = df[["CodigoUsuario","Usuario"]].drop_duplicates(keep="first").to_records(index=False)

    wb = writer.book
    ws = wb.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = ws

    fmt_day    = wb.add_format({"bold": True, "align":"center", "valign":"vcenter", "border":1})
    fmt_day_we = wb.add_format({"bold": True, "align":"center", "valign":"vcenter", "border":1,
                                "bg_color":"#FF0000", "font_color":"#FFFFFF"})
    fmt_hdr    = wb.add_format({"bold": True, "align":"center", "valign":"vcenter", "border":1})
    fmt_id     = wb.add_format({"border":1, "align":"center"})
    fmt_name   = wb.add_format({"border":1, "align":"left"})
    fmt_cell   = wb.add_format({"border":1, "align":"center"})
    fmt_hours  = wb.add_format({"border":1, "align":"center", "bg_color":"#C0C0C0", "num_format":"0.00"})

    start_day_col = 3
    header_row_day, header_row_sub, data_start_row = 1, 2, 4
    ws.set_column(0, 0, 14)  # CodigoUsuario
    ws.set_column(1, 1, 24)  # Usuario

    # Encabezados por día
    for d_idx, fecha in enumerate(fechas):
        c0 = start_day_col + d_idx*3
        c1, c2 = c0+1, c0+2
        ws.set_column(c0, c0, 10)
        ws.set_column(c1, c1, 10)
        ws.set_column(c2, c2, 12)
        lbl = str(day_to_label.get(fecha, "")).strip().lower()
        day_num = pd.to_datetime(fecha).day
        text = f"{day_num} - {lbl}"
        is_weekend = lbl.startswith(("sábado","domingo"))
        ws.merge_range(header_row_day, c0, header_row_day, c2, text, fmt_day_we if is_weekend else fmt_day)
        ws.write(header_row_sub, c0, "I", fmt_hdr)
        ws.write(header_row_sub, c1, "S", fmt_hdr)
        ws.write(header_row_sub, c2, "Horas", fmt_hdr)

    # Columna total
    total_col = start_day_col + 3*len(fechas)
    ws.set_column(total_col, total_col, 16)
    ws.write(header_row_day, total_col, "", fmt_day)
    ws.write(header_row_sub, total_col, "TOTAL HS PLAN", fmt_hdr)

    # Filas de datos
    for r_idx, (codigo, usuario) in enumerate(users):
        r = data_start_row + r_idx
        ws.write(r, 0, codigo,  fmt_id)
        ws.write(r, 1, usuario, fmt_name)
        row_total = 0.0
        for d_idx, fecha in enumerate(fechas):
            c0 = start_day_col + d_idx*3
            rec = lookup.get((codigo, usuario, fecha))
            if rec is None:
                ws.write(r, c0,   "", fmt_cell)
                ws.write(r, c0+1, "", fmt_cell)
                ws.write_number(r, c0+2, 0.0, fmt_hours)
                continue
            hi = rec.get("HoraIngreso")
            he = rec.get("HoraEgreso")
            th = rec.get("Total_elapsed_hours")
            ws.write(r, c0,   "" if pd.isna(hi) else str(hi), fmt_cell)
            ws.write(r, c0+1, "" if pd.isna(he) else str(he), fmt_cell)
            val = 0.0 if pd.isna(th) else float(th)
            row_total += val
            ws.write_number(r, c0+2, val, fmt_hours)
        ws.write_number(r, total_col, row_total, fmt_hours)

    ws.set_landscape()
    ws.set_margins(0.4, 0.4, 0.5, 0.5)

def build_outputs(calendar_df: pd.DataFrame, reloj_df: pd.DataFrame, min_gap_seconds: int):
    # 1) Filtro “succeeded”
    vdf_filtered = filter_pandas_auth_succeeded(reloj_df)

    # 2) Colapsar marcas separadas por < min_gap_seconds
    vdf_wo_false_ident = remove_false_identifications(vdf_filtered, min_gap_seconds)
    vdf_wo_false_ident_sorted = vdf_wo_false_ident.sort_values(["CodigoUsuario","Fecha"]).reset_index(drop=True)

    # 3) Tiempo trabajado por día y persona
    vdf_elapsed_time_day = elapsed_time_per_day(vdf_wo_false_ident_sorted)

    # 4) Agregar horas planificadas del Calendario
    vdf_hs_day = calendar_df.copy()
    vdf_hs_day["Fecha"] = pd.to_datetime(vdf_hs_day["Fecha"], errors="coerce", dayfirst=True)
    vdf_hs_day = vdf_hs_day.sort_values("Fecha")
    vdf_plus_calendar = add_horas_dia(vdf_elapsed_time_day, vdf_hs_day)
    vdf_plus_calendar = vdf_plus_calendar.sort_values(by=["Usuario","Fecha"])

    # 5) Diferencia vs plan
    vdf_plus_calendar["Diferencia_Horas"] = (vdf_plus_calendar["Total_elapsed_hours"] - vdf_plus_calendar["HorasDiaSemana"]).round(2)

    # 6) Completar fechas faltantes a partir del calendario
    vdf_enriched = enrich_with_missing_dates(vdf_plus_calendar, vdf_hs_day)
    vdf_enriched = vdf_enriched.sort_values(by=["Usuario","Fecha"])

    # 7) Descontar 1h de almuerzo si trabajó > 8h
    vdf_enriched["Total_elapsed_hours"] = np.where(
        vdf_enriched["Total_elapsed_hours"].notna() & (vdf_enriched["Total_elapsed_hours"] > 8),
        vdf_enriched["Total_elapsed_hours"] - 1,
        vdf_enriched["Total_elapsed_hours"]
    )

    # 8) Resumen totales
    vdf_resumen_totales = resumen_totales_con_total(vdf_enriched)

    # 9) Excel principal (todas las hojas + hoja “Resumen x dia” formateada)
    output_name = "Resumen_Reporte_Reloj_v1_" + datetime.now().strftime("%Y%m%d_%H%M") + ".xlsx"
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        # hojas con datos crudos y enriquecidos
        vdf_resumen_totales.to_excel(writer, sheet_name="Resumen", index=False)
        vdf_enriched.to_excel(writer, sheet_name="Detalle Usuario x dia Cal", index=False)
        vdf_plus_calendar.to_excel(writer, sheet_name="Detalle Usuario x dia", index=False)
        vdf_wo_false_ident_sorted.to_excel(writer, sheet_name="Datos Reloj GT 30 seg", index=False)
        vdf_filtered.to_excel(writer, sheet_name="Datos Reloj Ok", index=False)
        reloj_df.to_excel(writer, sheet_name="Datos Reloj", index=False)
        vdf_hs_day.to_excel(writer, sheet_name="Calendario", index=False)

        # hoja estilo “pivot”
        # para esa hoja necesitamos columnas HoraIngreso/HoraEgreso/Total_elapsed_hours por día:
        pivot_input = vdf_enriched.copy()
        # Si no existen estas 3 columnas, intentamos traerlas del plus_calendar:
        for c in ["HoraIngreso","HoraEgreso","Total_elapsed_hours"]:
            if c not in pivot_input.columns and c in vdf_plus_calendar.columns:
                pivot_input[c] = vdf_plus_calendar.set_index(["CodigoUsuario","Usuario","Fecha"])[c] \
                    .reindex(pivot_input.set_index(["CodigoUsuario","Usuario","Fecha"]).index).values
        make_pivot_like_sheet(pivot_input, writer, sheet_name="Resumen x dia")

    bio.seek(0)
    return output_name, bio, vdf_resumen_totales, vdf_enriched

# ===============================
# UI Streamlit
# ===============================
st.set_page_config(
    page_title="Procesador de Asistencia Zarate",
    page_icon="🫘"
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.image(LOGO_URL, width=70)

# Oculta la navegación nativa de Streamlit en la sidebar
st.markdown("""
<style>
/* Sidebar native nav */
section[data-testid="stSidebarNav"] { display: none !important; }
div[data-testid="stSidebarNav"]     { display: none !important; } /* fallback */
</style>
""", unsafe_allow_html=True)


st.sidebar.page_link("TeamIT_Procesador_Asistencia_Streamlit_v08.py",
                     label="01 - Procesador de Asistencia Zarate")
st.sidebar.page_link("pages/02_a_definir.py",
                     label="02 - A definir")


# Page content
header(st, "Procesador de Asistencia Zarate")

st.markdown("""
Subí el **Calendario** (CSV con columnas *Fecha, Horas, DiaSemana*) y el/los **Reloj** (CSV con columnas como *Fecha, Usuarios, Evento*, etc.).  
Elegí el **mínimo de segundos** para consolidar ingresos/egresos muy próximas (por defecto 30).  
Luego descargá el Excel generado.
""")

st.markdown("""
<style>
/* Ajusta el ancho del number_input a ~180px */
div[data-testid="stNumberInput"] {width: 180px;}
div[data-testid="stNumberInput"] > div {width: 180px;}
</style>
""", unsafe_allow_html=True)

col1, col2 = st.columns(2)
with col1:
    min_gap_seconds = st.number_input("Segundos mínimos entre ingresos/egresos (para descartar falsos ingresos/egresos)", min_value=0, value=30, step=1)
with col2:
    st.write("")  # spacer

calendar_file = st.file_uploader("Archivo de **Calendario** (CSV)", type=["csv"], accept_multiple_files=False)
reloj_files = st.file_uploader("Archivo(s) de **Reloj** (CSV)", type=["csv"], accept_multiple_files=True)

if st.button("Procesar"):
    if not calendar_file or not reloj_files:
        st.error("Falta subir el Calendario y al menos un archivo de Reloj.")
    else:
        try:
            cal_df = load_csv_to_pandas(calendar_file)
            raw_list = [load_csv_to_pandas(f) for f in reloj_files]
            reloj_df = pd.concat(raw_list, ignore_index=True) if len(raw_list) > 1 else raw_list[0]

            out_name, out_bytes, resumen_df, detalle_df = build_outputs(cal_df, reloj_df, min_gap_seconds)

            st.success("Procesamiento completado.")
            st.download_button(
                label="⬇️ Descargar Excel",
                data=out_bytes,
                file_name=out_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            with st.expander("Ver 'Resumen' (preview)"):
                st.dataframe(resumen_df.head(200), use_container_width=True)

            with st.expander("Ver 'Detalle Usuario x día Cal' (preview)"):
                st.dataframe(detalle_df.head(200), use_container_width=True)

        except Exception as e:
            st.exception(e)
