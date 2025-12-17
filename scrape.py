from __future__ import annotations

import re
import time
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.salute.gov.it/CercaFarmacie/Ricerca#FINE"

def clean(txt: str) -> str:
    if txt is None:
        return ""
    return re.sub(r"\s+", " ", txt).strip()

def scrape_roma() -> pd.DataFrame:
    rows_out: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # pon False si quieres ver el navegador
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded")

        # 1) Seleccionar Comune = ROMA (el dropdown suele estar arriba)
        #    Intentamos por label "Comune:" y si falla, por el propio select.
        try:
            page.get_by_label("Comune:").select_option(label="ROMA")
        except PWTimeout:
            # fallback: cualquier select que contenga la opción ROMA
            selects = page.locator("select")
            found = False
            for i in range(selects.count()):
                s = selects.nth(i)
                if s.locator("option", has_text="ROMA").count() > 0:
                    s.select_option(label="ROMA")
                    found = True
                    break
            if not found:
                raise RuntimeError("No he podido encontrar el selector de Comune para elegir ROMA.")

        # 2) Click en "Cerca"
        page.get_by_role("button", name=re.compile(r"^Cerca$", re.I)).click()

        # 3) Esperar a que aparezca la tabla de resultados (cabecera con Denominazione / Indirizzo)
        page.wait_for_selector("table:has-text('Denominazione'):has-text('Indirizzo')", timeout=30000)
        table = page.locator("table:has-text('Denominazione'):has-text('Indirizzo')").first

        # Helpers para paginación
        def read_current_page():
            page.wait_for_timeout(300)  # pequeño respiro para render
            trs = table.locator("tbody tr")
            n = trs.count()
            for r in range(n):
                tr = trs.nth(r)
                tds = tr.locator("td")
                # Esperamos columnas: Denominazione | Indirizzo | CAP | Comune | Provincia | Regione | Codice univoco | Partita IVA | Dettaglio
                # (si cambian, ajustamos índices)
                vals = [clean(tds.nth(i).inner_text()) for i in range(min(9, tds.count()))]

                # algunos "indirizzo" incluyen icono/pin: el inner_text suele venir bien igualmente
                rec = {
                    "Denominazione": vals[0] if len(vals) > 0 else "",
                    "Indirizzo": vals[1] if len(vals) > 1 else "",
                    "CAP": vals[2] if len(vals) > 2 else "",
                    "Comune": vals[3] if len(vals) > 3 else "",
                    "Provincia": vals[4] if len(vals) > 4 else "",
                    "Regione": vals[5] if len(vals) > 5 else "",
                    "Codice_univoco": vals[6] if len(vals) > 6 else "",
                    "Partita_IVA": vals[7] if len(vals) > 7 else "",
                }
                rows_out.append(rec)

        def go_next() -> bool:
            # En tu captura se ve un paginador con botones: <<  <  >  >>
            # Buscamos el botón ">" y comprobamos si está deshabilitado.
            next_btn = page.get_by_role("button", name=re.compile(r"^>$"))
            if next_btn.count() == 0:
                # fallback si no es role=button (a veces son input)
                next_btn = page.locator("text='>'").first

            # Si está disabled o no es visible, fin
            try:
                if not next_btn.is_visible():
                    return False
            except Exception:
                return False

            # Algunas webs deshabilitan con atributo disabled o clase
            disabled_attr = next_btn.get_attribute("disabled")
            class_attr = (next_btn.get_attribute("class") or "").lower()
            if disabled_attr is not None or "disabled" in class_attr:
                return False

            # Click y esperar a que cambie el contenido (heurística: cambia el primer nombre de farmacia)
            first_before = clean(table.locator("tbody tr").first.inner_text()) if table.locator("tbody tr").count() else ""
            next_btn.click()
            try:
                page.wait_for_function(
                    """(sel, prev) => {
                        const t = document.querySelector(sel);
                        if (!t) return false;
                        const r = t.querySelector('tbody tr');
                        if (!r) return false;
                        return r.innerText.trim() !== prev;
                    }""",
                    arg=["table", first_before],
                    timeout=15000,
                )
            except PWTimeout:
                # si no detecta cambio, igual cambió poco; continuamos con una pausa
                page.wait_for_timeout(800)
            return True

        # 4) Iterar páginas
        seen = set()
        while True:
            read_current_page()
            # Deduplicación básica por clave (por si la paginación repite por lag)
            df_tmp = pd.DataFrame(rows_out)
            df_tmp["__k"] = (
                df_tmp["Codice_univoco"].fillna("")
                + "|"
                + df_tmp["Partita_IVA"].fillna("")
                + "|"
                + df_tmp["Indirizzo"].fillna("")
            )
            new_rows = []
            for _, row in df_tmp.iterrows():
                k = row["__k"]
                if k in seen:
                    continue
                seen.add(k)
                new_rows.append({c: row[c] for c in df_tmp.columns if c != "__k"})
            rows_out = new_rows  # mantenemos solo únicos

            if not go_next():
                break

        browser.close()

    df = pd.DataFrame(rows_out)
    # Limpieza final
    df = df.drop_duplicates(subset=["Codice_univoco", "Partita_IVA", "Indirizzo"], keep="first")
    return df

if __name__ == "__main__":
    df = scrape_roma()
    print(f"Filas extraídas: {len(df)}")

    # Guardar outputs
    df.to_csv("farmacie_roma.csv", index=False, encoding="utf-8-sig")
    df.to_excel("farmacie_roma.xlsx", index=False)
    print("Guardado: farmacie_roma.csv y farmacie_roma.xlsx")
