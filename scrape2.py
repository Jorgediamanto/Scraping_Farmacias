from __future__ import annotations

import re
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
        browser = p.chromium.launch(headless=False, slow_mo=200)
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded")

        # Selects reales (según tu inspección)
        reg = page.locator("select[name='reg']")
        prv = page.locator("select[name='prv']")
        com = page.locator("select[name='com']")

        reg.wait_for(state="attached", timeout=30000)
        prv.wait_for(state="attached", timeout=30000)
        com.wait_for(state="attached", timeout=30000)

        print("✅ Seleccionando Regione = LAZIO")
        reg.select_option(label="LAZIO")

        # Esperar a que Provincia deje de ser solo "-"
        print("⏳ Esperando a que Provincia se cargue...")
        page.wait_for_function(
            """() => {
                const s = document.querySelector("select[name='prv']");
                if (!s) return false;
                const opts = Array.from(s.options).map(o => (o.textContent||'').trim());
                return opts.length > 1 && !opts.every(t => t === '-');
            }""",
            timeout=60000,
        )

        # Intentar seleccionar ROMA (o RM si sale como abreviatura)
        prov_opts = [clean(x) for x in prv.locator("option").all_inner_texts()]
        print("Provincia options (primeras 20):", prov_opts[:20])

        print("✅ Seleccionando Provincia = ROMA (o RM)")
        try:
            prv.select_option(label="ROMA")
        except Exception:
            prv.select_option(label="RM")

        # Esperar a que Comune deje de ser solo "-"
        print("⏳ Esperando a que Comune se cargue...")
        page.wait_for_function(
            """() => {
                const s = document.querySelector("select[name='com']");
                if (!s) return false;
                const opts = Array.from(s.options).map(o => (o.textContent||'').trim());
                return opts.length > 1 && !opts.every(t => t === '-');
            }""",
            timeout=60000,
        )

        com_opts = [clean(x) for x in com.locator("option").all_inner_texts()]
        print("Comune options (primeras 20):", com_opts[:20])

        print("✅ Seleccionando Comune = ROMA")
        com.select_option(label="ROMA")

        # Click en Cerca (puede ser input o button)
        print("🖱️ Click en Cerca")
        cerca_btn = page.locator("input[value='Cerca'], button:has-text('Cerca')").first
        cerca_btn.click()

        # Esperar tabla
        print("⏳ Esperando tabla de resultados...")
        page.wait_for_selector("table:has-text('Denominazione'):has-text('Indirizzo')", timeout=60000)
        table = page.locator("table:has-text('Denominazione'):has-text('Indirizzo')").first
        print("✅ Tabla encontrada")

        def read_current_page():
            page.wait_for_timeout(300)
            trs = table.locator("tbody tr")
            n = trs.count()
            for r in range(n):
                tr = trs.nth(r)
                tds = tr.locator("td")
                vals = [clean(tds.nth(i).inner_text()) for i in range(min(9, tds.count()))]
                rows_out.append(
                    {
                        "Denominazione": vals[0] if len(vals) > 0 else "",
                        "Indirizzo": vals[1] if len(vals) > 1 else "",
                        "CAP": vals[2] if len(vals) > 2 else "",
                        "Comune": vals[3] if len(vals) > 3 else "",
                        "Provincia": vals[4] if len(vals) > 4 else "",
                        "Regione": vals[5] if len(vals) > 5 else "",
                        "Codice_univoco": vals[6] if len(vals) > 6 else "",
                        "Partita_IVA": vals[7] if len(vals) > 7 else "",
                    }
                )

        def go_next() -> bool:
            next_btn = page.locator("button:has-text('>'), input[value='>']").first
            if next_btn.count() == 0:
                return False

            try:
                if not next_btn.is_visible():
                    return False
            except Exception:
                return False

            disabled_attr = next_btn.get_attribute("disabled")
            class_attr = (next_btn.get_attribute("class") or "").lower()
            if disabled_attr is not None or "disabled" in class_attr:
                return False

            first_before = clean(table.locator("tbody tr").first.inner_text()) if table.locator("tbody tr").count() else ""
            next_btn.click()
            try:
                page.wait_for_function(
                    """(prev) => {
                        const t = document.querySelector('table');
                        if (!t) return false;
                        const r = t.querySelector('tbody tr');
                        if (!r) return false;
                        return r.innerText.trim() !== prev;
                    }""",
                    arg=first_before,
                    timeout=15000,
                )
            except PWTimeout:
                page.wait_for_timeout(800)
            return True

        seen = set()
        while True:
            read_current_page()

            df_tmp = pd.DataFrame(rows_out)
            if df_tmp.empty:
                break

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
            rows_out = new_rows

            if not go_next():
                break

        browser.close()

    df = pd.DataFrame(rows_out).drop_duplicates(subset=["Codice_univoco", "Partita_IVA", "Indirizzo"], keep="first")
    return df


if __name__ == "__main__":
    df = scrape_roma()
    print(f"Filas extraídas: {len(df)}")

    df.to_csv("farmacie_roma.csv", index=False, encoding="utf-8-sig")
    df.to_excel("farmacie_roma.xlsx", index=False)
    print("Guardado: farmacie_roma.csv y farmacie_roma.xlsx")
