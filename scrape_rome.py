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
    all_rows: list[list[str]] = []

    with sync_playwright() as p:
        # MÁS RÁPIDO: headless + sin slow_mo
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Un poco más de margen a la red
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        # Selects reales
        reg = page.locator("select[name='reg']")
        prv = page.locator("select[name='prv']")
        com = page.locator("select[name='com']")

        reg.wait_for(state="attached", timeout=30000)

        # Regione -> LAZIO
        reg.select_option(label="LAZIO")

        # Esperar a que Provincia se cargue
        page.wait_for_function(
            """() => {
                const s = document.querySelector("select[name='prv']");
                if (!s) return false;
                const opts = Array.from(s.options).map(o => (o.textContent||'').trim());
                return opts.length > 1 && !opts.every(t => t === '-');
            }""",
            timeout=60000,
        )

        # Provincia -> ROMA (o RM)
        try:
            prv.select_option(label="ROMA")
        except Exception:
            prv.select_option(label="RM")

        # Esperar a que Comune se cargue
        page.wait_for_function(
            """() => {
                const s = document.querySelector("select[name='com']");
                if (!s) return false;
                const opts = Array.from(s.options).map(o => (o.textContent||'').trim());
                return opts.length > 1 && !opts.every(t => t === '-');
            }""",
            timeout=60000,
        )

        # Comune -> ROMA
        com.select_option(label="ROMA")

        # Click en Cerca (input o button)
        page.locator("input[value='Cerca'], button:has-text('Cerca')").first.click()

        # Esperar tabla
        page.wait_for_selector("table:has-text('Denominazione'):has-text('Indirizzo')", timeout=60000)

        # Extraer filas de la tabla en BLOQUE (mucho más rápido que inner_text celda a celda)
        def extract_table_rows() -> list[list[str]]:
            return page.evaluate(
                """() => {
                    const tables = Array.from(document.querySelectorAll("table"));
                    const t = tables.find(tb =>
                        tb.innerText.includes("Denominazione") && tb.innerText.includes("Indirizzo")
                    );
                    if (!t) return [];
                    const rows = Array.from(t.querySelectorAll("tbody tr"));
                    return rows.map(tr =>
                        Array.from(tr.querySelectorAll("td")).slice(0, 8).map(td =>
                            (td.innerText || "").replace(/\\s+/g, " ").trim()
                        )
                    );
                }"""
            )

        def click_next_and_wait() -> bool:
            # Botón siguiente: ">" (puede ser button o input)
            next_btn = page.locator("button:has-text('>'), input[value='>']").first
            if next_btn.count() == 0:
                return False

            # Si está deshabilitado
            disabled = next_btn.get_attribute("disabled")
            cls = (next_btn.get_attribute("class") or "").lower()
            if disabled is not None or "disabled" in cls:
                return False

            # Heurística: guardar primera fila antes y esperar a que cambie tras el click
            before = page.evaluate(
                """() => {
                    const t = Array.from(document.querySelectorAll("table"))
                      .find(tb => tb.innerText.includes("Denominazione") && tb.innerText.includes("Indirizzo"));
                    const r = t?.querySelector("tbody tr");
                    return r ? r.innerText.trim() : "";
                }"""
            )

            next_btn.click()

            try:
                page.wait_for_function(
                    """(prev) => {
                        const t = Array.from(document.querySelectorAll("table"))
                          .find(tb => tb.innerText.includes("Denominazione") && tb.innerText.includes("Indirizzo"));
                        const r = t?.querySelector("tbody tr");
                        if (!r) return false;
                        return r.innerText.trim() !== prev;
                    }""",
                    arg=before,
                    timeout=20000,
                )
            except PWTimeout:
                # si tarda, hacemos una pausa mínima
                page.wait_for_timeout(500)

            return True

        # Paginación
        while True:
            all_rows.extend(extract_table_rows())
            if not click_next_and_wait():
                break

        browser.close()

    # Convertir a DF (8 columnas)
    cols = [
        "Denominazione",
        "Indirizzo",
        "CAP",
        "Comune",
        "Provincia",
        "Regione",
        "Codice_univoco",
        "Partita_IVA",
    ]
    df = pd.DataFrame(all_rows, columns=cols)

    # Limpiar + deduplicar
    for c in cols:
        df[c] = df[c].astype(str).map(clean)

    df = df.drop_duplicates(subset=["Codice_univoco", "Partita_IVA", "Indirizzo"], keep="first")
    return df


if __name__ == "__main__":
    df = scrape_roma()
    print(f"Filas extraídas: {len(df)}")
    df.to_csv("farmacie_roma.csv", index=False, encoding="utf-8-sig")
    print("Guardado: farmacie_roma.csv")
