from __future__ import annotations

import re
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.salute.gov.it/CercaFarmacie/Ricerca#FINE"


def clean(txt: str) -> str:
    if txt is None:
        return ""
    return re.sub(r"\s+", " ", txt).strip()


def parse_results_counter(page) -> tuple[int, int]:
    """
    Lee texto tipo: 'risultati 1 - 10 di 952'
    Devuelve (end, total)
    """
    txt = page.locator("text=/risultati/i").first.inner_text()
    m = re.search(r"(\d+)\s*-\s*(\d+)\s*di\s*(\d+)", txt)
    if not m:
        raise RuntimeError(f"No pude parsear contador resultados: {txt}")
    _start, end, total = map(int, m.groups())
    return end, total


def extract_table_rows(page) -> list[list[str]]:
    # 8 columnas
    return page.evaluate(
        """() => {
            const t = Array.from(document.querySelectorAll("table"))
              .find(tb => tb.innerText.includes("Denominazione") && tb.innerText.includes("Indirizzo"));
            if (!t) return [];
            return Array.from(t.querySelectorAll("tbody tr")).map(tr =>
                Array.from(tr.querySelectorAll("td")).slice(0, 8).map(td =>
                    (td.innerText || "").replace(/\\s+/g, " ").trim()
                )
            );
        }"""
    )


def first_row_fingerprint(page) -> str:
    # una “firma” estable de la tabla (primera fila)
    return page.evaluate(
        """() => {
            const t = Array.from(document.querySelectorAll("table"))
              .find(tb => tb.innerText.includes("Denominazione") && tb.innerText.includes("Indirizzo"));
            const r = t?.querySelector("tbody tr");
            return r ? r.innerText.replace(/\\s+/g, " ").trim() : "";
        }"""
    )


def click_next_and_wait(page, prev_end: int, prev_fp: str, max_tries: int = 6) -> bool:
    """
    Intenta avanzar de página. Devuelve True si detecta cambio, False si no hay botón o no avanza.
    Detecta cambio por:
      - contador end aumenta, o
      - cambia fingerprint de primera fila
    """
    next_btn = page.locator(
        "a[title*='successiva' i], button:has-text('>'), input[value='>'], a:has-text('>')"
    ).first

    if next_btn.count() == 0:
        return False

    for _ in range(max_tries):
        try:
            next_btn.click()
        except Exception:
            page.wait_for_timeout(300)
            continue

        # Esperar a que ocurra *algo* (contador o tabla)
        try:
            page.wait_for_function(
                """(prevEnd, prevFp) => {
                    // contador
                    const el = Array.from(document.querySelectorAll("*"))
                      .find(n => n.innerText && /risultati/i.test(n.innerText));
                    let endNow = null;
                    if (el) {
                      const m = el.innerText.match(/\\d+\\s*-\\s*(\\d+)\\s*di\\s*(\\d+)/i);
                      if (m) endNow = parseInt(m[1], 10);
                    }

                    // fingerprint primera fila
                    const t = Array.from(document.querySelectorAll("table"))
                      .find(tb => tb.innerText.includes("Denominazione") && tb.innerText.includes("Indirizzo"));
                    const r = t?.querySelector("tbody tr");
                    const fpNow = r ? r.innerText.replace(/\\s+/g, " ").trim() : "";

                    const movedByCounter = (endNow !== null) && (endNow > prevEnd);
                    const movedByTable = (fpNow !== "") && (fpNow !== prevFp);

                    return movedByCounter || movedByTable;
                }""",
                arg=[prev_end, prev_fp],
                timeout=15000,
            )
            return True
        except PWTimeout:
            # no cambió aún → pausa y reintenta click
            page.wait_for_timeout(600)

    # tras varios intentos, asumimos que no avanza
    return False


def scrape_province(regione: str, provincia: str, out_csv: str, headless: bool = True) -> pd.DataFrame:
    all_rows: list[list[str]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=60000)

        reg = page.locator("select[name='reg']")
        prv = page.locator("select[name='prv']")
        com = page.locator("select[name='com']")

        reg.select_option(label=regione)

        page.wait_for_function(
            """() => {
                const s = document.querySelector("select[name='prv']");
                return s && s.options.length > 1;
            }""",
            timeout=60000,
        )

        prv.select_option(label=provincia)

        page.wait_for_function(
            """() => {
                const s = document.querySelector("select[name='com']");
                return s && s.options.length > 1;
            }""",
            timeout=60000,
        )

        # NO seleccionar comune → toda la provincia
        com.select_option(index=0)

        page.locator("input[value='Cerca'], button:has-text('Cerca')").first.click()

        page.wait_for_selector(
            "table:has-text('Denominazione'):has-text('Indirizzo')",
            timeout=60000,
        )

        # Loop paginación robusto
        while True:
            all_rows.extend(extract_table_rows(page))

            end, total = parse_results_counter(page)
            print(f"➡️ Progreso: {end}/{total}")

            if end >= total:
                break

            fp = first_row_fingerprint(page)
            moved = click_next_and_wait(page, prev_end=end, prev_fp=fp, max_tries=7)

            if not moved:
                # No reventamos: guardamos y salimos
                print("⚠️ No pude avanzar de página tras varios intentos. Guardando lo extraído y saliendo.")
                break

        browser.close()

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

    for c in cols:
        df[c] = df[c].astype(str).map(clean)

    df = df.drop_duplicates(subset=["Codice_univoco", "Partita_IVA", "Indirizzo"], keep="first").reset_index(drop=True)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Guardado {out_csv} ({len(df)} filas)")

    return df


if __name__ == "__main__":
    scrape_province(
        regione="LOMBARDIA",
        provincia="MILANO",
        out_csv="farmacie_lombardia_milano_provincia.csv",
        headless=True,
    )
