from __future__ import annotations

import re
import pandas as pd
from playwright.sync_api import sync_playwright

URL = "https://www.salute.gov.it/CercaFarmacie/Ricerca#FINE"


def clean(txt: str) -> str:
    if txt is None:
        return ""
    return re.sub(r"\s+", " ", txt).strip()


def parse_results_counter(page) -> tuple[int, int]:
    """
    Lee el texto tipo: 'risultati 1 - 10 di 426'
    Devuelve (fin_actual, total)
    """
    txt = page.locator("text=/risultati/i").first.inner_text()
    m = re.search(r"(\d+)\s*-\s*(\d+)\s*di\s*(\d+)", txt)
    if not m:
        raise RuntimeError(f"No pude parsear contador resultados: {txt}")
    start, end, total = map(int, m.groups())
    return end, total


def scrape_city(regione: str, provincia: str, comune: str, out_csv: str) -> pd.DataFrame:
    rows_out: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, wait_until="domcontentloaded")

        # Selects reales
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

        com.select_option(label=comune)

        page.locator("input[value='Cerca'], button:has-text('Cerca')").first.click()

        page.wait_for_selector(
            "table:has-text('Denominazione'):has-text('Indirizzo')",
            timeout=60000,
        )

        table = page.locator(
            "table:has-text('Denominazione'):has-text('Indirizzo')"
        ).first

        while True:
            # Leer filas actuales
            trs = table.locator("tbody tr")
            for i in range(trs.count()):
                tds = trs.nth(i).locator("td")
                vals = [clean(tds.nth(j).inner_text()) for j in range(min(9, tds.count()))]

                rows_out.append(
                    {
                        "Denominazione": vals[0],
                        "Indirizzo": vals[1],
                        "CAP": vals[2],
                        "Comune": vals[3],
                        "Provincia": vals[4],
                        "Regione": vals[5],
                        "Codice_univoco": vals[6],
                        "Partita_IVA": vals[7],
                    }
                )

            # Leer contador resultados
            end, total = parse_results_counter(page)
            print(f"➡️ Progreso: {end}/{total}")

            if end >= total:
                break

            # Click siguiente página
            next_btn = page.locator("button:has-text('>'), input[value='>']").first
            next_btn.click()
            page.wait_for_timeout(300)

        browser.close()

    df = (
        pd.DataFrame(rows_out)
        .drop_duplicates(
            subset=["Codice_univoco", "Partita_IVA", "Indirizzo"], keep="first"
        )
        .reset_index(drop=True)
    )

    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Guardado {out_csv} ({len(df)} filas)")

    return df


if __name__ == "__main__":
    scrape_city(
        regione="LOMBARDIA",
        provincia="MILANO",
        comune="MILANO",
        out_csv="farmacie_milano.csv",
    )
