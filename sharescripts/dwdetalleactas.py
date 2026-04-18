import json
import asyncio
import aiohttp
import os
from tqdm import tqdm

# ── Config ───────────────────────────────────────────────
INPUT_PATH = 'E:/1-Apps/Elecciones/proceso/actas.json'
OUTPUT_PATH = 'E:/1-Apps/Elecciones/proceso/detalleactas.jsonl'

CONCURRENCY = 20
RETRIES = 3
BATCH_SIZE = 500

# ── Cargar IDs ───────────────────────────────────────────
actas = json.load(open(INPUT_PATH, 'r', encoding='utf-8'))

# Solo presidenciales
actas = [a for a in actas if a.get('idEleccion') == 10]

# ── Evitar reprocesar ─────────────────────────
procesados = set()
if os.path.exists(OUTPUT_PATH):
    with open(OUTPUT_PATH, encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                if 'idActa' in data:
                    procesados.add(data['idActa'])
            except:
                pass

actas = [a for a in actas if a['id'] not in procesados]

print(f"Actas pendientes: {len(actas)}")

# ── Guardado incremental ─────────────────────────────────
def guardar_linea(data):
    with open(OUTPUT_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

# ── Fetch async ──────────────────────────────────────────
async def fetch_acta(session, idacta, semaphore):
    url = f'https://resultadoelectoral.onpe.gob.pe/presentacion-backend/actas/{idacta}'

    headers = {
        'accept': '*/*',
        'accept-language': 'es,es-ES;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,es-PE;q=0.5',
        'content-type': 'application/json',
        'priority': 'u=1, i',
        'referer': 'https://resultadoelectoral.onpe.gob.pe/main/actas',
        'sec-ch-ua': '"Microsoft Edge";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0',
    }

    for attempt in range(RETRIES):
        try:
            async with semaphore:
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        content_type = resp.headers.get('Content-Type', '')

                        if 'application/json' in content_type:
                            data = await resp.json()
                            return {
                                "idActa": idacta,
                                "data": data.get('data', {})
                            }
                        else:
                            text = await resp.text()
                            tqdm.write(f"⚠️ No JSON en acta {idacta} (Content-Type: {content_type})")
                            return None
    
                    else:
                        tqdm.write(f"[{resp.status}] acta {idacta}")
        except Exception as e:
            if attempt == RETRIES - 1:
                tqdm.write(f"❌ Error acta {idacta}: {e}")
            await asyncio.sleep(1 * (attempt + 1))

    return None

# ── Worker por lote ──────────────────────────────────────
async def procesar_lote(session, lote, pbar, semaphore):
    tasks = [fetch_acta(session, acta['id'], semaphore) for acta in lote]
    resultados = await asyncio.gather(*tasks)

    for r in resultados:
        if r:
            guardar_linea(r)
        pbar.update(1)

# ── Main ────────────────────────────────────────────────
async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY)

    pbar = tqdm(total=len(actas), desc="Descargando actas")

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(actas), BATCH_SIZE):
            lote = actas[i:i+BATCH_SIZE]
            await procesar_lote(session, lote, pbar, semaphore)

    pbar.close()

# ── Run ────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(main())