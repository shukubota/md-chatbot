import { DOMParser } from "https://deno.land/x/deno_dom/deno-dom-wasm.ts";
import { ensureDir } from "https://deno.land/std/fs/mod.ts";
import { join } from "https://deno.land/std/path/mod.ts";
import { delay } from "https://deno.land/std/async/delay.ts";

class YugiohScraper {
  private baseUrl = "https://www.db.yugioh-card.com/yugiohdb/card_search.action";
  private outputDir = "card_data";
  private batchSize = 100; // 同時実行数

  constructor() {
    this.initializeOutputDir();
  }

  private async initializeOutputDir() {
    await ensureDir(this.outputDir);
  }

  private async fetchWithRetry(url: string, retries = 3): Promise<Response> {
    for (let i = 0; i < retries; i++) {
      try {
        const response = await fetch(url, {
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
          },
        });
        if (response.ok) return response;
      } catch (error) {
        console.error(`Attempt ${i + 1} failed:`, error);
        if (i === retries - 1) throw error;
      }
      await delay(1000 * (i + 1));
    }
    throw new Error("Max retries reached");
  }

  private getCardSetText(doc: Document): string {
    const cardSetElement = doc.getElementById('CardSet');
    if (!cardSetElement) return "";

    return cardSetElement.textContent?.trim()
      .replace(/\s+/g, ' ')
      .replace(/\n+/g, ' ')
      .trim() ?? "";
  }

  private async getCardData(cid: number) {
    const url = `${this.baseUrl}?ope=2&cid=${cid}&request_locale=ja`;
    
    try {
      const response = await this.fetchWithRetry(url);
      const html = await response.text();

      const parser = new DOMParser();
      const doc = parser.parseFromString(html, "text/html");
      
      if (!doc) {
        throw new Error("Failed to parse HTML");
      }

      return {
        card_id: cid,
        card_info: this.getCardSetText(doc),
        timestamp: new Date().toISOString(),
        url: url,
      };
    } catch (error) {
      console.error(`Error fetching card ${cid}:`, error);
      return null;
    }
  }

  private async saveCardData(cardData: any) {
    if (!cardData) return;

    const cid = cardData.card_id;
    const filename = join(this.outputDir, `card_${cid}.json`);
    const jsonString = JSON.stringify(cardData, null, 2);
    
    try {
      await Deno.writeTextFile(filename, jsonString);
      // console.log(`Successfully saved card ${cid}`);
    } catch (error) {
      console.error(`Error saving card ${cid}:`, error);
    }
  }

  private async processBatch(cardIds: number[]) {
    const promises = cardIds.map(async (cid) => {
      try {
        console.log(`Processing card ID: ${cid}`);
        const cardData = await this.getCardData(cid);
        if (cardData) {
          if (cardData.card_info === "") {
              console.log(`No data found for card ${cid}`);
              return;
          }
          await this.saveCardData(cardData);
        } else {
          console.log(`No data found for card ${cid}`);
        }
      } catch (error) {
        console.error(`Error processing card ${cid}:`, error);
      }
    });

    await Promise.all(promises);
  }

  public async scrapeRange(startId: number, endId: number) {
    console.log(`Starting parallel scraping from ID ${startId} to ${endId}`);

    // カードIDの配列を生成
    const cardIds = Array.from(
      { length: endId - startId + 1 },
      (_, i) => startId + i
    );

    // バッチに分割して処理
    for (let i = 0; i < cardIds.length; i += this.batchSize) {
      const batch = cardIds.slice(i, i + this.batchSize);
      // console.log(`Processing batch ${i / this.batchSize + 1}`);
      
      await this.processBatch(batch);
      
      // バッチ間で少し待機してサーバーに負荷をかけすぎないようにする
      // if (i + this.batchSize < cardIds.length) {
      //   await delay(1000);
      // }
    }
  }

  // バッチサイズを設定するメソッド
  public setBatchSize(size: number) {
    this.batchSize = size;
  }
}

async function main() {
  try {
    const scraper = new YugiohScraper();
    
    // バッチサイズを設定（同時に処理するリクエスト数）
    scraper.setBatchSize(1000);
    
    const startId = 1000;
    const endId = 20000;
    
    await scraper.scrapeRange(startId, endId);
    
  } catch (error) {
    console.error("Error in main:", error);
  }
}

main();
