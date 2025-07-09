from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import re
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional
import logging
import requests
import unicodedata

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QuotesETL:
    """
    ETL (Extract, Transform, Load) class untuk scraping quotes
    Memisahkan proses menjadi tiga tahap yang jelas untuk maintenance yang mudah
    """
    
    def __init__(self, base_url: str = "http://quotes.toscrape.com", delay: float = 1.0):
        """
        Inisialisasi ETL processor
        
        Args:
            base_url (str): URL dasar website
            delay (float): Delay antar request dalam detik
        """
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Data containers untuk setiap tahap
        self.raw_data = []      # Data mentah dari Extract
        self.transformed_data = [] # Data yang sudah di-transform
        self.final_data = []    # Data final yang siap di-load
        
        # Counters dan metrics
        self.metrics = {
            'total_pages_scraped': 0,
            'total_quotes_extracted': 0,
            'total_quotes_cleaned': 0,
            'errors_encountered': 0,
            'duplicate_quotes_removed': 0
        }
    
    # ==================== EXTRACT PHASE ====================
    
    def extract_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """
        Extract: Mengambil konten HTML dari URL
        
        Args:
            url (str): URL yang akan di-extract
            
        Returns:
            BeautifulSoup: Parsed HTML content atau None jika gagal
        """
        try:
            logger.info(f"Extracting page: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error extracting page {url}: {e}")
            self.metrics['errors_encountered'] += 1
            return None
    
    def extract_quotes_from_soup(self, soup: BeautifulSoup, source_url: str) -> List[Dict]:
        """
        Extract: Mengambil data quotes mentah dari parsed HTML
        
        Args:
            soup (BeautifulSoup): Parsed HTML content
            source_url (str): URL sumber untuk referensi
            
        Returns:
            List[Dict]: List quotes mentah yang di-extract
        """
        raw_quotes = []
        quote_elements = soup.find_all('div', class_='quote')
        
        for idx, quote_elem in enumerate(quote_elements):
            try:
                # Extract semua elemen tanpa cleaning
                raw_quote = {
                    'raw_text': quote_elem.find('span', class_='text').get_text() if quote_elem.find('span', class_='text') else '',
                    'raw_author': quote_elem.find('small', class_='author').get_text() if quote_elem.find('small', class_='author') else '',
                    'raw_tags': [tag.get_text() for tag in quote_elem.find_all('a', class_='tag')],
                    'raw_author_link': quote_elem.find('a', href=lambda x: x and '/author/' in x),
                    'source_url': source_url,
                    'extraction_order': idx,
                    'extraction_timestamp': time.time()
                }
                
                # Handle author link
                if raw_quote['raw_author_link']:
                    raw_quote['raw_author_link'] = raw_quote['raw_author_link'].get('href', '')
                else:
                    raw_quote['raw_author_link'] = ''
                
                raw_quotes.append(raw_quote)
                
            except Exception as e:
                logger.warning(f"Error extracting quote {idx} from {source_url}: {e}")
                self.metrics['errors_encountered'] += 1
                continue
        
        return raw_quotes
    
    def extract_next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """
        Extract: Mengambil URL halaman berikutnya
        
        Args:
            soup (BeautifulSoup): Parsed HTML content
            current_url (str): URL halaman saat ini
            
        Returns:
            Optional[str]: URL halaman berikutnya atau None
        """
        try:
            nav = soup.find('nav')
            if nav:
                next_li = nav.find('li', class_='next')
                if next_li:
                    next_link = next_li.find('a')
                    if next_link and next_link.get('href'):
                        return urljoin(current_url, next_link['href'])
        except Exception as e:
            logger.warning(f"Error extracting next page URL: {e}")
        
        return None
    
    def extract_all_pages(self, start_url: str, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Extract: Orchestrate extraction dari semua halaman
        
        Args:
            start_url (str): URL halaman pertama
            max_pages (Optional[int]): Maksimal halaman yang akan di-extract
            
        Returns:
            List[Dict]: Semua data mentah yang berhasil di-extract
        """
        logger.info("=== STARTING EXTRACT PHASE ===")
        
        all_raw_data = []
        current_url = start_url
        page_count = 0
        
        while current_url and (max_pages is None or page_count < max_pages):
            # Extract page content
            soup = self.extract_page_content(current_url)
            if not soup:
                break
            
            # Extract quotes from current page
            page_quotes = self.extract_quotes_from_soup(soup, current_url)
            all_raw_data.extend(page_quotes)
            
            # Extract next page URL
            next_url = self.extract_next_page_url(soup, current_url)
            
            # Update metrics
            self.metrics['total_pages_scraped'] += 1
            self.metrics['total_quotes_extracted'] += len(page_quotes)
            
            logger.info(f"Extracted {len(page_quotes)} quotes from page {page_count + 1}")
            
            # Move to next page
            current_url = next_url
            page_count += 1
            
            # Delay between requests
            if current_url:
                time.sleep(self.delay)
        
        logger.info(f"Extract phase completed. Total quotes extracted: {len(all_raw_data)}")
        self.raw_data = all_raw_data
        return all_raw_data
    
    # ==================== TRANSFORM PHASE ====================
    

    def clean_quote_text(self, raw_text: str) -> str:
        if not raw_text:
            return ""

        # Normalisasi unicode → ASCII
        raw_text = unicodedata.normalize("NFKD", raw_text)

        # Hapus tanda kutip di awal dan akhir
        cleaned = raw_text.strip()
        if cleaned.startswith('“') and cleaned.endswith('”'):
            cleaned = cleaned[1:-1]
        
        # Normalisasi whitespace dan karakter tak diinginkan
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = cleaned.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        
        return cleaned.strip()

    
    def clean_author_name(self, raw_author: str) -> str:
        """
        Transform: Membersihkan nama pengarang
        
        Args:
            raw_author (str): Nama pengarang mentah
            
        Returns:
            str: Nama pengarang yang sudah dibersihkan
        """
        if not raw_author:
            return ""
        
        # Normalisasi whitespace dan kapitalisasi
        cleaned = raw_author.strip()
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Hapus karakter khusus yang tidak diinginkan
        cleaned = re.sub(r'[^\w\s\-\.]', '', cleaned)
        
        return cleaned.strip()
    
    def clean_tags(self, raw_tags: List[str]) -> List[str]:
        """
        Transform: Membersihkan dan menormalisasi tags
        
        Args:
            raw_tags (List[str]): List tags mentah
            
        Returns:
            List[str]: List tags yang sudah dibersihkan
        """
        if not raw_tags:
            return []
        
        cleaned_tags = []
        for tag in raw_tags:
            if tag:
                # Normalisasi tag
                cleaned_tag = tag.strip().lower()
                cleaned_tag = re.sub(r'[^\w\-]', '', cleaned_tag)
                
                if cleaned_tag and cleaned_tag not in cleaned_tags:
                    cleaned_tags.append(cleaned_tag)
        
        return sorted(cleaned_tags)
    
    def standardize_author_link(self, raw_link: str) -> str:
        """
        Transform: Standardisasi link author
        
        Args:
            raw_link (str): Link mentah
            
        Returns:
            str: Link yang sudah distandardisasi
        """
        if not raw_link:
            return ""
        
        # Pastikan link lengkap
        if raw_link.startswith('/'):
            return urljoin(self.base_url, raw_link)
        
        return raw_link
    
    def transform_single_quote(self, raw_quote: Dict) -> Dict:
        """
        Transform: Transformasi satu quote dari data mentah
        
        Args:
            raw_quote (Dict): Quote mentah dari extract phase
            
        Returns:
            Dict: Quote yang sudah di-transform
        """
        try:
            transformed_quote = {
                'quote_text': self.clean_quote_text(raw_quote.get('raw_text', '')),
                'author': self.clean_author_name(raw_quote.get('raw_author', '')),
                'tags': self.clean_tags(raw_quote.get('raw_tags', [])),
                'author_link': self.standardize_author_link(raw_quote.get('raw_author_link', '')),
                'source_url': raw_quote.get('source_url', ''),
                'extraction_order': raw_quote.get('extraction_order', 0),
                'word_count': len(raw_quote.get('raw_text', '').split()),
                'tag_count': len(raw_quote.get('raw_tags', [])),
                'character_count': len(raw_quote.get('raw_text', '')),
                'processed_timestamp': time.time()
            }
            
            # Validasi data penting
            if not transformed_quote['quote_text'] or not transformed_quote['author']:
                return None
            
            return transformed_quote
            
        except Exception as e:
            logger.error(f"Error transforming quote: {e}")
            self.metrics['errors_encountered'] += 1
            return None
    
    def remove_duplicates(self, quotes: List[Dict]) -> List[Dict]:
        """
        Transform: Menghapus duplikasi berdasarkan kombinasi quote_text dan author
        
        Args:
            quotes (List[Dict]): List quotes yang mungkin mengandung duplikasi
            
        Returns:
            List[Dict]: List quotes tanpa duplikasi
        """
        seen = set()
        unique_quotes = []
        
        for quote in quotes:
            # Buat key unik berdasarkan text dan author
            key = (quote['quote_text'].lower(), quote['author'].lower())
            
            if key not in seen:
                seen.add(key)
                unique_quotes.append(quote)
            else:
                self.metrics['duplicate_quotes_removed'] += 1
        
        return unique_quotes
    
    def transform_all_quotes(self, raw_quotes: List[Dict]) -> List[Dict]:
        """
        Transform: Orchestrate transformasi semua quotes
        
        Args:
            raw_quotes (List[Dict]): List semua quotes mentah
            
        Returns:
            List[Dict]: List quotes yang sudah di-transform
        """
        logger.info("=== STARTING TRANSFORM PHASE ===")
        
        transformed_quotes = []
        
        for raw_quote in raw_quotes:
            transformed_quote = self.transform_single_quote(raw_quote)
            if transformed_quote:
                transformed_quotes.append(transformed_quote)
        
        # Remove duplicates
        unique_quotes = self.remove_duplicates(transformed_quotes)
        
        # Update metrics
        self.metrics['total_quotes_cleaned'] = len(unique_quotes)
        
        logger.info(f"Transform phase completed. Clean quotes: {len(unique_quotes)}")
        logger.info(f"Removed duplicates: {self.metrics['duplicate_quotes_removed']}")
        
        self.transformed_data = unique_quotes
        return unique_quotes
    
    # ==================== LOAD PHASE ====================
    
    def validate_data(self, quotes: List[Dict]) -> List[Dict]:
        """
        Load: Validasi data sebelum disimpan
        
        Args:
            quotes (List[Dict]): List quotes yang akan divalidasi
            
        Returns:
            List[Dict]: List quotes yang valid
        """
        valid_quotes = []
        
        for quote in quotes:
            is_valid = True
            
            # Validasi required fields
            if not quote.get('quote_text') or len(quote['quote_text'].strip()) < 5:
                is_valid = False
            
            if not quote.get('author') or len(quote['author'].strip()) < 2:
                is_valid = False
            
            # Validasi panjang karakter
            if len(quote.get('quote_text', '')) > 1000:
                is_valid = False
            
            if is_valid:
                valid_quotes.append(quote)
            else:
                self.metrics['errors_encountered'] += 1
        
        return valid_quotes
    
    def prepare_for_csv(self, quotes: List[Dict]) -> List[Dict]:
        """
        Load: Persiapan data untuk format CSV
        
        Args:
            quotes (List[Dict]): List quotes yang akan disiapkan
            
        Returns:
            List[Dict]: List quotes yang siap untuk CSV
        """
        csv_ready_quotes = []
        
        for quote in quotes:
            csv_quote = {
                'quote': quote['quote_text'],
                'author': quote['author'],
                'tags': ', '.join(quote['tags']),
                'author_link': quote['author_link'],
                'word_count': quote['word_count'],
                'tag_count': quote['tag_count'],
                'character_count': quote['character_count'],
                'source_url': quote['source_url']
            }
            csv_ready_quotes.append(csv_quote)
        
        return csv_ready_quotes
    
    def save_to_csv(self, quotes: List[Dict], filename: str = 'quotes_clean.csv') -> bool:
        """
        Load: Simpan data ke CSV
        
        Args:
            quotes (List[Dict]): List quotes yang akan disimpan
            filename (str): Nama file CSV
            
        Returns:
            bool: Status berhasil atau tidak
        """
        try:
            df = pd.DataFrame(quotes)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Data berhasil disimpan ke {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False
    
    def save_to_json(self, quotes: List[Dict], filename: str = 'quotes_clean.json') -> bool:
        """
        Load: Simpan data ke JSON
        
        Args:
            quotes (List[Dict]): List quotes yang akan disimpan
            filename (str): Nama file JSON
            
        Returns:
            bool: Status berhasil atau tidak
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(quotes, f, indent=2, ensure_ascii=False)
            logger.info(f"Data berhasil disimpan ke {filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return False
    
    def generate_report(self, quotes: List[Dict]) -> Dict:
        """
        Load: Generate laporan dari data yang sudah diproses
        
        Args:
            quotes (List[Dict]): List quotes untuk generate laporan
            
        Returns:
            Dict: Laporan statistik
        """
        if not quotes:
            return {}
        
        # Hitung statistik
        authors = [q['author'] for q in quotes]
        all_tags = []
        for q in quotes:
            all_tags.extend(q['tags'])
        
        from collections import Counter
        author_counts = Counter(authors)
        tag_counts = Counter(all_tags)
        
        report = {
            'total_quotes': len(quotes),
            'unique_authors': len(set(authors)),
            'total_tags': len(all_tags),
            'unique_tags': len(set(all_tags)),
            'top_authors': dict(author_counts.most_common(5)),
            'top_tags': dict(tag_counts.most_common(10)),
            'avg_word_count': sum(q['word_count'] for q in quotes) / len(quotes),
            'avg_character_count': sum(q['character_count'] for q in quotes) / len(quotes),
            'processing_metrics': self.metrics
        }
        
        return report
    
    def load_data(self, transformed_quotes: List[Dict], output_prefix: str = 'quotes_final') -> bool:
        """
        Load: Orchestrate loading semua data
        
        Args:
            transformed_quotes (List[Dict]): List quotes yang sudah di-transform
            output_prefix (str): Prefix untuk nama file output
            
        Returns:
            bool: Status berhasil atau tidak
        """
        logger.info("=== STARTING LOAD PHASE ===")
        
        # Validasi data
        valid_quotes = self.validate_data(transformed_quotes)
        logger.info(f"Valid quotes after validation: {len(valid_quotes)}")
        
        if not valid_quotes:
            logger.error("No valid quotes to load")
            return False
        
        # Persiapkan untuk CSV
        csv_quotes = self.prepare_for_csv(valid_quotes)
        
        # Simpan ke berbagai format
        csv_success = self.save_to_csv(csv_quotes, f'{output_prefix}.csv')
        json_success = self.save_to_json(valid_quotes, f'{output_prefix}.json')
        
        # Generate dan simpan laporan
        report = self.generate_report(valid_quotes)
        report_success = self.save_to_json(report, f'{output_prefix}_report.json')
        
        # Print summary
        logger.info(f"=== FINAL SUMMARY ===")
        logger.info(f"Total quotes processed: {len(valid_quotes)}")
        logger.info(f"Unique authors: {report.get('unique_authors', 0)}")
        logger.info(f"Average word count: {report.get('avg_word_count', 0):.2f}")
        logger.info(f"Files created: {output_prefix}.csv, {output_prefix}.json, {output_prefix}_report.json")
        
        self.final_data = valid_quotes
        return csv_success and json_success and report_success
    
    # ==================== MAIN ETL ORCHESTRATOR ====================
    
    def run_etl_pipeline(self, start_url: str, max_pages: Optional[int] = None, 
                        output_prefix: str = 'quotes_final') -> bool:
        """
        Menjalankan seluruh pipeline ETL
        
        Args:
            start_url (str): URL halaman pertama
            max_pages (Optional[int]): Maksimal halaman yang akan diproses
            output_prefix (str): Prefix untuk nama file output
            
        Returns:
            bool: Status berhasil atau tidak
        """
        logger.info("🚀 STARTING ETL PIPELINE")
        logger.info("=" * 50)
        
        try:
            # EXTRACT
            raw_quotes = self.extract_all_pages(start_url, max_pages)
            if not raw_quotes:
                logger.error("Extract phase failed - no data extracted")
                return False
            
            # TRANSFORM
            transformed_quotes = self.transform_all_quotes(raw_quotes)
            if not transformed_quotes:
                logger.error("Transform phase failed - no clean data")
                return False
            
            # LOAD
            load_success = self.load_data(transformed_quotes, output_prefix)
            if not load_success:
                logger.error("Load phase failed")
                return False
            
            logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
            return True
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed with error: {e}")
            return False

# ==================== MAIN EXECUTION ====================
if __name__ == "__main__":
    # Inisialisasi ETL processor
    etl = QuotesETL(delay=1.0)
    
    # Jalankan ETL pipeline
    success = etl.run_etl_pipeline(
        start_url="http://quotes.toscrape.com/page/1/",
        max_pages=10,
        output_prefix="quotes_clean_etl"
    )
    
    if success:
        print("\n🎉 ETL process completed successfully!")
        print("Files created:")
        print("- quotes_clean_etl.csv (CSV format)")
        print("- quotes_clean_etl.json (JSON format)")
        print("- quotes_clean_etl_report.json (Statistics report)")
    else:
        print("\n❌ ETL process failed. Check logs for details.")