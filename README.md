# Quotes ETL Pipeline

Sistem ETL (Extract, Transform, Load) untuk scraping quotes dari website quotes.toscrape.com dengan arsitektur yang terstruktur dan mudah dipelihara.

## 🚀 Fitur Utama

- **Arsitektur ETL Terpisah**: Memisahkan proses Extract, Transform, dan Load untuk maintainability yang lebih baik
- **Error Handling**: Robust error handling dengan logging yang komprehensif
- **Data Validation**: Validasi data pada setiap tahap proses
- **Deduplication**: Menghapus duplikasi quotes berdasarkan kombinasi text dan author
- **Multi-format Output**: Mendukung output ke CSV dan JSON
- **Metrics & Reporting**: Generate laporan statistik dan metrics proses
- **Rate Limiting**: Delay antar request untuk menghindari overload server

## 📋 Requirements

```python
beautifulsoup4>=4.9.0
pandas>=1.3.0
requests>=2.25.0
```

## 🏗️ Arsitektur

### 1. Extract Phase
- **`extract_page_content()`**: Mengambil konten HTML dari URL
- **`extract_quotes_from_soup()`**: Parsing quotes dari HTML
- **`extract_next_page_url()`**: Mendapatkan URL halaman berikutnya
- **`extract_all_pages()`**: Orchestrate extraction dari semua halaman

### 2. Transform Phase
- **`clean_quote_text()`**: Membersihkan teks quote (normalisasi unicode, hapus quotes, whitespace)
- **`clean_author_name()`**: Normalisasi nama pengarang
- **`clean_tags()`**: Membersihkan dan menormalisasi tags
- **`remove_duplicates()`**: Menghapus duplikasi berdasarkan text + author
- **`transform_all_quotes()`**: Orchestrate transformasi semua data

### 3. Load Phase
- **`validate_data()`**: Validasi data sebelum disimpan
- **`save_to_csv()`**: Simpan ke format CSV
- **`save_to_json()`**: Simpan ke format JSON
- **`generate_report()`**: Generate laporan statistik
- **`load_data()`**: Orchestrate proses loading

## 📊 Data Structure

### Raw Data (Extract Phase)
```python
{
    'raw_text': 'Original quote text',
    'raw_author': 'Author name',
    'raw_tags': ['tag1', 'tag2'],
    'raw_author_link': '/author/author-name',
    'source_url': 'http://quotes.toscrape.com/page/1/',
    'extraction_order': 0,
    'extraction_timestamp': 1234567890.0
}
```

### Transformed Data (Transform Phase)
```python
{
    'quote_text': 'Cleaned quote text',
    'author': 'Cleaned Author Name',
    'tags': ['cleaned-tag1', 'cleaned-tag2'],
    'author_link': 'http://quotes.toscrape.com/author/author-name',
    'source_url': 'http://quotes.toscrape.com/page/1/',
    'word_count': 15,
    'tag_count': 2,
    'character_count': 85,
    'processed_timestamp': 1234567890.0
}
```

### Final CSV Output
```csv
quote,author,tags,author_link,word_count,tag_count,character_count,source_url
"Cleaned quote text","Author Name","tag1, tag2","http://...",15,2,85,"http://..."
```

## 📈 Metrics & Monitoring

Sistem mencatat berbagai metrics:
- `total_pages_scraped`: Jumlah halaman yang berhasil di-scrape
- `total_quotes_extracted`: Total quotes yang berhasil di-extract
- `total_quotes_cleaned`: Total quotes setelah cleaning
- `errors_encountered`: Jumlah error yang ditemukan
- `duplicate_quotes_removed`: Jumlah duplikasi yang dihapus

## 🔍 Data Cleaning Features

### Quote Text Cleaning
- Normalisasi unicode ke ASCII
- Hapus tanda kutip di awal dan akhir
- Normalisasi whitespace
- Hapus karakter newline, carriage return, tab

### Author Name Cleaning
- Normalisasi whitespace dan kapitalisasi
- Hapus karakter khusus yang tidak diinginkan
- Validasi minimum 2 karakter

### Tags Cleaning
- Konversi ke lowercase
- Hapus karakter non-alphanumeric (kecuali hyphen)
- Sort dan deduplicate
- Validasi tidak kosong

### Data Validation
- Quote text minimum 5 karakter
- Author name minimum 2 karakter
- Quote text maksimum 1000 karakter
- Validasi required fields

## 📁 Output Files

Sistem menghasilkan 3 file output:
1. **`{prefix}.csv`**: Data quotes dalam format CSV
2. **`{prefix}.json`**: Data quotes dalam format JSON
3. **`{prefix}_report.json`**: Laporan statistik dan metrics

### Report Structure
```json
{
  "total_quotes": 100,
  "unique_authors": 50,
  "total_tags": 200,
  "unique_tags": 150,
  "top_authors": {"Author 1": 5, "Author 2": 3},
  "top_tags": {"inspirational": 20, "life": 15},
  "avg_word_count": 12.5,
  "avg_character_count": 75.3,
  "processing_metrics": {...}
}
```

## 🛠️ Error Handling

- **Connection Errors**: Retry mechanism dengan logging
- **Parse Errors**: Skip item yang error, lanjutkan proses
- **Validation Errors**: Log dan skip data yang tidak valid
- **File I/O Errors**: Comprehensive error reporting

## 📝 Logging

Sistem menggunakan Python logging dengan format:
```
%(asctime)s - %(levelname)s - %(message)s
```

Level logging:
- `INFO`: Progress updates, berhasil proses
- `WARNING`: Non-critical errors, skip items
- `ERROR`: Critical errors yang mempengaruhi proses

## 🔧 Konfigurasi

### Inisialisasi Parameters
- `base_url`: URL dasar website (default: "http://quotes.toscrape.com")
- `delay`: Delay antar request dalam detik (default: 1.0)

### Pipeline Parameters
- `start_url`: URL halaman pertama
- `max_pages`: Maksimal halaman yang akan diproses (optional)
- `output_prefix`: Prefix untuk nama file output

## 🎯 Best Practices

1. **Rate Limiting**: Gunakan delay yang cukup untuk menghindari overload server
2. **Error Monitoring**: Monitor logs untuk error patterns
3. **Data Validation**: Selalu validasi data sebelum processing
4. **Backup**: Simpan raw data untuk reprocessing jika diperlukan
5. **Testing**: Test dengan jumlah halaman kecil terlebih dahulu

## 🐛 Troubleshooting

### Common Issues
- **Connection Timeout**: Increase delay atau check network
- **Empty Results**: Verify website structure masih sama
- **Memory Issues**: Process dalam batch untuk dataset besar
- **Encoding Issues**: Pastikan UTF-8 encoding pada file output

### Debug Mode
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📊 Performance

- **Processing Speed**: ~1-2 detik per halaman (dengan delay 1s)
- **Memory Usage**: ~1MB per 1000 quotes
- **Success Rate**: >95% dengan proper error handling

## 🚀 Future Enhancements

- [ ] Support untuk multiple websites
- [ ] Database integration (PostgreSQL, MongoDB)
- [ ] Async processing untuk speed improvement
- [ ] GUI interface
- [ ] Docker containerization
- [ ] Scheduled/cron job support

## 📄 License

MIT License - Feel free to use and modify as needed.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## 📞 Support

For issues and questions, please create an issue in the repository or contact the maintainer.
