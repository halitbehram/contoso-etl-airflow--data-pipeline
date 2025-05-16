# Contoso ETL Airflow Projesi

Bu proje, **ContosoRetailDW** veri ambarı üzerinde çalışan, Apache Airflow ile otomatikleştirilmiş bir ETL (Extract, Transform, Load) sürecini içerir.  
Proje kapsamında veriler çekilir, çeşitli analizlerle dönüştürülür ve çıktı olarak CSV/Excel dosyalarına kaydedilir.


## Kurulum

1. **Python Ortamı Oluşturun:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Windows için: venv\Scripts\activate
    ```

2. **Bağımlılıkları Yükleyin:**
    ```bash
    pip install -r requirements.txt
    ```

3. **Airflow Başlatma:**
    ```bash
    export AIRFLOW_HOME=~/airflow
    airflow db init
    airflow webserver --port 8080
    airflow scheduler
    ```

4. **Veritabanı Ayarları:**
    - `etl/extract/extract.py` içindeki SQL Server bağlantı bilgisini kendi ortamınıza göre güncelleyin.

## Kullanım

1. Airflow arayüzünde (`localhost:8080`) **etl_pipeline** DAG’ini etkinleştirip çalıştırın.
2. Pipeline çalıştıktan sonra çıktıları `/opt/airflow/output/contoso-etl-airflow/{TARIH}` klasöründe bulabilirsiniz.

## ETL Adımları

- **Extract:**  
  SQL Server’daki belirli tablolar çekilir.
- **Transform:**  
  Müşteri segmentasyonu (RFM), churn analizi, coğrafi analizler ve diğer iş analitikleri yapılır.
- **Load:**  
  Sonuçlar CSV ve Excel formatlarında kaydedilir.

## Özellikler

- Modüler Python kod yapısı
- Kolayca yeni analiz fonksiyonları eklenebilir
- Airflow ile zamanlanabilir ve otomatik çalışma
- Esnek çıktı dosya formatları (CSV/Excel)

## Katkıda Bulunma

- Katkılarınızı ve pull request’lerinizi memnuniyetle karşılıyoruz!
- Hataları veya iyileştirme fikirlerinizi issue olarak açabilirsiniz.

---

**Hazırlayan:**  
[Halit Behram Torunlu]



