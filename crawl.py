from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime
import urllib.request
import time
import os
import csv
from google.cloud import storage
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import google.auth

# Cloud SQL 연결 설정
instance_connection_name = "stylefinder:asia-northeast3:stylefinder"
db_user = "postgres"
db_pass = "{DB_PASSWORD}"
db_name = "stylefinder"

# 자격 증명 파일 경로 설정
credentials_path = "/home/whfdjq2324/crawling/stylefinder-9ecea8cb649c.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Cloud Storage 클라이언트 초기화
storage_client = storage.Client()
bucket_name = "stylefinder-clothes"
bucket = storage_client.bucket(bucket_name)

# Cloud SQL 연결 함수
connector = Connector(ip_type=IPTypes.PUBLIC)

def get_db_connection():
    def getconn():
        conn = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
        )
        return conn
    return getconn

# 폴더 및 파일 경로 설정
folder_path = '/home/whfdjq2324/crawling/image'
csv_file_path = '/home/whfdjq2324/crawling/dataCrawl.csv'
count_file_path = '/home/whfdjq2324/crawling/count.csv'
searchTag = input("Type searchTag: ")

# 이미지 저장 폴더 생성
if not os.path.isdir(folder_path + "/" + searchTag):
    os.mkdir(folder_path + "/" + searchTag)

step = 4

# 이전 카운트 값을 읽어오는 함수
def read_count():
    if os.path.isfile(count_file_path):
        with open(count_file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                return int(row[0])
    return 0

# 새로운 카운트 값을 저장하는 함수
def save_count(count):
    with open(count_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([count])

# 초기 카운트 값 읽기
count = read_count()

# 크롤링 실행 시 이미지 추가 횟수를 추적하는 변수
images_added = 0

# 크롬 드라이버 설정 (headless 모드)
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.binary_location = '/usr/bin/google-chrome'  # Chrome 브라우저의 경로를 지정합니다.

# Chrome 드라이버 경로 설정
service = Service('/usr/local/bin/chromedriver')
driver = webdriver.Chrome(service=service, options=options)

# 무신사 접속 및 검색어 입력
print("Navigating to Musinsa website...")
driver.get('https://www.musinsa.com/')
driver.maximize_window()

# 페이지 소스를 파일로 저장
print("Saving page source to file...")
with open("/home/whfdjq2324/crawling/page_source.html", "w", encoding="utf-8") as file:
    file.write(driver.page_source)
print("Page source saved to page_source.html")

# 페이지가 완전히 로드될 때까지 대기 (최대 30초)
wait = WebDriverWait(driver, 30)

try:
    # 검색창 요소 찾기 (XPATH 사용)
    print("Looking for search input...")
    search_input_xpath = '//*[@id="commonLayoutSearchForm"]'
    search_input = wait.until(EC.presence_of_element_located((By.XPATH, search_input_xpath)))
    print("Search input found.")

    # 검색어 입력
    print(f"Entering search term: {searchTag}")
    search_input.click()
    search_input.send_keys(searchTag)
    search_input.send_keys(Keys.RETURN)
    print("Search term entered.")

    # 무신사 상품 탭으로 이동 (XPATH 사용)
    print("Navigating to product tab...")
    product_tab_link = wait.until(
        EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/section/nav/a[2]'))
    )
    product_tab_link.click()
    print("Product tab clicked.")

except Exception as e:
    print(f"An error occurred: {e}")
    with open("/home/whfdjq2324/crawling/error_page_source.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    driver.quit()
    exit()

# 현재 페이지 정보 스크래핑
print("Scraping page source...")
pageString = driver.page_source
soup = BeautifulSoup(pageString, features='html.parser')
total_count = int(soup.find_all('a', attrs={'class': 'img-block'})[0]['data-bh-custom-total-count'])
print(f"Total count of items found: {total_count}")

# max_items 값을 1000으로 설정 (한 번의 크롤링 실행에서 최대 1000개)
max_items = 1000

# CSV 파일 생성 및 헤더 작성
file_exists = os.path.isfile(csv_file_path)
with open(csv_file_path, 'a', newline="", encoding='utf-8-sig') as f:
    writer = csv.writer(f)
    if not file_exists:
        dateLeng = [datetime.today().year, datetime.today().month, datetime.today().day]
        writer.writerow(dateLeng)
        info = ['product_name', 'original_price', 'discounted_price', 'discount_rate', 'image_link', 'search_keyword', 'purchase_link']
        writer.writerow(info)

    # 데이터베이스 연결 열기
    conn = get_db_connection()()
    cursor = conn.cursor()

    for _ in range(total_count):
        if images_added >= max_items or count >= total_count:
            break

        time.sleep(2)

        length = soup.find_all('a', attrs={'class': 'img-block'})
        print(f"Number of items on the page: {len(length)}")

        for i in range(len(length)):
            if images_added >= max_items or count >= total_count:
                break

            # 상품 정보 추출
            title = soup.find_all('a', attrs={'class': 'img-block'})[i]['title']
            price = soup.find_all('a', attrs={'class': 'img-block'})[i]['data-bh-content-meta2']
            salePrice = soup.find_all('a', attrs={'class': 'img-block'})[i]['data-bh-content-meta3']
            salePercentage = soup.find_all('a', attrs={'class': 'img-block'})[i]['data-bh-content-meta5']
            link = soup.find_all('a', attrs={'class': 'img-block'})[i]['href']
            img = "http:" + soup.find_all('img', attrs={'class': 'lazyload lazy'})[i]['data-original']

            try:
                cursor.execute("SELECT 1 FROM clothes WHERE purchase_link = %s", (link,))
                if cursor.fetchone():
                    print(f"이미 존재하는 데이터: {title}")
                    continue

                # Cloud Storage에 이미지 업로드
                # print(f"Uploading image {count + 1} to Cloud Storage...")
                blob = bucket.blob(f"{searchTag}/image{count + 1}.jpg")
                blob.upload_from_string(urllib.request.urlopen(img).read())
                # print(f"Image {count + 1} uploaded.")

                # Cloud SQL에 데이터 저장
                # print(f"Saving data {count + 1} to Cloud SQL...")
                sql = """
                    INSERT INTO clothes (product_name, original_price, discounted_price, discount_rate, image_link, search_keyword, purchase_link)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (title, price, salePrice, salePercentage, blob.public_url, searchTag, link))
                conn.commit()

                # CSV 파일에 데이터 저장
                writer.writerow([title, price, salePrice, salePercentage, blob.public_url, searchTag, link])

                # 새로운 카운트 값 저장
                count += 1
                images_added += 1
                save_count(count)

                print(f"Data {count} saved and count updated.")
            except Exception as e:
                print(f"An error occurred during transaction: {e}")
                conn.rollback()

        if images_added >= max_items:
            time.sleep(2)
            break

        print(f"Moving to next page, step: {step}")
        leng = '#goodsList > div.sorter-box.box > div > div > a:nth-child(' + str(step) + ')'
        try:
            next_page = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, leng)))
            driver.execute_script("arguments[0].scrollIntoView();", next_page)
            next_page.click()
        except Exception as e:
            print(f"An error occurred while clicking the next page: {e}")
            break

        step = step + 1
        if step == 14:
            step = step - 10

        # 페이지 정보 업데이트
        print("Updating page source...")
        pageString = driver.page_source
        soup = BeautifulSoup(pageString, features='html.parser')

    # 데이터베이스 연결 닫기
    cursor.close()
    conn.close()

# 크롬 드라이버 종료
print("Closing the driver...")
driver.quit()
print("Script completed.")
