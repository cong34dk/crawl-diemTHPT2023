import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('https://', adapter)

def Crawl_THPTQG(so_bao_danh):
    so_bao_danh = str(so_bao_danh).rjust(8, '0')
    URL = "https://vietnamnet.vn/giao-duc/diem-thi/tra-cuu-diem-thi-tot-nghiep-thpt/2023/{}.html".format(so_bao_danh)
    
    try:
        r = session.get(URL)
        r.raise_for_status()  # Raise an exception if the HTTP request fails
        
        if r.status_code != 404:
            soup = BeautifulSoup(r.content, 'html.parser')
            target = soup.find('div', attrs={'class': 'resultSearch__right'})
            table = target.find('tbody')
            rows = table.find_all('tr')
            placeHolder = []
            
            for row in rows:
                lst = row.find_all('td')
                cols = [ele.text.strip() for ele in lst]
                placeHolder.append([ele for ele in cols if ele])
            
            content = "{},{}\n".format(so_bao_danh, placeHolder)
        else:
            return None
        
        return content
    except Exception as e:
        print(f"Error while processing {so_bao_danh}: {e}")
        return None

if __name__ == "__main__":
    provinces = range(0, 65)
    
    for province in provinces:
        count = 0
        start = province*1000000 + 1
        lst = range(start, start + 1000000)
        NUM_WORKERS = cpu_count() * 8
        pool = Pool(NUM_WORKERS)
        result_iter = pool.imap(Crawl_THPTQG, lst)
        
        with open("Output.csv", "a", encoding='utf-8') as f:
            for result in result_iter:
                if result is not None:
                    f.write(result)
                    print("Đang xử lý {}".format(result.split(',')[0]))
                else:
                    count += 1
                    if count == 10:
                        break

    print("Finished crawling.")
