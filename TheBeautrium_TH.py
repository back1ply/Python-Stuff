import requests,random,pandas as pd,re,json
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from bs4 import BeautifulSoup as bs
from datetime import datetime



def test_a_proxy(proxy):
    h={"Accept":"*/*","Accept-Encoding":"gzip, deflate, br, zstd","Accept-Language":"en-US,en;q=0.5","Cache-Control":"no-cache","Connection":"keep-alive","DNT":"1","Host":"thebeautrium.com","Pragma":"no-cache","Priority":"u=4","Referer":"https://thebeautrium.com","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","TE":"trailers","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0"}
    proxy_fetch = {"https":f"http://{proxy}","http":f"http://{proxy}"}
    try:
        r=requests.get('https://thebeautrium.com/brands',headers=h,proxies=proxy_fetch,timeout=30)
        r.raise_for_status()
        return [proxy]
    except: return []


def get_product_list(brand_name):
    products_on_this_page=[]
    retailer_locale = 'The Beautrium'
    retailer_locale_name= 'The Beautrium TH'
    product_name='N/A'
    product_url='N/A'
    product_tiles=[]
    request_headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.5","Cache-Control":"no-cache","Connection":"keep-alive","Content-Length":"95","Content-Type":"application/json;charset=utf-8","DNT":"1","Host":"thebeautrium.com","Origin":"https://thebeautrium.com","Pragma":"no-cache","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","TE":"trailers","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0"}
    get_url='https://thebeautrium.com/ssr/api/v2/materials'
    request_body={"filter":{"brand_en":f"{brand_name}"},"page":1,"limit":1000,"sort":{"value":"new_arrivals"},"options":{}}
    retries=0
    while True:
        try:
            retries+=1
            if retries>11: 
                print(f'done max retries: {brand_name}')
                return []
            proxy = random.choice(proxies)
            proxy_fetch = {"https":f"http://{proxy}","http":f"http://{proxy}"}
            r=requests.post(get_url,headers=request_headers,json=request_body,proxies=proxy_fetch,timeout=100)
            if r.status_code!=404: r.raise_for_status()
            product_tiles=r.json()['data']
            break
        except: pass
    for product in product_tiles:
        try:
            if product['cat1_id']==17: continue# we dont want data from food suplement category         
        except: pass
        try: product_name=brand_name+' '+product['mat_name']
        except: 'N/A'
        try: product_url='https://thebeautrium.com/item/'+product['mat_id'].replace(' ','%20')
        except: continue
        try: photo_url=product['image_1']
        except: photo_url='N/A'
        if photo_url=='N/A':
            try: 
                all_images=[x for x in [product['image_2'],product['image_3'],product['image_4'],product['image_5']] if x]
                photo_url=all_images[0]
            except: photo_url='N/A'
        products_on_this_page.append([retailer_locale,retailer_locale_name,brand_name,product_name,product_url,date_today,photo_url])
    return products_on_this_page
    

def get_product_details(product_url,brand_name,product_name):
    products=[]
    retailer_locale = 'The Beautrium'
    retailer_locale_name= 'The Beautrium TH'
    key_ingredients='N/A'
    full_ingredients='N/A'
    product_size='N/A'
    variant_name='N/A'
    description='N/A'
    price='N/A'
    sale='N'
    sale_price='N/A'
    sold_out='N'
    product_size='N/A'
    variant_name='N/A'
    num_reviews='N/A'
    star_rating='N/A'
    tags='N/A'
    soup=None
    r=None
    request_headers = {"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.5","Cache-Control":"no-cache","Connection":"keep-alive","DNT":"1","Host":"thebeautrium.com","Pragma":"no-cache","Sec-Fetch-Dest":"document","Sec-Fetch-Mode":"navigate","Sec-Fetch-Site":"none","Sec-Fetch-User":"?1","Upgrade-Insecure-Requests":"1","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0"}
    retries=0
    while True:
        try:
            retries+=1
            if retries>10: return [[retailer_locale,retailer_locale_name,brand_name,product_name,product_url,description,full_ingredients,key_ingredients,price,sale,sale_price,sold_out,product_size,variant_name,num_reviews,star_rating,tags,date_today]]
            proxy = random.choice(proxies)
            proxy_fetch = {"https":f"http://{proxy}","http":f"http://{proxy}"}
            r=requests.get(product_url,headers=request_headers,proxies=proxy_fetch,timeout=60)
            soup=bs(r.content,'html.parser')
            if r.status_code!=404: r.raise_for_status()
            if not r.text.strip(): raise Exception
            break
        except: pass
    try: 
        description_div=soup.find('div',{'id':'nav-1-2-default-hor-left-underline--1'})
        description=' '.join([x for x in description_div.stripped_strings])
        description=re.sub(r'\s+', ' ', description).strip()
        if description=='':description='N/A'
    except: description='N/A'
    try: 
        full_ingredients = ' '.join(soup.find('div',{'id':'nav-1-2-default-hor-left-underline--4'}).stripped_strings)
        full_ingredients=re.sub(r'\s+', ' ', full_ingredients).strip()
        if full_ingredients=='':full_ingredients='N/A'
    except: full_ingredients='N/A'
    try: star_rating=soup.find('span',{'class':'rating mr-3'}).find('span').get_text(strip=True)
    except: star_rating='N/A'
    try: 
        num_reviews=soup.find('span',{'class':'review-count mx-3'}).get_text(strip=True)
        num_reviews=re.search(r'(\d+?)',num_reviews,flags=re.IGNORECASE).group(1)
    except: num_reviews='N/A'
    try: 
        price=soup.find('div',{'class':'original-price'}).get_text(strip=True).replace(',','').replace('฿','').strip()
        if not price.strip(): price='N/A'
    except: price='N/A'
    try: sale_price=soup.find('div',{'class':'price has-original'}).get_text(strip=True).replace(',','').replace('฿','').strip()
    except: sale_price='N/A'
    if price=='N/A' and sale_price=='N/A':
        try: price=soup.find('div',{'class':'price'}).get_text(strip=True).replace(',','').replace('฿','').strip()
        except: price='N/A'
    sale='N' if sale_price=='N/A' else 'Y'
    try: sold_out='N' if 'หยิบใส่รถเข็น' in soup.find('button',{'class':'btn btn-red w-100'}).get_text() else 'Y'
    except: sold_out='Y'
    try: 
        product_size=soup.find('a',{'class':'mat-size active'}).get_text(strip=True)
        try: product_size=re.search(r'(\d+(,?\d+?)?\s*(ml|g))',product_size,flags=re.IGNORECASE).group(1)
        except: product_size='N/A'
    except: product_size='N/A'
    if product_size=='N/A':
        try: product_size=re.search(r'(\d+(,?\d+?)?\s*(ml|g))',product_url,flags=re.IGNORECASE).group(1)
        except: product_size='N/A'
    try: 
        variations=json.loads(re.search(r'window.__PRELOADED_STATE__\s*=\s*(\{.*?\})</script>',r.text,flags=re.IGNORECASE).group(1))['materials']['material']['variants']
        if len(variations)==1: variations=[]
    except: variations=[]
    try: 
        tags_div=soup.find('div',{'class':'product-tags'}).find_all('div',{'class':'tag-group'})
        tags_list=[]
        for tag in tags_div:
            try: 
                tag_head=tag.find('h3',{'class':'tag-head'}).get_text(strip=True)
                tag_body=','.join([x.get_text(strip=True) for x in tag.find_all('span',{'class':'tag-value'})])
            except: continue
            tags_list.append(tag_head+':'+tag_body)
        tags='; '.join(tags_list).replace('::',':')
    except: tags='N/A'
    sub_products=[]
    for variant in variations:
        try: 
            variant_name=variant['variant_name_2']
            if not variant_name.strip(): variant_name=='N/A'
        except: variant_name='N/A'
        try: 
            product_size=variant['variant_name_1']
            if not product_size.strip(): product_size='N/A'
        except: product_size='N/A'
        sub_products.append([retailer_locale,retailer_locale_name,brand_name,product_name,product_url,description,full_ingredients,key_ingredients,price,sale,sale_price,sold_out,product_size,variant_name,num_reviews,star_rating,tags,date_today])
    products.append([retailer_locale,retailer_locale_name,brand_name,product_name,product_url,description,full_ingredients,key_ingredients,price,sale,sale_price,sold_out,product_size,variant_name,num_reviews,star_rating,tags,date_today])
    return sub_products if sub_products else products


def test_proxies_with_threads(proxies):
    working_proxies=[]
    with ThreadPoolExecutor(max_workers=30) as executor: 
        threads=[executor.submit(test_a_proxy, proxies[i]) for i in range(len(proxies))]
        completed=0
        for thread in as_completed(threads):
            completed+=1
            working_proxies.extend(thread.result())
            if completed%100==0: print(f'testing proxies: {len(threads)-completed} left')
    return working_proxies


def get_product_list_with_threads():
    request_headers = {"Accept":"application/json, text/plain, */*","Accept-Encoding":"gzip, deflate, br","Accept-Language":"en-US,en;q=0.5","Cache-Control":"no-cache","Connection":"keep-alive","DNT":"1","Host":"thebeautrium.com","Pragma":"no-cache","Referer":"https://thebeautrium.com/brands","Sec-Fetch-Dest":"empty","Sec-Fetch-Mode":"cors","Sec-Fetch-Site":"same-origin","TE":"trailers","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0"}
    proxy = random.choice(proxies)
    proxy_fetch = {"https":f"http://{proxy}","http":f"http://{proxy}"}
    product_list=[]
    retries=0
    brand_data=None
    while True:
        try:
            retries+=1
            if retries>2: break
            proxy = random.choice(proxies)
            proxy_fetch = {"https":f"http://{proxy}","http":f"http://{proxy}"}
            r=requests.get('https://thebeautrium.com/ssr/api/v2/brand',headers=request_headers,proxies=proxy_fetch,timeout=40)
            if r.status_code!=404: r.raise_for_status()
            brand_data=r.json()
        except: pass
    product_pages_list=[brand["brand_en"] for brand in brand_data]
    with ThreadPoolExecutor(max_workers=20) as executor:
        threads=[executor.submit(get_product_list,product_pages_list[i]) for i in range(len(product_pages_list))]
        completed=0
        for thread in as_completed(threads):
            completed+=1
            if completed%100==0: print(f'Getting Product list: {len(threads)-completed} brands left')
            product_list.extend(thread.result())
    product_list_table=pd.DataFrame(product_list,columns=['retailer_locale','retailer_locale_name','brand_name','product_name','product_url','scrape_date','photo_url'])
    product_list_table=product_list_table.drop_duplicates(subset=['product_url']).reset_index(drop=True)
    product_list_table.to_csv(f'product_list_table_thebeautrium_{date_today.replace("-","")}.csv',index=False)    
    return product_list_table


def get_product_details_with_threads(product_list_table):
    product_details=[]
    with ThreadPoolExecutor(max_workers=30) as executor: 
        threads=[executor.submit(get_product_details, product_list_table['product_url'][i],product_list_table['brand_name'][i],product_list_table['product_name'][i]) for i in range(len(product_list_table))]
        completed=0
        for thread in as_completed(threads):
            completed+=1
            product_details.extend(thread.result())
            if completed%100==0: print(f'Gettting Product details: {len(threads)-completed} left')
    product_details_table=pd.DataFrame(product_details,columns=['retailer_locale','retailer_locale_name','Brand Name','product_name','product_url','Descriptions','Full Ingredients','Key Ingredients','Price','Sale','Sale Price','Sold Out?','Product Size','Variant Name','num_reviews','star_rating','tags','scrape_date'])
    product_details_table.to_csv(f'product_details_table_thebeautrium_{date_today.replace("-","")}.csv',index=False)



def get_proxies(key,num_pages):
    proxies = []
    for i in range(num_pages):
        res = requests.get(f"https://proxy.webshare.io/api/proxy/list/?mode=direct&page={i+1}&page_size=100", headers={"Authorization": f'Token {key}'})
        for x in res.json()["results"]:
            proxy = f'{x["username"]}:{x["password"]}@{x["proxy_address"]}:{x["ports"]["http"]}'
            proxies.append(proxy)
    return proxies


date_today=datetime.today().strftime('%Y-%m-%d')
proxies_all=get_proxies('55claj5ht01nmuhnffwwovc6ycww0wc0ajxybeyu',20)
proxies=test_proxies_with_threads(proxies_all)
product_list = get_product_list_with_threads()
get_product_details_with_threads(product_list)
